from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

import dagster as dg
from pydantic import Field

USER_DEFINED_RAY_KEY = "dagster-ray/config"
DAGSTER_RAY_NAMESPACES_ENV_VAR = "DAGSTER_RAY_NAMESPACES"
DAGSTER_RAY_NAMESPACES_DEFAULT_VALUE = "ray"
DAGSTER_RAY_CLUSTER_EXPIRATION_SECONDS_ENV_VAR = "DAGSTER_RAY_CLUSTER_EXPIRATION_SECONDS"
DAGSTER_RAY_CLUSTER_EXPIRATION_SECONDS_DEFAULT_VALUE = str(4 * 60 * 60)


class Lifecycle(dg.Config):
    create: bool = Field(
        default=True,
        description="Whether to create the resource. If set to `False`, the user can manually call `.create` instead.",
    )
    wait: bool = Field(
        default=True,
        description="Whether to wait for the remote Ray cluster to become ready to accept connections. If set to `False`, the user can manually call `.wait` instead.",
    )
    connect: bool = Field(
        default=True,
        description="Whether to run `ray.init` against the remote Ray cluster. If set to `False`, the user can manually call `.connect` instead.",
    )
    cleanup: Literal["never", "always", "on_exception"] = Field(
        default="always",
        description="Resource cleanup policy. Determines when the resource should be deleted after Dagster step execution or during interruption.",
    )


class RayExecutionConfig(dg.Config):
    runtime_env: dict[str, Any] | None = Field(default=None, description="The runtime environment to use.")
    num_cpus: float | None = Field(default=None, description="The number of CPUs to allocate.")
    num_gpus: float | None = Field(default=None, description="The number of GPUs to allocate.")
    memory: int | None = Field(default=None, description="The amount of memory in bytes to allocate.")
    resources: dict[str, float] | None = Field(default=None, description="Custom resources to allocate.")

    @classmethod
    def from_tags(cls, address, tags: Mapping[str, str]) -> RayExecutionConfig:
        if USER_DEFINED_RAY_KEY in tags:
            return cls.model_validate_json(tags[USER_DEFINED_RAY_KEY])
        else:
            return cls(**{'address': address})


class RayJobSubmissionClientConfig(dg.Config):
    address: str = Field(..., description="The address of the Ray cluster to connect to.")
    metadata: dict[str, str] | None = Field(
        default=None,
        description="""Arbitrary metadata to store along with all jobs. New metadata
            specified per job will be merged with the global metadata provided here
            via a simple dict update.""",
    )
    headers: dict[str, Any] | None = Field(
        default=None,
        description="""Headers to use when sending requests to the HTTP job server, used
            for cases like authentication to a remote cluster.""",
    )
    cookies: dict[str, Any] | None = Field(
        default=None, description="Cookies to use when sending requests to the HTTP job server."
    )


class ExecutionOptionsConfig(dg.Config):
    cpu: int | None = None
    gpu: int | None = None
    object_store_memory: int | None = None


class RayDataExecutionOptions(dg.Config):
    execution_options: ExecutionOptionsConfig = Field(default_factory=ExecutionOptionsConfig)

    cpu_limit: int = 5000
    gpu_limit: int = 0
    verbose_progress: bool = True
    use_polars: bool = True

    def apply(self):
        import ray
        from ray.data import ExecutionResources

        ctx = ray.data.DatasetContext.get_current()

        ctx.execution_options.resource_limits = ExecutionResources.for_limits(
            cpu=self.execution_options.cpu,
            gpu=self.execution_options.gpu,
            object_store_memory=self.execution_options.object_store_memory,
        )

        ctx.verbose_progress = self.verbose_progress
        ctx.use_polars = self.use_polars

    def apply_remote(self):
        import ray

        @ray.remote
        def apply():
            self.apply()

        ray.get(apply.remote())
