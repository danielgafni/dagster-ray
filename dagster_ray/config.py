from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dagster import Config
from pydantic import Field

USER_DEFINED_RAY_KEY = "dagster-ray/config"


class RayExecutionConfig(Config):
    runtime_env: dict[str, Any] | None = Field(default=None, description="The runtime environment to use.")
    num_cpus: float | None = Field(default=None, description="The number of CPUs to allocate.")
    num_gpus: float | None = Field(default=None, description="The number of GPUs to allocate.")
    memory: int | None = Field(default=None, description="The amount of memory in bytes to allocate.")
    resources: dict[str, float] | None = Field(default=None, description="Custom resources to allocate.")

    @classmethod
    def from_tags(cls, tags: Mapping[str, str]) -> RayExecutionConfig:
        if USER_DEFINED_RAY_KEY in tags:
            return cls.parse_raw(tags[USER_DEFINED_RAY_KEY])
        else:
            return cls()


class RayJobSubmissionClientConfig(Config):
    address: str = Field(..., description="The address of the Ray cluster to connect to.")
    metadata: dict[str, Any] | None = Field(
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


class ExecutionOptionsConfig(Config):
    cpu: int | None = None
    gpu: int | None = None
    object_store_memory: int | None = None


class RayDataExecutionOptions(Config):
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
