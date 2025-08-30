from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Literal, Union, cast

import dagster as dg
from dagster import AssetExecutionContext, ConfigurableResource, InitResourceContext, OpExecutionContext
from pydantic import Field, PrivateAttr

# yes, `python-client` is actually the KubeRay package name
# https://github.com/ray-project/kuberay/issues/2078
from requests.exceptions import ConnectionError
from tenacity import retry, retry_if_exception_type, stop_after_delay
from typing_extensions import TypeAlias

from dagster_ray._base.utils import get_dagster_tags
from dagster_ray.config import RayDataExecutionOptions
from dagster_ray.utils import _process_dagster_env_vars, get_current_job_id

if TYPE_CHECKING:
    from ray._private.worker import BaseContext as RayBaseContext  # noqa

OpOrAssetExecutionContext: TypeAlias = Union[OpExecutionContext, AssetExecutionContext]


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
    cleanup: Literal["never", "except_failure", "always"] = Field(
        default="always", description="Whether to delete the resource after Dagster step completion."
    )


class BaseRayResource(ConfigurableResource, ABC):
    """
    Base class for Ray Resources.
    Defines the common interface and some utility methods.
    """

    lifecycle: Lifecycle = Field(default_factory=Lifecycle, description="Actions to perform during resource setup.")

    ray_init_options: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional keyword arguments to pass to `ray.init()` call, such as `runtime_env`, `num_cpus`, etc. Dagster's `EnvVar` is supported. More details in [Ray docs](https://docs.ray.io/en/latest/ray-core/api/doc/ray.init.html).",
    )
    data_execution_options: RayDataExecutionOptions = Field(default_factory=RayDataExecutionOptions)
    redis_port: int = Field(
        default=10001, description="Redis port for connection. Make sure to match with the actual available port."
    )
    dashboard_port: int = Field(
        default=8265, description="Dashboard port for connection. Make sure to match with the actual available port."
    )
    env_vars: dict[str, str] | None = Field(
        default_factory=dict, description="Environment variables to pass to the Ray cluster."
    )
    enable_tracing: bool = Field(
        default=False,
        description="Enable tracing: inject `RAY_PROFILING=1` and `RAY_task_events_report_interval_ms=0` into the Ray cluster configuration. This allows using `ray.timeline()` to fetch recorded task events. Learn more: https://docs.ray.io/en/latest/ray-core/api/doc/ray.timeline.html#ray-timeline",
    )
    enable_actor_task_logging: bool = Field(
        default=False,
        description="Enable actor task logging: inject `RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING=1` into the Ray cluster configuration.",
    )
    enable_debug_post_mortem: bool = Field(
        default=False,
        description="Enable post-mortem debugging: inject `RAY_DEBUG_POST_MORTEM=1` into the Ray cluster configuration. Learn more: https://docs.ray.io/en/latest/ray-observability/ray-distributed-debugger.html",
    )

    _context: RayBaseContext | None = PrivateAttr()

    @property
    def context(self) -> RayBaseContext:
        assert self._context is not None, "RayClusterResource not initialized"
        return self._context

    @property
    @abstractmethod
    def host(self) -> str:
        raise NotImplementedError()

    @property
    def ray_address(self) -> str:
        return f"ray://{self.host}:{self.redis_port}"

    @property
    def dashboard_url(self) -> str:
        return f"http://{self.host}:{self.dashboard_port}"

    @property
    def runtime_job_id(self) -> str:
        """
        Returns the Ray Job ID for the current job which was created with `ray.init()`.
        :return:
        """
        return get_current_job_id()

    def create(self, context: InitResourceContext | OpOrAssetExecutionContext):
        pass

    def wait(self, context: InitResourceContext | OpOrAssetExecutionContext):
        pass

    @retry(stop=stop_after_delay(120), retry=retry_if_exception_type(ConnectionError), reraise=True)
    def connect(self, context: OpOrAssetExecutionContext | InitResourceContext) -> RayBaseContext:
        assert context.log is not None

        import ray

        init_options = _process_dagster_env_vars(self.ray_init_options.copy())

        # cleanup None values from runtime_env.env_vars since Ray doesn't like them

        if "runtime_env" in init_options and "env_vars" in init_options["runtime_env"]:
            init_options["runtime_env"]["env_vars"] = {
                k: v for k, v in init_options["runtime_env"]["env_vars"].items() if v is not None
            }

        init_options["ignore_reinit_error"] = init_options.get("ignore_reinit_error", True)

        self.data_execution_options.apply()

        self._context = ray.init(
            address=self.ray_address,
            **init_options,
        )
        self.data_execution_options.apply()
        self.data_execution_options.apply_remote()
        context.log.info("Initialized Ray in client mode!")
        return cast("RayBaseContext", self._context)

    def get_dagster_tags(self, context: InitResourceContext | OpOrAssetExecutionContext) -> dict[str, str]:
        tags = get_dagster_tags(context)
        return tags

    def get_env_vars_to_inject(self) -> dict[str, str]:
        vars: dict[str, str] = self.env_vars or {}
        if self.enable_debug_post_mortem:
            vars["RAY_DEBUG_POST_MORTEM"] = "1"
        if self.enable_tracing:
            vars["RAY_PROFILING"] = "1"
            vars["RAY_task_events_report_interval_ms"] = "0"
        if self.enable_actor_task_logging:
            vars["RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING"] = "1"
        return vars
