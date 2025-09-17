from __future__ import annotations

import contextlib
from abc import ABC, abstractmethod
from collections.abc import Generator
from typing import TYPE_CHECKING, Any, Literal, Optional, Union, cast

import dagster as dg
from dagster import AssetExecutionContext, ConfigurableResource, InitResourceContext, OpExecutionContext
from pydantic import Field, PrivateAttr

# yes, `python-client` is actually the KubeRay package name
# https://github.com/ray-project/kuberay/issues/2078
from requests.exceptions import ConnectionError
from tenacity import retry, retry_if_exception_type, stop_after_delay
from typing_extensions import Self, TypeAlias

from dagster_ray._base.utils import get_dagster_tags
from dagster_ray.config import RayDataExecutionOptions
from dagster_ray.types import AnyDagsterContext
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
    cleanup: Literal["never", "always", "on_exception"] = Field(
        default="always",
        description="Resource cleanup policy. Determines when the resource should be deleted after Dagster step execution or during interruption.",
    )


class BaseRayResource(ConfigurableResource, ABC):
    """Base class for Ray Resources providing a common interface for Ray cluster management.

    This abstract base class defines the interface that all Ray resources must implement,
    providing a backend-agnostic way to interact with Ray clusters. Concrete implementations
    include LocalRay for local development and KubeRay resources for Kubernetes deployments.

    The BaseRayResource handles the lifecycle of Ray clusters including creation, connection,
    and cleanup, with configurable policies for each stage.

    Examples:
        Use as a type annotation for backend-agnostic code:
        ```python
        from dagster import asset
        from dagster_ray import RayResource

        @asset
        def my_asset(ray_cluster: RayResource):
            # Works with any Ray backend
            import ray
            return ray.get(ray.put("hello"))
        ```

        Manual lifecycle management:
        ```python
        from dagster_ray import Lifecycle

        ray_resource = SomeRayResource(
            lifecycle=Lifecycle(
                create=False,  # Don't auto-create
                connect=False  # Don't auto-connect
            )
        )
        ```

    Note:
        This is an abstract class and cannot be instantiated directly. Use concrete
        implementations like LocalRay or KubeRayCluster instead.
    """

    lifecycle: Lifecycle = Field(default_factory=Lifecycle, description="Actions to perform during resource setup.")
    timeout: float = Field(default=600.0, description="Timeout for Ray readiness in seconds")
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
    enable_legacy_debugger: bool = Field(
        default=False,
        description="Enable legacy debugger: inject `RAY_DEBUG=legacy` into the Ray cluster configuration. Learn more: https://docs.ray.io/en/latest/ray-observability/user-guides/debug-apps/ray-debugging.html#using-the-ray-debugger",
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
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError()

    @property
    def display_name(self) -> str:
        return self.name

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

    @contextlib.contextmanager
    def yield_for_execution(self, context: InitResourceContext) -> Generator[Self, None, None]:
        exception_occurred = None
        try:
            if self.lifecycle.create:
                self._create(context)
                if self.lifecycle.wait:
                    self._wait(context)
                    if self.lifecycle.connect:
                        self._connect(context)
            yield self
        except BaseException as e:
            exception_occurred = e
            raise
        finally:
            self.cleanup(context, exception_occurred)

    def _create(self, context: AnyDagsterContext):
        assert context.log is not None
        if not self.created:
            try:
                self.create(context)
                context.log.info(f"Created {self.display_name}.")
            except BaseException:
                context.log.exception(f"Failed to create {self.display_name}")
                raise

    def _wait(self, context: AnyDagsterContext):
        assert context.log is not None
        self._create(context)
        if not self.ready:
            context.log.info(f"Waiting for {self.display_name} to become ready (timeout={self.timeout:.0f}s)...")
            try:
                self.wait(context)
            except BaseException:
                context.log.exception(f"Failed to wait for {self.display_name} readiness")
                raise

    def _connect(self, context: AnyDagsterContext):
        assert context.log is not None
        self._wait(context)
        if not self.connected:
            try:
                self.connect(context)
            except BaseException:
                context.log.exception(f"Failed to connect to {self.display_name}")
                raise
            context.log.info(f"Initialized Ray Client with {self.display_name}")

    def create(self, context: AnyDagsterContext):
        pass

    def wait(self, context: AnyDagsterContext):
        pass

    @retry(stop=stop_after_delay(120), retry=retry_if_exception_type(ConnectionError), reraise=True)
    def connect(self, context: AnyDagsterContext) -> RayBaseContext:
        assert context.log is not None

        import ray

        init_options = _process_dagster_env_vars(self.ray_init_options.copy())

        # cleanup None values from runtime_env.env_vars since Ray doesn't like them

        if "runtime_env" in init_options and "env_vars" in init_options["runtime_env"]:
            init_options["runtime_env"]["env_vars"] = {
                k: v for k, v in init_options["runtime_env"]["env_vars"].items() if v is not None
            }

        init_options["runtime_env"] = init_options.get("runtime_env", {})
        init_options["runtime_env"]["env_vars"] = init_options["runtime_env"].get("env_vars", {})

        for var, value in self.get_env_vars_to_inject().items():
            init_options["runtime_env"]["env_vars"][var] = value

        self.data_execution_options.apply()

        self._context = ray.init(
            address=self.ray_address,
            **init_options,
        )
        self.data_execution_options.apply()
        self.data_execution_options.apply_remote()
        context.log.info("Initialized Ray in client mode!")
        return cast("RayBaseContext", self._context)

    def delete(self, context: AnyDagsterContext):
        pass

    def cleanup(self, context: AnyDagsterContext, exception: Optional[BaseException]):  # noqa: UP007
        assert context.log is not None

        if self.lifecycle.cleanup == "never":
            to_delete = False
        elif not self.created:
            to_delete = False
        elif self.lifecycle.cleanup == "always":
            to_delete = True
        elif self.lifecycle.cleanup == "on_exception":
            to_delete = exception is not None
        else:
            to_delete = False

        if to_delete:
            self.delete(context)
            context.log.info(f'Deleted {self.display_name} according to cleanup policy "{self.lifecycle.cleanup}"')

        if self.connected and hasattr(self, "_context") and self._context is not None:
            self._context.disconnect()

    def get_dagster_tags(self, context: AnyDagsterContext) -> dict[str, str]:
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
        if self.enable_legacy_debugger:
            vars["RAY_DEBUG"] = "legacy"
        return vars

    @property
    def created(self) -> bool:
        return hasattr(self, "_name") and self._name is not None

    @property
    def ready(self) -> bool:
        return hasattr(self, "_host") and self._host is not None

    @property
    def connected(self) -> bool:
        return hasattr(self, "_context") and self._context is not None
