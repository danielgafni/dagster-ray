from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Union, cast

from dagster import AssetExecutionContext, ConfigurableResource, InitResourceContext, OpExecutionContext
from pydantic import Field, PrivateAttr

# yes, `python-client` is actually the KubeRay package name
# https://github.com/ray-project/kuberay/issues/2078
from requests.exceptions import ConnectionError
from tenacity import retry, retry_if_exception_type, stop_after_delay
from typing_extensions import TypeAlias

from dagster_ray._base.utils import get_dagster_tags
from dagster_ray.config import RayDataExecutionOptions
from dagster_ray.utils import _process_dagster_env_vars

if TYPE_CHECKING:
    from ray._private.worker import BaseContext as RayBaseContext  # noqa

OpOrAssetExecutionContext: TypeAlias = Union[OpExecutionContext, AssetExecutionContext]


class BaseRayResource(ConfigurableResource, ABC):
    """
    Base class for Ray Resources.
    Defines the common interface and some utility methods.
    """

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

    _context: RayBaseContext | None = PrivateAttr()

    def setup_for_execution(self, context: InitResourceContext) -> None:
        raise NotImplementedError(
            "This is an abstract resource, it's not meant to be provided directly. "
            "Use a backend-specific resource instead."
        )

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
        import ray

        return ray.get_runtime_context().get_job_id()

    @retry(stop=stop_after_delay(120), retry=retry_if_exception_type(ConnectionError), reraise=True)
    def init_ray(self, context: OpOrAssetExecutionContext | InitResourceContext) -> RayBaseContext:
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
        context.log.info("Initialized Ray!")
        return cast("RayBaseContext", self._context)

    def get_dagster_tags(self, context: InitResourceContext) -> dict[str, str]:
        tags = get_dagster_tags(context)
        return tags

    def _get_step_key(self, context: InitResourceContext) -> str:
        # just return a random string
        # since we want a fresh cluster every time
        return str(uuid.uuid4())
