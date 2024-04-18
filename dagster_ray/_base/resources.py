import sys
import uuid
from abc import ABC, abstractmethod
from typing import Optional, cast

from dagster import ConfigurableResource, InitResourceContext
from pydantic import Field, PrivateAttr

# yes, `python-client` is actually the KubeRay package name
# https://github.com/ray-project/kuberay/issues/2078
from requests.exceptions import ConnectionError
from tenacity import retry, retry_if_exception_type, stop_after_delay

from dagster_ray.config import RayDataExecutionOptions

if sys.version_info >= (3, 11):
    pass
else:
    pass

import ray
from ray._private.worker import BaseContext as RayBaseContext  # noqa


class BaseRayResource(ConfigurableResource, ABC):
    """
    Base class for Ray Resources.
    Defines the common interface and some utility methods.
    """

    data_execution_options: RayDataExecutionOptions = Field(default_factory=RayDataExecutionOptions)
    redis_port: int = Field(
        default=10001, description="Redis port for connection. Make sure to match with the actual available port."
    )
    dashboard_port: int = Field(
        default=8265, description="Dashboard port for connection. Make sure to match with the actual available port."
    )

    _context: Optional[RayBaseContext] = PrivateAttr()

    @property
    def context(self) -> "RayBaseContext":
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
        return ray.get_runtime_context().get_job_id()

    @retry(stop=stop_after_delay(120), retry=retry_if_exception_type(ConnectionError), reraise=True)
    def init_ray(self) -> "RayBaseContext":
        self.data_execution_options.apply()
        self._context = ray.init(address=self.ray_address, ignore_reinit_error=True)
        self.data_execution_options.apply()
        self.data_execution_options.apply_remote()
        return cast(RayBaseContext, self._context)

    def _get_step_key(self, context: InitResourceContext) -> str:
        # just return a random string
        # since we want a fresh cluster every time
        return str(uuid.uuid4())
