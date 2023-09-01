import os
from contextlib import contextmanager
from pprint import pformat
from typing import Generator, Optional

from dagster import ConfigurableResource, InitResourceContext
from dagster._config.pythonic_config import TResValue

from dagster_ray.configs import RuntimeEnvConfig

import ray


class RayResource(ConfigurableResource):
    """
    A resource that provides access to the Ray cluster.
    """
    address: Optional[str]
    local: bool = False
    runtime_env: Optional[RuntimeEnvConfig] = None
    # todo: add requirements_path

    @contextmanager
    def yield_for_execution(self, context: InitResourceContext) -> Generator[TResValue, None, None]:
        """
        Setups Ray and yields the resource.
        Warning: this will shutdown the Ray process after execution.
        """

        # raise RuntimeError()

        if self.runtime_env is not None:
            runtime_env = self.runtime_env.dict()
        else:
            runtime_env = {}

        if runtime_env.get("container") is not None and runtime_env["container"].get("image") is None:
            runtime_env["container"]["image"] = context.dagster_run.tags.get("dagster/image")

        ray.init(
             self.address,
             runtime_env=runtime_env
        )
        if self.address is not None:
            context.log.info(f"Connected to Ray Cluster at {self.address}")

        yield self

        ray.shutdown()


RAY_RESOURCE_CONFIG_SCHEMA = RayResource.to_fields_dict()
