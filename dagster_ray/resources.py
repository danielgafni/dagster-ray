import contextlib
import os
import sys
from collections.abc import Generator

from dagster import InitResourceContext

# yes, `python-client` is actually the KubeRay package name
# https://github.com/ray-project/kuberay/issues/2078


if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from ray._private.worker import BaseContext as RayBaseContext  # noqa

from dagster_ray._base.resources import BaseRayResource


class LocalRay(BaseRayResource):
    """
    Dummy Resource.
    Is useful for testing and local development.
    Provides the same interface as actual Resources.
    """

    @property
    def host(self) -> str:
        return "127.0.0.1"

    # the address is overwritten by None since ray.init does not behave as expected
    # when the host is set to localhost or 127.0.0.1
    @property
    def ray_address(self) -> None:  # type: ignore
        return None

    @contextlib.contextmanager
    def yield_for_execution(self, context: InitResourceContext) -> Generator[Self, None, None]:
        assert context.log is not None
        assert context.dagster_run is not None

        env_vars_to_inject = self.get_env_vars_to_inject()

        if env_vars_to_inject:
            context.log.warning("Setting debugging environment variables prior to starting Ray")
            for key, value in env_vars_to_inject.items():
                os.environ[key] = value

        context.log.debug("Connecting to a local Ray cluster...")

        self.init_ray(context)

        yield self

        if self._context is not None:
            self._context.disconnect()
