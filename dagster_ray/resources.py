import contextlib
import sys
from typing import Generator

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

    @contextlib.contextmanager
    def yield_for_execution(self, context: InitResourceContext) -> Generator[Self, None, None]:
        assert context.log is not None
        assert context.dagster_run is not None

        context.log.debug(f"Ray host: {self.host}")

        self.init_ray(context)

        yield self

        if self._context is not None:
            self._context.disconnect()
