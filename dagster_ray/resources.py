import sys

# yes, `python-client` is actually the KubeRay package name
# https://github.com/ray-project/kuberay/issues/2078


if sys.version_info >= (3, 11):
    pass
else:
    pass

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

    @property
    def name(self) -> str:
        return "LocalRay"
