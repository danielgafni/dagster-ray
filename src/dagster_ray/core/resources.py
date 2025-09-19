from dagster_ray._base.resources import RayResource


class LocalRay(RayResource):
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
