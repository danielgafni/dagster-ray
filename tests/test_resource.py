import ray
from dagster import asset, materialize

from dagster_ray.resource import RAY_RESOURCE_CONFIG_SCHEMA, RayResource


def test_resource(ray_resource: RayResource):
    @asset
    def my_asset(ray_resource: RayResource) -> None:
        @ray.remote
        def f():
            return 1

        res = ray.get(f.remote())

        assert res == 1

    materialize([my_asset], resources={"ray_resource": ray_resource})


def test_config_schema():
    assert "address" in RAY_RESOURCE_CONFIG_SCHEMA
    assert "runtime_env" in RAY_RESOURCE_CONFIG_SCHEMA
