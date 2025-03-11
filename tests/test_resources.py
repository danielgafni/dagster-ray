import os

import dagster as dg

from dagster_ray import LocalRay, RayResource


def test_runtime_env():
    import ray

    ray.init

    @ray.remote
    def my_func():
        assert os.environ["FOO"] == "BAR"

    @dg.asset
    def my_asset(ray_cluster: RayResource) -> None:
        ray.get(my_func.remote())

    ray_cluster = LocalRay(ray_init_options={"runtime_env": {"env_vars": {"FOO": "BAR"}}})

    with dg.DagsterInstance.ephemeral() as instance:
        dg.materialize(
            assets=[my_asset],
            instance=instance,
            resources={"ray_cluster": ray_cluster},
        )


def test_runtime_env_env_var():
    import ray

    ray.init

    @ray.remote
    def my_func():
        assert os.environ["FOO"] == "BAR"

    @dg.asset
    def my_asset(ray_cluster: RayResource) -> None:
        ray.get(my_func.remote())

    os.environ["BAR"] = "BAR"

    with dg.DagsterInstance.ephemeral() as instance:
        dg.materialize(
            assets=[my_asset],
            instance=instance,
            resources={
                "ray_cluster": LocalRay(ray_init_options={"runtime_env": {"env_vars": {"FOO": dg.EnvVar("BAR")}}})
            },
        )

        dg.materialize(
            assets=[my_asset],
            instance=instance,
            resources={
                "ray_cluster": LocalRay(ray_init_options={"runtime_env": {"env_vars": {"FOO": {"env": "BAR"}}}})
            },
        )
