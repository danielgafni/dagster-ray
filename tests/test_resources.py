import os
from typing import Any

import dagster as dg
import pytest

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


@pytest.mark.parametrize(
    "ray_init_options",
    [
        {"runtime_env": {"env_vars": {"FOO": dg.EnvVar("BAR")}}},
        {"runtime_env": {"env_vars": {"FOO": {"env": "BAR"}}}},
        {"runtime_env": {"env_vars": {"FOO": dg.EnvVar("BAR"), "BAZ": dg.EnvVar("QUIX")}}},
        {"runtime_env": {"env_vars": {"FOO": dg.EnvVar("BAR"), "BAZ": {"env": "QUIX"}}}},
    ],
)
def test_runtime_env_env_var(ray_init_options: dict[str, Any]):
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
            resources={"ray_cluster": LocalRay(ray_init_options=ray_init_options)},
        )
