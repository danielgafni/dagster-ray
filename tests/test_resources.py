import os
from collections.abc import Sequence
from typing import Any

import dagster as dg
import pytest
import pytest_cases

from dagster_ray import Lifecycle, LocalRay, RayResource


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


def test_debug_mode():
    import ray

    ray.init

    @ray.remote
    def my_func():
        assert os.environ["RAY_PROFILING"] == "1"
        assert os.environ["RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING"] == "1"
        assert os.environ["RAY_DEBUG_POST_MORTEM"] == "1"

    @dg.asset
    def my_asset(ray_cluster: RayResource) -> None:
        ray.get(my_func.remote())

    ray_cluster = LocalRay(enable_tracing=True, enable_actor_task_logging=True, enable_debug_post_mortem=True)

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


@pytest_cases.fixture
@pytest.mark.parametrize("create", (True, False))
@pytest.mark.parametrize("wait", (True, False))
@pytest.mark.parametrize("connect", (True, False))
def lifecycle(create: bool, wait: bool, connect: bool) -> Lifecycle:
    return Lifecycle(create=create, wait=wait, connect=connect)


@pytest.fixture
def assets_for_lifecycle_tests(lifecycle: Lifecycle) -> Sequence[dg.AssetsDefinition]:
    @dg.asset
    def skipped_create_wait(context: dg.AssetExecutionContext, lifecycle: Lifecycle, ray_resource: RayResource) -> None:
        if not lifecycle.create:
            ray_resource.wait(context)
            assert ray_resource.ready

    @dg.asset
    def skipped_create_connect(
        context: dg.AssetExecutionContext, lifecycle: Lifecycle, ray_resource: RayResource
    ) -> None:
        if not lifecycle.create:
            ray_resource.connect(context)
            assert ray_resource.connected

    @dg.asset
    def skipped_wait_connect(
        context: dg.AssetExecutionContext, lifecycle: Lifecycle, ray_resource: RayResource
    ) -> None:
        if not lifecycle.wait:
            ray_resource.connect(context)
            assert ray_resource.connected

    return [skipped_create_wait, skipped_create_connect, skipped_wait_connect]


def run_lifecycle_options_test(
    dagster_instance: dg.DagsterInstance,
    ray_resource: RayResource,
    assets_for_lifecycle_tests: Sequence[dg.AssetsDefinition],
):
    for asset in assets_for_lifecycle_tests:
        dagster_instance.materialize(assets=[asset], resource={"ray_resource": ray_resource})


def test_lifecycle_options(
    lifecycle: Lifecycle,
    dagster_instance: dg.DagsterInstance,
    assets_for_lifecycle_tests: Sequence[dg.AssetsDefinition],
):
    run_lifecycle_options_test(
        dagster_instance=dagster_instance,
        assets_for_lifecycle_tests=assets_for_lifecycle_tests,
        ray_resource=LocalRay(lifecycle=lifecycle),
    )
