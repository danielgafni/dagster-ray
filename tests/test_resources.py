import os
from typing import Any
from unittest.mock import patch

import dagster as dg
import pytest

from dagster_ray import LocalRay, RayResource


def test_runtime_env():
    import ray

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


def test_worker_process_setup_hook():
    import ray

    from dagster_ray.configs import RayDataExecutionOptions

    ray_cluster = LocalRay(worker_process_setup_hook="my_module.setup")

    with (
        patch.object(ray, "init", return_value=None) as mock_init,
        patch.object(RayDataExecutionOptions, "apply"),
        patch.object(RayDataExecutionOptions, "apply_remote"),
    ):
        ray_cluster.connect(dg.build_init_resource_context())

    _, kwargs = mock_init.call_args
    assert kwargs["runtime_env"]["worker_process_setup_hook"] == "my_module.setup"


def test_worker_process_setup_hook_explicit_overrides_runtime_env():
    """Explicit field takes priority over runtime_env dict key."""
    import ray

    from dagster_ray.configs import RayDataExecutionOptions

    ray_cluster = LocalRay(
        worker_process_setup_hook="my_module.explicit",
        ray_init_options={"runtime_env": {"worker_process_setup_hook": "my_module.from_dict"}},
    )

    with (
        patch.object(ray, "init", return_value=None) as mock_init,
        patch.object(RayDataExecutionOptions, "apply"),
        patch.object(RayDataExecutionOptions, "apply_remote"),
    ):
        ray_cluster.connect(dg.build_init_resource_context())

    _, kwargs = mock_init.call_args
    assert kwargs["runtime_env"]["worker_process_setup_hook"] == "my_module.explicit"


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
