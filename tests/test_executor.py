from collections.abc import Iterator

import pytest
from dagster import (
    DagsterInstance,
    execute_job,
    instance_for_test,
    job,
    op,
    reconstructable,
)

from dagster_ray import RayIOManager
from dagster_ray.executor import ray_executor


@op
def return_one() -> int:
    return 1


@op
def return_two() -> int:
    return 2


@op
def sum_one_and_two(a: int, b: int) -> int:
    res = a + b

    assert res == 3

    return res


@job(executor_def=ray_executor)
def my_job():
    return_two_result = return_two()
    return_one_result = return_one()
    sum_one_and_two(return_one_result, return_two_result)


@op
def failed_op() -> None:
    raise RuntimeError("This op failed!")


@job(executor_def=ray_executor)
def my_failing_job():
    failed_op()


@pytest.fixture
def dagster_instance() -> Iterator[DagsterInstance]:
    with instance_for_test() as instance:
        yield instance


def test_ray_executor(local_ray_address: str, dagster_instance: DagsterInstance):
    result = execute_job(
        job=reconstructable(my_job),
        instance=dagster_instance,
        run_config={
            "execution": {
                "config": {
                    "ray": {"address": local_ray_address},
                }
            }
        },
    )

    assert result.success, result.get_step_failure_events()[0].event_specific_data


ray_io_manager = RayIOManager()


@job(executor_def=ray_executor, resource_defs={"io_manager": ray_io_manager})
def my_job_with_ray_io_manager():
    return_two_result = return_two()
    return_one_result = return_one()
    sum_one_and_two(return_one_result, return_two_result)


def test_ray_executor_with_ray_io_manager(local_ray_address: str, dagster_instance: DagsterInstance):
    result = execute_job(
        job=reconstructable(my_job_with_ray_io_manager),
        instance=dagster_instance,
        run_config={
            "execution": {
                "config": {
                    "ray": {"address": local_ray_address},
                }
            }
        },
    )

    assert result.success, result.get_step_failure_events()[0].event_specific_data


def test_ray_executor_local_failing(local_ray_address: str, dagster_instance: DagsterInstance):
    result = execute_job(
        job=reconstructable(my_failing_job),
        instance=dagster_instance,
        run_config={
            "execution": {
                "config": {
                    "ray": {"address": local_ray_address},
                }
            }
        },
    )
    assert result.get_failed_step_keys() == {"failed_op"}


RUNTIME_ENV = {"pip": ["polars"], "env_vars": {"FOO": "bar"}}


def runtime_env_checks():
    import polars  # noqa  # type: ignore
    import os

    assert os.environ["FOO"] == "bar"


@op
def op_testing_runtime_env():
    runtime_env_checks()


@job(executor_def=ray_executor)
def job_testing_runtime_env():
    op_testing_runtime_env()


def test_ray_executor_local_runtime_env(local_ray_address: str, dagster_instance: DagsterInstance):
    # first test this runtime_env just with ray

    import ray

    ray.get(ray.remote(runtime_env=RUNTIME_ENV)(runtime_env_checks).remote())

    result = execute_job(
        job=reconstructable(job_testing_runtime_env),
        instance=dagster_instance,
        run_config={
            "execution": {
                "config": {
                    "ray": {"address": local_ray_address, "runtime_env": RUNTIME_ENV},
                }
            }
        },
    )
    assert result.success, result.get_step_failure_events()[0].event_specific_data


@op(tags={"dagster-ray/config": {"runtime_env": RUNTIME_ENV}})
def op_with_user_provided_runtime_env():
    runtime_env_checks()


@job(executor_def=ray_executor)
def job_with_user_provided_runtime_env():
    op_with_user_provided_runtime_env()


def test_ray_executor_local_user_provided_runtime_env(local_ray_address: str, dagster_instance: DagsterInstance):
    # first test this runtime_env just with ray

    import ray

    ray.get(ray.remote(runtime_env=RUNTIME_ENV)(runtime_env_checks).remote())

    result = execute_job(
        job=reconstructable(job_with_user_provided_runtime_env),
        instance=dagster_instance,
        run_config={
            "execution": {
                "config": {
                    "ray": {"address": local_ray_address},
                }
            }
        },
    )
    assert result.success, result.get_step_failure_events()[0].event_specific_data
