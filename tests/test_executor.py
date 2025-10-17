import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import dagster as dg
import pytest

from dagster_ray import RayIOManager
from dagster_ray.core.executor import ray_executor


@dg.op
def return_one() -> int:
    return 1


@dg.op
def return_two() -> int:
    return 2


@dg.op
def sum_one_and_two(a: int, b: int) -> int:
    res = a + b

    assert res == 3

    return res


@dg.job(executor_def=ray_executor)
def my_job():
    return_two_result = return_two()
    return_one_result = return_one()
    sum_one_and_two(return_one_result, return_two_result)


@dg.op
def failed_op() -> None:
    raise RuntimeError("This op failed!")


@dg.job(executor_def=ray_executor)
def my_failing_job():
    failed_op()


@pytest.fixture
def dagster_instance() -> Iterator[dg.DagsterInstance]:
    with dg.instance_for_test() as instance:
        yield instance


def test_ray_executor(local_ray_address: str, dagster_instance: dg.DagsterInstance):
    result = dg.execute_job(
        job=dg.reconstructable(my_job),
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


@dg.job(executor_def=ray_executor, resource_defs={"io_manager": ray_io_manager})
def my_job_with_ray_io_manager():
    return_two_result = return_two()
    return_one_result = return_one()
    sum_one_and_two(return_one_result, return_two_result)


def test_ray_executor_with_ray_io_manager(local_ray_address: str, dagster_instance: dg.DagsterInstance):
    result = dg.execute_job(
        job=dg.reconstructable(my_job_with_ray_io_manager),
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


def test_ray_executor_local_failing(local_ray_address: str, dagster_instance: dg.DagsterInstance):
    result = dg.execute_job(
        job=dg.reconstructable(my_failing_job),
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


@dg.op
def op_testing_runtime_env():
    runtime_env_checks()


@dg.job(executor_def=ray_executor)
def job_testing_runtime_env():
    op_testing_runtime_env()


def test_ray_executor_local_runtime_env(local_ray_address: str, dagster_instance: dg.DagsterInstance):
    # first test this runtime_env just with ray

    import ray

    ray.get(ray.remote(runtime_env=RUNTIME_ENV)(runtime_env_checks).remote())

    result = dg.execute_job(
        job=dg.reconstructable(job_testing_runtime_env),
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


@dg.op(tags={"dagster-ray/config": {"runtime_env": RUNTIME_ENV}})
def op_with_user_provided_runtime_env():
    runtime_env_checks()


@dg.job(executor_def=ray_executor)
def job_with_user_provided_runtime_env():
    op_with_user_provided_runtime_env()


def test_ray_executor_local_user_provided_runtime_env(local_ray_address: str, dagster_instance: dg.DagsterInstance):
    # first test this runtime_env just with ray

    import ray

    ray.get(ray.remote(runtime_env=RUNTIME_ENV)(runtime_env_checks).remote())

    result = dg.execute_job(
        job=dg.reconstructable(job_with_user_provided_runtime_env),
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


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT])
def test_ray_executor_termination(local_ray_address: str, tmp_path, sig):
    """Test that Ray executor properly terminates jobs when the parent process is interrupted."""
    from ray.job_submission import JobStatus, JobSubmissionClient

    # Path to the test script
    script_path = str(Path(__file__).parent / "scripts/launch_dagster_run_with_ray_executor.py")

    assert Path(script_path).exists(), f"Test script not found at {script_path}"

    # Create a temp directory for the DagsterInstance
    temp_dir = tmp_path / "dagster_home"
    temp_dir.mkdir()

    # Create signal file path
    signal_file = tmp_path / "job_started.signal"

    # Build command line arguments
    cmd_args = [
        sys.executable,
        script_path,
        "--ray-address",
        local_ray_address,
        "--temp-dir",
        str(temp_dir),
        "--signal-file",
        str(signal_file),
    ]

    # Start the subprocess with stdout capture
    process = subprocess.Popen(cmd_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Wait for the signal file to appear (indicating job has started), with timeout
    max_wait = 10  # seconds
    start_time = time.time()
    while not signal_file.exists() and time.time() - start_time < max_wait:
        time.sleep(0.1)

    assert signal_file.exists(), f"Signal file {signal_file} was not created within {max_wait} seconds"

    # Interrupt the process with the specified signal
    process.send_signal(sig)

    # Wait for process to terminate and capture output
    try:
        stdout, stderr = process.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate()

    sys.stdout.write(stdout)
    sys.stderr.write(stderr)

    # Connect to Ray and verify all jobs were stopped
    client = JobSubmissionClient(local_ray_address)

    # Get all jobs and verify none are still running
    jobs = client.list_jobs()

    # Filter to jobs with dagster metadata (our jobs)
    dagster_jobs = [
        job_details
        for job_details in jobs
        if job_details.metadata and job_details.metadata.get("dagster/job") == "long_running_job"
    ]

    # Verify all dagster jobs are stopped
    sig_name = "SIGTERM" if sig == signal.SIGTERM else "SIGINT"
    for job_details in dagster_jobs:
        status = client.get_job_status(job_details.submission_id)  # pyright: ignore[reportArgumentType]
        assert status not in (JobStatus.RUNNING, JobStatus.PENDING), (
            f"Ray job {job_details.submission_id} should have been stopped after {sig_name}, "
            f"but status is {status}. This indicates terminate_step() either wasn't called or didn't work properly."
        )
