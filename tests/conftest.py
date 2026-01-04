import logging
import os
import sys
from collections.abc import Iterator

import dagster as dg
import pytest
from _pytest.tmpdir import TempPathFactory
from dagster._core.types.loadable_target_origin import LoadableTargetOrigin

# this import only works under `pytest` because of pytest.ini_options.pythonpath setting
from tests.utils import InProcessCodeLocationOrigin  # pyright:ignore[reportAttributeAccessIssue]

logging.getLogger("alembic").setLevel(logging.WARNING)


@pytest.fixture
def dagster_instance(tmp_path_factory: TempPathFactory) -> dg.DagsterInstance:
    return dg.DagsterInstance.ephemeral(tempdir=str(tmp_path_factory.mktemp("dagster_home")))


@pytest.fixture
def local_ray_address() -> Iterator[str]:
    """Fixture that provides a local Ray address for testing.

    Uses function scope so each test gets a fresh Ray context.
    RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER=0 in pyproject.toml ensures Ray uses 127.0.0.1
    instead of auto-detecting an IP that might be from Docker networks (k3d).

    Yields the dashboard URL (http://127.0.0.1:8265) for JobSubmissionClient.
    """
    import ray

    context = ray.init(
        ignore_reinit_error=True,
        runtime_env={"env_vars": {"RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING": "1"}},
    )

    # Get the dashboard URL for JobSubmissionClient
    dashboard_url = context.dashboard_url
    if dashboard_url and not dashboard_url.startswith("http"):
        dashboard_url = f"http://{dashboard_url}"

    yield dashboard_url or "http://127.0.0.1:8265"

    context.disconnect()


@pytest.fixture
def code_location_origin() -> InProcessCodeLocationOrigin:
    return InProcessCodeLocationOrigin(
        loadable_target_origin=LoadableTargetOrigin(
            executable_path=sys.executable,
            module_name=("dagster_ray.tests.kuberay.conftest"),
            working_directory=os.getcwd(),
            attribute="lmao",
        ),
        location_name="test_location",
    )
