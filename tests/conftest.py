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
    import ray

    context = ray.init(
        ignore_reinit_error=True, runtime_env={"env_vars": {"RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING": "1"}}
    )

    yield "auto"

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
