import logging
from collections.abc import Iterator

import pytest
from _pytest.tmpdir import TempPathFactory
from dagster import DagsterInstance

logging.getLogger("alembic").setLevel(logging.WARNING)


@pytest.fixture
def dagster_instance(tmp_path_factory: TempPathFactory) -> DagsterInstance:
    return DagsterInstance.ephemeral(tempdir=str(tmp_path_factory.mktemp("dagster_home")))


@pytest.fixture
def local_ray_address() -> Iterator[str]:
    import ray

    context = ray.init(
        ignore_reinit_error=True, runtime_env={"env_vars": {"RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING": "1"}}
    )

    yield "auto"

    context.disconnect()
