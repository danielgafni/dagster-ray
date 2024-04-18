import logging

import pytest
from _pytest.tmpdir import TempPathFactory
from dagster import DagsterInstance

logging.getLogger("alembic").setLevel(logging.WARNING)


@pytest.fixture
def dagster_instance(tmp_path_factory: TempPathFactory) -> DagsterInstance:
    return DagsterInstance.ephemeral(tempdir=str(tmp_path_factory.mktemp("dagster_home")))
