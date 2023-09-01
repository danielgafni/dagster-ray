import logging

import pytest
from _pytest.tmpdir import TempPathFactory
from dagster import DagsterInstance

from dagster_ray.resource import RayResource


logging.getLogger("alembic.runtime.migration").setLevel(logging.WARNING)


@pytest.fixture
def dagster_instance(tmp_path_factory: TempPathFactory) -> DagsterInstance:
    return DagsterInstance.ephemeral(tempdir=str(tmp_path_factory.mktemp("dagster_home")))


@pytest.fixture(scope="session")
def ray_resource():
    return RayResource()
