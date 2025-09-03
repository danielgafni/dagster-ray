import logging
from collections.abc import Iterator, Sequence

import dagster as dg
import pytest
import pytest_cases
from _pytest.tmpdir import TempPathFactory
from dagster import DagsterInstance

from dagster_ray import Lifecycle, RayResource

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
