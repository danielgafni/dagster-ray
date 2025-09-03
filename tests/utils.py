from collections.abc import Sequence

import dagster as dg
from dagster import ExecuteInProcessResult

from dagster_ray import RayResource


def get_saved_path(result: ExecuteInProcessResult, asset_name: str) -> str:
    path = (
        list(filter(lambda evt: evt.is_handled_output, result.events_for_node(asset_name)))[0]
        .event_specific_data.metadata["path"]  # type: ignore
        .value
    )  # type: ignore[index,union-attr]
    assert isinstance(path, str)
    return path


def run_lifecycle_options_test(
    dagster_instance: dg.DagsterInstance,
    ray_resource: RayResource,
    assets_for_lifecycle_tests: Sequence[dg.AssetsDefinition],
):
    for asset in assets_for_lifecycle_tests:
        dagster_instance.materialize(assets=[asset], resource={"ray_resource": ray_resource})
