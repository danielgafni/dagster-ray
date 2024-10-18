import sys
from pathlib import Path

import pytest
from dagster import AssetExecutionContext, DagsterEventType, EventRecordsFilter, asset, materialize
from dagster._core.definitions.data_version import (
    DATA_VERSION_IS_USER_PROVIDED_TAG,
    DATA_VERSION_TAG,
)
from dagster._core.instance_for_test import instance_for_test
from ray.job_submission import JobSubmissionClient  # noqa: TID253

from dagster_ray import PipesRayJobClient

LOCAL_SCRIPT_PATH = Path(__file__).parent / "scripts" / "remote_job.py"


@pytest.fixture
def pipes_ray_job_client(local_ray_address: str) -> PipesRayJobClient:
    return PipesRayJobClient(client=JobSubmissionClient(address=local_ray_address))


def test_ray_job_pipes(pipes_ray_job_client: PipesRayJobClient, capsys):
    @asset
    def my_asset(context: AssetExecutionContext, pipes_ray_job_client: PipesRayJobClient):
        result = pipes_ray_job_client.run(
            context=context,
            submit_job_params={
                "entrypoint": f"{sys.executable} {LOCAL_SCRIPT_PATH}",
            },
            extras={"foo": "bar"},
        ).get_materialize_result()

        return result

    with instance_for_test() as instance:
        result = materialize(
            [my_asset],
            resources={"pipes_ray_job_client": pipes_ray_job_client},
            instance=instance,
        )

        captured = capsys.readouterr()

        print(captured.out)
        print(captured.err, file=sys.stderr)

        mat_evts = result.get_asset_materialization_events()

        mat = instance.get_latest_materialization_event(my_asset.key)
        instance.get_event_records(event_records_filter=EventRecordsFilter(event_type=DagsterEventType.LOGS_CAPTURED))

        assert len(mat_evts) == 1

        assert result.success
        assert mat
        assert mat and mat.asset_materialization
        assert mat.asset_materialization.metadata["some_metric"].value == 0
        assert mat.asset_materialization.tags
        assert mat.asset_materialization.tags[DATA_VERSION_TAG] == "alpha"
        assert mat.asset_materialization.tags[DATA_VERSION_IS_USER_PROVIDED_TAG]

        assert "Hello from stdout!" in captured.out
        assert "Hello from stderr!" in captured.out
        assert "Hello from Ray Pipes!" in captured.err
