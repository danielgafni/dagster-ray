import sys

import pytest
import ray  # noqa: TID253
from dagster import AssetExecutionContext, DagsterEventType, EventRecordsFilter, asset, materialize
from dagster._core.definitions.data_version import (
    DATA_VERSION_IS_USER_PROVIDED_TAG,
    DATA_VERSION_TAG,
)
from dagster._core.instance_for_test import instance_for_test
from pytest_kubernetes.providers import AClusterManager

from dagster_ray.kuberay.client import RayJobClient
from dagster_ray.kuberay.pipes import PipesKubeRayJobClient

RAY_JOB = {
    "apiVersion": "ray.io/v1",
    "kind": "RayJob",
    "metadata": {
        "annotations": {},
        "namespace": "ray",
    },
    "spec": {
        "activeDeadlineSeconds": 10800,
        "entrypoint": "python /src/tests/kuberay/scripts/remote_job.py",
        "entrypointNumCpus": 0.1,
        "rayClusterSpec": {
            "autoscalerOptions": {
                "imagePullPolicy": "IfNotPresent",
                "securityContext": {"runAsUser": 0},
            },
            "enableInTreeAutoscaling": False,
            "headGroupSpec": {
                "rayStartParams": {"dashboard-host": "0.0.0.0", "num-cpus": "1", "num-gpus": "0"},
                "serviceType": "ClusterIP",
                "template": {
                    "spec": {
                        "affinity": {},
                        "containers": [
                            {
                                "env": [],
                                "imagePullPolicy": "IfNotPresent",
                                "name": "ray-head",
                                "securityContext": {"runAsUser": 0},
                            }
                        ],
                        # "serviceAccountName": "ray",
                        "volumes": [],
                    },
                },
            },
            "rayVersion": ray.__version__,
            "workerGroupSpecs": [],
        },
        "shutdownAfterJobFinishes": False,  # helpful for debugging tests1
        "submissionMode": "K8sJobMode",
        "ttlSecondsAfterFinished": 300,
    },
}


@pytest.fixture(scope="session")
def pipes_rayjob_client(k8s_with_kuberay: AClusterManager):
    return PipesKubeRayJobClient(
        client=RayJobClient(
            config_file=str(k8s_with_kuberay.kubeconfig),
        ),
        port_forward=True,
    )


def test_rayjob_pipes(pipes_rayjob_client: PipesKubeRayJobClient, dagster_ray_image: str, capsys):
    @asset
    def my_asset(context: AssetExecutionContext, pipes_rayjob_client: PipesKubeRayJobClient):
        result = pipes_rayjob_client.run(
            context=context,
            ray_job=RAY_JOB,
            extras={"foo": "bar"},
        ).get_materialize_result()

        return result

    with instance_for_test() as instance:
        result = materialize(
            [my_asset],
            resources={"pipes_rayjob_client": pipes_rayjob_client},
            instance=instance,
            tags={"dagster/image": dagster_ray_image},
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
