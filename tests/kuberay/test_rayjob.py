import re

import ray  # noqa: TID253
from dagster import AssetExecutionContext, asset, materialize
from dagster._core.definitions.data_version import (
    DATA_VERSION_IS_USER_PROVIDED_TAG,
    DATA_VERSION_TAG,
)
from dagster._core.instance_for_test import instance_for_test
from pytest_kubernetes.providers import AClusterManager

from dagster_ray.kuberay.pipes import PipesRayJobClient

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
        "entrypointNumCpus": 0.05,
        "rayClusterSpec": {
            "autoscalerOptions": {
                "idleTimeoutSeconds": 60,
                "imagePullPolicy": "IfNotPresent",
                "resources": {"limits": {"cpu": "1", "memory": "1Gi"}, "requests": {"cpu": "1", "memory": "1Gi"}},
                "securityContext": {"runAsUser": 0},
            },
            "enableInTreeAutoscaling": False,
            "headGroupSpec": {
                "rayStartParams": {"dashboard-host": "0.0.0.0", "num-cpus": "0", "num-gpus": "0"},
                "serviceType": "ClusterIP",
                "template": {
                    "metadata": {"annotations": {}},
                    "spec": {
                        "affinity": {},
                        "containers": [
                            {
                                "env": [],
                                "imagePullPolicy": "IfNotPresent",
                                "name": "ray-head",
                                "resources": {
                                    "limits": {"cpu": "500m", "memory": "800M"},
                                    "requests": {"cpu": "500m", "memory": "800M"},
                                },
                                "securityContext": {"runAsUser": 0},
                            }
                        ],
                        "serviceAccountName": "ray",
                        "volumes": [],
                    },
                },
            },
            "rayVersion": ray.__version__,
            "workerGroupSpecs": [],
        },
        "shutdownAfterJobFinishes": True,
        "submissionMode": "K8sJobMode",
        "ttlSecondsAfterFinished": 60,
    },
}


def test_rayjob_pipes(k8s_with_kuberay: AClusterManager, capsys):
    @asset
    def my_asset(context: AssetExecutionContext, pipes_rayjob_client: PipesRayJobClient) -> None:
        pipes_rayjob_client.run(
            context=context,
            ray_job=RAY_JOB,
            extras={"foo": "bar"},
        )

    with instance_for_test() as instance:
        result = materialize([my_asset], resources={"pipes_rayjob_client": PipesRayJobClient()}, instance=instance)

        assert result.success

        # read stdout
        captured = capsys.readouterr()
        assert re.search(r"dagster - INFO - [^\n]+ - Hello from Ray!\n", captured.out, re.MULTILINE)

        mat_evts = result.get_asset_materialization_events()
        assert len(mat_evts) == 1

        mat = instance.get_latest_materialization_event(my_asset.key)
        assert mat
        assert mat and mat.asset_materialization
        assert mat.asset_materialization.metadata["some_metric"].value == 0
        assert mat.asset_materialization.tags
        assert mat.asset_materialization.tags[DATA_VERSION_TAG] == "alpha"
        assert mat.asset_materialization.tags[DATA_VERSION_IS_USER_PROVIDED_TAG]
