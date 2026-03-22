import sys
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
import ray  # noqa: TID253
import yaml
from dagster._core.definitions.data_version import (
    DATA_VERSION_IS_USER_PROVIDED_TAG,
    DATA_VERSION_TAG,
)
from pytest_kubernetes.providers import AClusterManager

from dagster_ray import PipesRayJobClient
from dagster_ray.core.pipes import SubmitJobParams
from dagster_ray.kuberay.client import RayJobClient
from dagster_ray.kuberay.pipes import PipesKubeRayJobClient

ENTRYPOINT = "python /src/tests/scripts/remote_job.py"

RAY_JOB = {
    "apiVersion": "ray.io/v1",
    "kind": "RayJob",
    "metadata": {
        "annotations": {},
        "namespace": "ray",
    },
    "spec": {
        "activeDeadlineSeconds": 10800,
        "entrypoint": ENTRYPOINT,
        "runtimeEnvYAML": yaml.dump({"env_vars": {"FOO": "1E-5"}}, default_style='"'),
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
        # "ttlSecondsAfterFinished": 300,  # doesn't work together with shutdownAfterJobFinishes starting from KubeRay 1.4.0
    },
}


@pytest.fixture(scope="session")
def pipes_kube_rayjob_client(k8s_with_kuberay: AClusterManager):
    return PipesKubeRayJobClient(
        client=RayJobClient(
            kube_config=str(k8s_with_kuberay.kubeconfig),
        ),
        port_forward=True,
    )


@pytest.fixture(scope="session")
def pipes_ray_job_client(k8s_with_raycluster: tuple[dict[str, str], AClusterManager]):
    hosts, k8s = k8s_with_raycluster
    return PipesRayJobClient(address=hosts["dashboard"])


@pytest.fixture(scope="session")
def pipes_ray_job_client_with_auth(k8s_with_raycluster_with_auth: tuple[dict[str, str], str, AClusterManager]):
    hosts, auth_token, k8s = k8s_with_raycluster_with_auth
    return PipesRayJobClient(
        address=hosts["dashboard"],
        headers={"Authorization": f"Bearer {auth_token}"},
    )


SUBMIT_JOB_PARAMS: SubmitJobParams = {
    "entrypoint": ENTRYPOINT,
    "runtime_env": {"env_vars": {"FOO": "1E-5"}},
    "entrypoint_num_cpus": 0.1,
}


def _assert_pipes_result(
    result: dg.ExecuteInProcessResult,
    instance: dg.DagsterInstance,
    asset_key: dg.AssetKey,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured = capsys.readouterr()
    print(captured.out)
    print(captured.err, file=sys.stderr)

    mat_evts = result.get_asset_materialization_events()
    mat = instance.get_latest_materialization_event(asset_key)
    instance.get_event_records(event_records_filter=dg.EventRecordsFilter(event_type=dg.DagsterEventType.LOGS_CAPTURED))

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


def test_rayjob_pipes_with_template(pipes_kube_rayjob_client: PipesKubeRayJobClient, dagster_ray_image: str, capsys):
    """submit_job_params merged into an explicit ray_job template."""

    @dg.asset
    def my_asset(context: dg.AssetExecutionContext, pipes_kube_rayjob_client: PipesKubeRayJobClient):
        return pipes_kube_rayjob_client.run(
            context=context,
            submit_job_params=SUBMIT_JOB_PARAMS,
            ray_job=RAY_JOB,
            extras={"foo": "bar"},
        ).get_materialize_result()

    with dg.instance_for_test() as instance:
        result = dg.materialize(
            [my_asset],
            resources={"pipes_kube_rayjob_client": pipes_kube_rayjob_client},
            instance=instance,
            tags={"dagster/image": dagster_ray_image},
        )
        _assert_pipes_result(result, instance, my_asset.key, capsys)


def test_rayjob_pipes_ray_job_only(pipes_kube_rayjob_client: PipesKubeRayJobClient, dagster_ray_image: str, capsys):
    """ray_job template only, no submit_job_params."""

    @dg.asset
    def my_asset(context: dg.AssetExecutionContext, pipes_kube_rayjob_client: PipesKubeRayJobClient):
        return pipes_kube_rayjob_client.run(
            context=context,
            ray_job=RAY_JOB,
            extras={"foo": "bar"},
        ).get_materialize_result()

    with dg.instance_for_test() as instance:
        result = dg.materialize(
            [my_asset],
            resources={"pipes_kube_rayjob_client": pipes_kube_rayjob_client},
            instance=instance,
            tags={"dagster/image": dagster_ray_image},
        )
        _assert_pipes_result(result, instance, my_asset.key, capsys)


def _assert_ray_job_pipes(pipes_ray_job_client: PipesRayJobClient, capsys) -> None:
    @dg.asset
    def my_asset(context: dg.AssetExecutionContext, pipes_ray_job_client: PipesRayJobClient):
        return pipes_ray_job_client.run(
            context=context,
            submit_job_params={"entrypoint": ENTRYPOINT, "entrypoint_num_cpus": 0.1},
            extras={"foo": "bar"},
        ).get_materialize_result()

    with dg.instance_for_test() as instance:
        result = dg.materialize(
            [my_asset],
            resources={"pipes_ray_job_client": pipes_ray_job_client},
            instance=instance,
        )

        captured = capsys.readouterr()

        print(captured.out)
        print(captured.err, file=sys.stderr)

        mat_evts = result.get_asset_materialization_events()

        mat = instance.get_latest_materialization_event(my_asset.key)
        instance.get_event_records(
            event_records_filter=dg.EventRecordsFilter(event_type=dg.DagsterEventType.LOGS_CAPTURED)
        )

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


def test_ray_job_pipes(pipes_ray_job_client: PipesRayJobClient, capsys):
    _assert_ray_job_pipes(pipes_ray_job_client, capsys)


@pytest.mark.kuberay_auth
def test_ray_job_pipes_fails_without_auth(
    k8s_with_raycluster_with_auth: tuple[dict[str, str], str, AClusterManager],
):
    hosts, _auth_token, _k8s = k8s_with_raycluster_with_auth
    # JobSubmissionClient connects eagerly in __init__, so construction itself raises on 401
    with pytest.raises(Exception):
        PipesRayJobClient(address=hosts["dashboard"])


@pytest.mark.kuberay_auth
def test_ray_job_pipes_with_auth(pipes_ray_job_client_with_auth: PipesRayJobClient, capsys):
    _assert_ray_job_pipes(pipes_ray_job_client_with_auth, capsys)


def test_pipes_kube_rayjob_client_params_forwarded():
    """Verifies that address/headers set on PipesKubeRayJobClient
    are forwarded to RayClusterClient.job_submission_client() without being dropped.

    No K8s or Ray infrastructure required. This is the only viable approach because
    PipesKubeRayJobClient creates ephemeral clusters and the address/cluster name
    aren't known until the RayJob creates them, making integration testing infeasible.
    """
    address = "https://ray.example.com"
    headers = {"Authorization": "Bearer secret-token"}
    captured_kwargs: dict = {}

    mock_jsc = MagicMock()
    mock_jsc.get_address.return_value = address

    @contextmanager
    def fake_job_submission_client(**kwargs):
        captured_kwargs.update(kwargs)
        yield mock_jsc

    mock_client = MagicMock()
    mock_client.get_ray_cluster_name.return_value = "test-raycluster"
    mock_client.ray_cluster_client.job_submission_client = fake_job_submission_client

    pipes_client = PipesKubeRayJobClient(
        client=mock_client,
        address=address,
        headers=headers,
    )

    mock_session = MagicMock()
    mock_session.get_bootstrap_env_vars.return_value = {}

    mock_context = MagicMock()
    mock_context.run.tags = {}
    mock_context.run.dagster_execution_info = {}

    start_response = {
        "metadata": {"name": "test-job", "namespace": "ray"},
        "status": {"jobId": "fake-job-id"},
    }

    with (
        patch("dagster_ray.kuberay.pipes.open_pipes_session") as mock_open_session,
        patch.object(pipes_client, "_start", return_value=start_response),
        patch.object(pipes_client, "_wait_for_completion", return_value=None),
    ):
        mock_open_session.return_value.__enter__.return_value = mock_session
        mock_open_session.return_value.__exit__ = MagicMock(return_value=False)

        pipes_client.run(
            context=mock_context,
            submit_job_params={"entrypoint": "python test.py"},
            ray_job={
                "metadata": {"name": "test-job", "namespace": "ray"},
                "spec": {
                    "entrypoint": "python test.py",
                    "rayClusterSpec": {
                        "headGroupSpec": {"template": {"spec": {"containers": []}}},
                        "workerGroupSpecs": [],
                    },
                },
            },
        )

    assert captured_kwargs["address"] == address
    assert captured_kwargs["headers"] == headers


def test_pipes_kube_rayjob_client_params_forwarded_on_termination():
    """Verifies that address/headers are forwarded to client.terminate() on interruption.

    Regression test: previously _terminate() did not forward connection params,
    causing a ConnectionError when trying to reach the in-cluster dashboard IP.
    """
    from dagster._core.errors import DagsterExecutionInterruptedError

    address = "https://ray.example.com"
    headers = {"Authorization": "Bearer secret-token"}

    mock_jsc = MagicMock()
    mock_jsc.get_address.return_value = address

    @contextmanager
    def fake_job_submission_client(**kwargs):
        yield mock_jsc

    mock_client = MagicMock()
    mock_client.get_ray_cluster_name.return_value = "test-raycluster"
    mock_client.ray_cluster_client.job_submission_client = fake_job_submission_client

    pipes_client = PipesKubeRayJobClient(
        client=mock_client,
        address=address,
        headers=headers,
        forward_termination=True,
    )

    mock_session = MagicMock()
    mock_session.get_bootstrap_env_vars.return_value = {}

    mock_context = MagicMock()
    mock_context.run.tags = {}
    mock_context.run.dagster_execution_info = {}

    start_response = {
        "metadata": {"name": "test-job", "namespace": "ray"},
        "status": {"jobId": "fake-job-id"},
    }

    with (
        patch("dagster_ray.kuberay.pipes.open_pipes_session") as mock_open_session,
        patch.object(pipes_client, "_start", return_value=start_response),
        patch.object(pipes_client, "_wait_for_completion", side_effect=DagsterExecutionInterruptedError()),
    ):
        mock_open_session.return_value.__enter__.return_value = mock_session
        mock_open_session.return_value.__exit__ = MagicMock(return_value=False)

        with pytest.raises(DagsterExecutionInterruptedError):
            pipes_client.run(
                context=mock_context,
                submit_job_params={"entrypoint": "python test.py"},
                ray_job={
                    "metadata": {"name": "test-job", "namespace": "ray"},
                    "spec": {
                        "entrypoint": "python test.py",
                        "rayClusterSpec": {
                            "headGroupSpec": {"template": {"spec": {"containers": []}}},
                            "workerGroupSpecs": [],
                        },
                    },
                },
            )

    mock_client.terminate.assert_called_once()
    _, terminate_kwargs = mock_client.terminate.call_args
    assert terminate_kwargs["address"] == address
    assert terminate_kwargs["headers"] == headers


BASE_RAY_JOB: dict = {
    "apiVersion": "ray.io/v1",
    "kind": "RayJob",
    "metadata": {"name": "test-job", "namespace": "ray"},
    "spec": {
        "entrypoint": "python original.py",
        "runtimeEnvYAML": yaml.dump({"env_vars": {"OLD": "value"}}, default_style='"'),
        "entrypointNumCpus": 0.5,
        "rayClusterSpec": {
            "headGroupSpec": {"template": {"spec": {"containers": [{"name": "ray-head"}]}}},
            "workerGroupSpecs": [],
        },
    },
}


@pytest.mark.parametrize(
    "submit_job_params, expected_overrides",
    [
        pytest.param(
            {"entrypoint": "python new.py"},
            {"entrypoint": "python new.py"},
            id="entrypoint_only",
        ),
        pytest.param(
            {
                "entrypoint": "python new.py",
                "entrypoint_num_cpus": 2.0,
                "entrypoint_num_gpus": 1.0,
                "entrypoint_memory": 4_000_000_000,
            },
            {
                "entrypoint": "python new.py",
                "entrypointNumCpus": 2.0,
                "entrypointNumGpus": 1.0,
                "entrypointMemory": 4_000_000_000,
            },
            id="numeric_fields",
        ),
        pytest.param(
            {
                "entrypoint": "python new.py",
                "entrypoint_resources": {"Custom1": 1, "Custom2": 5},
            },
            {
                "entrypoint": "python new.py",
                "entrypointResources": '"{\\"Custom1\\": 1, \\"Custom2\\": 5}"',
            },
            id="custom_resources",
        ),
        pytest.param(
            {
                "entrypoint": "python new.py",
                "metadata": {"user": "test"},
                "submission_id": "my-job-123",
            },
            {
                "entrypoint": "python new.py",
                "metadata": {"user": "test"},
                "jobId": "my-job-123",
            },
            id="metadata_and_submission_id",
        ),
    ],
)
def test_merge_submit_params_into_ray_job(
    submit_job_params: SubmitJobParams,
    expected_overrides: dict,
) -> None:
    from dagster_ray.kuberay.pipes import _merge_submit_params_into_ray_job

    result = _merge_submit_params_into_ray_job(BASE_RAY_JOB, submit_job_params)

    for key, expected_value in expected_overrides.items():
        assert result["spec"][key] == expected_value

    assert result["metadata"] == BASE_RAY_JOB["metadata"]
    assert result is not BASE_RAY_JOB
