"""Tests for `RayClusterSpec` / `RayJobSpec` -> Kubernetes manifest conversion.

Cluster-free: everything here calls `to_k8s` directly.
"""

import json
from typing import Any

import dagster as dg
import pytest

from dagster_ray.kuberay.client.rayjob.client import (
    FAILED_JOB_DEPLOYMENT_STATUSES,
    format_job_deployment_failure,
)
from dagster_ray.kuberay.configs import (
    AuthOptions,
    RayClusterSpec,
    RayJobConfig,
    RayJobSpec,
)

IMAGE = "test-image"


@pytest.fixture
def context() -> dg.InitResourceContext:
    return dg.build_init_resource_context()


def to_k8s(spec: RayJobSpec | RayClusterSpec, context: dg.InitResourceContext) -> dict[str, Any]:
    return spec.to_k8s(context, image=IMAGE)


# `RayClusterSpec` and `RayJobSpec` are PermissiveConfig, so Pydantic accepts undeclared fields.
# Those used to be silently dropped by `to_k8s`, letting a config validate and a run succeed
# while the field never reached Kubernetes.


def test_rayjob_spec_passes_camel_case_extra_through(context) -> None:
    spec = RayJobSpec(someFutureCrdField=300)  # type: ignore[call-arg]
    assert to_k8s(spec, context)["someFutureCrdField"] == 300


def test_rayjob_spec_converts_snake_case_extra_to_camel_case(context) -> None:
    spec = RayJobSpec(some_future_crd_field=300)  # type: ignore[call-arg]
    manifest = to_k8s(spec, context)
    assert manifest["someFutureCrdField"] == 300
    assert "some_future_crd_field" not in manifest


def test_raycluster_spec_passes_extra_through(context) -> None:
    spec = RayClusterSpec(someFutureCrdField="hello")  # type: ignore[call-arg]
    assert to_k8s(spec, context)["someFutureCrdField"] == "hello"


def test_extra_on_nested_ray_cluster_spec_reaches_manifest(context) -> None:
    spec = RayJobSpec(ray_cluster_spec=RayClusterSpec(someFutureCrdField="hello"))  # type: ignore[call-arg]
    assert to_k8s(spec, context)["rayClusterSpec"]["someFutureCrdField"] == "hello"


def test_extra_from_dagster_run_config_reaches_manifest() -> None:
    """The real user path: PermissiveConfig maps to dg.Permissive() in the config schema, so
    arbitrary keys pass validation from YAML/run config. They used to be dropped from there."""

    captured: dict[str, Any] = {}

    class Res(dg.ConfigurableResource):
        ray_job: RayJobConfig = RayJobConfig()

    @dg.op
    def probe_op(res: Res):
        captured.update(res.ray_job.spec.to_k8s(dg.build_init_resource_context(), image=IMAGE))

    @dg.job
    def probe_job():
        probe_op()

    result = probe_job.execute_in_process(
        resources={"res": Res.configure_at_launch()},
        run_config={
            "resources": {
                "res": {
                    "config": {
                        "ray_job": {
                            "spec": {
                                "someFutureCrdField": 77,
                                "another_future_crd_field": "hello",
                            }
                        }
                    }
                }
            }
        },
    )

    assert result.success
    assert captured["someFutureCrdField"] == 77
    assert captured["anotherFutureCrdField"] == "hello"


def test_extra_colliding_with_declared_field_raises(context) -> None:
    """Pydantic doesn't catch this: with no alias generator, `activeDeadlineSeconds` is just a
    different key from `active_deadline_seconds` and is kept alongside the declared field."""
    spec = RayJobSpec(activeDeadlineSeconds=5)  # type: ignore[call-arg]
    assert spec.active_deadline_seconds == 60 * 60 * 24  # declared field keeps its default

    with pytest.raises(ValueError, match="collides with the 'activeDeadlineSeconds' key"):
        to_k8s(spec, context)


def test_declared_fields_all_reach_the_manifest(context) -> None:
    """Guards the hand-maintained snake_case -> camelCase dict literals in `to_k8s` against a
    field being added to a spec class but forgotten there.

    Each field is set to a distinctive value because fields defaulting to None are pruned by
    `remove_none_from_dict` and would look missing regardless.
    """
    sentinels: dict[str, Any] = {
        "active_deadline_seconds": 11,
        "pre_running_deadline_seconds": 27,
        "backoff_limit": 12,
        "submitter_pod_template": {"sentinel": 13},
        "submitter_config": {"sentinel": 14},
        "metadata": {"sentinel": 15},
        "cluster_selector": {"sentinel": "16"},
        "managed_by": "sentinel-17",
        "deletion_strategy": {"sentinel": 18},
        "runtime_env_yaml": "sentinel: 19",
        "job_id": "sentinel-20",
        "submission_mode": "HTTPMode",
        "entrypoint_resources": "sentinel-22",
        "entrypoint_num_cpus": 23.0,
        "entrypoint_memory": 24.0,
        "entrypoint_num_gpus": 25.0,
        "ttl_seconds_after_finished": 26,
        "shutdown_after_job_finishes": False,
        "suspend": True,
    }
    # every declared field is covered, so a newly added one fails here until it's listed
    assert set(sentinels) | {"ray_cluster_spec"} == set(RayJobSpec.model_fields)

    manifest = to_k8s(RayJobSpec(ray_cluster_spec=None, **sentinels), context)
    emitted = list(manifest.values())

    for name, value in sentinels.items():
        assert value in emitted, f"{name!r} (={value!r}) is missing from the RayJob manifest"


def test_pre_running_deadline_seconds_is_omitted_by_default(context) -> None:
    """The field only exists in KubeRay 1.6.0+. Omitting it keeps the manifest valid for the
    older operator versions in the test matrix, which would otherwise prune it silently."""
    assert "preRunningDeadlineSeconds" not in to_k8s(RayJobSpec(), context)


def test_pre_running_deadline_seconds_reaches_manifest(context) -> None:
    spec = RayJobSpec(pre_running_deadline_seconds=600)
    assert to_k8s(spec, context)["preRunningDeadlineSeconds"] == 600


def test_auth_options_is_serialized_as_a_dict(context) -> None:
    """`authOptions` used to emit the AuthOptions model object, making the manifest
    unserializable: `AttributeError: 'AuthOptions' object has no attribute 'openapi_types'`."""
    from kubernetes.client import ApiClient

    manifest = to_k8s(RayClusterSpec(auth_options=AuthOptions()), context)

    assert manifest["authOptions"] == {"mode": "token"}
    json.dumps(manifest)
    ApiClient().sanitize_for_serialization(manifest)


# Without KubeRay's `reason`, a deadline failure is indistinguishable from any other
# deployment failure — the message used to interpolate only the bare status string.


def test_format_job_deployment_failure_names_the_pre_running_deadline() -> None:
    message = format_job_deployment_failure(
        "my-job",
        "my-ns",
        {"jobDeploymentStatus": "Failed", "reason": "PreRunningDeadlineExceeded"},
    )
    assert "my-ns/my-job" in message
    assert "PreRunningDeadlineExceeded" in message
    assert "preRunningDeadlineSeconds" in message


def test_format_job_deployment_failure_includes_reason_and_message() -> None:
    message = format_job_deployment_failure(
        "my-job",
        "my-ns",
        {"jobDeploymentStatus": "Failed", "reason": "AppFailed", "message": "boom"},
    )
    assert "AppFailed" in message
    assert "boom" in message


def test_format_job_deployment_failure_without_a_reason() -> None:
    """KubeRay doesn't always set `reason`; the message must still identify the job."""
    message = format_job_deployment_failure("my-job", "my-ns", {"jobDeploymentStatus": "Failed"})
    assert message == "RayJob my-ns/my-job deployment failed"


# KubeRay validates the RayJob spec in the controller rather than an admission webhook, so an
# invalid spec is not rejected at creation — the RayJob is created and then parked in
# ValidationFailed.


def test_format_job_deployment_failure_reports_validation_failed() -> None:
    """ValidationFailed is a distinct jobDeploymentStatus that KubeRay excludes from
    IsJobDeploymentTerminal, but a RayJob never leaves it — so it must read as terminal."""
    message = format_job_deployment_failure(
        "my-job",
        "my-ns",
        {
            "jobDeploymentStatus": "ValidationFailed",
            "reason": "ValidationFailed",
            "message": "RayJobDeletionPolicy feature gate must be enabled to use DeletionStrategy",
        },
    )
    assert "rejected by the KubeRay controller" in message
    assert "feature gate must be enabled" in message


def test_validation_failed_is_treated_as_a_failed_deployment() -> None:
    assert "ValidationFailed" in FAILED_JOB_DEPLOYMENT_STATUSES
    assert "Failed" in FAILED_JOB_DEPLOYMENT_STATUSES


# A failure before the job reaches Running is invisible in the RayCluster status: the cluster may
# look healthy, or may never be created at all. Without polling the RayJob's own
# jobDeploymentStatus, KubeRayInteractiveJob reported these as a plain timeout with no reason.


def _rayjob_client_with_statuses(statuses: list[dict[str, Any]]) -> Any:
    from unittest.mock import MagicMock

    from dagster_ray.kuberay.client import RayJobClient

    client = RayJobClient.__new__(RayJobClient)
    client.get_status = MagicMock(side_effect=statuses * 100)  # type: ignore[method-assign]
    return client


@pytest.mark.parametrize("failed_status", ["Failed", "ValidationFailed"])
def test_raise_if_deployment_failed_raises_on_failure(failed_status) -> None:
    client = _rayjob_client_with_statuses([{"jobDeploymentStatus": failed_status, "reason": "SomeReason"}])
    with pytest.raises(RuntimeError, match="SomeReason"):
        client.raise_if_deployment_failed("my-job", "my-ns")


@pytest.mark.parametrize("status", ["Initializing", "Waiting", "Running", "Complete"])
def test_raise_if_deployment_failed_is_quiet_otherwise(status) -> None:
    client = _rayjob_client_with_statuses([{"jobDeploymentStatus": status}])
    client.raise_if_deployment_failed("my-job", "my-ns")


def test_get_ray_cluster_name_fails_fast_on_rejected_spec() -> None:
    """A rejected spec never gets a RayCluster, so waiting for `rayClusterName` would otherwise
    burn the whole timeout and then report the wrong problem."""
    client = _rayjob_client_with_statuses(
        [
            {
                "jobDeploymentStatus": "ValidationFailed",
                "reason": "ValidationFailed",
                "message": "RayJobDeletionPolicy feature gate must be enabled to use DeletionStrategy",
            }
        ]
    )
    with pytest.raises(RuntimeError, match="feature gate must be enabled"):
        client.get_ray_cluster_name("my-job", "my-ns", timeout=30)


@pytest.mark.parametrize("ready_status", ["Waiting", "Running", "Complete"])
def test_wait_until_deployment_ready_returns_once_the_cluster_is_up(ready_status) -> None:
    """The controller only leaves `Initializing` after RayCluster.Status.State is Ready, so these
    statuses imply a usable cluster."""
    client = _rayjob_client_with_statuses(
        [{"jobDeploymentStatus": "Initializing"}, {"jobDeploymentStatus": ready_status}]
    )
    assert client.wait_until_deployment_ready("my-job", "my-ns", timeout=30)["jobDeploymentStatus"] == ready_status


def test_wait_until_deployment_ready_fails_with_the_deadline_reason() -> None:
    """Previously this stalled in the RayCluster wait, which cannot observe the RayJob's deadline,
    and surfaced as a bare timeout."""
    client = _rayjob_client_with_statuses(
        [
            {"jobDeploymentStatus": "Initializing"},
            {"jobDeploymentStatus": "Failed", "reason": "PreRunningDeadlineExceeded"},
        ]
    )
    with pytest.raises(RuntimeError, match="PreRunningDeadlineExceeded"):
        client.wait_until_deployment_ready("my-job", "my-ns", timeout=30)


def test_wait_until_deployment_ready_times_out_with_the_last_status() -> None:
    """A job stuck pre-Running with no deadline set must still report what it was doing."""
    client = _rayjob_client_with_statuses([{"jobDeploymentStatus": "Initializing"}])
    with pytest.raises(TimeoutError, match="Initializing"):
        client.wait_until_deployment_ready("my-job", "my-ns", timeout=0.3, poll_interval=0.1)
