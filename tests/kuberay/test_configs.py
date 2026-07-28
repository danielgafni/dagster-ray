"""Tests for `RayClusterSpec` / `RayJobSpec` -> Kubernetes manifest conversion.

Cluster-free: everything here calls `to_k8s` directly.
"""

import json
from typing import Any

import dagster as dg
import pytest
from pydantic import ValidationError

from dagster_ray.kuberay.client.rayjob.client import (
    FAILED_JOB_DEPLOYMENT_STATUSES,
    format_job_deployment_failure,
)
from dagster_ray.kuberay.configs import (
    AuthOptions,
    RayClusterSpec,
    RayClusterUpgradeStrategy,
    RayJobConfig,
    RayJobSpec,
)

IMAGE = "test-image"
RAY_VERSION_WITH_AUTH = "2.54.0"
RAY_VERSION_WITH_K8S_AUTH = "2.55.0"


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

    manifest = to_k8s(RayClusterSpec(ray_version=RAY_VERSION_WITH_AUTH, auth_options=AuthOptions()), context)

    assert manifest["authOptions"] == {"mode": "token"}
    json.dumps(manifest)
    ApiClient().sanitize_for_serialization(manifest)


def test_auth_options_declared_fields_all_reach_the_manifest(context) -> None:
    """`secretName` and `enableK8sTokenAuth` were accepted by AuthOptions but silently dropped
    on the way to the manifest, and being a strict Config it had no escape hatch either — so
    token auth via a Secret was unreachable.

    The two are checked separately because KubeRay treats them as mutually exclusive.
    """
    with_secret = RayClusterSpec(
        ray_version=RAY_VERSION_WITH_AUTH,
        auth_options=AuthOptions(mode="token", secret_name="my-secret"),
    )
    assert to_k8s(with_secret, context)["authOptions"] == {"mode": "token", "secretName": "my-secret"}

    with_k8s_auth = RayClusterSpec(
        ray_version=RAY_VERSION_WITH_K8S_AUTH,
        auth_options=AuthOptions(mode="token", enable_k8s_token_auth=True),
    )
    assert to_k8s(with_k8s_auth, context)["authOptions"] == {"mode": "token", "enableK8sTokenAuth": True}

    assert set(AuthOptions.model_fields) == {"mode", "secret_name", "enable_k8s_token_auth"}


def test_auth_options_omits_unset_fields(context) -> None:
    manifest = to_k8s(RayClusterSpec(auth_options=AuthOptions(mode="disabled")), context)
    assert manifest["authOptions"] == {"mode": "disabled"}


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"someFutureCrdField": 1}, "someFutureCrdField"),
        ({"some_future_crd_field": 1}, "someFutureCrdField"),
    ],
)
def test_auth_options_passes_extras_through(context, kwargs, expected) -> None:
    auth_options = AuthOptions(**kwargs)
    spec = RayClusterSpec(ray_version=RAY_VERSION_WITH_AUTH, auth_options=auth_options)
    assert to_k8s(spec, context)["authOptions"][expected] == 1


def test_upgrade_strategy_passes_extras_through(context) -> None:
    strategy = RayClusterUpgradeStrategy(type="Recreate", some_future_crd_field=1)  # type: ignore[call-arg]
    assert to_k8s(RayClusterSpec(upgrade_strategy=strategy), context)["upgradeStrategy"] == {
        "type": "Recreate",
        "someFutureCrdField": 1,
    }


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


def test_deletion_strategy_is_omitted_by_default(context) -> None:
    """`deletionStrategy` requires KubeRay 1.5.0+ *and* the RayJobDeletionPolicy feature gate.
    With the gate off the controller moves the RayJob to ValidationFailed, so sending it by
    default made dagster-ray unusable on 1.5.x, where the gate is alpha and off by default.

    Cleanup is governed by `shutdownAfterJobFinishes` instead, which works on every version.
    """
    manifest = to_k8s(RayJobSpec(), context)
    assert "deletionStrategy" not in manifest
    assert manifest["shutdownAfterJobFinishes"] is True


def test_deletion_rules_reach_the_manifest(context) -> None:
    rules = [{"policy": "DeleteCluster", "condition": {"jobStatus": "FAILED", "ttlSeconds": 600}}]
    spec = RayJobSpec(
        shutdown_after_job_finishes=False,
        ttl_seconds_after_finished=None,
        deletion_strategy={"deletionRules": rules},
    )
    assert to_k8s(spec, context)["deletionStrategy"] == {"deletionRules": rules}


# KubeRay validates the RayJob spec in the controller rather than an admission webhook, so an
# invalid combination is not rejected at creation — the RayJob is created and then parked in
# ValidationFailed. These encode the rules from `validateDeletionConfiguration` so a future
# default change can't silently reintroduce a combination KubeRay refuses.


def assert_kuberay_deletion_config_valid(manifest: dict[str, Any]) -> None:
    shutdown = manifest.get("shutdownAfterJobFinishes", False)
    ttl = manifest.get("ttlSecondsAfterFinished", 0) or 0
    strategy = manifest.get("deletionStrategy")

    assert shutdown or ttl <= 0, "shutdownAfterJobFinishes=False with ttlSecondsAfterFinished>0 is rejected by KubeRay"

    if strategy is None:
        return

    legacy = "onSuccess" in strategy or "onFailure" in strategy
    rules = bool(strategy.get("deletionRules"))

    assert legacy or rules, "deletionStrategy requires onSuccess+onFailure or deletionRules"
    assert not (legacy and rules), "legacy policies and deletionRules are mutually exclusive"
    assert not (rules and shutdown), "deletionRules and shutdownAfterJobFinishes are mutually exclusive"

    if legacy:
        assert "onSuccess" in strategy and "onFailure" in strategy, (
            "legacy deletionStrategy requires BOTH onSuccess and onFailure"
        )

    # TTLs must be non-decreasing along DeleteWorkers -> DeleteCluster -> DeleteSelf per condition
    order = ["DeleteWorkers", "DeleteCluster", "DeleteSelf"]
    by_condition: dict[str, dict[str, int]] = {}
    for rule in strategy.get("deletionRules", []):
        condition = rule["condition"]
        assert ("jobStatus" in condition) != ("jobDeploymentStatus" in condition), (
            "exactly one of jobStatus and jobDeploymentStatus must be set"
        )
        key = str(condition.get("jobStatus") or condition.get("jobDeploymentStatus"))
        policies = by_condition.setdefault(key, {})
        assert rule["policy"] not in policies, f"duplicate rule for {rule['policy']} and {key}"
        policies[rule["policy"]] = condition.get("ttlSeconds", 0)

    for key, policies in by_condition.items():
        ttls = [policies[p] for p in order if p in policies]
        assert ttls == sorted(ttls), f"TTLs for {key} must be non-decreasing across {order}"


def test_default_spec_is_a_valid_kuberay_deletion_config(context) -> None:
    assert_kuberay_deletion_config_valid(to_k8s(RayJobSpec(), context))


def test_documented_deletion_rules_config_is_valid(context) -> None:
    """The configuration recommended in docs/tutorial/kuberay.md must satisfy KubeRay's rules."""
    spec = RayJobSpec(
        shutdown_after_job_finishes=False,
        ttl_seconds_after_finished=None,
        deletion_strategy={
            "deletionRules": [
                {"policy": "DeleteWorkers", "condition": {"jobStatus": "FAILED", "ttlSeconds": 100}},
                {"policy": "DeleteCluster", "condition": {"jobStatus": "FAILED", "ttlSeconds": 600}},
                {"policy": "DeleteCluster", "condition": {"jobStatus": "SUCCEEDED", "ttlSeconds": 0}},
            ]
        },
    )
    assert_kuberay_deletion_config_valid(to_k8s(spec, context))


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        # the combination users hit by setting deletionRules without clearing our other defaults
        (
            dict(deletion_strategy={"deletionRules": [{"policy": "DeleteSelf", "condition": {"jobStatus": "FAILED"}}]}),
            "mutually exclusive",
        ),
        (dict(shutdown_after_job_finishes=False), "rejected by KubeRay"),
        (dict(deletion_strategy={"onSuccess": {"policy": "DeleteCluster"}}), "BOTH onSuccess and onFailure"),
    ],
)
def test_invalid_deletion_configs_are_detected(context, kwargs, match) -> None:
    """Confirms the checker above actually catches what KubeRay rejects."""
    with pytest.raises(AssertionError, match=match):
        assert_kuberay_deletion_config_valid(to_k8s(RayJobSpec(**kwargs), context))


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


def test_upgrade_strategy_is_omitted_by_default(context) -> None:
    """Only exists in KubeRay 1.6.0+; older operators prune it silently."""
    assert "upgradeStrategy" not in to_k8s(RayClusterSpec(), context)


@pytest.mark.parametrize("strategy_type", ["Recreate", "None"])
def test_upgrade_strategy_reaches_manifest(context, strategy_type) -> None:
    spec = RayClusterSpec(upgrade_strategy=RayClusterUpgradeStrategy(type=strategy_type))
    assert to_k8s(spec, context)["upgradeStrategy"] == {"type": strategy_type}


def test_upgrade_strategy_requires_a_type() -> None:
    """`type` is optional in the CRD, but Dagster's config system cannot resolve an optional
    Literal, and an upgradeStrategy without a type is meaningless. So require it rather than
    emitting an explicit null."""
    with pytest.raises(ValidationError):
        RayClusterUpgradeStrategy()  # type: ignore[call-arg]


def test_upgrade_strategy_is_serializable(context) -> None:
    """Same failure mode as auth_options: emitting the model object breaks the k8s client."""
    from kubernetes.client import ApiClient

    manifest = to_k8s(RayClusterSpec(upgrade_strategy=RayClusterUpgradeStrategy(type="Recreate")), context)
    json.dumps(manifest)
    ApiClient().sanitize_for_serialization(manifest)


def test_raycluster_declared_fields_all_reach_the_manifest(context) -> None:
    """The RayClusterSpec counterpart of the RayJobSpec guard above."""
    sentinels: dict[str, Any] = {
        "suspend": True,
        "managed_by": "sentinel-managed-by",
        "autoscaler_options": {"sentinel": 1},
        "head_service_annotations": {"sentinel": "2"},
        "enable_in_tree_autoscaling": True,
        "gcs_fault_tolerance_options": {"sentinel": 3},
        "ray_version": "sentinel-ray-version",
        "auth_options": AuthOptions(mode="disabled"),
        "upgrade_strategy": RayClusterUpgradeStrategy(type="Recreate"),
    }
    # head_group_spec/worker_group_specs are covered by the env-var and image injection tests
    assert set(sentinels) | {"head_group_spec", "worker_group_specs"} == set(RayClusterSpec.model_fields)

    manifest = to_k8s(RayClusterSpec(**sentinels), context)
    emitted = list(manifest.values())

    for name, value in sentinels.items():
        # sub-configs are dumped to dicts on the way out
        expected = value.to_k8s() if isinstance(value, AuthOptions | RayClusterUpgradeStrategy) else value
        assert expected in emitted, f"{name!r} (={expected!r}) is missing from the RayCluster manifest"


# KubeRay validates the RayCluster in its controller, not an admission webhook, so an invalid
# auth_options combination produces a RayCluster that is created and then never reconciled — no
# `.status` is ever written. Verified against KubeRay 1.6.2: a spec with `authOptions.mode: token`
# and no `rayVersion` gets an `InvalidRayClusterSpec` warning event and an empty status, which
# dagster-ray previously surfaced as a 600s "timed out waiting for status" naming the wrong
# problem. These mirror `ValidateRayClusterSpec` so the real reason surfaces immediately.


def test_token_auth_requires_ray_version() -> None:
    with pytest.raises(ValidationError, match="requires ray_version"):
        RayClusterSpec(auth_options=AuthOptions(mode="token"))


def test_token_auth_accepts_a_supported_ray_version(context) -> None:
    spec = RayClusterSpec(ray_version=RAY_VERSION_WITH_AUTH, auth_options=AuthOptions(mode="token"))
    manifest = to_k8s(spec, context)
    assert manifest["authOptions"] == {"mode": "token"}
    assert manifest["rayVersion"] == RAY_VERSION_WITH_AUTH


def test_token_auth_rejects_an_old_ray_version() -> None:
    with pytest.raises(ValidationError, match="requires Ray 2.52.0 or later"):
        RayClusterSpec(ray_version="2.51.0", auth_options=AuthOptions(mode="token"))


def test_token_auth_rejects_an_unparseable_ray_version() -> None:
    with pytest.raises(ValidationError, match="not a valid version"):
        RayClusterSpec(ray_version="not-a-version", auth_options=AuthOptions(mode="token"))


def test_disabled_auth_does_not_require_ray_version(context) -> None:
    """`mode='disabled'` is not auth-enabled, so KubeRay imposes no version requirement."""
    spec = RayClusterSpec(auth_options=AuthOptions(mode="disabled"))
    assert to_k8s(spec, context)["authOptions"] == {"mode": "disabled"}


def test_no_auth_options_does_not_require_ray_version(context) -> None:
    assert "rayVersion" not in to_k8s(RayClusterSpec(), context)


def test_k8s_token_auth_requires_token_mode() -> None:
    with pytest.raises(ValidationError, match="requires auth_options.mode='token'"):
        RayClusterSpec(
            ray_version=RAY_VERSION_WITH_K8S_AUTH,
            auth_options=AuthOptions(mode="disabled", enable_k8s_token_auth=True),
        )


def test_k8s_token_auth_requires_newer_ray() -> None:
    with pytest.raises(ValidationError, match="requires Ray 2.55.0 or later"):
        RayClusterSpec(
            ray_version=RAY_VERSION_WITH_AUTH,
            auth_options=AuthOptions(mode="token", enable_k8s_token_auth=True),
        )


def test_k8s_token_auth_conflicts_with_secret_name() -> None:
    with pytest.raises(ValidationError, match="mutually exclusive"):
        RayClusterSpec(
            ray_version=RAY_VERSION_WITH_K8S_AUTH,
            auth_options=AuthOptions(mode="token", enable_k8s_token_auth=True, secret_name="s"),
        )


def test_k8s_token_auth_is_accepted_when_valid(context) -> None:
    spec = RayClusterSpec(
        ray_version=RAY_VERSION_WITH_K8S_AUTH,
        auth_options=AuthOptions(mode="token", enable_k8s_token_auth=True),
    )
    assert to_k8s(spec, context)["authOptions"] == {"mode": "token", "enableK8sTokenAuth": True}


def test_k8s_token_auth_is_rejected_for_rayjob() -> None:
    """KubeRay marks the RayJob spec invalid rather than ignoring the field."""
    with pytest.raises(ValidationError, match="does not support .* for RayJob"):
        RayJobSpec(
            ray_cluster_spec=RayClusterSpec(
                ray_version=RAY_VERSION_WITH_K8S_AUTH,
                auth_options=AuthOptions(mode="token", enable_k8s_token_auth=True),
            )
        )


def test_token_auth_without_k8s_auth_is_allowed_for_rayjob() -> None:
    spec = RayJobSpec(
        ray_cluster_spec=RayClusterSpec(
            ray_version=RAY_VERSION_WITH_AUTH,
            auth_options=AuthOptions(mode="token", secret_name="s"),
        )
    )
    assert spec.ray_cluster_spec is not None


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
