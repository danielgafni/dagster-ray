"""Tests for `RayClusterSpec` / `RayJobSpec` -> Kubernetes manifest conversion.

Cluster-free: everything here calls `to_k8s` directly.
"""

import json
from typing import Any

import dagster as dg
import pytest

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
    spec = RayJobSpec(preRunningDeadlineSeconds=300)  # type: ignore[call-arg]
    assert to_k8s(spec, context)["preRunningDeadlineSeconds"] == 300


def test_rayjob_spec_converts_snake_case_extra_to_camel_case(context) -> None:
    spec = RayJobSpec(pre_running_deadline_seconds=300)  # type: ignore[call-arg]
    manifest = to_k8s(spec, context)
    assert manifest["preRunningDeadlineSeconds"] == 300
    assert "pre_running_deadline_seconds" not in manifest


def test_raycluster_spec_passes_extra_through(context) -> None:
    spec = RayClusterSpec(someBrandNewField="hello")  # type: ignore[call-arg]
    assert to_k8s(spec, context)["someBrandNewField"] == "hello"


def test_extra_on_nested_ray_cluster_spec_reaches_manifest(context) -> None:
    spec = RayJobSpec(ray_cluster_spec=RayClusterSpec(someBrandNewField="hello"))  # type: ignore[call-arg]
    assert to_k8s(spec, context)["rayClusterSpec"]["someBrandNewField"] == "hello"


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
                                "preRunningDeadlineSeconds": 77,
                                "some_brand_new_field": "hello",
                            }
                        }
                    }
                }
            }
        },
    )

    assert result.success
    assert captured["preRunningDeadlineSeconds"] == 77
    assert captured["someBrandNewField"] == "hello"


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


def test_auth_options_is_serialized_as_a_dict(context) -> None:
    """`authOptions` used to emit the AuthOptions model object, making the manifest
    unserializable: `AttributeError: 'AuthOptions' object has no attribute 'openapi_types'`."""
    from kubernetes.client import ApiClient

    manifest = to_k8s(RayClusterSpec(auth_options=AuthOptions()), context)

    assert manifest["authOptions"] == {"mode": "token"}
    json.dumps(manifest)
    ApiClient().sanitize_for_serialization(manifest)
