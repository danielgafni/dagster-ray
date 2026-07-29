import os
import warnings
from collections.abc import Mapping
from typing import Any, Literal

import dagster as dg
from packaging.version import InvalidVersion, Version
from pydantic import Field, model_validator
from typing_extensions import Self

from dagster_ray.kuberay.utils import merge_extra_k8s_fields, remove_none_from_dict
from dagster_ray.types import AnyDagsterContext

in_k8s = os.environ.get("KUBERNETES_SERVICE_HOST") is not None


DEFAULT_AUTOSCALER_OPTIONS = {
    "upscalingMode": "Default",
    "idleTimeoutSeconds": 60,
    "env": [],
    "envFrom": [],
    "resources": {
        "limits": {"cpu": "50m", "memory": "0.1Gi"},
        "requests": {"cpu": "50m", "memory": "0.1Gi"},
    },
}
DEFAULT_HEAD_GROUP_SPEC = {
    "serviceType": "ClusterIP",
    "rayStartParams": {},
    "metadata": {
        "labels": {},
        "annotations": {},
    },
    "template": {
        "spec": {
            "imagePullSecrets": [],
            "containers": [
                {
                    "volumeMounts": [
                        {"mountPath": "/tmp/ray", "name": "ray-logs"},
                    ],
                    "name": "head",
                    "imagePullPolicy": "Always",
                },
            ],
            "volumes": [
                {"name": "ray-logs", "emptyDir": {}},
            ],
            "affinity": {},
            "tolerations": [],
            "nodeSelector": {},
        },
    },
}
DEFAULT_WORKER_GROUP_SPECS = [
    {
        "groupName": "workers",
        "replicas": 0,
        "minReplicas": 0,
        "maxReplicas": 1,
        "rayStartParams": {},
        "template": {
            "metadata": {"labels": {}, "annotations": {}},
            "spec": {
                "imagePullSecrets": [],
                "containers": [
                    {
                        "volumeMounts": [{"mountPath": "/tmp/ray", "name": "ray-logs"}],
                        "name": "worker",
                        "imagePullPolicy": "Always",
                    }
                ],
                "volumes": [
                    {"name": "ray-logs", "emptyDir": {}},
                ],
                "affinity": {},
                "tolerations": [],
                "nodeSelector": {},
            },
        },
    }
]


MISSING_IMAGE_MESSAGE = "Image is missing from the `RayCluster` spec, from the top-level Dagster resource config, and the Dagster run does not have a `dagster/image` tag. Please use one of these options to specify the image."


# Minimum Ray versions KubeRay enforces for `authOptions`, from `ValidateRayClusterSpec`.
MIN_RAY_VERSION_FOR_TOKEN_AUTH = "2.52.0"
MIN_RAY_VERSION_FOR_K8S_TOKEN_AUTH = "2.55.0"


class AuthOptions(dg.PermissiveConfig):
    """[AuthOptions](https://ray-project.github.io/kuberay/reference/api/#authoptions) for the Ray cluster.

    Every field the CRD supports is meant to be declared here. As an escape hatch for any that is missing, undeclared fields are passed through to the Kubernetes manifest, in either `snake_case` or `camelCase`. See [Extra Spec Fields](../tutorial/kuberay.md#extra-spec-fields).
    """

    mode: Literal["token", "disabled"] = "token"
    secret_name: str | None = Field(
        default=None,
        description="Name of the `Secret` holding the authentication token. The `Secret` must have an `auth_token` data key. If set, KubeRay skips generating a per-`RayCluster` token `Secret`. Requires KubeRay 1.6.0.",
    )
    enable_k8s_token_auth: bool | None = Field(
        default=None,
        description=(
            "Delegate authentication to the Kubernetes API server. Sets `RAY_ENABLE_K8S_TOKEN_AUTH=true` on all Ray pods; "
            "the ServiceAccount token mounted into Raylets must be granted the `ray:write` custom verb via RBAC. "
            f"Requires `mode='token'`, Ray {MIN_RAY_VERSION_FOR_K8S_TOKEN_AUTH} or later, and KubeRay 1.6.0, and cannot be "
            "combined with `secret_name` — the token comes from the mounted ServiceAccount rather than a `Secret`. "
            "KubeRay rejects it for `RayJob` and `RayService`, so it cannot be used with "
            "[`KubeRayInteractiveJob`][dagster_ray.kuberay.KubeRayInteractiveJob] — use "
            "[`KubeRayCluster`][dagster_ray.kuberay.KubeRayCluster]."
        ),
    )

    def to_k8s(self) -> dict[str, Any]:
        """Convert into Kubernetes manifests in camelCase format"""
        return merge_extra_k8s_fields(
            remove_none_from_dict(
                {
                    "mode": self.mode,
                    "secretName": self.secret_name,
                    "enableK8sTokenAuth": self.enable_k8s_token_auth,
                }
            ),
            self.model_extra,
        )


class RayClusterUpgradeStrategy(dg.PermissiveConfig):
    """[RayClusterUpgradeStrategy](https://ray-project.github.io/kuberay/reference/api/#rayclusterupgradestrategy) for the Ray cluster.

    Every field the CRD supports is meant to be declared here. As an escape hatch for any that is missing, undeclared fields are passed through to the Kubernetes manifest, in either `snake_case` or `camelCase`. See [Extra Spec Fields](../tutorial/kuberay.md#extra-spec-fields).
    """

    type: Literal["Recreate", "None"] = Field(
        description='Strategy used when upgrading the `RayCluster` pods. `Recreate` deletes all existing pods before creating new ones; the string `"None"` creates no new pods. Note that `"None"` is a KubeRay strategy name, distinct from leaving `upgrade_strategy` itself unset.',
    )

    def to_k8s(self) -> dict[str, Any]:
        """Convert into Kubernetes manifests in camelCase format"""
        return merge_extra_k8s_fields(remove_none_from_dict({"type": self.type}), self.model_extra)


class RayClusterSpec(dg.PermissiveConfig):
    """[RayCluster spec](https://ray-project.github.io/kuberay/reference/api/#rayclusterspec) configuration options. A few sensible defaults are provided for convenience.

    Every field the CRD supports is meant to be declared here. As an escape hatch for any that is missing, undeclared fields are passed through to the Kubernetes manifest, in either `snake_case` or `camelCase`. See [Extra Spec Fields](../tutorial/kuberay.md#extra-spec-fields).
    """

    suspend: bool | None = None
    managed_by: str | None = None
    autoscaler_options: dict[str, Any] = DEFAULT_AUTOSCALER_OPTIONS
    head_service_annotations: dict[str, str] | None = None
    enable_in_tree_autoscaling: bool = False
    gcs_fault_tolerance_options: dict[str, Any] | None = None
    head_group_spec: dict[str, Any] = DEFAULT_HEAD_GROUP_SPEC
    ray_version: str | None = None
    worker_group_specs: list[dict[str, Any]] = DEFAULT_WORKER_GROUP_SPECS
    auth_options: AuthOptions | None = None
    upgrade_strategy: RayClusterUpgradeStrategy | None = Field(
        default=None,
        description="Scaling policy used when upgrading the `RayCluster`. See [RayClusterUpgradeStrategy](https://ray-project.github.io/kuberay/reference/api/#rayclusterupgradestrategy). Requires KubeRay 1.6.0: older operators prune the field without an error.",
    )

    @model_validator(mode="after")
    def _validate_auth_options(self) -> Self:
        """Reject `auth_options` combinations KubeRay refuses, at config time.

        KubeRay validates the `RayCluster` in its controller rather than an admission webhook, so an
        invalid spec is created successfully and then never reconciled — no `.status` is written at
        all. Waiting on such a cluster burns the full timeout and then reports "timed out waiting for
        status", naming the wrong problem entirely. These mirror `ValidateRayClusterSpec` so the real
        reason surfaces immediately.
        """
        if self.auth_options is None:
            return self

        k8s_token_auth = bool(self.auth_options.enable_k8s_token_auth)

        if self.auth_options.mode != "token":
            if k8s_token_auth:
                raise ValueError(
                    "auth_options.enable_k8s_token_auth requires auth_options.mode='token', got "
                    f"mode={self.auth_options.mode!r}."
                )
            return self

        if self.ray_version is None:
            raise ValueError(
                "auth_options.mode='token' requires ray_version to be set (Ray "
                f"{MIN_RAY_VERSION_FOR_TOKEN_AUTH} or later). Without it KubeRay marks the RayCluster "
                "spec invalid and never reconciles it, so it never reports a status."
            )

        try:
            ray_version = Version(self.ray_version)
        except InvalidVersion:
            raise ValueError(
                f"ray_version {self.ray_version!r} is not a valid version, and auth_options.mode='token' "
                "requires KubeRay to parse it."
            ) from None

        if ray_version < Version(MIN_RAY_VERSION_FOR_TOKEN_AUTH):
            raise ValueError(
                f"auth_options.mode='token' requires Ray {MIN_RAY_VERSION_FOR_TOKEN_AUTH} or later, got "
                f"ray_version={self.ray_version!r}."
            )

        if k8s_token_auth:
            if self.auth_options.secret_name is not None:
                raise ValueError(
                    "auth_options.enable_k8s_token_auth and auth_options.secret_name are mutually "
                    "exclusive — the token comes from the mounted ServiceAccount, not a Secret."
                )
            if ray_version < Version(MIN_RAY_VERSION_FOR_K8S_TOKEN_AUTH):
                raise ValueError(
                    "auth_options.enable_k8s_token_auth requires Ray "
                    f"{MIN_RAY_VERSION_FOR_K8S_TOKEN_AUTH} or later, got ray_version={self.ray_version!r}."
                )

        return self

    def to_k8s(
        self,
        context: AnyDagsterContext,
        image: str | None = None,  # is injected into headgroup and workergroups, unless already specified there
        env_vars: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Convert into Kubernetes manifests in camelCase format and inject additional information"""

        assert context.log is not None

        # TODO: inject self.redis_port and self.dashboard_port into the RayCluster configuration
        # TODO: auto-apply some tags from dagster-k8s/config

        head_group_spec = self.head_group_spec.copy()
        worker_group_specs = self.worker_group_specs.copy()

        k8s_env_vars: list[dict[str, Any]] = []

        if env_vars:
            for key, value in env_vars.items():
                k8s_env_vars.append({"name": key, "value": value})

        def update_group_spec(group_spec: dict[str, Any]):
            # TODO: only inject if the container has a `dagster.io/inject-image` annotation or smth
            if group_spec["template"]["spec"]["containers"][0].get("image") is None:
                if image is None:
                    raise ValueError(MISSING_IMAGE_MESSAGE)
                else:
                    group_spec["template"]["spec"]["containers"][0]["image"] = image

            for container in group_spec["template"]["spec"]["containers"]:
                container["env"] = container.get("env", []) + k8s_env_vars

        update_group_spec(head_group_spec)
        for worker_group_spec in worker_group_specs:
            update_group_spec(worker_group_spec)

        return merge_extra_k8s_fields(
            remove_none_from_dict(
                {
                    "enableInTreeAutoscaling": self.enable_in_tree_autoscaling,
                    "autoscalerOptions": self.autoscaler_options,
                    "headGroupSpec": head_group_spec,
                    "workerGroupSpecs": worker_group_specs,
                    "suspend": self.suspend,
                    "managedBy": self.managed_by,
                    "headServiceAnnotations": self.head_service_annotations,
                    "gcsFaultToleranceOptions": self.gcs_fault_tolerance_options,
                    "rayVersion": self.ray_version,
                    "authOptions": self.auth_options.to_k8s() if self.auth_options is not None else None,
                    "upgradeStrategy": self.upgrade_strategy.to_k8s() if self.upgrade_strategy is not None else None,
                }
            ),
            self.model_extra,
        )


class RayClusterConfig(dg.Config):
    kind: str = "RayCluster"
    api_version: str = "ray.io/v1"
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Kubernetes metadata, except the name field can be omitted. In this case it will be generated by `dagster-ray`.",
    )
    spec: RayClusterSpec = Field(default_factory=RayClusterSpec)

    @property
    def namespace(self) -> str:
        return self.metadata.get("namespace", "default")

    def to_k8s(
        self,
        context: AnyDagsterContext,
        image: str | None = None,  # is injected into headgroup and workergroups, unless already specified there
        labels: Mapping[str, str] | None = None,
        annotations: Mapping[str, str] | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        assert context.log is not None
        """Convert into Kubernetes manifests in camelCase format and inject additional information"""

        labels = labels or {}
        annotations = annotations or {}

        return {
            "apiVersion": self.api_version,
            "kind": self.kind,
            "metadata": remove_none_from_dict(
                {
                    "name": self.metadata.get("name"),
                    "labels": {**(self.metadata.get("labels", {}) or {}), **labels},
                    "annotations": {**self.metadata.get("annotations", {}), **annotations},
                }
            ),
            "spec": self.spec.to_k8s(context=context, image=image, env_vars=env_vars),
        }


class RayJobSpec(dg.PermissiveConfig):
    """[RayJob spec](https://ray-project.github.io/kuberay/reference/api/#rayjobspec) configuration options. A few sensible defaults are provided for convenience.

    Every field the CRD supports is meant to be declared here. As an escape hatch for any that is missing, undeclared fields are passed through to the Kubernetes manifest, in either `snake_case` or `camelCase`. See [Extra Spec Fields](../tutorial/kuberay.md#extra-spec-fields).
    """

    active_deadline_seconds: int = 60 * 60 * 24  # 24 hours
    pre_running_deadline_seconds: int | None = Field(
        default=None,
        description="Deadline for the `RayJob` to reach the `Running` state, measured from `.status.startTime`. If it doesn't, KubeRay fails the job with reason `PreRunningDeadlineExceeded`. Useful for reaping jobs stuck in `Initializing` or `Waiting` — for example when the `RayCluster` can never be scheduled. Unset means no deadline. Requires KubeRay 1.6.0: older operators prune the field without an error.",
    )
    backoff_limit: int = 0
    ray_cluster_spec: RayClusterSpec | None = Field(default_factory=RayClusterSpec)
    submitter_pod_template: dict[str, Any] | None = None
    submitter_config: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None
    cluster_selector: dict[str, str] | None = None
    managed_by: str | None = None
    deletion_strategy: dict[str, Any] | None = Field(
        default=None,
        description="`RayJob` cleanup policy. Unset by default: cleanup is governed by `shutdown_after_job_finishes`, which deletes the `RayCluster` once the job succeeds or fails. Requires KubeRay 1.5.0 (the field was named `deletionPolicy` before that) **and** the `RayJobDeletionPolicy` feature gate — the KubeRay controller fails the `RayJob` with `ValidationFailed` if the gate is off. The gate is alpha in 1.5.x (off by default) and beta in 1.6.x (on by default). See [Deletion Strategy](../tutorial/kuberay.md#deletion-strategy).",
    )
    runtime_env_yaml: str | None = None
    job_id: str | None = None
    submission_mode: Literal["K8sJobMode", "HTTPMode", "InteractiveMode", "SidecarMode"] = "K8sJobMode"
    entrypoint_resources: str | None = None
    entrypoint_num_cpus: float | None = None
    entrypoint_memory: float | None = None
    entrypoint_num_gpus: float | None = None
    ttl_seconds_after_finished: int | None = 5 * 60  # 5 minutes
    shutdown_after_job_finishes: bool = True
    suspend: bool | None = None

    @model_validator(mode="after")
    def _reject_k8s_token_auth(self) -> Self:
        """KubeRay rejects `enableK8sTokenAuth` for `RayJob` outright rather than ignoring it.

        As with the `RayCluster` checks, the rejection happens in the controller, so without this the
        `RayJob` is created and then parked in `ValidationFailed`.
        """
        auth_options = self.ray_cluster_spec.auth_options if self.ray_cluster_spec is not None else None

        if auth_options is not None and auth_options.enable_k8s_token_auth:
            raise ValueError(
                "KubeRay does not support auth_options.enable_k8s_token_auth for RayJob and marks the "
                "spec invalid. Use KubeRayCluster for Kubernetes-delegated token auth."
            )

        return self

    def to_k8s(
        self,
        context: AnyDagsterContext,
        image: str | None = None,  # is injected into headgroup and workergroups, unless already specified there
        env_vars: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Convert into Kubernetes manifests in camelCase format and inject additional information"""
        return merge_extra_k8s_fields(
            remove_none_from_dict(
                {
                    "activeDeadlineSeconds": self.active_deadline_seconds,
                    "preRunningDeadlineSeconds": self.pre_running_deadline_seconds,
                    "backoffLimit": self.backoff_limit,
                    "submitterPodTemplate": self.submitter_pod_template,
                    "submitterConfig": self.submitter_config,
                    "metadata": self.metadata,
                    "clusterSelector": self.cluster_selector,
                    "managedBy": self.managed_by,
                    "deletionStrategy": self.deletion_strategy,
                    "runtimeEnvYAML": self.runtime_env_yaml,
                    "jobId": self.job_id,
                    "submissionMode": self.submission_mode,
                    "entrypointResources": self.entrypoint_resources,
                    "entrypointNumCpus": self.entrypoint_num_cpus,
                    "entrypointMemory": self.entrypoint_memory,
                    "entrypointNumGpus": self.entrypoint_num_gpus,
                    "ttlSecondsAfterFinished": self.ttl_seconds_after_finished,
                    "shutdownAfterJobFinishes": self.shutdown_after_job_finishes,
                    "suspend": self.suspend,
                    "rayClusterSpec": self.ray_cluster_spec.to_k8s(context=context, image=image, env_vars=env_vars)
                    if self.ray_cluster_spec is not None
                    else None,
                }
            ),
            self.model_extra,
        )


class RayJobConfig(dg.Config):
    kind: str = "RayJob"
    api_version: str = "ray.io/v1"
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Kubernetes metadata, except the name field can be omitted. In this case it will be generated by `dagster-ray`.",
    )
    spec: RayJobSpec = Field(default_factory=RayJobSpec)

    @property
    def namespace(self) -> str:
        return self.metadata.get("namespace", "default")

    def to_k8s(
        self,
        context: AnyDagsterContext,
        image: str | None = None,  # is injected into headgroup and workergroups, unless already specified there
        labels: Mapping[str, str] | None = None,
        annotations: Mapping[str, str] | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Convert into Kubernetes manifests in camelCase format and inject additional information"""

        labels = labels or {}
        annotations = annotations or {}

        return {
            "apiVersion": self.api_version,
            "kind": self.kind,
            "metadata": remove_none_from_dict(
                {
                    "name": self.metadata.get("name"),
                    "labels": {**(self.metadata.get("labels", {}) or {}), **labels},
                    "annotations": {**self.metadata.get("annotations", {}), **annotations},
                }
            ),
            "spec": self.spec.to_k8s(
                context=context,
                image=image,
                env_vars=env_vars,
            ),
        }


class MatchDagsterLabels(dg.Config):
    cluster_sharing: bool = Field(default=True, description="Whether to match on `dagster/cluster-sharing=true` label.")
    code_location: bool = Field(
        default=True,
        description="Whether to match on `dagster/code-location` label. The value will be taken from the current Dagster code location.",
    )
    resource_key: bool = Field(
        default=True,
        description="Whether to match on `dagster/resource-key` label. The value will be taken from the current Dagster resource key.",
    )
    git_sha: bool = Field(
        default=True,
        description="Whether to match on `dagster/git-sha` label. The value will be taken from `DAGSTER_CLOUD_GIT_SHA` environment variable.",
    )  # TODO: we really should have common env vars for this, not just Dagster Plus specific
    run_id: bool = Field(
        default=False,
        description="Whether to match on `dagster/run-id` label. The value will be taken from the current Dagster run ID.",
    )


DEFAULT_CLUSTER_SHARING_TTL_SECONDS = 30 * 60.0


class ClusterSharingHeartbeat(dg.Config):
    """Controls background renewal of the cluster sharing lock while the Dagster step is running.

    Without renewal, the lock expires `ttl_seconds` after step start and the garbage collection
    sensor may delete the cluster while the step is still using it. With renewal, `ttl_seconds`
    can stay short: idle clusters are reaped promptly, active steps survive.

    Each renewal updates the lock's `heartbeat_at` timestamp; `created_at` always points at the
    initial lock placement. A hanging step renews its lock indefinitely — set the
    `dagster/max_runtime` tag on runs to bound step runtime.
    """

    enabled: bool = Field(
        default=True,
        description="Whether to renew the cluster sharing lock in the background while the Dagster step is running.",
    )
    refresh_seconds: float = Field(
        default=10.0,
        description="How often to renew the lock. Must be well below `ClusterSharing.ttl_seconds` — if a renewal is missed, the lock must not have expired yet, or the cluster can be deleted mid-step.",
    )


class ClusterSharing(dg.Config):
    """Defines the strategy for sharing `RayCluster` resources with other Dagster steps.

    By default, the cluster is expected to be created by Dagster during one of the previously executed steps.

    !!!note

        Cluster sharing uses the Kubernetes Lease API (`coordination.k8s.io`) for leader election
        to coordinate cluster creation across parallel steps. The Dagster ServiceAccount must have
        `create`, `get`, and `delete` permissions on `leases` in the `coordination.k8s.io` API group.
    """

    enabled: bool = Field(default=False, description="Whether to enable sharing of RayClusters.")
    match_dagster_labels: MatchDagsterLabels = Field(
        default_factory=MatchDagsterLabels, description="Configuration for matching on Dagster-generated labels."
    )
    match_labels: dict[str, str] | None = Field(
        default=None, description="Additional user-provided labels to match on."
    )
    ttl_seconds: float = Field(
        default=DEFAULT_CLUSTER_SHARING_TTL_SECONDS,
        description="Time to live for the lock placed on the `RayCluster` resource, marking it as in use by the current Dagster step. The lock is renewed periodically while the step is running (see `heartbeat`), so this only needs to cover the gap between renewals.",
    )
    heartbeat: ClusterSharingHeartbeat = Field(
        default_factory=ClusterSharingHeartbeat,
        description="Configuration for background renewal of the cluster sharing lock while the Dagster step is running.",
    )

    @model_validator(mode="after")
    def _warn_on_low_heartbeat_headroom(self) -> Self:
        """Warn on a misconfiguration that would silently reintroduce mid-step deletion.

        The heartbeat renews the lock every `refresh_seconds`; the lock expires `ttl_seconds`
        after the last renewal. If `refresh_seconds` isn't comfortably below `ttl_seconds`, a
        single missed renewal (a slow API call, a paused thread) lets the lock expire while the
        step is still running, and the garbage collection sensor may delete the cluster mid-step.
        We recommend at least 2x headroom.
        """
        if self.enabled and self.heartbeat.enabled and self.heartbeat.refresh_seconds * 2 > self.ttl_seconds:
            warnings.warn(
                f"cluster_sharing.heartbeat.refresh_seconds ({self.heartbeat.refresh_seconds}) should be at most "
                f"half of cluster_sharing.ttl_seconds ({self.ttl_seconds}) so a missed heartbeat can't let the "
                f"lock expire while the step is still running, which would let the garbage collection sensor "
                f"delete the cluster mid-step. Lower refresh_seconds or raise ttl_seconds.",
                stacklevel=2,
            )
        return self
