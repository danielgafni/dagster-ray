import os
from collections.abc import Mapping
from typing import Any, Literal

import dagster as dg
from pydantic import Field

from dagster_ray.kuberay.utils import remove_none_from_dict
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


class RayClusterSpec(dg.PermissiveConfig):
    """[RayCluster spec](https://ray-project.github.io/kuberay/reference/api/#rayclusterspec) configuration options. A few sensible defaults are provided for convenience."""

    suspend: bool | None = None
    managed_by: str | None = None
    autoscaler_options: dict[str, Any] = DEFAULT_AUTOSCALER_OPTIONS
    head_service_annotations: dict[str, str] | None = None
    enable_in_tree_autoscaling: bool = False
    gcs_fault_tolerance_options: dict[str, Any] | None = None
    head_group_spec: dict[str, Any] = DEFAULT_HEAD_GROUP_SPEC
    ray_version: str | None = None
    worker_group_specs: list[dict[str, Any]] = DEFAULT_WORKER_GROUP_SPECS

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

        return remove_none_from_dict(
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
            }
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
    """[RayJob spec](https://ray-project.github.io/kuberay/reference/api/#rayjobspec) configuration options. A few sensible defaults are provided for convenience."""

    active_deadline_seconds: int = 60 * 60 * 24  # 24 hours
    backoff_limit: int = 0
    ray_cluster_spec: RayClusterSpec | None = Field(default_factory=RayClusterSpec)
    submitter_pod_template: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None
    cluster_selector: dict[str, str] | None = None
    managed_by: str | None = None
    deletion_strategy: dict[str, Any] | None = Field(
        default_factory=lambda: {"onFailure": {"policy": "DeleteCluster"}, "onSuccess": {"policy": "DeleteCluster"}}
    )
    runtime_env_yaml: str | None = None
    job_id: str | None = None
    submission_mode: Literal["K8sJobMode", "HTTPMode", "InteractiveMode"] = "K8sJobMode"
    entrypoint_resources: str | None = None
    entrypoint_num_cpus: float | None = None
    entrypoint_memory: float | None = None
    entrypoint_num_gpus: float | None = None
    ttl_seconds_after_finished: int | None = 5 * 60  # 5 minutes
    shutdown_after_job_finishes: bool = True
    suspend: bool | None = None

    def to_k8s(
        self,
        context: AnyDagsterContext,
        image: str | None = None,  # is injected into headgroup and workergroups, unless already specified there
        env_vars: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Convert into Kubernetes manifests in camelCase format and inject additional information"""
        return remove_none_from_dict(
            {
                "activeDeadlineSeconds": self.active_deadline_seconds,
                "backoffLimit": self.backoff_limit,
                "submitterPodTemplate": self.submitter_pod_template,
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


class ClusterSharing(dg.Config):
    """Defines the strategy for sharing `RayCluster` resources with other Dagster steps.

    By default, the cluster is expected to be created by Dagster during one of the previously executed steps."""

    enabled: bool = Field(default=False, description="Whether to enable sharing of RayClusters.")
    match_dagster_labels: MatchDagsterLabels = Field(
        default_factory=MatchDagsterLabels, description="Configuration for matching on Dagster-generated labels."
    )
    match_labels: dict[str, str] | None = Field(
        default=None, description="Additional user-provided labels to match on."
    )
    ttl_seconds: float = Field(
        default=DEFAULT_CLUSTER_SHARING_TTL_SECONDS,
        description="Time to live for the lock placed on the `RayCluster` resource, marking it as in use by the current Dagster step.",
    )
