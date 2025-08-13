from __future__ import annotations

import os
from typing import Any, Literal

from dagster import Config
from pydantic import Field

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


class MetadataWithoutName(Config):
    namespace: str | None = None
    labels: dict[str, str] | None = None
    annotations: dict[str, str] | None = None


class MetadataWithOptionalName(MetadataWithoutName):
    """This config almost matches the original Kubernetes metadata, except name which can be ommitted to be set automatically by `dagster-ray`."""

    name: str | None = None


class Metadata(MetadataWithoutName):
    name: str


class RayClusterSpec(Config):
    """[RayCluster spec](https://ray-project.github.io/kuberay/reference/api/#rayclusterspec) configuration options. A few sensible defaults are provided for convenience."""

    suspend: bool = False
    managed_by: str | None = None
    autoscaler_options: dict[str, Any] = DEFAULT_AUTOSCALER_OPTIONS  # TODO: add a dedicated Config type
    head_service_annotations: dict[str, str] | None = None
    enable_in_tree_autoscaling: bool = False
    gcs_fault_tolerance_options: dict[str, Any] | None = None
    head_group_spec: dict[str, Any] = DEFAULT_HEAD_GROUP_SPEC  # TODO: add a dedicated Config type
    ray_version: str | None = None
    worker_group_specs: list[dict[str, Any]] = DEFAULT_WORKER_GROUP_SPECS  # TODO: add a dedicated Config type


class RayClusterConfig(Config):
    kind: str = "RayCluster"
    api_version: str = "ray.io/v1"
    metadata: MetadataWithOptionalName = Field(default_factory=MetadataWithOptionalName)
    spec: RayClusterSpec = Field(default_factory=RayClusterSpec)


class RayJobSpec(Config):
    """[RayJob spec](https://ray-project.github.io/kuberay/reference/api/#rayjobspec) configuration options. A few sensible defaults are provided for convenience."""

    active_deadline_seconds: int = 60 * 60 * 24  # 24 hours
    backoff_limit: int = 0
    ray_cluster_spec: RayClusterSpec = Field(default_factory=RayClusterSpec)
    submitter_pod_template: dict[str, Any] | None = None
    cluster_selector: dict[str, str] | None = None
    managed_by: str | None = None
    deletion_strategy: dict[str, Any] | None = None
    runtime_env_yaml: str | None = None
    job_id: str | None = None
    submission_mode: Literal["K8sJobMode", "HTTPMode", "InteractiveMode"] = "K8sJobMode"
    entrypoint_resources: str | None = None
    entrypoint_num_cpus: float
    entrypoint_memory: float
    entrypoint_num_gpus: float
    ttl_seconds_after_finished: int = 60 * 60  # 1 hour
    shutdown_after_job_finishes: bool = True
    suspend: bool = False


class RayJobConfig(Config):
    kind: str = "RayJob"
    api_version: str = "ray.io/v1"
    metadata: MetadataWithOptionalName = Field(default_factory=MetadataWithOptionalName)
    spec: RayJobSpec = Field(default_factory=RayJobSpec)
