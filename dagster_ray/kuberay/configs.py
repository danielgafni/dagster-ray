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


class RayClusterConfig(Config):
    image: str | None = None
    namespace: str = "ray"
    enable_in_tree_autoscaling: bool = False
    autoscaler_options: dict[str, Any] = DEFAULT_AUTOSCALER_OPTIONS  # TODO: add a dedicated Config type
    head_group_spec: dict[str, Any] = DEFAULT_HEAD_GROUP_SPEC  # TODO: add a dedicated Config type
    worker_group_specs: list[dict[str, Any]] = DEFAULT_WORKER_GROUP_SPECS  # TODO: add a dedicated Config type


class RayJobConfig(Config):
    entrypoint_num_cpus: float
    entrypoint_memory: float
    entrypoint_num_gpus: int
    suspend: bool = False
    annotations: dict[str, str] | None = None
    labels: dict[str, str] | None = None
    shutdown_after_job_finishes: bool = True
    ttl_seconds_after_finished: int = 60 * 10  # 10 minutes
    active_deadline_seconds: int = 60 * 60 * 24  # 24 hours
    submission_mode: Literal["K8sJobMode", "HTTPMode"] = "K8sJobMode"
    runtime_env_yaml: str | None = None
    cluster: RayClusterConfig = Field(default_factory=RayClusterConfig)
