import os
from typing import Any, Dict, List, Optional

from dagster import Config

in_k8s = os.environ.get("KUBERNETES_SERVICE_HOST") is not None
IS_PROD = os.getenv("DAGSTER_CLOUD_DEPLOYMENT_NAME") == "prod"
DEFAULT_AUTOSCALER_OPTIONS = {
    "upscalingMode": "Default",
    "idleTimeoutSeconds": 60,
    "env": [],
    "envFrom": [],
    "resources": {
        "limits": {"cpu": "1000m", "memory": "1Gi"},
        "requests": {"cpu": "1000m", "memory": "1Gi"},
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
                        # {"mountPath": "/tmp/ray", "name": "log-volume"},
                    ],
                    "name": "head",
                    "imagePullPolicy": "Always",
                },
            ],
            "volumes": [
                {"name": "log-volume", "emptyDir": {}},
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
                        "volumeMounts": [
                            #  {"mountPath": "/tmp/ray", "name": "log-volume"}
                        ],
                        "name": "worker",
                        "imagePullPolicy": "Always",
                    }
                ],
                "volumes": [
                    {"name": "log-volume", "emptyDir": {}},
                ],
                "affinity": {},
                "tolerations": [],
                "nodeSelector": {},
            },
        },
    }
]


class RayClusterConfig(Config):
    image: Optional[str] = None
    namespace: str = "kuberay"
    enable_in_tree_autoscaling: bool = False
    autoscaler_options: Dict[str, Any] = DEFAULT_AUTOSCALER_OPTIONS  # TODO: add a dedicated Config type
    head_group_spec: Dict[str, Any] = DEFAULT_HEAD_GROUP_SPEC  # TODO: add a dedicated Config type
    worker_group_specs: List[Dict[str, Any]] = DEFAULT_WORKER_GROUP_SPECS  # TODO: add a dedicated Config type


DEFAULT_DEPLOYMENT_NAME = (
    os.getenv("DAGSTER_CLOUD_DEPLOYMENT_NAME")
    if os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT") == "0"
    else os.getenv("DAGSTER_CLOUD_GIT_BRANCH")
) or "dev"
