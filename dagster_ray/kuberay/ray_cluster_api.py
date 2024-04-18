from typing import Any, Optional

from kubernetes import client, config
from python_client.kuberay_cluster_api import RayClusterApi


class PatchedRayClusterApi(RayClusterApi):
    """
    List of modifications to the original RayClusterApi:
     - allow passing config_file and context to __init__
    """

    def __init__(self, config_file: Optional[str], context: Optional[str] = None):
        self.kube_config: Optional[Any] = config.load_kube_config(config_file=config_file, context=context)
        self.api = client.CustomObjectsApi()
        self.core_v1_api = client.CoreV1Api()
