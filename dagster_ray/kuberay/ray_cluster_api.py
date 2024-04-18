import time
from typing import Any, Optional

from kubernetes import client, config
from kubernetes.client import ApiException
from python_client import constants
from python_client.kuberay_cluster_api import RayClusterApi, log


class PatchedRayClusterApi(RayClusterApi):
    """
    List of modifications to the original RayClusterApi:
     - allow passing config_file and context to __init__\
     - fixed get_ray_cluster_status hard-querying 'status' field which is not always present
    """

    def __init__(self, config_file: Optional[str], context: Optional[str] = None):
        self.kube_config: Optional[Any] = config.load_kube_config(config_file=config_file, context=context)
        self.api = client.CustomObjectsApi()
        self.core_v1_api = client.CoreV1Api()

    def get_ray_cluster_status(
        self, name: str, k8s_namespace: str = "default", timeout: int = 60, delay_between_attempts: int = 5
    ) -> Any:
        while timeout > 0:
            try:
                resource: Any = self.api.get_namespaced_custom_object_status(
                    group=constants.GROUP,
                    version=constants.VERSION,
                    plural=constants.PLURAL,
                    name=name,
                    namespace=k8s_namespace,
                )
            except ApiException as e:
                if e.status == 404:
                    log.error("raycluster resource is not found. error = {}".format(e))
                    return None
                else:
                    log.error("error fetching custom resource: {}".format(e))
                    return None

            if resource.get("status"):  # <- fixed here, was ["status"]
                return resource["status"]
            else:
                log.info("raycluster {} status not set yet, waiting...".format(name))
                time.sleep(delay_between_attempts)
                timeout -= delay_between_attempts

        log.info("raycluster {} status not set yet, timing out...".format(name))
        return None
