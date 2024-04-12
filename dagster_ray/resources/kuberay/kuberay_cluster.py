import contextlib
import hashlib
import os
import random
import re
import string
import sys
import uuid
from typing import Any, Dict, Generator, List, Optional, cast

import dagster._check as check
from dagster import ConfigurableResource, InitResourceContext
from kubernetes import config
from pydantic import Field, PrivateAttr

# yes, `python-client` is actually the KubeRay package name
# https://github.com/ray-project/kuberay/issues/2078
from python_client import kuberay_cluster_api
from requests.exceptions import ConnectionError
from tenacity import retry, retry_if_exception_type, stop_after_delay

from dagster_ray.config import RayDataExecutionOptions

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

import ray
from dagster import Config, DagsterRun, DagsterRunStatus
from ray._private.worker import BaseContext as RayBaseContext  # noqa

in_k8s = os.environ.get("KUBERNETES_SERVICE_HOST") is not None
IS_PROD = os.getenv("DAGSTER_CLOUD_DEPLOYMENT_NAME") == "prod"


class RayClusterConfig(Config):
    image: Optional[str] = None
    namespace: str = "kuberay"

    autoscaler_options: Dict[str, Any]  # TODO: add a dedicated Config type
    head_group_spec: Dict[str, Any]  # TODO: add a dedicated Config type
    worker_group_specs = List[Dict[str, Any]]  # TODO: add a dedicated Config type

    def build_raycluster(
        self,
        cluster_name: str,
        image: str,
        labels: Optional[Dict[str, str]] = None,  # TODO: use in RayCluster labels
    ) -> Dict[str, Any]:
        return {
            "apiVersion": "ray.io/v1alpha1",
            "kind": "RayCluster",
            "metadata": {
                "name": cluster_name,
                "labels": labels or {},
            },
            "spec": {
                "enableInTreeAutoscaling": True,
                "autoscalerOptions": {
                    "upscalingMode": "Default",
                    "idleTimeoutSeconds": 60,
                    "env": [],
                    "envFrom": [],
                    "resources": {
                        "limits": {"cpu": "1000m", "memory": "1Gi"},
                        "requests": {"cpu": "1000m", "memory": "1Gi"},
                    },
                },
                "headGroupSpec": {
                    "serviceType": "ClusterIP",
                    "rayStartParams": {"dashboard-host": "0.0.0.0"},
                    "template": {
                        "spec": {
                            "imagePullSecrets": [],
                            "containers": [
                                {
                                    "volumeMounts": [
                                        {"mountPath": "/tmp/ray", "name": "log-volume"},
                                    ],
                                    "name": "ray-head",
                                    "image": image,
                                }
                            ],
                            "volumes": [
                                {"name": "log-volume", "emptyDir": {}},
                            ],
                            "affinity": {},
                            "tolerations": [],
                            "nodeSelector": {},
                        },
                        "metadata": {
                            "annotations": {},
                        },
                    },
                },
                "workerGroupSpecs": [],
            },
        }


DAGSTER_DEPLOYMENT_NAME = "TODO: change this"


class KubeRayCluster(ConfigurableResource):
    """
    Provides a RayCluster for the current step selection
    The cluster is automatically deleted after steps execution
    """

    ray_cluster: RayClusterConfig
    data_execution_options: RayDataExecutionOptions = RayDataExecutionOptions()
    disable_cluster_cleanup: bool = False
    skip_init: bool = False
    local_mode: bool = Field(
        default=True if DAGSTER_DEPLOYMENT_NAME == "local" else False,
        description="If to run Ray locally without creating an actual RayCluster.",
    )

    _context: Optional[RayBaseContext] = PrivateAttr()
    _host: str = PrivateAttr()
    _kuberay_api: kuberay_cluster_api.RayClusterApi = PrivateAttr()

    @contextlib.contextmanager
    def yield_for_execution(self, context: InitResourceContext) -> Generator[Self, None, None]:
        assert context.log is not None
        assert context.dagster_run is not None

        self._load_kubeconfig()

        self.kuberay_api = kuberay_cluster_api.RayClusterApi()

        cluster_name = self._get_ray_cluster_step_name(context)

        # for now, self.cluster is a raw dict
        namespace = self.cluster["namespace"]

        self._host = f"{cluster_name}-head-svc.{namespace}.svc.cluster.local"

        try:
            if not self.kuberay_api.list_ray_clusters(  # just a safety measure, no need to recreate the cluster for step retries
                k8s_namespace=namespace,
                label_selector=f"dagster.io/clusterName={cluster_name}",
            ):
                cluster_body = self.ray_cluster.build_raycluster(
                    cluster_name=cluster_name,
                    image=(self.ray_cluster.image or context.dagster_run.tags["dagster/image"]),
                    labels={
                        "dagster.io/run_id": context.run_id,
                        "dagster.io/clusterName": cluster_name,
                        # TODO: add more labels
                    },
                )

                self.kuberay_api.create_ray_cluster(body=cluster_body, k8s_namespace=namespace)
                context.log.info(f"Created RayCluster {namespace}/{cluster_name}. Waiting for it to become ready...")
                self.kuberay_api.wait_until_ray_cluster_running(cluster_name, k8s_namespace=namespace)
                context.log.info(
                    f"Run `kubectl -n {namespace} port-forward svc/{cluster_name}-head-svc 8265:8265 6379:6379 10001:10001` to connect to RayCluster "
                    f"and open localhost:8265 to view the Ray Dashboard"
                )

            context.log.debug(f"Ray host: {self.host}")

            if not self.skip_init:
                self.init_ray()
                context.log.info("Initialized Ray!")
            else:
                self._context = None

            self.data_execution_options.apply()

            yield self
        except Exception as e:
            context.log.critical("Couldn't create or connect to RayCluster!")
            self._maybe_cleanup_raycluster(context, cluster_name, namespace=namespace)
            raise e

        self._maybe_cleanup_raycluster(context, cluster_name, namespace=namespace)
        if self._context is not None:
            self._context.disconnect()

    @property
    def context(self) -> "RayBaseContext":
        assert self._context is not None, "RayClusterResource not initialized"
        return self._context

    @property
    def host(self) -> str:
        return self._host

    @property
    def ray_address(self) -> str:
        return f"ray://{self.host}:10001"

    @property
    def dashboard_url(self) -> str:
        return f"http://{self.host}:8265"

    @property
    def runtime_job_id(self) -> str:
        """
        Returns the Ray Job ID for the current job which was created with `ray.init()`.
        :return:
        """
        return ray.get_runtime_context().get_job_id()

    @retry(stop=stop_after_delay(120), retry=retry_if_exception_type(ConnectionError))
    def init_ray(self) -> "RayBaseContext":
        self._context = ray.init(address=self.ray_address, ignore_reinit_error=True)
        return cast(RayBaseContext, self._context)

    def _maybe_cleanup_raycluster(self, context: InitResourceContext, cluster_name: str, namespace: str):
        assert context.log is not None

        if self.local_mode:
            return

        if (
            not self.disable_cluster_cleanup
            and cast(DagsterRun, context.dagster_run).status != DagsterRunStatus.FAILURE
        ):
            self.kuberay_api.delete_ray_cluster(cluster_name, k8s_namespace=namespace)
            context.log.info(f"Deleted RayCluster {namespace}/{cluster_name}")
        else:
            context.log.warning(
                f"Skipping RayCluster {cluster_name} deletion because `disable_cluster_cleanup` "
                f"config parameter is set to `True` or the run failed. "
                f"It may be still be deleted by the automatic cleanup job."
            )

    def _get_step_key(self, context: InitResourceContext) -> str:
        # just return a random string
        # since we want a fresh cluster every time
        return str(uuid.uuid4())

    def _get_ray_cluster_step_name(self, context: InitResourceContext) -> str:
        assert isinstance(context.run_id, str)
        assert context.dagster_run is not None

        # try to make the name as short as possible

        deployment_name = (
            os.getenv("DAGSTER_CLOUD_DEPLOYMENT_NAME")
            if os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT") == "0"
            else os.getenv("DAGSTER_CLOUD_GIT_BRANCH")
        )
        deployment_name = (deployment_name or "dev").replace("-", "")

        cluster_name_prefix = f"dr-{deployment_name[:8]}-{context.run_id[:8]}"

        dagster_user_email = context.dagster_run.tags.get("user")
        if dagster_user_email is not None:
            cluster_name_prefix += f"-{dagster_user_email.replace('.', '').replace('-', '').split('@')[0][:6]}"

        step_key = self._get_step_key(context)

        name_key = get_k8s_object_name(
            context.run_id,
            step_key,
        )

        step_name = f"{cluster_name_prefix}-{name_key}".lower()
        step_name = re.sub(r"[^-0-9a-z]", "-", step_name)

        return step_name

    @staticmethod
    def _load_kubeconfig():
        try:
            config.load_kube_config()
        except config.config_exception.ConfigException:
            config.load_incluster_config()


def get_k8s_object_name(run_id: str, step_key: Optional[str] = None):
    """Creates a unique (short!) identifier to name k8s objects based on run ID and step key(s).

    K8s Job names are limited to 63 characters, because they are used as labels. For more info, see:

    https://kubernetes.io/docs/concepts/overview/working-with-objects/names/
    """
    check.str_param(run_id, "run_id")
    check.opt_str_param(step_key, "step_key")
    if not step_key:
        letters = string.ascii_lowercase
        step_key = "".join(random.choice(letters) for i in range(20))

    # Creates 32-bit signed int, so could be negative
    name_hash = hashlib.md5((run_id + step_key).encode("utf-8"))

    return name_hash.hexdigest()[:8]
