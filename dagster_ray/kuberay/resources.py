import contextlib
import hashlib
import os
import random
import re
import string
import sys
from typing import Any, Dict, Generator, List, Optional, cast

import dagster._check as check
from dagster import InitResourceContext
from kubernetes import client, config, watch
from pydantic import Field, PrivateAttr

# yes, `python-client` is actually the KubeRay package name
# https://github.com/ray-project/kuberay/issues/2078
from python_client import kuberay_cluster_api

from dagster_ray.kuberay.ray_cluster_api import PatchedRayClusterApi

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from dagster import Config, DagsterRun, DagsterRunStatus
from ray._private.worker import BaseContext as RayBaseContext  # noqa

from dagster_ray._base.resources import BaseRayResource

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
    "rayStartParams": {"dashboard-host": "0.0.0.0"},
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
                        {"mountPath": "/tmp/ray", "name": "log-volume"},
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
#
#
# class WorkerGroupSpecConfig(Config):
#     imagePullSecrets: List[Dict[str, Any]] = []
#     containers: List[Dict[str, Any]] = [{
#                                     "volumeMounts": [
#                                         {"mountPath": "/tmp/ray", "name": "log-volume"},
#                                     ],
#                                     "name": "worker",
#                                 }]
#     volumes: List[Dict[str, Any]] = []
#     affinity: Dict[str, Any] = {}
#     tolerations: List[Dict[str, Any]] = []
#     nodeSelector: Dict[str, Any] = {}
#
#
#
# class WorkerGroupTemplateConfig(Config):
#     spec: WorkerGroupSpecConfig = WorkerGroupSpecConfig()
#
#
# class WorkerGroupConfig(Config):
#     template: WorkerGroupTemplateConfig = WorkerGroupTemplateConfig()
#
# class HeadGroupConfig(Config):
#     serviceType: str = "ClusterIP"
#     rayStartParams: Dict[str, Any] = {"dashboard-host": "0.0.0.0"}
#     template: GroupTemplateConfig = GroupTemplateConfig()


DEFAULT_WORKER_GROUP_SPECS = [
    {
        "groupName": "workers",
        "rayStartParams": {},
        "template": {
            "metadata": {"labels": {}, "annotations": {}},
            "spec": {
                "imagePullSecrets": [],
                "containers": [
                    {
                        "volumeMounts": [
                            {"mountPath": "/tmp/ray", "name": "log-volume"},
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


DEFAULT_CLUSTER_NAME_PREFIX = (
    os.getenv("DAGSTER_CLOUD_DEPLOYMENT_NAME")
    if os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT") == "0"
    else os.getenv("DAGSTER_CLOUD_GIT_BRANCH")
)
DEFAULT_CLUSTER_NAME_PREFIX = DEFAULT_CLUSTER_NAME_PREFIX or "dev"


class KubeRayCluster(BaseRayResource):
    """
    Provides a RayCluster for the current step selection
    The cluster is automatically deleted after steps execution
    """

    cluster_name_prefix: str = Field(
        default=DEFAULT_CLUSTER_NAME_PREFIX,
        description="Prefix for the RayCluster name. It's recommended to match it with the Dagster deployment name. Dagster Cloud variables are used for the default value.",
    )
    ray_cluster: RayClusterConfig = Field(default_factory=RayClusterConfig)
    disable_cluster_cleanup: bool = False
    skip_init: bool = False

    kubeconfig_file: Optional[str] = None

    _cluster_name: str = PrivateAttr()
    _host: str = PrivateAttr()
    _kuberay_api: PatchedRayClusterApi = PrivateAttr()
    _k8s_api: client.CustomObjectsApi = PrivateAttr()
    _k8s_core_api: client.CoreV1Api = PrivateAttr()

    @property
    def host(self) -> str:
        if self._host is None:
            raise ValueError("RayClusterResource not initialized")
        return self._host

    @property
    def namespace(self) -> str:
        return self.ray_cluster.namespace  # TODO: add namespace auto-creation logic

    @property
    def cluster_name(self) -> str:
        if self._cluster_name is None:
            raise ValueError("RayClusterResource not initialized")
        return self._cluster_name

    @property
    def kuberay_api(self) -> kuberay_cluster_api.RayClusterApi:
        if self._kuberay_api is None:
            raise ValueError("RayClusterResource not initialized")
        return self._kuberay_api

    @property
    def k8s_api(self) -> client.CustomObjectsApi:
        if self._k8s_api is None:
            raise ValueError("RayClusterResource not initialized")
        return self._k8s_api

    @contextlib.contextmanager
    def yield_for_execution(self, context: InitResourceContext) -> Generator[Self, None, None]:
        assert context.log is not None
        assert context.dagster_run is not None

        self._load_kubeconfig(self.kubeconfig_file)

        self._kuberay_api = PatchedRayClusterApi(config_file=self.kubeconfig_file)
        self._k8s_api = client.CustomObjectsApi()
        self._k8s_core_api = client.CoreV1Api()

        self._cluster_name = self._get_ray_cluster_step_name(context)

        # self._host = f"{self.cluster_name}-head-svc.{self.namespace}.svc.cluster.local"

        try:
            # just a safety measure, no need to recreate the cluster for step retries or smth
            if not self.kuberay_api.list_ray_clusters(
                k8s_namespace=self.namespace,
                label_selector=f"dagster.io/cluster={self.cluster_name}",
            )["items"]:
                cluster_body = self._build_raycluster(
                    image=(self.ray_cluster.image or context.dagster_run.tags["dagster/image"]),
                    labels={
                        "dagster.io/run_id": cast(str, context.run_id),
                        "dagster.io/cluster": self.cluster_name,
                        # TODO: add more labels
                    },
                )

                self.kuberay_api.create_ray_cluster(body=cluster_body, k8s_namespace=self.namespace)
                context.log.info(
                    f"Created RayCluster {self.namespace}/{self.cluster_name}. Waiting for it to become ready..."
                )

                self._wait_raycluster_ready()

                # TODO: currently this will only work from withing the cluster
                # find a way to make it work from outside
                # probably would need a LoadBalancer/Ingress
                self._host = self.kuberay_api.get_ray_cluster(name=self.cluster_name, k8s_namespace=self.namespace)[
                    "status"
                ]["head"]["serviceIP"]

                context.log.info("RayCluster is ready! Connection command:")

                context.log.info(
                    f"`kubectl -n {self.namespace} port-forward svc/{self.cluster_name}-head-svc 8265:8265 6379:6379 10001:10001`"
                )

            context.log.debug(f"Ray host: {self.host}")

            if not self.skip_init:
                self.init_ray()
                context.log.info("Initialized Ray!")
            else:
                self._context = None

            yield self
        except Exception as e:
            context.log.critical("Couldn't create or connect to RayCluster!")
            self._maybe_cleanup_raycluster(context)
            raise e

        self._maybe_cleanup_raycluster(context)
        if self._context is not None:
            self._context.disconnect()

    def _build_raycluster(
        self,
        image: str,
        labels: Optional[Dict[str, str]] = None,  # TODO: use in RayCluster labels
    ) -> Dict[str, Any]:
        """
        Builds a RayCluster from the provided configuration, while injecting custom image and labels (only known during resource setup)
        """
        # TODO: inject self.redis_port and self.dashboard_port into the RayCluster configuration

        image = self.ray_cluster.image or image
        head_group_spec = self.ray_cluster.head_group_spec.copy()
        worker_group_specs = self.ray_cluster.worker_group_specs.copy()

        def update_group_spec(group_spec: Dict[str, Any]):
            # TODO: only inject if the container has a `dagster.io/inject-image` annotation or smth
            if group_spec["template"]["spec"]["containers"][0].get("image") is None:
                group_spec["template"]["spec"]["containers"][0]["image"] = image

            if group_spec.get("metadata") is None:
                group_spec["metadata"] = {"labels": labels or {}}
            else:
                if group_spec["metadata"].get("labels") is None:
                    group_spec["metadata"]["labels"] = labels or {}
                else:
                    group_spec["metadata"]["labels"].update(labels or {})

        update_group_spec(head_group_spec)
        for worker_group_spec in worker_group_specs:
            update_group_spec(worker_group_spec)

        return {
            "apiVersion": "ray.io/v1alpha1",
            "kind": "RayCluster",
            "metadata": {
                "name": self.cluster_name,
                "labels": labels or {},
            },
            "spec": {
                "enableInTreeAutoscaling": self.ray_cluster.enable_in_tree_autoscaling,
                "autoscalerOptions": self.ray_cluster.autoscaler_options,
                "headGroupSpec": head_group_spec,
                "workerGroupSpecs": worker_group_specs,
            },
        }

    def _wait_raycluster_ready(self):
        if not self.kuberay_api.wait_until_ray_cluster_running(self.cluster_name, k8s_namespace=self.namespace):
            status = self.kuberay_api.get_ray_cluster_status(self.cluster_name, k8s_namespace=self.namespace)
            raise Exception(f"RayCluster {self.namespace}/{self.cluster_name} failed to start: {status}")

        # the above code only checks for RayCluster creation
        # not for head pod readiness

        w = watch.Watch()
        for event in w.stream(
            func=self._k8s_core_api.list_namespaced_pod,
            namespace=self.namespace,
            label_selector=f"ray.io/cluster={self.cluster_name},ray.io/group=headgroup",
            timeout_seconds=60,
        ):
            if event["object"].status.phase == "Running":  # type: ignore
                w.stop()
                return
            # event.type: ADDED, MODIFIED, DELETED
            if event["type"] == "DELETED":  # type: ignore
                # Pod was deleted while we were waiting for it to start.
                w.stop()
                return

    def _maybe_cleanup_raycluster(self, context: InitResourceContext):
        assert context.log is not None

        if (
            not self.disable_cluster_cleanup
            and cast(DagsterRun, context.dagster_run).status != DagsterRunStatus.FAILURE
        ):
            self.kuberay_api.delete_ray_cluster(self.cluster_name, k8s_namespace=self.namespace)
            context.log.info(f"Deleted RayCluster {self.namespace}/{self.cluster_name}")
        else:
            context.log.warning(
                f"Skipping RayCluster {self.cluster_name} deletion because `disable_cluster_cleanup` "
                f"config parameter is set to `True` or the run failed. "
                f"It may be still be deleted by the automatic cleanup job."
            )

    def _get_ray_cluster_step_name(self, context: InitResourceContext) -> str:
        assert isinstance(context.run_id, str)
        assert context.dagster_run is not None

        # try to make the name as short as possible

        cluster_name_prefix = f"dr-{self.cluster_name_prefix.replace('-', '')[:8]}-{context.run_id[:8]}"

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
    def _load_kubeconfig(kubeconfig_file: Optional[str] = None):
        try:
            config.load_kube_config(config_file=kubeconfig_file)
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
