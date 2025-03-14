from __future__ import annotations

import contextlib
import hashlib
import random
import re
import string
import sys
from collections.abc import Generator
from typing import TYPE_CHECKING, Any, cast

import dagster._check as check
from dagster import ConfigurableResource, InitResourceContext
from dagster._annotations import beta
from pydantic import Field, PrivateAttr

from dagster_ray._base.constants import DEFAULT_DEPLOYMENT_NAME
from dagster_ray.kuberay.client import RayClusterClient

# yes, `python-client` is actually the KubeRay package name
# https://github.com/ray-project/kuberay/issues/2078
from dagster_ray.kuberay.configs import RayClusterConfig
from dagster_ray.kuberay.utils import normalize_k8s_label_values

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from dagster import DagsterRun, DagsterRunStatus
from ray._private.worker import BaseContext as RayBaseContext  # noqa

from dagster_ray._base.resources import BaseRayResource
from dagster_ray.kuberay.client.base import load_kubeconfig

if TYPE_CHECKING:
    import kubernetes


@beta
class RayClusterClientResource(ConfigurableResource):
    kube_context: str | None = None
    kubeconfig_file: str | None = None

    _raycluster_client: RayClusterClient = PrivateAttr()
    _k8s_api: kubernetes.client.CustomObjectsApi = PrivateAttr()
    _k8s_core_api: kubernetes.client.CoreV1Api = PrivateAttr()

    @property
    def client(self) -> RayClusterClient:
        if self._raycluster_client is None:
            raise ValueError(f"{self.__class__.__name__} not initialized")
        return self._raycluster_client

    @property
    def k8s(self) -> kubernetes.client.CustomObjectsApi:
        if self._k8s_api is None:
            raise ValueError(f"{self.__class__.__name__} not initialized")
        return self._k8s_api

    @property
    def k8s_core(self) -> kubernetes.client.CoreV1Api:
        if self._k8s_core_api is None:
            raise ValueError(f"{self.__class__.__name__} not initialized")
        return self._k8s_core_api

    def setup_for_execution(self, context: InitResourceContext) -> None:
        import kubernetes

        load_kubeconfig(context=self.kube_context, config_file=self.kubeconfig_file)

        self._raycluster_client = RayClusterClient(context=self.kube_context, config_file=self.kubeconfig_file)
        self._k8s_api = kubernetes.client.CustomObjectsApi()
        self._k8s_core_api = kubernetes.client.CoreV1Api()


@beta
class KubeRayCluster(BaseRayResource):
    """
    Provides a RayCluster for the current step selection
    The cluster is automatically deleted after steps execution
    """

    deployment_name: str = Field(
        default=DEFAULT_DEPLOYMENT_NAME,
        description="Prefix for the RayCluster name. Dagster Cloud variables are used to determine the default value.",
    )
    ray_cluster: RayClusterConfig = Field(default_factory=RayClusterConfig)
    skip_cleanup: bool = False
    skip_init: bool = False
    timeout: int = Field(default=600, description="Timeout in seconds for the RayCluster to become ready")

    client: RayClusterClientResource = Field(default_factory=RayClusterClientResource)

    _cluster_name: str = PrivateAttr()
    _host: str = PrivateAttr()

    @property
    def host(self) -> str:
        if self._host is None:
            raise ValueError(f"{self.__class__.__name__} not initialized")
        return self._host

    @property
    def namespace(self) -> str:
        return self.ray_cluster.namespace  # TODO: add namespace auto-creation logic

    @property
    def cluster_name(self) -> str:
        if self._cluster_name is None:
            raise ValueError(f"{self.__class__.__name__}not initialized")
        return self._cluster_name

    def get_dagster_tags(self, context: InitResourceContext) -> dict[str, str]:
        tags = super().get_dagster_tags(context=context)
        tags.update({"dagster.io/cluster": self.cluster_name, "dagster.io/deployment": self.deployment_name})
        return tags

    @contextlib.contextmanager
    def yield_for_execution(self, context: InitResourceContext) -> Generator[Self, None, None]:
        assert context.log is not None
        assert context.dagster_run is not None

        self.client.setup_for_execution(context)

        self._cluster_name = self._get_ray_cluster_step_name(context)

        # self._host = f"{self.cluster_name}-head-svc.{self.namespace}.svc.cluster.local"

        try:
            # just a safety measure, no need to recreate the cluster for step retries or smth
            if not self.client.client.list(
                namespace=self.namespace,
                label_selector=f"dagster.io/cluster={self.cluster_name}",
            )["items"]:
                cluster_body = self._build_raycluster(
                    image=(self.ray_cluster.image or context.dagster_run.tags["dagster/image"]),
                    labels=normalize_k8s_label_values(self.get_dagster_tags(context)),
                )

                resource = self.client.client.create(body=cluster_body, namespace=self.namespace)
                if not resource:
                    raise RuntimeError(f"Couldn't create RayCluster {self.namespace}/{self.cluster_name}")

                context.log.info(
                    f"Created RayCluster {self.namespace}/{self.cluster_name}. Waiting for it to become ready..."
                )

                self._wait_raycluster_ready()

                self._host = self.client.client.get_status(name=self.cluster_name, namespace=self.namespace)[  # type: ignore
                    "head"
                ]["serviceIP"]

                context.log.info("RayCluster is ready! Connection command:")

                context.log.info(
                    f"`kubectl -n {self.namespace} port-forward svc/{self.cluster_name}-head-svc 8265:8265 6379:6379 10001:10001`"
                )

            context.log.debug(f"Ray host: {self.host}")

            if not self.skip_init:
                self.init_ray(context)
            else:
                self._context = None

            yield self
        except Exception as e:
            context.log.critical(f"Couldn't create or connect to RayCluster {self.namespace}/{self.cluster_name}!")
            self._maybe_cleanup_raycluster(context)
            raise e

        self._maybe_cleanup_raycluster(context)
        if self._context is not None:
            self._context.disconnect()

    def _build_raycluster(
        self,
        image: str,
        labels: dict[str, str] | None = None,  # TODO: use in RayCluster labels
    ) -> dict[str, Any]:
        """
        Builds a RayCluster from the provided configuration, while injecting custom image and labels (only known during resource setup)
        """
        # TODO: inject self.redis_port and self.dashboard_port into the RayCluster configuration
        # TODO: autoa-apply some tags from dagster-k8s/config

        labels = labels or {}
        assert isinstance(labels, dict)

        image = self.ray_cluster.image or image
        head_group_spec = self.ray_cluster.head_group_spec.copy()
        worker_group_specs = self.ray_cluster.worker_group_specs.copy()

        def update_group_spec(group_spec: dict[str, Any]):
            # TODO: only inject if the container has a `dagster.io/inject-image` annotation or smth
            if group_spec["template"]["spec"]["containers"][0].get("image") is None:
                group_spec["template"]["spec"]["containers"][0]["image"] = image

            if group_spec.get("metadata") is None:
                group_spec["metadata"] = {"labels": labels}
            else:
                if group_spec["metadata"].get("labels") is None:
                    group_spec["metadata"]["labels"] = labels
                else:
                    group_spec["metadata"]["labels"].update(labels)

        update_group_spec(head_group_spec)
        for worker_group_spec in worker_group_specs:
            update_group_spec(worker_group_spec)

        return {
            "apiVersion": "ray.io/v1",
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
        import kubernetes

        self.client.client.wait_until_ready(self.cluster_name, namespace=self.namespace, timeout=self.timeout)

        # the above code only checks for RayCluster creation
        # not for head pod readiness

        w = kubernetes.watch.Watch()
        for event in w.stream(
            func=self.client.k8s_core.list_namespaced_pod,
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

        if not self.skip_cleanup and cast(DagsterRun, context.dagster_run).status != DagsterRunStatus.FAILURE:
            self.client.client.delete(self.cluster_name, namespace=self.namespace)
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
        cluster_name_prefix = f"dg-{self.deployment_name.replace('-', '')[:8]}-{context.run_id[:8]}"

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


def get_k8s_object_name(run_id: str, step_key: str | None = None):
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
