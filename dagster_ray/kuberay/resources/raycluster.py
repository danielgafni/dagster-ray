from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from dagster import ConfigurableResource, InitResourceContext
from dagster._annotations import beta
from pydantic import Field, PrivateAttr
from typing_extensions import override

from dagster_ray.kuberay.client import RayClusterClient
from dagster_ray.kuberay.configs import RayClusterConfig
from dagster_ray.kuberay.resources.base import BaseKubeRayResourceConfig
from dagster_ray.kuberay.utils import normalize_k8s_label_values
from dagster_ray.types import AnyDagsterContext

if sys.version_info >= (3, 11):
    pass
else:
    pass

from ray._private.worker import BaseContext as RayBaseContext  # noqa

from dagster_ray._base.resources import BaseRayResource, Lifecycle, OpOrAssetExecutionContext
from dagster_ray.kuberay.client.base import load_kubeconfig

if TYPE_CHECKING:
    import kubernetes


@beta
class RayClusterClientResource(ConfigurableResource[RayClusterClient]):
    kube_context: str | None = None
    kubeconfig_file: str | None = None

    _raycluster_client: RayClusterClient = PrivateAttr()
    _k8s_api: kubernetes.client.CustomObjectsApi = PrivateAttr()
    _k8s_core_api: kubernetes.client.CoreV1Api = PrivateAttr()

    @property
    def client(self) -> RayClusterClient:
        if not hasattr(self, "_raycluster_client"):
            raise ValueError(f"{self.__class__.__name__} not initialized")
        return self._raycluster_client

    @property
    def k8s(self) -> kubernetes.client.CustomObjectsApi:
        if not hasattr(self, "_k8s_api"):
            raise ValueError(f"{self.__class__.__name__} not initialized")
        return self._k8s_api

    @property
    def k8s_core(self) -> kubernetes.client.CoreV1Api:
        if not hasattr(self, "_k8s_core_api"):
            raise ValueError(f"{self.__class__.__name__} not initialized")
        return self._k8s_core_api

    def setup_for_execution(self, context: InitResourceContext) -> None:
        import kubernetes

        load_kubeconfig(context=self.kube_context, config_file=self.kubeconfig_file)

        self._raycluster_client = RayClusterClient(context=self.kube_context, config_file=self.kubeconfig_file)
        self._k8s_api = kubernetes.client.CustomObjectsApi()
        self._k8s_core_api = kubernetes.client.CoreV1Api()


@beta
class KubeRayCluster(BaseKubeRayResourceConfig, BaseRayResource):
    """
    Provides a `RayCluster` for Dagster steps.
    """

    lifecycle: Lifecycle = Field(
        default_factory=lambda: Lifecycle(cleanup="always"), description="Actions to perform during resource setup."
    )

    ray_cluster: RayClusterConfig = Field(
        default_factory=RayClusterConfig, description="Kubernetes `RayCluster` CR configuration."
    )
    client: RayClusterClientResource = Field(
        default_factory=RayClusterClientResource, description="Kubernetes `RayCluster` client"
    )
    log_cluster_conditions: bool = Field(
        default=True,
        description="Whether to log RayCluster conditions while waiting for the RayCluster to become ready. For more information, see https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/observability.html#raycluster-status-conditions.",
    )

    _name: str = PrivateAttr()
    _host: str = PrivateAttr()

    @property
    def host(self) -> str:
        if not hasattr(self, "_host"):
            raise ValueError(f"{self.__class__.__name__} not initialized")
        return self._host

    @property
    def name(self) -> str:
        if not hasattr(self, "_name") and self.ray_cluster.metadata.get("name") is None:
            raise ValueError(f"{self.__class__.__name__} not initialized")
        elif (name := self.ray_cluster.metadata.get("name")) is not None:
            return name
        else:
            return self._name

    @property
    def namespace(self) -> str:
        return self.ray_cluster.namespace

    @property
    @override
    def display_name(self) -> str:
        return (
            f"RayCluster {self.namespace}/{self.name}" if self.created else f"RayCluster in namespace {self.namespace}"
        )

    def get_dagster_tags(self, context: InitResourceContext | OpOrAssetExecutionContext) -> dict[str, str]:
        tags = super().get_dagster_tags(context=context)
        tags.update({"dagster/deployment": self.deployment_name})
        return tags

    @override
    def create(self, context: InitResourceContext | OpOrAssetExecutionContext):
        assert context.log is not None
        assert context.dagster_run is not None

        self._name = self.ray_cluster.metadata.get("name") or self._get_step_name(context)

        # just a safety measure, no need to recreate the cluster for step retries or smth
        if not self.client.client.get(
            name=self.name,
            namespace=self.namespace,
        ):
            k8s_manifest = self.ray_cluster.to_k8s(
                context,
                image=(self.image or context.dagster_run.tags.get("dagster/image")),
                labels=normalize_k8s_label_values(self.get_dagster_tags(context)),
                env_vars=self.get_env_vars_to_inject(),
            )

            k8s_manifest["metadata"]["name"] = self.name

            resource = self.client.client.create(body=k8s_manifest, namespace=self.namespace)
            if not resource:
                raise RuntimeError(f"Couldn't create {self.display_name}")

    @override
    def wait(self, context: InitResourceContext | OpOrAssetExecutionContext):
        assert context.log is not None
        assert context.dagster_run is not None

        self.client.client.wait_until_ready(
            self.name,
            namespace=self.namespace,
            timeout=self.timeout,
            poll_interval=self.poll_interval,
            log_cluster_conditions=self.log_cluster_conditions,
        )

        self._host = self.client.client.get_status(
            name=self.name, namespace=self.namespace, timeout=self.timeout, poll_interval=self.poll_interval
        )[  # pyright: ignore
            "head"
        ]["serviceIP"]

        msg = f"RayCluster {self.namespace}/{self.name} is ready! Connection command:\n"
        msg += f"kubectl -n {self.namespace} port-forward svc/{self.name}-head-svc 8265:8265 6379:6379 10001:10001"

        context.log.info(msg)

    @override
    def delete(self, context: AnyDagsterContext):
        self.client.client.delete(self.name, namespace=self.namespace)
