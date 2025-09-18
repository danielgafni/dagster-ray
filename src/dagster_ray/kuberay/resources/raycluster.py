from typing import Optional

import dagster as dg
from pydantic import Field, PrivateAttr
from typing_extensions import override

from dagster_ray._base.resources import BaseRayResource, Lifecycle
from dagster_ray.kuberay.client import RayClusterClient
from dagster_ray.kuberay.client.base import load_kubeconfig
from dagster_ray.kuberay.configs import RayClusterConfig
from dagster_ray.kuberay.resources.base import BaseKubeRayResourceConfig
from dagster_ray.kuberay.utils import normalize_k8s_label_values
from dagster_ray.types import AnyDagsterContext


class KubeRayClusterClientResource(dg.ConfigurableResource[RayClusterClient]):
    """This configurable resource provides a `dagster_ray.kuberay.client.RayClusterClient`."""

    kube_context: Optional[str] = None
    kube_config: Optional[str] = None

    def create_resource(self, context: dg.InitResourceContext) -> RayClusterClient:
        load_kubeconfig(context=self.kube_context, config_file=self.kube_config)

        return RayClusterClient(kube_context=self.kube_context, kube_config=self.kube_config)


class KubeRayCluster(BaseKubeRayResourceConfig, BaseRayResource):
    """
    Provides a `RayCluster` for Dagster steps.

    It is advised to use `dagster_ray.kuberay.KubeRayInteractiveJob` with KubeRay >= 1.3.0 instead.

    Info:
        Image defaults to `dagster/image` run tag.

    Tip:
        Make sure `ray[full]` is available in the image.
    """

    lifecycle: Lifecycle = Field(
        default_factory=lambda: Lifecycle(cleanup="always"), description="Actions to perform during resource setup."
    )

    ray_cluster: RayClusterConfig = Field(
        default_factory=RayClusterConfig, description="Kubernetes `RayCluster` CR configuration."
    )
    client: dg.ResourceDependency[RayClusterClient] = Field(  # pyright: ignore[reportAssignmentType]
        default_factory=KubeRayClusterClientResource, description="Kubernetes `RayCluster` client"
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

    def get_dagster_tags(self, context: AnyDagsterContext) -> dict[str, str]:
        tags = super().get_dagster_tags(context=context)
        tags.update({"dagster/deployment": self.deployment_name})
        return tags

    @override
    def create(self, context: AnyDagsterContext):
        assert context.log is not None
        assert context.dagster_run is not None

        self._name = self.ray_cluster.metadata.get("name") or self._get_step_name(context)

        # just a safety measure, no need to recreate the cluster for step retries or smth
        if not self.client.get(
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

            resource = self.client.create(body=k8s_manifest, namespace=self.namespace)
            if not resource:
                raise RuntimeError(f"Couldn't create {self.display_name}")

    @override
    def wait(self, context: AnyDagsterContext):
        assert context.log is not None
        assert context.dagster_run is not None

        self.client.wait_until_ready(
            self.name,
            namespace=self.namespace,
            timeout=self.timeout,
            poll_interval=self.poll_interval,
            log_cluster_conditions=self.log_cluster_conditions,
        )

        self._host = self.client.get_status(
            name=self.name, namespace=self.namespace, timeout=self.timeout, poll_interval=self.poll_interval
        )[  # pyright: ignore
            "head"
        ]["serviceIP"]

        msg = f"RayCluster {self.namespace}/{self.name} is ready! Connection command:\n"
        msg += f"kubectl -n {self.namespace} port-forward svc/{self.name}-head-svc 8265:8265 6379:6379 10001:10001"

        context.log.info(msg)

    @override
    def delete(self, context: AnyDagsterContext):
        self.client.delete(self.name, namespace=self.namespace)
