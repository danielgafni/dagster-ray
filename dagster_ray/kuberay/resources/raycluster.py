from __future__ import annotations

import contextlib
import sys
from collections.abc import Generator
from typing import TYPE_CHECKING, cast

from dagster import ConfigurableResource, InitResourceContext
from dagster._annotations import beta
from pydantic import Field, PrivateAttr
from typing_extensions import override

from dagster_ray.kuberay.client import RayClusterClient
from dagster_ray.kuberay.configs import RayClusterConfig
from dagster_ray.kuberay.resources.base import BaseKubeRayResourceConfig
from dagster_ray.kuberay.utils import normalize_k8s_label_values

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from dagster import DagsterRun, DagsterRunStatus
from ray._private.worker import BaseContext as RayBaseContext  # noqa

from dagster_ray._base.resources import BaseRayResource, OpOrAssetExecutionContext
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
    Provides a RayCluster for the current step selection
    The cluster is automatically deleted after steps execution
    """

    skip_cleanup: bool = Field(default=False, description="Skip `RayCluster` deletion after step execution")
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

    _cluster_name: str = PrivateAttr()
    _host: str = PrivateAttr()

    @property
    def host(self) -> str:
        if not hasattr(self, "_host"):
            raise ValueError(f"{self.__class__.__name__} not initialized")
        return self._host

    @property
    def cluster_name(self) -> str:
        if not hasattr(self, "_cluster_name") and self.ray_cluster.metadata.get("name") is None:
            raise ValueError(f"{self.__class__.__name__} not initialized")
        elif (name := self.ray_cluster.metadata.get("name")) is not None:
            return name
        else:
            return self._cluster_name

    @property
    def namespace(self) -> str:
        return self.ray_cluster.namespace

    def get_dagster_tags(self, context: InitResourceContext | OpOrAssetExecutionContext) -> dict[str, str]:
        tags = super().get_dagster_tags(context=context)
        tags.update({"dagster/deployment": self.deployment_name})
        return tags

    @contextlib.contextmanager
    def yield_for_execution(self, context: InitResourceContext) -> Generator[Self, None, None]:
        assert context.log is not None
        assert context.dagster_run is not None

        if self.lifecycle.create:
            self.create(context)
            if self.lifecycle.wait:
                self.wait(context)
                if self.lifecycle.connect:
                    self.connect(context)

        yield self

        self._maybe_cleanup_raycluster(context)
        if self._context is not None:
            self._context.disconnect()

    @override
    def create(self, context: InitResourceContext | OpOrAssetExecutionContext):
        assert context.log is not None
        assert context.dagster_run is not None

        self._cluster_name = self.ray_cluster.metadata.get("name") or self._get_step_name(context)

        try:
            # just a safety measure, no need to recreate the cluster for step retries or smth
            if not self.client.client.get(
                name=self.cluster_name,
                namespace=self.namespace,
            ):
                k8s_manifest = self.ray_cluster.to_k8s(
                    context,
                    image=(self.image or context.dagster_run.tags.get("dagster/image")),
                    labels=normalize_k8s_label_values(self.get_dagster_tags(context)),
                    env_vars=self.get_env_vars_to_inject(),
                )

                k8s_manifest["metadata"]["name"] = self.cluster_name

                resource = self.client.client.create(body=k8s_manifest, namespace=self.namespace)
                if not resource:
                    raise RuntimeError(f"Couldn't create RayCluster {self.namespace}/{self.cluster_name}")

                context.log.info(
                    f"Created RayCluster {self.namespace}/{self.cluster_name}. Waiting for it to become ready (timeout={self.timeout:.0f}s)..."
                )
        except BaseException as e:
            context.log.critical(f"Couldn't create RayCluster {self.namespace}/{self.cluster_name}!")
            self._maybe_cleanup_raycluster(context)
            raise e

    def wait(self, context: InitResourceContext | OpOrAssetExecutionContext):
        assert context.log is not None
        assert context.dagster_run is not None

        try:
            self._wait_raycluster_ready()

            self._host = self.client.client.get_status(name=self.cluster_name, namespace=self.namespace)[  # pyright: ignore
                "head"
            ]["serviceIP"]

            msg = f"RayCluster {self.namespace}/{self.cluster_name} is ready! Connection command:\n"
            msg += f"kubectl -n {self.namespace} port-forward svc/{self.cluster_name}-head-svc 8265:8265 6379:6379 10001:10001"

            context.log.info(msg)

        except BaseException as e:
            context.log.critical(f"Couldn't connect to RayCluster {self.namespace}/{self.cluster_name}!")
            self._maybe_cleanup_raycluster(context)
            raise e

    def _wait_raycluster_ready(self):
        self.client.client.wait_until_ready(
            self.cluster_name,
            namespace=self.namespace,
            timeout=self.timeout,
            log_cluster_conditions=self.log_cluster_conditions,
        )

    def _maybe_cleanup_raycluster(self, context: InitResourceContext | OpOrAssetExecutionContext):
        assert context.log is not None

        if (
            not self.skip_cleanup
            and hasattr(self, "_cluster_name")
            and cast(DagsterRun, context.dagster_run).status != DagsterRunStatus.FAILURE
        ):
            self.client.client.delete(self.cluster_name, namespace=self.namespace)
            context.log.info(f"Deleted RayCluster {self.namespace}/{self.cluster_name}")
        else:
            context.log.warning(
                f"Skipping RayCluster {self.cluster_name} deletion because `lifecycle.create` config parameter is set to `False` and the run has failed."
            )
