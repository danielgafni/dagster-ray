from __future__ import annotations

import contextlib
import re
import sys
from collections.abc import Generator
from typing import TYPE_CHECKING, Any, cast

from dagster import ConfigurableResource, InitResourceContext
from dagster._annotations import beta
from pydantic import Field, PrivateAttr

from dagster_ray._base.constants import DEFAULT_DEPLOYMENT_NAME
from dagster_ray.kuberay.client import RayClusterClient
from dagster_ray.kuberay.configs import RayClusterConfig
from dagster_ray.kuberay.utils import get_k8s_object_name, normalize_k8s_label_values, remove_none_from_dict

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

    image: str | None = Field(
        default=None,
        description="Image to inject into the `RayCluster` spec. Defaults to `dagster/image` run tag. Images already provided in the `RayCluster` spec won't be overridden.",
    )
    skip_cleanup: bool = Field(default=False, description="Skip `RayCluster` deletion after step execution")
    skip_setup: bool = Field(
        default=False,
        description="Skip `RayCluster` creation before step execution. If this is set to True, a manual call to .create_and_wait is required.",
    )
    skip_init: bool = Field(default=False, description="Do not run `ray.init` automatically")
    timeout: int = Field(default=600, description="Timeout in seconds for the `RayCluster` to become ready")
    ray_cluster: RayClusterConfig = Field(
        default_factory=RayClusterConfig, description="Kubernetes configuration for the `RayCluster`"
    )
    client: RayClusterClientResource = Field(
        default_factory=RayClusterClientResource, description="Kubernetes `RayCluster` client"
    )

    deployment_name: str = Field(
        default=DEFAULT_DEPLOYMENT_NAME,
        description="Prefix for the RayCluster name. Dagster Cloud variables are used to determine the default value.",
    )
    env_vars: list[Any] = Field(
        default_factory=list,
        description="Environment variables to inject into all RayCluster containers in Kubernetes format.",
    )
    ray_cluster: RayClusterConfig = Field(default_factory=RayClusterConfig)
    log_cluster_conditions: bool = Field(
        default=True,
        description="Whether to log RayCluster conditions while waiting for the RayCluster to become ready. For more information, see https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/observability.html#raycluster-status-conditions.",
    )

    _cluster_name: str = PrivateAttr()
    _host: str = PrivateAttr()

    @property
    def host(self) -> str:
        if self._host is None:
            raise ValueError(f"{self.__class__.__name__} not initialized")
        return self._host

    @property
    def cluster_name(self) -> str:
        if self._cluster_name is None and self.ray_cluster.metadata.get("name") is None:
            raise ValueError(f"{self.__class__.__name__}not initialized")
        elif (name := self.ray_cluster.metadata.get("name")) is not None:
            return name
        else:
            return self._cluster_name

    @property
    def namespace(self) -> str:
        return self.ray_cluster.namespace

    def get_dagster_tags(self, context: InitResourceContext | OpOrAssetExecutionContext) -> dict[str, str]:
        tags = super().get_dagster_tags(context=context)
        tags.update({"dagster.io/cluster": self.cluster_name, "dagster.io/deployment": self.deployment_name})
        return tags

    def _get_step_name(self, context: InitResourceContext | OpOrAssetExecutionContext) -> str:
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

    @contextlib.contextmanager
    def yield_for_execution(self, context: InitResourceContext) -> Generator[Self, None, None]:
        assert context.log is not None
        assert context.dagster_run is not None

        if not self.skip_setup:
            self.create_and_wait(context)

        yield self

        self._maybe_cleanup_raycluster(context)
        if self._context is not None:
            self._context.disconnect()

    def create_and_wait(self, context: InitResourceContext | OpOrAssetExecutionContext):
        assert context.log is not None
        assert context.dagster_run is not None

        self._cluster_name = self.ray_cluster.metadata.get("name") or self._get_step_name(context)

        try:
            # just a safety measure, no need to recreate the cluster for step retries or smth
            if not self.client.client.list(
                namespace=self.namespace,
                label_selector=f"dagster.io/cluster={self.cluster_name}",
            )["items"]:
                cluster_body = self._build_raycluster(
                    context,
                    image=(self.image or context.dagster_run.tags["dagster/image"]),
                    labels=normalize_k8s_label_values(self.get_dagster_tags(context)),
                )

                resource = self.client.client.create(body=cluster_body, namespace=self.namespace)
                if not resource:
                    raise RuntimeError(f"Couldn't create RayCluster {self.namespace}/{self.cluster_name}")

                context.log.info(
                    f"Created RayCluster {self.namespace}/{self.cluster_name}. Waiting for it to become ready (timeout={self.timeout:.0f}s)..."
                )

                self._wait_raycluster_ready()

                self._host = self.client.client.get_status(name=self.cluster_name, namespace=self.namespace)[  # pyright: ignore
                    "head"
                ]["serviceIP"]

                msg = f"RayCluster {self.namespace}/{self.cluster_name} is ready! Connection command:\n"
                msg += f"kubectl -n {self.namespace} port-forward svc/{self.cluster_name}-head-svc 8265:8265 6379:6379 10001:10001"

                context.log.info(msg)

            if not self.skip_init:
                self.init_ray(context)
            else:
                self._context = None

        except BaseException as e:
            context.log.critical(f"Couldn't create or connect to RayCluster {self.namespace}/{self.cluster_name}!")
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

        if not self.skip_cleanup and cast(DagsterRun, context.dagster_run).status != DagsterRunStatus.FAILURE:
            self.client.client.delete(self.cluster_name, namespace=self.namespace)
            context.log.info(f"Deleted RayCluster {self.namespace}/{self.cluster_name}")
        else:
            context.log.warning(
                f"Skipping RayCluster {self.cluster_name} deletion because `disable_cluster_cleanup` "
                f"config parameter is set to `True` and the run has failed."
            )

    def _build_raycluster(
        self,
        context: InitResourceContext | OpOrAssetExecutionContext,
        image: str | None = None,  # is injected into headgroup and workergroups, unless already specified there
        labels: dict[str, str] | None = None,  # TODO: use in RayCluster labels
    ) -> dict[str, Any]:
        assert context.log is not None
        """
        Builds a RayCluster from the provided configuration, while injecting custom image and labels (only known during resource setup)
        """

        # TODO: inject self.redis_port and self.dashboard_port into the RayCluster configuration
        # TODO: auto-apply some tags from dagster-k8s/config

        labels = labels or {}
        assert isinstance(labels, dict)

        head_group_spec = self.ray_cluster.spec.head_group_spec.copy()
        worker_group_specs = self.ray_cluster.spec.worker_group_specs.copy()

        env_vars = self.env_vars.copy()

        env_vars_to_inject = self.get_env_vars_to_inject()

        if env_vars_to_inject:
            context.log.info("Injecting debugging environment variables into the RayCluster configuration")
            for key, value in self.get_env_vars_to_inject().items():
                env_vars.append({"name": key, "value": value})

        def update_group_spec(group_spec: dict[str, Any]):
            # TODO: only inject if the container has a `dagster.io/inject-image` annotation or smth
            if group_spec["template"]["spec"]["containers"][0].get("image") is None:
                group_spec["template"]["spec"]["containers"][0]["image"] = image

            for container in group_spec["template"]["spec"]["containers"]:
                container["env"] = container.get("env", []) + env_vars

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
            "apiVersion": self.ray_cluster.api_version,
            "kind": self.ray_cluster.kind,
            "metadata": {
                "name": self.cluster_name,
                "labels": {**(self.ray_cluster.metadata.get("labels", {}) or {}), **labels},
                "annotations": self.ray_cluster.metadata.get("annotations", {}),
            },
            "spec": remove_none_from_dict(
                {
                    "enableInTreeAutoscaling": self.ray_cluster.spec.enable_in_tree_autoscaling,
                    "autoscalerOptions": self.ray_cluster.spec.autoscaler_options,
                    "headGroupSpec": head_group_spec,
                    "workerGroupSpecs": worker_group_specs,
                    "suspend": self.ray_cluster.spec.suspend,
                    "managedBy": self.ray_cluster.spec.managed_by,
                    "headServiceAnnotations": self.ray_cluster.spec.head_service_annotations,
                    "gcsFaultToleranceOptions": self.ray_cluster.spec.gcs_fault_tolerance_options,
                    "rayVersion": self.ray_cluster.spec.ray_version,
                }
            ),
        }
