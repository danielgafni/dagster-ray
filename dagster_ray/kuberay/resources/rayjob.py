import contextlib
from collections.abc import Generator
from typing import TYPE_CHECKING, Literal

import dagster as dg
from dagster import ConfigurableResource, InitResourceContext
from dagster._annotations import beta
from pydantic import Field, PrivateAttr
from typing_extensions import Self, override

from dagster_ray._base.resources import BaseRayResource, OpOrAssetExecutionContext
from dagster_ray.kuberay.client import RayJobClient
from dagster_ray.kuberay.client.base import load_kubeconfig
from dagster_ray.kuberay.configs import RayJobConfig, RayJobSpec
from dagster_ray.kuberay.resources.base import BaseKubeRayResourceConfig
from dagster_ray.kuberay.utils import normalize_k8s_label_values

if TYPE_CHECKING:
    from ray._private.worker import BaseContext as RayBaseContext  # noqa

if TYPE_CHECKING:
    pass


@beta
class KubeRayJobClientResource(ConfigurableResource[RayJobClient]):
    kube_context: "str | None" = None
    kubeconfig_file: "str | None" = None

    @contextlib.contextmanager
    def yield_for_execution(self, context: InitResourceContext) -> Generator[RayJobClient, None, None]:
        load_kubeconfig(context=self.kube_context, config_file=self.kubeconfig_file)

        yield RayJobClient(context=self.kube_context, config_file=self.kubeconfig_file)


class InteractiveRayJobSpec(RayJobSpec):
    submission_mode: Literal["InteractiveMode"] = "InteractiveMode"  # pyright: ignore[reportIncompatibleVariableOverride]


class InteractiveRayJobConfig(RayJobConfig):
    spec: InteractiveRayJobSpec = Field(default_factory=InteractiveRayJobSpec)  # pyright: ignore[reportIncompatibleVariableOverride]


@beta
class KubeRayInteractiveJob(BaseKubeRayResourceConfig, BaseRayResource):
    """
    Provides a `RayJob` for the current step selection
    The cluster is automatically deleted after steps execution
    """

    ray_job: InteractiveRayJobConfig = Field(
        default_factory=InteractiveRayJobConfig, description="Configuration for the Kubernetes `RayJob` CR"
    )
    client: dg.ResourceDependency[RayJobClient] = Field(
        # default_factory=KubeRayJobClientResource,
        description="Kubernetes `RayJob` client"
    )

    log_cluster_conditions: bool = Field(
        default=True,
        description="Whether to log `RayCluster` conditions while waiting for the RayCluster to become ready. For more information, see https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/observability.html#raycluster-status-conditions.",
    )

    _job_name: str = PrivateAttr()
    _cluster_name: str = PrivateAttr()
    _host: str = PrivateAttr()

    @property
    @override
    def host(self) -> str:
        if not hasattr(self, "_host"):
            raise ValueError(f"{self.__class__.__name__} not initialized")
        return self._host

    @property
    def job_name(self) -> str:
        if not hasattr(self, "_job_name"):
            raise ValueError(f"{self.__class__.__name__} not initialized")
        elif (name := self.ray_job.metadata.get("name")) is not None:
            return name
        else:
            return self._job_name

    @property
    @override
    def cluster_name(self) -> str:
        if not hasattr(self, "_cluster_name"):
            raise ValueError(f"{self.__class__.__name__} not initialized")
        else:
            return self._cluster_name

    @property
    @override
    def namespace(self) -> str:
        return self.ray_job.namespace

    @override
    def get_dagster_tags(self, context: "InitResourceContext | OpOrAssetExecutionContext") -> dict[str, str]:
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

        if self._context is not None:
            self._context.disconnect()

    @override
    def create(self, context: "InitResourceContext | OpOrAssetExecutionContext"):
        assert context.log is not None
        assert context.dagster_run is not None

        self._job_name = self.ray_job.metadata.get("name") or self._get_step_name(context)

        try:
            k8s_manifest = self.ray_job.to_k8s(
                context,
                image=(self.image or context.dagster_run.tags.get("dagster/image")),
                labels=normalize_k8s_label_values(self.get_dagster_tags(context)),
            )

            k8s_manifest["metadata"]["name"] = self.job_name

            if not self.client.get(
                name=self.job_name,
                namespace=self.namespace,
            ):
                resource = self.client.create(body=k8s_manifest, namespace=self.namespace)

                if not resource:
                    raise RuntimeError(f"Couldn't create RayJob {self.namespace}/{self.cluster_name}")

            context.log.info(
                f"Created RayJob {self.namespace}/{self.job_name}. Waiting for it to become ready (timeout={self.timeout:.0f}s)..."
            )
        except BaseException as e:
            context.log.critical(f"Couldn't create RayJob {self.namespace}/{self.job_name}!")
            raise e

    @override
    def wait(self, context: "OpOrAssetExecutionContext | InitResourceContext"):
        assert context.log is not None
        assert context.dagster_run is not None

        try:
            self.client.wait_until_ready(
                name=self.job_name, namespace=self.namespace, log_cluster_conditions=self.log_cluster_conditions
            )
            self._cluster_name = self.client.get_ray_cluster_name(self.job_name, self.namespace)
            self._host = self.client.ray_cluster_client.get_status(name=self.cluster_name, namespace=self.namespace)[  # pyright: ignore
                "head"
            ]["serviceIP"]

            msg = f"RayJob {self.namespace}/{self.job_name} has created a RayCluster {self.namespace}/{self.cluster_name}! Connection command:\n"
            msg += f"kubectl -n {self.namespace} port-forward svc/{self.cluster_name}-head-svc 8265:8265 6379:6379 10001:10001"

            context.log.info(msg)

        except BaseException as e:
            context.log.critical(
                f"Couldn't connect to RayCluster {self.namespace}/{self.cluster_name} associated with RayJob {self.namespace}/{self.job_name}!"
            )
            raise e

    @override
    def connect(self, context: "OpOrAssetExecutionContext | InitResourceContext") -> "RayBaseContext":
        """Connect to Ray and place a `ray.io/job-submission-id` annotation on the `RayJob` to bind the client session to the `RayJob` CR. Requires KubeRay 1.3.0.

        This procedure is described in https://github.com/ray-project/kuberay/pull/2364
        """
        ray_context = super().connect(context)

        # now get the job id and annotate it
        self.client.update(
            name=self.job_name,
            namespace=self.namespace,
            body={"metadata": {"annotations": {"ray.io/job-submission-id": self.runtime_job_id}}},
        )

        return ray_context
