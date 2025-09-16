from typing import TYPE_CHECKING, Literal, Optional

import dagster as dg
from dagster import ConfigurableResource
from dagster._annotations import beta
from pydantic import Field, PrivateAttr
from typing_extensions import override

from dagster_ray._base.resources import BaseRayResource, Lifecycle
from dagster_ray.kuberay.client import RayJobClient
from dagster_ray.kuberay.client.base import load_kubeconfig
from dagster_ray.kuberay.configs import RayJobConfig, RayJobSpec
from dagster_ray.kuberay.resources.base import BaseKubeRayResourceConfig
from dagster_ray.kuberay.utils import normalize_k8s_label_values
from dagster_ray.types import AnyDagsterContext

if TYPE_CHECKING:
    from ray._private.worker import BaseContext as RayBaseContext  # noqa

if TYPE_CHECKING:
    pass


@beta
class KubeRayJobClientResource(ConfigurableResource[RayJobClient]):
    kube_context: Optional[str] = None
    kubeconfig_file: Optional[str] = None

    def create_resource(self, context: dg.InitResourceContext):
        load_kubeconfig(context=self.kube_context, config_file=self.kubeconfig_file)
        return RayJobClient(context=self.kube_context, config_file=self.kubeconfig_file)


class InteractiveRayJobSpec(RayJobSpec):
    submission_mode: Literal["InteractiveMode"] = "InteractiveMode"  # pyright: ignore[reportIncompatibleVariableOverride]


class InteractiveRayJobConfig(RayJobConfig):
    spec: InteractiveRayJobSpec = Field(default_factory=InteractiveRayJobSpec)  # pyright: ignore[reportIncompatibleVariableOverride]


@beta
class KubeRayInteractiveJob(BaseRayResource, BaseKubeRayResourceConfig):
    """
    Provides a `RayJob` for Dagster steps.
    """

    lifecycle: Lifecycle = Field(
        default_factory=lambda: Lifecycle(
            cleanup="on_exception"  # RayJob has it's own lifecycle management so it makes sense to only interfere when the step has been cancelled, otherwise it will be left in Waiting state (see https://github.com/ray-project/kuberay/issues/4037)
        ),
        description="Actions to perform during resource setup.",
    )

    ray_job: InteractiveRayJobConfig = Field(
        default_factory=InteractiveRayJobConfig, description="Configuration for the Kubernetes `RayJob` CR"
    )
    client: dg.ResourceDependency[RayJobClient] = Field(  # pyright: ignore[reportAssignmentType]
        default_factory=KubeRayJobClientResource, description="Kubernetes `RayJob` client"
    )

    log_cluster_conditions: bool = Field(
        default=True,
        description="Whether to log `RayCluster` conditions while waiting for the RayCluster to become ready. For more information, see https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/observability.html#raycluster-status-conditions.",
    )

    _name: str = PrivateAttr()
    _cluster_name: str = PrivateAttr()
    _host: str = PrivateAttr()

    @property
    @override
    def host(self) -> str:
        if not hasattr(self, "_host"):
            raise ValueError(f"{self.__class__.__name__} not initialized")
        return self._host

    @property
    def name(self) -> str:
        if not hasattr(self, "_name"):
            raise ValueError(f"{self.__class__.__name__} is not initialized")
        elif (name := self.ray_job.metadata.get("name")) is not None:
            return name
        else:
            return self._name

    @property
    @override
    def namespace(self) -> str:
        return self.ray_job.namespace

    @property
    @override
    def display_name(self) -> str:
        return f"RayJob {self.namespace}/{self.name}" if self.created else f"RayJob in namespace {self.namespace}"

    @property
    def cluster_name(self) -> str:
        if not hasattr(self, "_cluster_name"):
            raise ValueError(f"{self.__class__.__name__} not initialized")
        else:
            return self._cluster_name

    @override
    def get_dagster_tags(self, context: AnyDagsterContext) -> dict[str, str]:
        tags = super().get_dagster_tags(context=context)
        tags.update({"dagster/deployment": self.deployment_name})
        return tags

    @override
    def create(self, context: AnyDagsterContext):
        assert context.log is not None
        assert context.dagster_run is not None

        self._name = self.ray_job.metadata.get("name") or self._get_step_name(context)

        k8s_manifest = self.ray_job.to_k8s(
            context,
            image=(self.image or context.dagster_run.tags.get("dagster/image")),
            labels=normalize_k8s_label_values(self.get_dagster_tags(context)),
        )

        k8s_manifest["metadata"]["name"] = self.name

        if not self.client.get(
            name=self.name,
            namespace=self.namespace,
        ):
            resource = self.client.create(body=k8s_manifest, namespace=self.namespace)

            if not resource:
                raise RuntimeError(f"Couldn't create {self.display_name}")

    @override
    def wait(self, context: AnyDagsterContext):
        assert context.log is not None
        assert context.dagster_run is not None

        self.client.wait_until_ready(
            name=self.name,
            namespace=self.namespace,
            log_cluster_conditions=self.log_cluster_conditions,
            timeout=self.timeout,
            poll_interval=self.poll_interval,
        )
        self._cluster_name = self.client.get_ray_cluster_name(
            self.name, self.namespace, timeout=self.timeout, poll_interval=self.poll_interval
        )
        self._host = self.client.ray_cluster_client.get_status(
            name=self.cluster_name, namespace=self.namespace, timeout=self.timeout, poll_interval=self.poll_interval
        )[  # pyright: ignore
            "head"
        ]["serviceIP"]

        msg = f"RayJob {self.namespace}/{self.name} has created a RayCluster {self.namespace}/{self.cluster_name}! Connection command:\n"
        msg += (
            f"kubectl -n {self.namespace} port-forward svc/{self.cluster_name}-head-svc 8265:8265 6379:6379 10001:10001"
        )

        context.log.info(msg)

    @override
    def connect(self, context: AnyDagsterContext) -> "RayBaseContext":
        """Connect to Ray and set `RayJobSpec.jobId` to bind the client session to the `RayJob` CR. Requires KubeRay 1.3.0.

        This procedure is described in https://github.com/ray-project/kuberay/pull/2364
        """
        ray_context = super().connect(context)

        # now point the RayJob at our ray job
        self.client.update(
            name=self.name,
            namespace=self.namespace,
            body={"spec": {"jobId": self.runtime_job_id}},
        )

        return ray_context

    @override
    def delete(self, context: AnyDagsterContext):
        self.client.delete(self.name, namespace=self.namespace)
