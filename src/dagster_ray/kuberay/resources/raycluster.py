from collections.abc import Sequence
from datetime import datetime
from typing import cast

import dagster as dg
from pydantic import Field, PrivateAttr
from typing_extensions import override

from dagster_ray._base.cluster_sharing_lock import ClusterSharingLock
from dagster_ray._base.resources import RayResource
from dagster_ray.configs import Lifecycle
from dagster_ray.kuberay.client import RayClusterClient
from dagster_ray.kuberay.configs import ClusterSharing, RayClusterConfig
from dagster_ray.kuberay.resources.base import BaseKubeRayResourceConfig
from dagster_ray.kuberay.utils import normalize_k8s_label_values
from dagster_ray.types import AnyDagsterContext


class KubeRayClusterClientResource(dg.ConfigurableResource[RayClusterClient]):
    """This configurable resource provides a [dagster_ray.kuberay.client.RayClusterClient][]."""

    kube_context: str | None = None
    kube_config: str | None = None

    def create_resource(self, context: dg.InitResourceContext) -> RayClusterClient:
        client = RayClusterClient(kube_context=self.kube_context, kube_config=self.kube_config)
        return client


class KubeRayCluster(BaseKubeRayResourceConfig, RayResource):
    """
    Provides a Ray Cluster for Dagster steps.

    It is advised to use [`KubeRayInteractiveJob`][dagster_ray.kuberay.resources.KubeRayInteractiveJob] with KubeRay >= 1.3.0 instead.

    Info:
        Image defaults to `dagster/image` run tag.

    Tip:
        Make sure `ray[full]` is available in the image.
    """

    lifecycle: Lifecycle = Field(
        default_factory=lambda: Lifecycle(cleanup="always"), description="Actions to perform during resource setup."
    )

    cluster_sharing: ClusterSharing = Field(
        default_factory=ClusterSharing,
        description="Configuration for sharing the `RayCluster` across Dagster steps. Existing clusters matching this configuration will be reused without recreating them. A `dagster/sharing=true` label will be applied to the `RayCluster`, and a `dagster/lock-<run-id>-<step-id>=<lock>` annotation will be placed on the `RayCluster` to mark it as being used by this step. Cleanup will only proceed if the `RayCluster` is not being used by any other steps, therefore cluster sharing should be used in conjunction with [dagster_ray.kuberay.sensors.cleanup_expired_kuberay_clusters][] sensor.",
    )

    ray_cluster: RayClusterConfig = Field(
        default_factory=RayClusterConfig, description="Kubernetes `RayCluster` CR configuration."
    )
    client: dg.ResourceDependency[RayClusterClient] = Field(  # pyright: ignore[reportAssignmentType]
        default_factory=KubeRayClusterClientResource, description="Kubernetes `RayCluster` client"
    )
    log_cluster_conditions: bool = Field(
        default=True,
        description="Whether to log RayCluster conditions while waiting for the RayCluster to become ready. Learn more: [KubeRay docs](https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/observability.html#raycluster-status-conditions).",
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
        if self.cluster_sharing.enabled:
            tags.update({"dagster/cluster-sharing": "true"})
        return tags

    def get_k8s_labels(self, context: AnyDagsterContext) -> dict[str, str]:
        return normalize_k8s_label_values(self.get_dagster_tags(context))

    def get_image(self, context: AnyDagsterContext) -> str | None:
        assert context.dagster_run is not None
        return self.image or context.dagster_run.tags.get("dagster/image")

    @override
    def create(self, context: AnyDagsterContext):
        assert context.log is not None
        assert context.dagster_run is not None

        labels = self.get_k8s_labels(context)
        annotations: dict[str, str] = {}

        if self.cluster_sharing.enabled:
            label_selector = self.get_sharing_label_selector(context)
            context.log.info(
                f"RayCluster sharing is enabled. Looking for clusters matching label selector: {label_selector}"
            )
            # check whether a cluster matching the sharing config already exists
            matching_clusters = self.client.list(
                label_selector=label_selector,
                namespace=self.namespace,
            )
            if matching_clusters and (clusters := matching_clusters.get("items", [])):
                cluster_name = clusters[0]["metadata"]["name"]
                context.log.info(
                    f"Found {len(matching_clusters)} clusters matching the label selector. Using the first one: {cluster_name}"
                )
                self._name = cluster_name

                # place a lock on the cluster

                self.client.update_json_patch(
                    name=cluster_name,
                    namespace=self.namespace,
                    body=[
                        {
                            "op": "add",
                            "path": "/metadata/annotations",
                            "value": self.get_sharing_lock_annotations(context),
                        }
                    ],
                )

                return
            else:
                context.log.info("No matching clusters found. Creating a new one.")

                # mark the cluster as being used by this step
                annotations.update(self.get_sharing_lock_annotations(context))

        self._name = self.ray_cluster.metadata.get("name") or self._get_step_name(context)

        # just a safety measure, no need to recreate the cluster for step retries or smth
        if not self.client.get(
            name=self.name,
            namespace=self.namespace,
        ):
            k8s_manifest = self.ray_cluster.to_k8s(
                context,
                image=self.get_image(context),
                labels=labels,
                annotations=annotations,
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
            failure_tolerance_timeout=self.failure_tolerance_timeout,
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

    @override
    def cleanup(self, context: AnyDagsterContext, exception: BaseException | None):
        assert context.log is not None
        assert context.run_id is not None
        # we don't want to perform cleanup if:
        # - cluster sharing is enabled
        # - cluster has at least one lock that hasn't expired yet
        if self.cluster_sharing.enabled:
            # get sharing locks created by this or another Dagster step
            alive_locks = self.get_cluster_sharing_alive_locks(context)
            if len(alive_locks) > 0:
                context.log.info(
                    f"Skipping cluster cleanup due to active cluster sharing locks: {', '.join([lock.identifier for lock in alive_locks])}"
                )
                return

        super().cleanup(context, exception)

    def get_sharing_label_selector(self, context: AnyDagsterContext) -> str:
        """This method combines user-provided label selectors from the sharing config with default (dagster-generated) labels to match on.

        User-provided labels take priority. This method can be overridden to customize cluster sharing behavior.
        """
        labels = self.get_k8s_labels(context)

        dagster_match_labels: dict[str, str] = {}

        if self.cluster_sharing:
            if self.cluster_sharing.match_dagster_labels.cluster_sharing:
                dagster_match_labels["dagster/cluster-sharing"] = "true"
            if self.cluster_sharing.match_dagster_labels.code_location and (
                code_location := labels.get("dagster/code-location")
            ):
                dagster_match_labels["dagster/code-location"] = code_location
            if self.cluster_sharing.match_dagster_labels.resource_key:
                dagster_match_labels["dagster/resource-key"] = labels["dagster/resource-key"]
            if self.cluster_sharing.match_dagster_labels.run_id:
                dagster_match_labels["dagster/run-id"] = labels["dagster/run-id"]
            if self.cluster_sharing.match_dagster_labels.commit_sha and (
                commit_sha := labels.get("dagster/commit-sha")
            ):
                dagster_match_labels["dagster/commit-sha"] = commit_sha

        combined_match_labels = {
            **dagster_match_labels,
            **(self.cluster_sharing.match_labels or {}),
        }

        return ",".join([f"{key}={value}" for key, value in combined_match_labels.items()])

    def get_sharing_lock_annotations(self, context: AnyDagsterContext) -> dict[str, str]:
        assert context.run_id is not None
        assert context.dagster_run is not None

        annotations = {}
        if context.dagster_run.step_keys_to_execute:
            for step_key in context.dagster_run.step_keys_to_execute:
                lock = ClusterSharingLock(
                    run_id=context.run_id,
                    step_key=step_key,
                    ttl_seconds=self.cluster_sharing.ttl_seconds,
                    created_at=datetime.now(),
                )
                annotations[f"dagster/lock-{lock.identifier}"] = lock.model_dump_json()
        return annotations

    def get_cluster_sharing_alive_locks(self, context: AnyDagsterContext) -> Sequence[ClusterSharingLock]:
        locks = ClusterSharingLock.parse_all_locks(
            cast(
                dict[str, str],
                self.client.get(name=self.name, namespace=self.namespace).get("metadata", {}).get("annotations", {}),
            )
        )
        return ClusterSharingLock.get_alive_locks(locks)
