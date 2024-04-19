from typing import List

from dagster import Config, DagsterRunStatus, OpExecutionContext, RunsFilter, op
from pydantic import Field

from dagster_ray.kuberay.configs import DEFAULT_DEPLOYMENT_NAME
from dagster_ray.kuberay.resources import KubeRayAPI


class DeleteKubeRayClustersConfig(Config):
    namespace: str = "kuberay"
    cluster_names: List[str] = Field(default_factory=list, description="Specific RayCluster names to delete")


@op(description="Deletes KubeRay clusters from Kubernetes", name="delete_kuberay_clusters")
def delete_kuberay_clusters_op(
    context: OpExecutionContext,
    config: DeleteKubeRayClustersConfig,
    kuberay_api: KubeRayAPI,
) -> None:
    for cluster_name in config.cluster_names:
        try:
            if kuberay_api.kuberay.get_ray_cluster(name=cluster_name, k8s_namespace=config.namespace).get("items"):
                kuberay_api.kuberay.delete_ray_cluster(name=cluster_name, k8s_namespace=config.namespace)
                context.log.info(f"RayCluster {config.namespace}/{cluster_name} deleted!")
            else:
                context.log.warning(f"RayCluster {config.namespace}/{cluster_name} doesn't exist")
        except Exception as e:  # noqa
            context.log.exception(f"Couldn't delete RayCluster {config.namespace}/{cluster_name}")


class CleanupKuberayClustersConfig(Config):
    namespace: str = "kuberay"
    label_selector: str = Field(
        default=f"dagster.io/deployment={DEFAULT_DEPLOYMENT_NAME}", description="Label selector to filter RayClusters"
    )


@op(
    description="Deletes KubeRay clusters which do not correspond to any active Dagster Runs in this deployment",
    name="cleanup_kuberay_clusters",
)
def cleanup_kuberay_clusters_op(
    context: OpExecutionContext,
    config: CleanupKuberayClustersConfig,
    kuberay_api: KubeRayAPI,
) -> None:
    current_runs = context.instance.get_runs(
        filters=RunsFilter(
            statuses=[
                DagsterRunStatus.STARTED,
                DagsterRunStatus.QUEUED,
                DagsterRunStatus.CANCELING,
            ]
        )
    )

    clusters = kuberay_api.kuberay.list_ray_clusters(
        k8s_namespace=config.namespace,
        label_selector=config.label_selector,
    )["items"]

    # filter clusters by current runs using dagster.io/run_id label

    old_cluster_names = [
        cluster["metadata"]["name"]
        for cluster in clusters
        if not any(run.run_id == cluster["metadata"]["labels"]["dagster.io/run_id"] for run in current_runs)
    ]

    for cluster_name in old_cluster_names:
        try:
            kuberay_api.kuberay.delete_ray_cluster(name=cluster_name, k8s_namespace=config.namespace)
            context.log.info(f"RayCluster {config.namespace}/{cluster_name} deleted!")
        except:  # noqa
            context.log.exception(f"Couldn't delete RayCluster {config.namespace}/{cluster_name}")
