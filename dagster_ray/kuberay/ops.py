from dagster import Config, DagsterRunStatus, OpExecutionContext, RunsFilter, op
from pydantic import Field

from dagster_ray._base.constants import DEFAULT_DEPLOYMENT_NAME
from dagster_ray.kuberay.resources import RayClusterClientResource


class DeleteKubeRayClustersConfig(Config):
    namespace: str = "kuberay"
    cluster_names: list[str] = Field(default_factory=list, description="List of RayCluster names to delete")


@op(description="Deletes RayCluster resources from Kubernetes", name="delete_kuberay_clusters")
def delete_kuberay_clusters_op(
    context: OpExecutionContext,
    config: DeleteKubeRayClustersConfig,
    client: RayClusterClientResource,
) -> None:
    for cluster_name in config.cluster_names:
        try:
            if client.client.get(name=cluster_name, namespace=config.namespace).get("items"):
                client.client.delete(name=cluster_name, namespace=config.namespace)
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
    description="Deletes RayCluster resources which do not correspond to any active Dagster Runs in this deployment from Kubernetes",
    name="cleanup_kuberay_clusters",
)
def cleanup_kuberay_clusters_op(
    context: OpExecutionContext,
    config: CleanupKuberayClustersConfig,
    client: RayClusterClientResource,
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

    clusters = client.client.list(
        namespace=config.namespace,
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
            client.client.delete(name=cluster_name, namespace=config.namespace)
            context.log.info(f"RayCluster {config.namespace}/{cluster_name} deleted!")
        except:  # noqa
            context.log.exception(f"Couldn't delete RayCluster {config.namespace}/{cluster_name}")
