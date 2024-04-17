from typing import List

from dagster import Config, OpExecutionContext, op

from dagster_ray.resources.kuberay.utils import check_exists, stop


class DeleteRayClustersConfig(Config):
    cluster_names: List[str]
    namespace: str = "ray"


@op(description="Deletes RayClusters from Kubernetes")
def delete_ray_clusters_op(context: OpExecutionContext, config: DeleteRayClustersConfig) -> None:
    for cluster_name in config.cluster_names:
        try:
            if check_exists(cluster_name, namespace=config.namespace):
                stop(cluster_name, namespace=config.namespace)
                context.log.info(f"RayCluster {cluster_name} deleted")
            else:
                context.log.debug(f"RayCluster {cluster_name} doesn't exist")
        except Exception as e:  # noqa
            context.log.error(f"Couldn't delete RayCluster {cluster_name}")
            context.log.exception(str(e))
