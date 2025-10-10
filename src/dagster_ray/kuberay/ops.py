import dagster as dg
from pydantic import Field

from dagster_ray.kuberay.client.raycluster.client import RayClusterClient


class RayClusterRef(dg.Config):
    name: str
    namespace: str | None = None


class DeleteKubeRayClustersConfig(dg.Config):
    namespace: str | None = "ray"
    match_labels: dict[str, str] | None = Field(
        default=None,
        description="Label selector to filter RayCluster resources to delete. Is mutually exclusive with `clusters`.",
    )
    clusters: list[RayClusterRef] | None = Field(
        default=None,
        description="List of `RayCluster` resources to delete. Namespaces can be omitted if the top-level namespace field is specified. Is mutually exclusive with `match_labels`.",
    )


@dg.op(description="Deletes RayCluster resources")
def delete_kuberay_clusters_op(
    context: dg.OpExecutionContext,
    config: DeleteKubeRayClustersConfig,
    raycluster_client: dg.ResourceParam[RayClusterClient],
) -> None:
    if config.match_labels and config.clusters:
        raise ValueError("Only one of `match_labels` or `clusters` can be specified")

    elif config.match_labels is not None:
        assert config.namespace is not None, "Namespace must be specified when using match_labels"
        label_selector = ",".join([f"{key}={value}" for key, value in config.match_labels.items()])
        response = raycluster_client.list(namespace=config.namespace, label_selector=label_selector)
        if clusters := response.get("items", []):
            for cluster in clusters:
                raycluster_client.delete(name=cluster["metadata"]["name"], namespace=config.namespace)
            context.log.info(
                f"{len(clusters)} `RayCluster` resources matching label selector `{config.match_labels}` have been deleted!"
            )

    elif config.clusters is not None:
        if config.clusters:
            for cluster in config.clusters:
                namespace = cluster.namespace or config.namespace
                assert namespace is not None, (
                    f"Namespace must be specified either as a top-level field or on each cluster entry. {cluster.name} does not have a namespace specified"
                )
                raycluster_client.delete(name=cluster.name, namespace=namespace)
            context.log.info(f"{len(config.clusters)} `RayCluster` resources have been deleted!")
    else:
        raise ValueError("Either `match_labels` or `clusters` must be specified")
