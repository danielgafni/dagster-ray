import os
from collections.abc import Generator
from datetime import datetime, timezone
from typing import cast

import dagster as dg

from dagster_ray._base.cluster_sharing_lock import ClusterSharingLock
from dagster_ray.configs import (
    DAGSTER_RAY_CLUSTER_EXPIRATION_SECONDS_DEFAULT_VALUE,
    DAGSTER_RAY_CLUSTER_EXPIRATION_SECONDS_ENV_VAR,
    DAGSTER_RAY_NAMESPACES_ENV_VAR,
)
from dagster_ray.kuberay.client.raycluster.client import RayClusterClient
from dagster_ray.kuberay.jobs import delete_kuberay_clusters
from dagster_ray.kuberay.ops import DeleteKubeRayClustersConfig, RayClusterRef


@dg.sensor(job=delete_kuberay_clusters, minimum_interval_seconds=5 * 60)
def cleanup_expired_kuberay_clusters(
    context: dg.SensorEvaluationContext,
    raycluster_client: dg.ResourceParam[RayClusterClient],
) -> Generator[dg.RunRequest | dg.SkipReason, None, None]:
    f"""A Dagster sensor that monitors shared `RayCluster` resources created by the current code location and submits jobs to delete clusters that either:
        - use [Cluster Sharing](../tutorial/#cluster-sharing) (`dagster/cluster-sharing=true`) and have expired
        - are older than `DAGSTER_RAY_CLUSTER_EXPIRATION_SECONDS` (defaults to 4 hours)

    By default it monitors the `ray` namespace. This can be configured by setting `{DAGSTER_RAY_NAMESPACES_ENV_VAR}` (accepts a comma-separated list of namespaces)."""
    assert context.code_location_origin is not None

    found_any = False
    namespaces = os.environ.get(DAGSTER_RAY_NAMESPACES_ENV_VAR, "ray").split(",")
    expiration_seconds = int(
        os.environ.get(
            DAGSTER_RAY_CLUSTER_EXPIRATION_SECONDS_ENV_VAR, DAGSTER_RAY_CLUSTER_EXPIRATION_SECONDS_DEFAULT_VALUE
        )
    )

    for namespace in namespaces:
        cluster_names = []
        for cluster in raycluster_client.list(
            namespace=namespace,
            label_selector=f"dagster/code-location={context.code_location_origin.location_name}",
        ).get("items", []):
            if cluster["metadata"].get("labels", {}).get("dagster/cluster-sharing") == "true":
                locks = ClusterSharingLock.parse_all_locks(
                    cast(dict[str, str], cluster.get("metadata", {}).get("annotations", {}))
                )
                alive_locks = ClusterSharingLock.get_alive_locks(locks)
                if not alive_locks:
                    context.log.info(
                        f"Found expired RayCluster with cluster sharing enabled: {cluster['metadata']['namespace']}/{cluster['metadata']['name']}"
                    )
                    cluster_names.append(cluster["metadata"]["name"])
            else:
                # check if the cluster age since creation time exceeds expiration_seconds

                if cluster.get("metadata", {}).get("creationTimestamp"):
                    cluster_age = datetime.now(timezone.utc) - datetime.strptime(
                        cluster["metadata"]["creationTimestamp"], "%Y-%m-%dT%H:%M:%SZ"
                    ).replace(tzinfo=timezone.utc)
                    if cluster_age.total_seconds() >= expiration_seconds:
                        context.log.info(
                            f"Found expired RayCluster (time since creation exceeds {expiration_seconds} seconds): {cluster['metadata']['namespace']}/{cluster['metadata']['name']}"
                        )
                        cluster_names.append(cluster["metadata"]["name"])

        if len(cluster_names) > 0:
            found_any = True
            yield dg.RunRequest(
                run_config=dg.RunConfig(
                    ops={
                        "delete_kuberay_clusters_op": DeleteKubeRayClustersConfig(
                            namespace=namespace,
                            clusters=[RayClusterRef(name=name) for name in cluster_names],
                        )
                    }
                )
            )

    if not found_any:
        yield dg.SkipReason(f"No expired RayClusters found in namespaces: {namespaces}")
