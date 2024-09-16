from dagster import job

from dagster_ray.kuberay.ops import cleanup_kuberay_clusters_op, delete_kuberay_clusters_op


@job(description="Deletes RayCluster resources from Kubernetes", name="delete_kuberay_rayclusters")
def delete_kuberay_clusters():
    delete_kuberay_clusters_op()


@job(
    description="Deletes RayCluster resources which do not correspond to any active Dagster Runs in this deployment from Kubernetes",
    name="cleanup_kuberay_rayclusters",
)
def cleanup_kuberay_clusters():
    cleanup_kuberay_clusters_op()
