from dagster import job

from dagster_ray.kuberay.ops import cleanup_kuberay_clusters_op, delete_kuberay_clusters_op


@job(description="Deletes KubeRay clusters from Kubernetes", name="delete_kuberay_clusters")
def delete_kuberay_clusters():
    delete_kuberay_clusters_op()


@job(
    description="Deletes KubeRay clusters which do not correspond to any active Dagster Runs in this deployment",
    name="cleanup_kuberay_clusters",
)
def cleanup_kuberay_clusters():
    cleanup_kuberay_clusters_op()
