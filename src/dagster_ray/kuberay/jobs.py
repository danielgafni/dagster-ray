import dagster as dg

from dagster_ray.kuberay.ops import delete_kuberay_clusters_op


@dg.job(description="Deletes KubeRay `RayCluster` resources", name="delete_kuberay_clusters")
def delete_kuberay_clusters():
    delete_kuberay_clusters_op()
