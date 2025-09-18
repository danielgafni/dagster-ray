import dagster as dg

from dagster_ray.kuberay.jobs import cleanup_kuberay_clusters

cleanup_kuberay_clusters_daily = dg.ScheduleDefinition(
    job=cleanup_kuberay_clusters,
    cron_schedule="0 0 * * *",
    name="cleanup_kuberay_clusters_schedule_daily",
)
