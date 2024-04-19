from dagster import ScheduleDefinition

from dagster_ray.kuberay.jobs import cleanup_kuberay_clusters

cleanup_kuberay_clusters_daily = ScheduleDefinition(
    job=cleanup_kuberay_clusters,
    cron_schedule="0 0 * * *",
    name="cleanup_kuberay_clusters_schedule_daily",
)
