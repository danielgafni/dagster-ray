# from dagster import (
#     DagsterRunStatus,
#     RunConfig,
#     RunRequest,
#     RunsFilter,
#     ScheduleEvaluationContext,
#     SkipReason,
#     schedule,
# )
#
#
# @schedule(
#     job=delete_ray_clusters,
#     cron_schedule="0 * * * *",
#     description="Deletes old KubeRay cluster created by Dagster which don't correspond to any current Runs",
# )
# def cleanup_old_kuberay_clusters(context: ScheduleEvaluationContext):
#     current_runs = context.instance.get_runs(
#         filters=RunsFilter(
#             statuses=[
#                 DagsterRunStatus.STARTED,
#                 DagsterRunStatus.QUEUED,
#                 DagsterRunStatus.CANCELING,
#             ]
#         )
#     )
#     deployed_names = list_deployed_names()
#
#     dangling_cluster_names = []
#
#     for cluster_name in deployed_names:
#         if cluster_name.startswith("dagster-run-"):
#             dagster_short_run_id = cluster_name.split("-")[2]
#
#             is_dangling = True
#             for run in current_runs:
#                 if run.run_id.startswith(dagster_short_run_id):
#                     is_dangling = False
#
#             if is_dangling:
#                 dangling_cluster_names.append(cluster_name)
#
#     if len(dangling_cluster_names) > 0:
#         return RunRequest(
#             run_key=None,
#             run_config=RunConfig(
#                 ops={"delete_ray_clusters_op": DeleteRayClustersConfig(cluster_names=dangling_cluster_names)}
#             ),
#         )
#     else:
#         return SkipReason(skip_message="No dangling RayClusters were found")
