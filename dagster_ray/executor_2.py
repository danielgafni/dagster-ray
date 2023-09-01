# import os
# from pathlib import Path
# from typing import Iterator, List, Optional, cast, Dict, Any, Sequence
#
# import kubernetes.config
# import ray
# from dagster import (
#     Field,
#     IntSource,
#     Noneable,
#     StringSource,
#     _check as check,
#     executor, DagsterInstance, JobExecutionResult,
# )
# from dagster._core.definitions import ReconstructableJob
# from dagster._core.definitions.executor_definition import multiple_process_executor_requirements
# from dagster._core.definitions.metadata import MetadataValue
# from dagster._core.events import DagsterEvent, EngineEventData
# from dagster._core.execution.api import execute_plan_iterator, create_execution_plan, execute_plan, execute_job, \
#     ReexecutionOptions
# from dagster._core.execution.plan.state import KnownExecutionState
# from dagster._core.execution.retries import RetryMode, get_retries_config
# from dagster._core.execution.tags import get_tag_concurrency_limits_config
# from dagster._core.executor.base import Executor
# from dagster._core.executor.init import InitExecutorContext
# from dagster._core.executor.step_delegating import (
#     CheckStepHealthResult,
#     StepDelegatingExecutor,
#     StepHandler,
#     StepHandlerContext,
# )
# from ray import ObjectRef
# from ray.actor import ActorHandle
# from dagster._utils.merger import merge_dicts
#
# USER_DEFINED_RAY_CONFIG_SCHEMA = {}
#
# RAY_TIMEOUT = 5.
#
# RAY_EXECUTOR_CONFIG_SCHEMA = {
#         "host": Field(str, is_required=False, description="Ray Cluster host"),
#         "port": Field(int, is_required=False, description="Ray Cluster port"),
#         "code_dir": Field(str, is_required=False),
#         "requirements_path": Field(str, is_required=False),
#         "image": Field(str, default_value=os.getenv("DAGSTER_CURRENT_IMAGE"), is_required=False),
#         "max_concurrency": Field(int, is_required=False),
#         "storage": Field(Noneable(str), is_required=False, description="Shared storage path for Ray workers"),
#         "resources": Field(Noneable(Dict[str, Any]),
#         is_required=False, description="Resources to pass to Ray workers"),
#         "retries": get_retries_config(),
#         "max_concurrent": Field(
#             IntSource,
#             is_required=False,
#             description=(
#                 "Limit on the number of pods that will run concurrently within the scope "
#                 "of a Dagster run. Note that this limit is per run, not global."
#             ),
#         ),
#         "tag_concurrency_limits": get_tag_concurrency_limits_config(),
#         "step_k8s_config": Field(
#             dict,
#             is_required=False,
#             description="Raw Ray configuration for each step launched by the executor.",
#         )
# }
#
#
#
#
# class RayStepHandler(StepHandler):
#     @property
#     def name(self):
#         return "RayStepHandler"
#
#     def __init__(
#         self,
#         host: str,
#         port: int = 6379,
#         image: Optional[str] = None,
#         code_dir: Optional[str] = None,
#         requirements_path: Optional[str] = None,
#         storage: Optional[str] = None,
#         resources: Optional[Dict[str, Any]] = None,
#     ):
#         super().__init__()
#
#         self._image = check.str_param(image, "image")
#         self._host = check.opt_str_param(host, "host", default="localhost")
#         self._port = check.int_param(port, "port")
#         self._code_dir = check.opt_str_param(code_dir, "code_dir")
#         self._requirements_path = check.opt_str_param(requirements_path, "requirements_path")
#         self._storage = check.opt_str_param(storage, "storage")
#         self._resources = check.opt_dict_param(resources, "resources")
#
#         runtime_env = {}
#
#         if self._code_dir is not None:
#             runtime_env["working_dir"] = self._code_dir
#
#         if self._requirements_path is not None:
#             reqs = Path(self._requirements_path).read_text().split("\n")
#             runtime_env["pip"] = reqs
#
#         if self._image is not None:
#             runtime_env["container"] = {
#                 "image": self._image,
#             }
#
#         ray.init(
#             address=f"ray://{host}:{port}",
#             runtime_env=runtime_env,
#             storage=self._storage,
#         )
#
#         self.steps_refs: Dict[str, ObjectRef] = {}
#
#     def _get_step_key(self, step_handler_context: StepHandlerContext) -> str:
#
#         step_keys_to_execute = cast(
#             List[str], step_handler_context.execute_step_args.step_keys_to_execute
#         )
#         assert len(step_keys_to_execute) == 1, "Launching multiple steps is not currently supported"
#         return step_keys_to_execute[0]
#
#     @ray.remote
#     def execute_on_ray(self, step_handler_context: StepHandlerContext) -> JobExecutionResult:
#         step_key = self._get_step_key(step_handler_context)
#         instance = step_handler_context.instance
#         dagster_run = step_handler_context.dagster_run
#         run_config = step_handler_context.dagster_run.run_config
#
#         return execute_job(
#             job=step_handler_context._plan_context.reconstructable_job,  # noqa
#             instance=instance,
#             run_config=run_config,
#             op_selection=step_handler_context.dagster_run.resolved_op_selection,
#             reexecution_options=ReexecutionOptions(
#                 parent_run_id=dagster_run.get_parent_run_id(),
#                 step_selection=[step_key]
#             ),
#             tags=dagster_run.tags,
#         )
#
#     def launch_step(self, step_handler_context: StepHandlerContext) -> Iterator[DagsterEvent]:
#         step_key = self._get_step_key(step_handler_context)
#         run = step_handler_context.dagster_run
#         labels = {
#             "dagster/job": run.job_name,
#             "dagster/op": step_key,
#             "dagster/run-id": step_handler_context.execute_step_args.run_id,
#         }
#         if run.external_job_origin:
#             labels["dagster/code-location"] = (
#                 run.external_job_origin.external_repository_origin.code_location_origin.location_name
#             )
#
#         yield DagsterEvent.step_worker_starting(
#             step_handler_context.get_step_context(step_key),
#             message=f'Executing step "{step_key}" in Ray Cluster',
#             metadata={
#                 "uri": MetadataValue.path(f"ray://{self._host}:{self._port}"),
#             },
#         )
#
#         self.steps_refs[step_key] = self.execute_on_ray.options(num_cpus=..., num_gpus=...)
#         .remote(step_handler_context)
#
#     def check_step_health(self, step_handler_context: StepHandlerContext) -> CheckStepHealthResult:
#         step_key = self._get_step_key(step_handler_context)
#
#         complete, _ = ray.wait([self.steps_refs[step_key]], timeout=0, fetch_local=False)
#
#         if status.failed:
#             return CheckStepHealthResult.unhealthy(
#                 reason=f"Discovered failed Kubernetes job {job_name} for step {step_key}.",
#             )
#
#         return CheckStepHealthResult.healthy()
#
#     def terminate_step(self, step_handler_context: StepHandlerContext) -> Iterator[DagsterEvent]:
#         step_key = self._get_step_key(step_handler_context)
#
#         job_name = self._get_k8s_step_job_name(step_handler_context)
#         container_context = self._get_container_context(step_handler_context)
#
#         yield DagsterEvent.engine_event(
#             step_handler_context.get_step_context(step_key),
#             message=f"Deleting Kubernetes job {job_name} for step",
#             event_specific_data=EngineEventData(),
#         )
#
#         self._api_client.delete_job(job_name=job_name, namespace=container_context.namespace)
#
#     def _wait_fetch(self, ref: ObjectRef) -> Optional[Any]:
#         complete, _ = ray.wait([ref], timeout=0, fetch_local=True)
#
#         if complete:
#             return ray.get(ref, timeout=RAY_TIMEOUT)
#         else:
#             return None
#
#     def _execute_in_ray(self, actor: ActorHandle, step_handler_context: StepHandlerContext):
#         finished = actor.start.remote()
#         next_ref = actor.next.remote()
#         while True:
#             # check finished before getting any events
#             # is_finished = wait_fetch(finished)
#
#             # will block here, not ideal
#             # this required in case is_finished is but the next return is in flight
#             ret = self._wait_fetch(next_ref)
#
#             if ret == (1, 1, 1):
#                 # with capture_interrupts():
#                 try:
#                     # will except if the call did
#                     _ = ray.get(finished, timeout=RAY_TIMEOUT)
#                 except (
#                         Exception,
#                         KeyboardInterrupt,
#                         DagsterExecutionInterruptedError,
#                 ):
#                     yield DagsterEvent.engine_event(
#                         step_context,
#                         message="interrupted",
#                         event_specific_data=EngineEventData(),
#                     )
#                 return
#             if ret is None or isinstance(ret, DagsterEvent):
#                 next_ref = actor.next.remote()
#                 yield ret
#             elif not ret:
#                 # ret is false (wait_fetch not ready)
#                 yield None
#             else:
#                 check.failed("Unexpected return value from actor {}".format(type(ret)))
#             # continue
#             # try:
#             #     # throws exception when actor is dead
#             #     ray.get(actor.heartbeat.remote(), timeout=RAY_TIMEOUT)
#             # except ray.exceptions.RayActorError:
#             #     return
#             # except ray.exceptions.GetTimeoutError:
#             #     yield DagsterEvent.engine_event(
#             #             step_context,
#             #             message=f"get timed out",
#             #             event_specific_data=EngineEventData(),
#             #         )
#             # raise
#
#
# @executor(
#     name="ray",
#     config_schema=RAY_EXECUTOR_CONFIG_SCHEMA,
#     requirements=multiple_process_executor_requirements(),
# )
# def ray_executor(init_context: InitExecutorContext) -> Executor:
#     """Executor which launches steps with Ray
#
#     `max_concurrent` limits the number of steps that will execute concurrently for one run. By default
#     there is no limit- it will maximally parallel as allowed by the DAG. Note that this is not a
#     global limit.
#
#     Configuration set using `tags` on a `@job` will only apply to the `run` level. For configuration
#     to apply at each `step` it must be set using `tags` for each `@op`.
#     """
#     # run_launcher = (
#     #     init_context.instance.run_launcher
#     #     if isinstance(init_context.instance.run_launcher, K8sRunLauncher)
#     #     else None
#     # )
#
#     exc_cfg = init_context.executor_config
#
#     return StepDelegatingExecutor(
#         RayStepHandler(
#             ...
#         ),
#         retries=RetryMode.from_config(exc_cfg["retries"]),  # type: ignore
#         max_concurrent=check.opt_int_elem(exc_cfg, "max_concurrent"),
#         tag_concurrency_limits=check.opt_list_elem(exc_cfg, "tag_concurrency_limits"),
#         should_verify_step=True,
#     )
#
