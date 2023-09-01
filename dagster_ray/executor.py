# import multiprocessing
# import os
# import socket
# from pathlib import Path
# from typing import Optional, Dict, Any
#
# import ray
# from dagster import Field, MetadataEntry, Noneable
# from dagster import _check as check
# from dagster import executor, InitExecutorContext
# from dagster._core.errors import (
#     DagsterExecutionInterruptedError,
#     DagsterSubprocessError,
# )
# from dagster._core.events import DagsterEvent, EngineEventData
# from dagster._core.execution.api import create_execution_plan, execute_plan_iterator
# from dagster._core.execution.context.system import PlanOrchestrationContext
# from dagster._core.execution.context_creation_job import create_context_free_log_manager
# from dagster._core.execution.plan.plan import ExecutionPlan
# from dagster._core.execution.retries import RetryMode
# from dagster._core.executor.base import Executor
# from dagster._core.instance import DagsterInstance
# from dagster._utils.timing import format_duration, time_execution_scope
# from ray import remote
#
#
# RAY_TIMEOUT = 1
#
#
# @remote(num_cpus=1)  # TODO configurable per step
# class RayProcessCommand:
#     def __init__(
#         self,
#         run_config,
#         pipeline_run,
#         instance_ref,
#         step_key,
#         recon_pipeline,
#         retry_mode,
#         known_state,
#     ):
#         self.run_config = run_config
#         self.pipeline_run = pipeline_run
#         self.instance_ref = instance_ref
#
#         self.step_key = step_key
#         self.recon_pipeline = recon_pipeline
#         self.retry_mode = retry_mode
#         self.known_state = known_state
#
#         self.events = []
#         self.execution_complete = False
#
#     def heartbeat(self):
#         return True
#
#     def get_events(self):
#         return self.events
#
#     def start(self):
#         for event in self.execute():
#             self.events.append(event)
#
#         return True
#
#     def next(self):
#         if self.events:
#             return self.events.pop(0)
#         return None
#
#     def execute(self):
#         pipeline = self.recon_pipeline
#         with DagsterInstance.from_ref(self.instance_ref) as instance:
#             execution_plan = create_execution_plan(
#                 pipeline=pipeline,
#                 run_config=self.run_config,
#                 mode=self.pipeline_run.mode,
#                 step_keys_to_execute=[self.step_key],
#                 known_state=self.known_state,
#             )
#
#             log_manager = create_context_free_log_manager(instance, self.pipeline_run)
#
#             yield DagsterEvent.step_worker_started(
#                 log_manager,
#                 self.pipeline_run.pipeline_name,
#                 # TODO modify to make sense in ray context
#                 message='Executing step "{}" in subprocess.'.format(self.step_key),
#                 metadata_entries=[
#                     MetadataEntry("pid", value=str(os.getpid())),
#                 ],
#                 step_key=self.step_key,
#             )
#
#             try:
#                 yield from execute_plan_iterator(
#                     execution_plan,
#                     pipeline,
#                     self.pipeline_run,
#                     run_config=self.run_config,
#                     retry_mode=self.retry_mode.for_inner_plan(),
#                     instance=instance,
#                 )
#             except:
#                 raise ValueError()
#         yield (1, 1, 1)
#
#
# class RayExecutor(Executor):
#     def __init__(
#         self,
#         retries: RetryMode,
#         max_concurrent: int,
#         resources: Optional[Dict[str, Any]] = None
#     ):
#         self._retries = check.inst_param(retries, "retries", RetryMode)
#         max_concurrent = max_concurrent if max_concurrent else multiprocessing.cpu_count()  # TODO whats smart?
#         self._max_concurrent = check.int_param(max_concurrent, "max_concurrent")
#
#         resources = resources if resources is not None else {}
#         self._resources = resources.copy()
#
#     @property
#     def retries(self):
#         return self._retries
#
#     def execute(self, plan_context, execution_plan):
#         check.inst_param(plan_context, "plan_context", PlanOrchestrationContext)
#         check.inst_param(execution_plan, "execution_plan", ExecutionPlan)
#
#         pipeline = plan_context.reconstructable_pipeline
#         limit = self._max_concurrent
#         host = socket.gethostname()
#         yield DagsterEvent.engine_event(
#             plan_context,
#             f"Executing steps using ray executor: driver host ({host})",
#             ## TODO: change?
#             event_specific_data=EngineEventData.multiprocess(
#                 host, step_keys_to_execute=execution_plan.step_keys_to_execute
#             ),
#         )
#
#         # It would be good to implement a reference tracking algorithm here so we could
#         # garbage collect results that are no longer needed by any steps
#         # https://github.com/dagster-io/dagster/issues/811
#         with time_execution_scope() as timer_result:
#             with execution_plan.start(retry_mode=self.retries) as active_execution:
#                 active_iters = {}
#                 actors = {}
#                 stopping = False
#
#                 while (not stopping and not active_execution.is_complete) or active_iters:
#                     if active_execution.check_for_interrupts():
#                         step_keys, active_actors = zip(*actors.items())
#                         yield DagsterEvent.engine_event(
#                             plan_context,
#                             "Ray executor: received termination signal - " "killing all actors",
#                             EngineEventData.interrupted(list(step_keys)),
#                         )
#                         stopping = True
#                         active_execution.mark_interrupted()
#
#                         # cancels all active actors
#                         def kill_actor(actor):
#                             try:
#                                 ray.kill(actor)
#                             except ray.exceptions.RayActorError:
#                                 # actor already dead
#                                 pass
#
#                         list(map(kill_actor, active_actors))
#
#                     # start iterators
#                     while len(active_iters) < limit and not stopping:
#                         steps = active_execution.get_steps_to_execute(limit=(limit - len(active_iters)))
#
#                         if not steps:
#                             break
#
#                         for step in steps:
#                             step_context = plan_context.for_step(step)
#                             # need max_concurrency otherwise get will block when
#                             # when trying to fetch any events
#                             # thread concurrency is considered experimental
#                             # consider async
#
#                             actor = RayProcessCommand.options(
#                                 max_concurrency=2,
#                                 # num_cpus=step_context.tags
#                                 **self._resources,
#                             ).remote(
#                                 run_config=step_context.run_config,
#                                 pipeline_run=step_context.pipeline_run,
#                                 instance_ref=step_context.instance.get_ref(),
#                                 step_key=step.key,
#                                 recon_pipeline=pipeline,
#                                 retry_mode=self.retries,
#                                 known_state=active_execution.get_known_state(),
#                             )
#                             # term_events[step.key] = multiproc_ctx.Event()
#                             active_iters[step.key] = execute_step_out_of_process(actor, step_context)
#                             actors[step.key] = actor
#
#                     # process active iterators
#                     empty_iters = []
#                     for key, step_iter in active_iters.items():
#                         try:
#                             event_or_none = next(step_iter)
#                             if event_or_none is None:
#                                 continue
#                             else:
#                                 yield event_or_none
#                                 active_execution.handle_event(event_or_none)
#
#                         # except ChildProcessCrashException as crash:
#                         #     serializable_error = serializable_error_info_from_exc_info(
#                         #         sys.exc_info()
#                         #     )
#                         #     step_context = plan_context.for_step(
#                         #         active_execution.get_step_by_key(key)
#                         #     )
#                         #     yield DagsterEvent.engine_event(
#                         #         step_context,
#                         #         (
#                         #             "Multiprocess executor: child process for step {step_key} "
#                         #             "unexpectedly exited with code {exit_code}"
#                         #         ).format(step_key=key, exit_code=crash.exit_code),
#                         #         EngineEventData.engine_error(serializable_error),
#                         #     )
#                         #     step_failure_event = DagsterEvent.step_failure_event(
#                         #         step_context=plan_context.for_step(
#                         #             active_execution.get_step_by_key(key)
#                         #         ),
#                         #         step_failure_data=StepFailureData(
#                         #             error=serializable_error, user_failure_data=None
#                         #         ),
#                         #     )
#                         #     active_execution.handle_event(step_failure_event)
#                         #     yield step_failure_event
#                         #     empty_iters.append(key)
#                         except StopIteration:
#                             # raise Exception()
#                             empty_iters.append(key)
#                     # clear and mark complete finished iterators
#                     # plan_context.log.info(list(actors.keys()))
#                     for key in empty_iters:
#                         del active_iters[key]
#                         del actors[key]
#                         active_execution.verify_complete(plan_context, key)
#
#                     # process skipped and abandoned steps
#                     yield from active_execution.plan_events_iterator(plan_context)
#
#                 # TODO: handle errors properly
#                 errs = {actor: None for step_key, actor in active_iters.items() if not actor.heartbeat()}
#
#                 # After termination starts, raise an interrupted exception once all subprocesses
#                 # have finished cleaning up (and the only errors were from being interrupted)
#                 if (
#                     stopping
#                     and (not active_iters)
#                     and all([err_info.cls_name == "DagsterExecutionInterruptedError" for err_info in errs.values()])
#                 ):
#                     yield DagsterEvent.engine_event(
#                         plan_context,
#                         "Ray executor: killed all active child processes",
#                         event_specific_data=EngineEventData(),
#                     )
#                     raise DagsterExecutionInterruptedError()
#                 elif errs:
#                     raise DagsterSubprocessError(
#                         "During ray execution errors occurred in actors:\n{error_list}".format(
#                             error_list="\n".join(
#                                 [
#                                     "In process {pid}: {err}".format(pid=pid, err=err.to_string())
#                                     for pid, err in errs.items()
#                                 ]
#                             )
#                         ),
#                         subprocess_error_infos=list(errs.values()),
#                     )
#
#         yield DagsterEvent.engine_event(
#             plan_context,
#             "Ray executor: driver exiting after {duration}".format(
#                 duration=format_duration(timer_result.millis),
#             ),
#             event_specific_data=EngineEventData.multiprocess(os.getpid()),
#         )
#
#
# def wait_fetch(obj):
#     complete, _ = ray.wait([obj], timeout=0, fetch_local=True)
#
#     if complete:
#         return ray.get(obj, timeout=RAY_TIMEOUT)
#
#     return False
#
#
# def execute_step_out_of_process(actor, step_context):
#     # engine_event
#     # yield DagsterEvent.step_worker_starting(
#     #     step_context,
#     #     'Launching ray actor for "{}".'.format(step.key),
#     #     metadata_entries=[],
#     # )
#     # raise Exception(command.__dir__())
#     finished = actor.start.remote()
#     next_ref = actor.next.remote()
#     # ray.get(finished)
#     while True:
#         # check finished before getting any events
#         # is_finished = wait_fetch(finished)
#
#         # will block here, not ideal
#         # this required in case is_finished is but the next return is in flight
#         ret = wait_fetch(next_ref)
#
#         # step_context.log.info(f"{ret}, {is_finished}")
#
#         if ret == (1, 1, 1):
#             # with capture_interrupts():
#             try:
#                 # will except if the call did
#                 _ = ray.get(finished, timeout=RAY_TIMEOUT)
#             except (
#                 Exception,
#                 KeyboardInterrupt,
#                 DagsterExecutionInterruptedError,
#             ):
#                 yield DagsterEvent.engine_event(
#                     step_context,
#                     message="interrupted",
#                     event_specific_data=EngineEventData(),
#                 )
#             return
#         if ret is None or isinstance(ret, DagsterEvent):
#             next_ref = actor.next.remote()
#             yield ret
#         elif not ret:
#             # ret is false (wait_fetch not ready)
#             yield None
#         else:
#             check.failed("Unexpected return value from actor {}".format(type(ret)))
#         # continue
#         # try:
#         #     # throws exception when actor is dead
#         #     ray.get(actor.heartbeat.remote(), timeout=RAY_TIMEOUT)
#         # except ray.exceptions.RayActorError:
#         #     return
#         # except ray.exceptions.GetTimeoutError:
#         #     yield DagsterEvent.engine_event(
#         #             step_context,
#         #             message=f"get timed out",
#         #             event_specific_data=EngineEventData(),
#         #         )
#         # raise
#
#
# @executor(
#     name="ray",
#     config_schema=
# )
# def ray_executor(init_context: InitExecutorContext):
#     """Ray-based executor."""
#     cwd = Path(os.getcwd())
#
#     exc_cfg = init_context.executor_config
#     ray_host = exc_cfg.get("host", "localhost")
#     ray_port = exc_cfg.get("port", 10001)
#     code_dir = exc_cfg.get("code_dir", cwd.as_posix())
#     requirements_path = exc_cfg.get("requirements_path")
#     max_concurrency = exc_cfg.get("max_concurrency", 10)
#
#     reqs = []
#     if requirements_path is not None:
#         reqs = Path(requirements_path).read_text().split("\n")
#
#     ray.init(
#         f"ray://{ray_host}:{ray_port}", runtime_env={"working_dir": code_dir, "pip": reqs},
#         storage=exc_cfg.get("storage")
#     )
#     return RayExecutor(retries=RetryMode.DISABLED, max_concurrent=max_concurrency)
