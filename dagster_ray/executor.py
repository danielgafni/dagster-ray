from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, cast

from dagster import (
    _check as check,
)
from dagster import (
    executor,
)
from dagster._core.definitions.executor_definition import multiple_process_executor_requirements
from dagster._core.definitions.metadata import MetadataValue
from dagster._core.events import DagsterEvent, EngineEventData
from dagster._core.execution.retries import RetryMode, get_retries_config
from dagster._core.execution.tags import get_tag_concurrency_limits_config
from dagster._core.executor.base import Executor
from dagster._core.executor.init import InitExecutorContext
from dagster._core.executor.step_delegating import (
    CheckStepHealthResult,
    StepDelegatingExecutor,
    StepHandler,
    StepHandlerContext,
)
from dagster._utils.merger import merge_dicts
from dagster_k8s.job import (
    get_k8s_job_name,
)
from dagster_k8s.launcher import K8sRunLauncher

from dagster_ray.config import RayExecutionConfig, RayJobSubmissionClientConfig

if TYPE_CHECKING:
    pass


class RayExecutorConfig(RayExecutionConfig, RayJobSubmissionClientConfig): ...


_RAY_CONFIG_SCHEMA = RayExecutorConfig.to_config_schema().as_field()

_RAY_EXECUTOR_CONFIG_SCHEMA = merge_dicts(
    {"ray": _RAY_CONFIG_SCHEMA},  # type: ignore
    {"retries": get_retries_config(), "tag_concurrency_limits": get_tag_concurrency_limits_config()},
)


@executor(
    name="ray",
    config_schema=_RAY_EXECUTOR_CONFIG_SCHEMA,
    requirements=multiple_process_executor_requirements(),
)
def ray_executor(init_context: InitExecutorContext) -> Executor:
    """Executes steps by submitting them as Ray jobs.

    The steps are started inside the Ray cluster directly.
    """
    # TODO: some RunLauncher config values can be automatically passed to the executor
    run_launcher = (  # noqa
        init_context.instance.run_launcher if isinstance(init_context.instance.run_launcher, K8sRunLauncher) else None
    )

    exc_cfg = init_context.executor_config

    ray_cfg = RayExecutorConfig(**exc_cfg["ray"])  # type: ignore

    return StepDelegatingExecutor(
        RayStepHandler(address=ray_cfg.address, runtime_env=ray_cfg.runtime_env),
        retries=RetryMode.from_config(exc_cfg["retries"]),  # type: ignore
        max_concurrent=check.opt_int_elem(exc_cfg, "max_concurrent"),
        tag_concurrency_limits=check.opt_list_elem(exc_cfg, "tag_concurrency_limits"),
        should_verify_step=True,
    )


class RayStepHandler(StepHandler):
    @property
    def name(self):
        return "RayStepHandler"

    def __init__(
        self,
        address: str,
        runtime_env: Optional[Dict[str, Any]] = None,
        num_cpus: Optional[int] = None,
        num_gpus: Optional[int] = None,
        memory: Optional[int] = None,
        resources: Optional[Dict[str, float]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        cookies: Optional[Dict[str, str]] = None,
    ):
        super().__init__()

        from ray.job_submission import JobSubmissionClient

        self.client = JobSubmissionClient(address, metadata=metadata, headers=headers, cookies=cookies)
        self.runtime_env = runtime_env or {}
        self.num_cpus = num_cpus
        self.num_gpus = num_gpus
        self.memory = memory
        self.resources = resources

    def _get_step_key(self, step_handler_context: StepHandlerContext) -> str:
        step_keys_to_execute = cast(List[str], step_handler_context.execute_step_args.step_keys_to_execute)
        assert len(step_keys_to_execute) == 1, "Launching multiple steps is not currently supported"
        return step_keys_to_execute[0]

    def _get_ray_job_submission_id(self, step_handler_context: StepHandlerContext):
        step_key = self._get_step_key(step_handler_context)

        name_key = get_k8s_job_name(
            step_handler_context.execute_step_args.run_id,
            step_key,
        )

        if step_handler_context.execute_step_args.known_state:
            retry_state = step_handler_context.execute_step_args.known_state.get_retry_state()
            if retry_state.get_attempt_count(step_key):
                return "dagster-step-%s-%d" % (name_key, retry_state.get_attempt_count(step_key))

        return "dagster-step-%s" % (name_key)

    def launch_step(self, step_handler_context: StepHandlerContext) -> Iterator[DagsterEvent]:
        step_key = self._get_step_key(step_handler_context)

        submission_id = self._get_ray_job_submission_id(step_handler_context)

        run = step_handler_context.dagster_run
        labels = {
            "dagster/job": run.job_name,
            "dagster/op": step_key,
            "dagster/run-id": step_handler_context.execute_step_args.run_id,
        }
        if run.external_job_origin:
            labels["dagster/code-location"] = (
                run.external_job_origin.repository_origin.code_location_origin.location_name
            )

        user_provided_config = RayExecutionConfig.from_tags({**step_handler_context.step_tags[step_key]})

        runtime_env = (user_provided_config.runtime_env or self.runtime_env).copy()

        dagster_env_vars = {
            "DAGSTER_RUN_JOB_NAME": run.job_name,
            "DAGSTER_RUN_STEP_KEY": step_key,
            **{env["name"]: env["value"] for env in step_handler_context.execute_step_args.get_command_env()},
        }

        runtime_env["env_vars"] = {**dagster_env_vars, **runtime_env.get("env_vars", {})}  # type: ignore

        num_cpus = self.num_cpus or user_provided_config.num_cpus
        num_gpus = self.num_gpus or user_provided_config.num_gpus
        memory = self.memory or user_provided_config.memory
        resources = self.resources or {}
        resources.update(user_provided_config.resources or {})

        yield DagsterEvent.step_worker_starting(
            step_handler_context.get_step_context(step_key),
            message=f'Executing step "{step_key}" in Ray job {submission_id}.',
            metadata={
                "Ray Submission ID": MetadataValue.text(submission_id),
            },
        )

        self.client.submit_job(
            entrypoint=" ".join(
                step_handler_context.execute_step_args.get_command_args(skip_serialized_namedtuple=True)
            ),
            submission_id=submission_id,
            metadata=labels,
            runtime_env=runtime_env,
            entrypoint_num_cpus=num_cpus,
            entrypoint_num_gpus=num_gpus,
            entrypoint_memory=memory,
            entrypoint_resources=resources,
        )

    def check_step_health(self, step_handler_context: StepHandlerContext) -> CheckStepHealthResult:
        from ray.job_submission import JobStatus

        step_key = self._get_step_key(step_handler_context)

        submission_id = self._get_ray_job_submission_id(step_handler_context)

        try:
            status = self.client.get_job_status(submission_id)
        except RuntimeError:
            return CheckStepHealthResult.unhealthy(
                reason=f"Ray job {submission_id} for step {step_key} could not be found."
            )

        if status == JobStatus.FAILED:
            job_details = self.client.get_job_info(submission_id)

            reason = f"Discovered failed Ray job {submission_id} for step {step_key}."

            if job_details.error_type:
                reason += f" Error type: {job_details.error_type}."

            if job_details.message:
                reason += f" Message: {job_details.message}."

            return CheckStepHealthResult.unhealthy(reason=reason)

        return CheckStepHealthResult.healthy()

    def terminate_step(self, step_handler_context: StepHandlerContext) -> Iterator[DagsterEvent]:
        step_key = self._get_step_key(step_handler_context)

        submission_id = self._get_ray_job_submission_id(step_handler_context)

        yield DagsterEvent.engine_event(
            step_handler_context.get_step_context(step_key),
            message=f"Deleting Ray job {submission_id} for step",
            event_specific_data=EngineEventData(),
        )

        self.client.stop_job(submission_id)
