from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Optional, cast

import dagster
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
from dagster._core.remote_representation.origin import RemoteJobOrigin
from dagster._utils.merger import merge_dicts
from packaging.version import Version
from pydantic import Field

from dagster_ray.config import RayExecutionConfig, RayJobSubmissionClientConfig
from dagster_ray.kuberay.resources import get_k8s_object_name
from dagster_ray.run_launcher import RayRunLauncher
from dagster_ray.utils import resolve_env_vars_list

if TYPE_CHECKING:
    from ray.job_submission import JobSubmissionClient


class RayExecutorConfig(RayExecutionConfig, RayJobSubmissionClientConfig):
    env_vars: list[str] | None = Field(
        default=None,
        description="A list of environment variables to inject into the Job. Each can be of the form KEY=VALUE or just KEY (in which case the value will be pulled from the current process).",
    )
    address: str | None = Field(default=None, description="The address of the Ray cluster to connect to.")  # type: ignore
    # sorry for the long name, but it has to be very clear what this is doing
    inherit_job_submission_client_from_ray_run_launcher: bool = True


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
    When used together with the `RayRunLauncher`, the executor can inherit the job submission client configuration.
    This behavior can be disabled by setting `inherit_job_submission_client_from_ray_run_launcher` to `False`.
    """
    from ray.job_submission import JobSubmissionClient

    exc_cfg = init_context.executor_config
    ray_cfg = RayExecutorConfig(**exc_cfg["ray"])  # type: ignore

    if ray_cfg.inherit_job_submission_client_from_ray_run_launcher and isinstance(
        init_context.instance.run_launcher, RayRunLauncher
    ):
        # TODO: some RunLauncher config values can be automatically passed to the executor
        client = init_context.instance.run_launcher.client
    else:
        client = JobSubmissionClient(
            ray_cfg.address, metadata=ray_cfg.metadata, headers=ray_cfg.headers, cookies=ray_cfg.cookies
        )

    return StepDelegatingExecutor(
        RayStepHandler(
            client=client,
            env_vars=ray_cfg.env_vars,
            runtime_env=ray_cfg.runtime_env,
            num_cpus=ray_cfg.num_cpus,
            num_gpus=ray_cfg.num_gpus,
            memory=ray_cfg.memory,
            resources=ray_cfg.resources,
        ),
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
        client: JobSubmissionClient,
        env_vars: list[str] | None,
        runtime_env: dict[str, Any] | None,
        num_cpus: float | None,
        num_gpus: float | None,
        memory: int | None,
        resources: dict[str, float] | None,
    ):
        super().__init__()

        self.client = client
        self.env_vars = env_vars or []
        self.runtime_env = runtime_env or {}
        self.num_cpus = num_cpus
        self.num_gpus = num_gpus
        self.memory = memory
        self.resources = resources

    def _get_step_key(self, step_handler_context: StepHandlerContext) -> str:
        step_keys_to_execute = cast(list[str], step_handler_context.execute_step_args.step_keys_to_execute)
        assert len(step_keys_to_execute) == 1, "Launching multiple steps is not currently supported"
        return step_keys_to_execute[0]

    def _get_ray_job_submission_id(self, step_handler_context: StepHandlerContext):
        step_key = self._get_step_key(step_handler_context)

        name_key = get_k8s_object_name(
            step_handler_context.execute_step_args.run_id,
            step_key,
        )

        if step_handler_context.execute_step_args.known_state:
            retry_state = step_handler_context.execute_step_args.known_state.get_retry_state()
            if retry_state.get_attempt_count(step_key):
                return f"dagster-step-{name_key}-{retry_state.get_attempt_count(step_key)}"

        return f"dagster-step-{name_key}"

    def launch_step(self, step_handler_context: StepHandlerContext) -> Iterator[DagsterEvent]:
        step_key = self._get_step_key(step_handler_context)

        submission_id = self._get_ray_job_submission_id(step_handler_context)

        run = step_handler_context.dagster_run
        labels = {
            "dagster/job": run.job_name,
            "dagster/op": step_key,
            "dagster/run-id": step_handler_context.execute_step_args.run_id,
        }

        if Version(dagster.__version__) >= Version("1.8.12"):
            remote_job_origin = run.remote_job_origin  # type: ignore
        else:
            remote_job_origin = run.external_job_origin  # type: ignore

        remote_job_origin = cast(Optional[RemoteJobOrigin], remote_job_origin)

        if remote_job_origin:
            labels["dagster/code-location"] = remote_job_origin.repository_origin.code_location_origin.location_name

        user_provided_config = RayExecutionConfig.from_tags({**step_handler_context.step_tags[step_key]})

        # note! ray modifies the user-provided runtime_env, so we copy it
        runtime_env = (user_provided_config.runtime_env or self.runtime_env).copy()

        dagster_env_vars = {
            "DAGSTER_RUN_JOB_NAME": run.job_name,
            "DAGSTER_RUN_STEP_KEY": step_key,
            **{env["name"]: env["value"] for env in step_handler_context.execute_step_args.get_command_env()},
        }

        runtime_env["env_vars"] = {**dagster_env_vars, **runtime_env.get("env_vars", {})}  # type: ignore

        runtime_env["env_vars"].update(resolve_env_vars_list(self.env_vars))

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
            message=f"Stopping Ray job {submission_id} for step",
            event_specific_data=EngineEventData(),
        )

        self.client.stop_job(submission_id)
