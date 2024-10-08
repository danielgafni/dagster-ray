import logging
import sys
from typing import Any, Dict, Optional, Sequence

from dagster import _check as check
from dagster._cli.api import ExecuteRunArgs  # type: ignore
from dagster._config.config_schema import UserConfigSchema
from dagster._core.events import EngineEventData
from dagster._core.launcher import LaunchRunContext, ResumeRunContext, RunLauncher
from dagster._core.launcher.base import CheckRunHealthResult, WorkerStatus
from dagster._core.storage.dagster_run import DagsterRun, DagsterRunStatus
from dagster._grpc.types import ResumeRunArgs
from dagster._serdes import ConfigurableClass, ConfigurableClassData
from dagster._utils.error import serializable_error_info_from_exc_info

from dagster_ray.config import RayExecutionConfig, RayJobSubmissionClientConfig


def get_job_submission_id_from_run_id(run_id: str, resume_attempt_number=None):
    return f"dagster-run-{run_id}" + ("" if not resume_attempt_number else f"-{resume_attempt_number}")


class RayLauncherConfig(RayExecutionConfig, RayJobSubmissionClientConfig): ...


class RayRunLauncher(RunLauncher, ConfigurableClass):
    def __init__(
        self,
        address: str,
        metadata: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        cookies: Optional[Dict[str, Any]] = None,
        runtime_env: Optional[Dict[str, Any]] = None,
        num_cpus: Optional[int] = None,
        num_gpus: Optional[int] = None,
        memory: Optional[int] = None,
        resources: Optional[Dict[str, float]] = None,
        inst_data: Optional[ConfigurableClassData] = None,
    ):
        """RunLauncher that starts a Ray job (incluster mode) for each Dagster run.

        Encapsulates each run in a separate, isolated invocation of ``ray.job_submission.JobSubmissionClient``.

        You can configure a Dagster instance to use this RunLauncher by adding a section to your
        ``dagster.yaml`` like the following:

        .. code-block:: yaml

            run_launcher:
                module: dagster_ray
                class: RayRunLauncher
                    config:
                        address: <your_ray_address>

        Fields such as `num_cpus` set via `dagster-ray/config` Run tag will override the yaml configuration.

        """

        from ray.job_submission import JobSubmissionClient

        self._inst_data = check.opt_inst_param(inst_data, "inst_data", ConfigurableClassData)

        self.client = JobSubmissionClient(address, metadata=metadata, headers=headers, cookies=cookies)

        self.address = address
        self.metadata = metadata
        self.headers = headers
        self.cookies = cookies
        self.runtime_env = runtime_env
        self.num_cpus = num_cpus
        self.num_gpus = num_gpus
        self.memory = memory
        self.resources = resources

        super().__init__()

    @property
    def inst_data(self) -> Optional[ConfigurableClassData]:
        return self._inst_data

    @classmethod
    def config_type(cls) -> UserConfigSchema:
        return RayLauncherConfig.to_fields_dict()

    @classmethod
    def from_config_value(cls, inst_data, config_value):
        return cls(inst_data=inst_data, **config_value)

    @property
    def supports_resume_run(self):
        return True

    @property
    def supports_check_run_worker_health(self):
        return True

    @property
    def supports_run_worker_crash_recovery(self):
        return True

    def launch_run(self, context: LaunchRunContext) -> None:
        run = context.dagster_run
        submission_id = get_job_submission_id_from_run_id(run.run_id)
        job_origin = check.not_none(run.job_code_origin)

        args = ExecuteRunArgs(
            job_origin=job_origin,
            run_id=run.run_id,
            instance_ref=self._instance.get_ref(),
            set_exit_code_on_failure=True,
        ).get_command_args()

        self._launch_ray_job(submission_id, args, run)

    def _launch_ray_job(self, submission_id: str, args: Sequence[str], run: DagsterRun):
        job_origin = check.not_none(run.job_code_origin)

        labels = {
            "dagster/job": job_origin.job_name,
            "dagster/run-id": run.run_id,
        }
        if run.external_job_origin:
            labels["dagster/code-location"] = (
                run.external_job_origin.repository_origin.code_location_origin.location_name
            )

        cfg_from_tags = RayLauncherConfig.from_tags(run.tags)

        runtime_env = cfg_from_tags.runtime_env or self.runtime_env or {}
        num_cpus = cfg_from_tags.num_cpus or self.num_cpus
        num_gpus = cfg_from_tags.num_gpus or self.num_gpus
        memory = cfg_from_tags.memory or self.memory
        resources = cfg_from_tags.resources or self.resources

        runtime_env["env_vars"] = runtime_env.get("env_vars", {})
        runtime_env["env_vars"].update(
            {
                "DAGSTER_RUN_JOB_NAME": job_origin.job_name,
            }
        )

        self._instance.report_engine_event(
            "Creating Ray run job",
            run,
            EngineEventData(
                {
                    "Ray Job Submission ID": submission_id,
                    "Run ID": run.run_id,
                }
            ),
            cls=self.__class__,
        )

        self.client.submit_job(
            submission_id=submission_id,
            entrypoint=" ".join(args),
            runtime_env=runtime_env,
            entrypoint_num_cpus=num_cpus,
            entrypoint_num_gpus=num_gpus,
            entrypoint_memory=memory,
            entrypoint_resources=resources,
            metadata=labels,
        )

        self._instance.report_engine_event(
            "Ray run job created",
            run,
            cls=self.__class__,
        )

    def resume_run(self, context: ResumeRunContext) -> None:
        run = context.dagster_run
        submission_id = get_job_submission_id_from_run_id(
            run.run_id, resume_attempt_number=context.resume_attempt_number
        )
        job_origin = check.not_none(run.job_code_origin)

        args = ResumeRunArgs(
            job_origin=job_origin,
            run_id=run.run_id,
            instance_ref=self._instance.get_ref(),
            set_exit_code_on_failure=True,
        ).get_command_args()

        self._launch_ray_job(submission_id, args, run)

    def terminate(self, run_id: str) -> bool:
        check.str_param(run_id, "run_id")
        run = self._instance.get_run_by_id(run_id)

        if not run or run.is_finished:
            return False

        self._instance.report_run_canceling(run)

        submission_id = get_job_submission_id_from_run_id(
            run.run_id, resume_attempt_number=self._instance.count_resume_run_attempts(run.run_id)
        )

        try:
            termination_result = self.client.stop_job(submission_id)
            if termination_result:
                self._instance.report_engine_event(
                    message="Run was terminated successfully.",
                    dagster_run=run,
                    cls=self.__class__,
                )
            else:
                self._instance.report_engine_event(
                    message="Run was terminated succesfully (the Ray job was already terminated).",
                    dagster_run=run,
                    cls=self.__class__,
                )
            return termination_result
        except RuntimeError:
            self._instance.report_engine_event(
                message="Run was not terminated successfully; encountered error in stop_job",
                dagster_run=run,
                engine_event_data=EngineEventData.engine_error(serializable_error_info_from_exc_info(sys.exc_info())),
                cls=self.__class__,
            )
            return False

    def get_run_worker_debug_info(
        self, run: DagsterRun, include_container_logs: Optional[bool] = True
    ) -> Optional[str]:
        try:
            job_details = [j for j in self.client.list_jobs() if (j.metadata or {}).get("dagster/run-id") == run.run_id]

            return "---\n---".join(str(j) for j in job_details)

        except RuntimeError:
            logging.exception("Error trying to get debug information for failed Ray jobs")

    def check_run_worker_health(self, run: DagsterRun):
        from ray.job_submission import JobStatus

        if self.supports_run_worker_crash_recovery:
            resume_attempt_number = self._instance.count_resume_run_attempts(run.run_id)
        else:
            resume_attempt_number = None

        submission_id = get_job_submission_id_from_run_id(run.run_id, resume_attempt_number=resume_attempt_number)

        try:
            status = self.client.get_job_status(submission_id)
        except RuntimeError:
            return CheckRunHealthResult(
                WorkerStatus.UNKNOWN, str(serializable_error_info_from_exc_info(sys.exc_info()))
            )

        # If the run is in a non-terminal (and non-STARTING) state but the ray job is not active,
        # something went wrong
        if run.status in (DagsterRunStatus.STARTED, DagsterRunStatus.CANCELING) and status.is_terminal():
            return CheckRunHealthResult(
                WorkerStatus.FAILED, f"Run has not completed but Ray job has is in status: {status}"
            )

        elif status == JobStatus.FAILED:
            job_details = self.client.get_job_info(submission_id)
            return CheckRunHealthResult(WorkerStatus.FAILED, f"Ray job failed. Message: {job_details.message}")
        elif status == JobStatus.SUCCEEDED:
            return CheckRunHealthResult(WorkerStatus.SUCCESS)
        elif status in {JobStatus.RUNNING, JobStatus.PENDING}:
            return CheckRunHealthResult(WorkerStatus.RUNNING)

        # safe return in case more statuses are introduced later
        return CheckRunHealthResult(WorkerStatus.RUNNING)
