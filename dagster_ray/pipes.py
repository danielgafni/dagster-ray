from __future__ import annotations

import asyncio
import logging
import random
import string
import threading
import time
from collections.abc import AsyncIterator, Generator, Iterator
from contextlib import contextmanager
from functools import partial
from typing import TYPE_CHECKING, Any, TypedDict, Union, cast

import dagster._check as check
from dagster import AssetExecutionContext, OpExecutionContext, PipesClient
from dagster._annotations import beta
from dagster._core.definitions.resource_annotation import TreatAsResourceParam
from dagster._core.errors import DagsterExecutionInterruptedError
from dagster._core.pipes.client import (
    PipesClientCompletedInvocation,
    PipesContextInjector,
    PipesMessageReader,
)
from dagster._core.pipes.context import PipesLaunchedData, PipesMessageHandler, PipesSession
from dagster._core.pipes.utils import (
    PipesEnvContextInjector,
    _join_thread,
    extract_message_or_forward_to_stdout,
    open_pipes_session,
)
from dagster_pipes import PipesDefaultMessageWriter, PipesExtras, PipesParams
from typing_extensions import NotRequired, TypeAlias

from dagster_ray._base.utils import get_dagster_tags

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ray.job_submission import JobStatus, JobSubmissionClient


OpOrAssetExecutionContext: TypeAlias = Union[OpExecutionContext, AssetExecutionContext]


PIPES_LAUNCHED_EXTRAS_RAY_ADDRESS_KEY = "ray_address"
PIPES_LAUNCHED_EXTRAS_RAY_JOB_ID_KEY = "ray_job_id"


def tail_logs(tailer: AsyncIterator[str]) -> Generator[str | None, None, None]:
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    while True:
        try:
            yield loop.run_until_complete(tailer.__anext__())  # type: ignore
        except (StopAsyncIteration, ValueError):
            break
        except Exception:
            logger.exception("Encountered exception during ray job log tailing")
            yield None


@beta
class PipesRayJobMessageReader(PipesMessageReader):
    f"""
    Dagster Pipes message reader for receiving messages from a Ray job.
    Will extract Dagster events and forward the rest to stdout.

    Waits for PipesSession.report_launched to be called with `{PIPES_LAUNCHED_EXTRAS_RAY_ADDRESS_KEY}` and `{PIPES_LAUNCHED_EXTRAS_RAY_JOB_ID_KEY}` keys
    populated in `extras` in order to start message reading.

    Args:
        _job_submission_client_kwargs (Optional[dict[str, Any]]): additional arguments for `ray.job_submission.JobSubmissionClient`. The `address` value is expected to be set via `PipesSession.report_launched`, while other arguments can be set via this parameter.
    """

    _client: JobSubmissionClient | None
    _job_id: str | None

    def __init__(self, job_submission_client_kwargs: dict[str, Any] | None = None):
        self._job_submission_client_kwargs = job_submission_client_kwargs
        self._thread: threading.Thread | None = None
        self.session_closed = threading.Event()
        self._job_id = None
        self._client = None
        self.thread_ready = threading.Event()

        self.completed = threading.Event()

    @property
    def client(self) -> JobSubmissionClient:
        if self._client is None:
            raise RuntimeError(
                "client is only available after it has been seeded in the Pipes client via PipesSession.report_launched"
            )
        else:
            return self._client

    @property
    def job_id(self) -> str:
        if self._job_id is None:
            raise RuntimeError(
                "job_id is only available after it has been seeded in the Pipes client via PipesSession.report_launched"
            )
        else:
            return self._job_id

    def on_launched(self, launched_payload: PipesLaunchedData) -> None:
        from ray.job_submission import JobSubmissionClient

        self._client = JobSubmissionClient(
            address=launched_payload["extras"][PIPES_LAUNCHED_EXTRAS_RAY_ADDRESS_KEY],
            **(self._job_submission_client_kwargs or {}),
        )
        self._job_id = launched_payload["extras"][PIPES_LAUNCHED_EXTRAS_RAY_JOB_ID_KEY]

        super().on_launched(launched_payload)

    def messages_are_readable(self, params: PipesParams) -> bool:
        return self._job_id is not None and self._client is not None

    @contextmanager
    def read_messages(self, handler: PipesMessageHandler) -> Iterator[PipesParams]:
        try:
            self._thread = threading.Thread(
                target=partial(self.handle_job_logs, handler),
                daemon=True,
            )
            self._thread.start()
            yield {PipesDefaultMessageWriter.STDIO_KEY: PipesDefaultMessageWriter.STDOUT}
        finally:
            self.terminate(handler)

    def terminate(self, handler: PipesMessageHandler) -> None:
        self.session_closed.set()

        if self._thread is not None:
            _join_thread(self._thread, "ray job logs tailing")
            if self._job_id is None:
                handler._context.log.warning(
                    "[Pipes] PipesRayJobMessageReader._job_id is not set. It's expected to be set via PipesSession.report_launched."
                )

    def no_messages_debug_text(self) -> str:
        return f"Tried to read messages from Ray Job logs, but no messages were received. Make sure PipesSession.report_launched has been called with `{PIPES_LAUNCHED_EXTRAS_RAY_JOB_ID_KEY}` set under `extras` field."

    def handle_job_logs(
        self,
        handler: PipesMessageHandler,
    ):
        try:
            while (self._job_id is None or self._client is None) and not self.session_closed.is_set():
                time.sleep(0.1)

            self.thread_ready.set()

            if self.session_closed.is_set():
                handler._context.log.warning(
                    "[pipes] PipesSession.report_launched was not called before session was closed."
                )
                return
            else:
                handler._context.log.debug("[pipes] PipesRayJobMessageReader is ready to begin reading ray job logs")

            assert self._job_id is not None and self._client is not None, (
                "Job ID and client are required for tailing logs. Expected on_launched to set them by this line."
            )

            session_closed_at = None

            tailer = self.client.tail_job_logs(job_id=self.job_id)

            for chunk in tail_logs(tailer):
                if chunk:
                    for log_line in chunk.split("\n"):
                        if log_line:
                            extract_message_or_forward_to_stdout(handler, log_line)

                if self.session_closed.is_set():
                    if session_closed_at is None:
                        session_closed_at = time.time()

                    if time.time() - session_closed_at > 30:  # wait for 30 seconds to flush all logs
                        self.completed.set()
                        return
        except:
            raise
        finally:
            self.completed.set()


class SubmitJobParams(TypedDict):
    entrypoint: str
    runtime_env: NotRequired[dict[str, Any]]
    metadata: NotRequired[dict[str, str]]
    submission_id: NotRequired[str]
    entrypoint_num_cpus: NotRequired[float]
    entrypoint_num_gpus: NotRequired[float]
    entrypoint_memory: NotRequired[int]
    entrypoint_resources: NotRequired[dict[str, float]]


class EnrichedSubmitJobParams(TypedDict):
    entrypoint: str
    runtime_env: dict[str, Any]
    metadata: dict[str, str]
    submission_id: str
    entrypoint_num_cpus: NotRequired[float]
    entrypoint_num_gpus: NotRequired[float]
    entrypoint_memory: NotRequired[int]
    entrypoint_resources: NotRequired[dict[str, float]]


def generate_job_id() -> str:
    letters = string.ascii_lowercase
    random_letters = "".join(random.choice(letters) for i in range(10))
    return f"pipes-{random_letters}"


@beta
class PipesRayJobClient(PipesClient, TreatAsResourceParam):
    """A pipes client for running Ray jobs on remote clusters.

    Starts the job directly on the Ray cluster and reads the logs from the job.

    Args:
        client (JobSubmissionClient): The Ray job submission client
        context_injector (Optional[PipesContextInjector]): A context injector to use to inject
            context into the Ray job. Defaults to :py:class:`PipesEnvContextInjector`.
        message_reader (Optional[PipesMessageReader]): A message reader to use to read messages
            from the glue job run. Defaults to :py:class:`PipesRayJobMessageReader`.
        forward_termination (bool): Whether to cancel the `RayJob` job run when the Dagster process receives a termination signal.
        timeout (int): Timeout for various internal interactions with the Kubernetes RayJob.
        poll_interval (int): Interval at which to poll the Kubernetes for status updates.
        Is useful when running in a local environment.
    """

    def __init__(
        self,
        client: JobSubmissionClient,
        context_injector: PipesContextInjector | None = None,
        message_reader: PipesMessageReader | None = None,
        forward_termination: bool = True,
        timeout: int = 600,
        poll_interval: int = 5,
    ):
        self.client = client
        self._context_injector = context_injector or PipesEnvContextInjector()
        self._message_reader = message_reader or PipesRayJobMessageReader()

        self.forward_termination = check.bool_param(forward_termination, "forward_termination")
        self.timeout = check.int_param(timeout, "timeout")
        self.poll_interval = check.int_param(poll_interval, "poll_interval")

        self._job_submission_client: JobSubmissionClient | None = None

    def run(  # type: ignore
        self,
        *,
        context: OpOrAssetExecutionContext,
        submit_job_params: SubmitJobParams,
        extras: PipesExtras | None = None,
    ) -> PipesClientCompletedInvocation:
        """
        Execute a RayJob, enriched with the Pipes protocol.

        Args:
            context (OpExecutionContext): Current Dagster op or asset context.
            ray_job (Dict[str, Any]): RayJob specification. `API reference <https://ray-project.github.io/kuberay/reference/api/#rayjob>`_.
            extras (Optional[Dict[str, Any]]): Additional information to pass to the Pipes session.
        """

        with open_pipes_session(
            context=context,
            message_reader=self._message_reader,
            context_injector=self._context_injector,
            extras=extras,
        ) as session:
            enriched_submit_job_params = self._enrich_submit_job_params(context, session, submit_job_params)

            job_id = self._start(context, session, enriched_submit_job_params)

            try:
                # self._read_messages(context, job_id)
                self._wait_for_completion(context, job_id)
                return PipesClientCompletedInvocation(session, metadata={"Ray Job ID": job_id})

            except DagsterExecutionInterruptedError:
                if self.forward_termination:
                    context.log.warning(f"[pipes] Dagster process interrupted! Will terminate RayJob {job_id}.")
                    self._terminate(context, job_id)
                raise

    def get_dagster_tags(self, context: OpOrAssetExecutionContext) -> dict[str, str]:
        tags = get_dagster_tags(context)
        return tags

    def _enrich_submit_job_params(
        self, context: OpOrAssetExecutionContext, session: PipesSession, submit_job_params: SubmitJobParams
    ) -> EnrichedSubmitJobParams:
        runtime_env = submit_job_params.get("runtime_env", {})
        metadata = submit_job_params.get("metadata", {})

        env_vars = session.get_bootstrap_env_vars()

        metadata.update(self.get_dagster_tags(context))
        runtime_env["env_vars"] = runtime_env.get("env_vars", {})
        runtime_env["env_vars"].update(env_vars)

        submit_job_params["metadata"] = metadata
        submit_job_params["runtime_env"] = runtime_env
        submit_job_params["submission_id"] = submit_job_params.get("submission_id", generate_job_id())

        return cast(EnrichedSubmitJobParams, submit_job_params)

    def _start(
        self, context: OpOrAssetExecutionContext, session: PipesSession, submit_job_params: EnrichedSubmitJobParams
    ) -> str:
        submission_id = submit_job_params["submission_id"]

        context.log.info(f"[pipes] Starting Ray job {submission_id}...")

        job_id = self.client.submit_job(**submit_job_params)

        context.log.info(f"[pipes] Ray job {job_id} started.")

        session.report_launched(
            {
                "extras": {
                    PIPES_LAUNCHED_EXTRAS_RAY_JOB_ID_KEY: job_id,
                    PIPES_LAUNCHED_EXTRAS_RAY_ADDRESS_KEY: self.client.get_address(),
                }
            }
        )

        return job_id

    def _wait_for_completion(self, context: OpOrAssetExecutionContext, job_id: str) -> JobStatus:
        from ray.job_submission import JobStatus

        context.log.info(f"[pipes] Waiting for RayJob {job_id} to complete...")

        while True:
            status = self.client.get_job_status(job_id)

            if status.is_terminal():
                if status in {JobStatus.FAILED, JobStatus.STOPPED}:
                    job_details = self.client.get_job_info(job_id)
                    raise RuntimeError(
                        f"[pipes] RayJob {job_id} failed with status {status}. Message:\n{job_details.message}"
                    )

                return status

            time.sleep(self.poll_interval)

    def _terminate(self, context: OpOrAssetExecutionContext, job_id: str) -> None:
        context.log.info(f"[pipes] Terminating RayJob {job_id} ...")
        self.client.stop_job(job_id)
        context.log.info(f"[pipes] Ray job {job_id} terminated.")
