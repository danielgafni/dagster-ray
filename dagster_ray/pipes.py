from __future__ import annotations

import random
import string
import threading
import time
from collections.abc import Generator, Iterator
from contextlib import contextmanager
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
from dagster._core.pipes.context import PipesMessageHandler, PipesSession
from dagster._core.pipes.utils import (
    PipesEnvContextInjector,
    _join_thread,
    extract_message_or_forward_to_stdout,
    open_pipes_session,
)
from dagster_pipes import PipesDefaultMessageWriter, PipesExtras, PipesParams
from typing_extensions import NotRequired, TypeAlias

from dagster_ray._base.utils import get_dagster_tags

if TYPE_CHECKING:
    from ray.job_submission import JobStatus, JobSubmissionClient


OpOrAssetExecutionContext: TypeAlias = Union[OpExecutionContext, AssetExecutionContext]


@beta
class PipesRayJobMessageReader(PipesMessageReader):
    """
    Dagster Pipes message reader for receiving messages from a Ray job.
    Will extract Dagster events and forward the rest to stdout.
    """

    def __init__(self):
        self._handler: PipesMessageHandler | None = None
        self._thread: threading.Thread | None = None
        self.session_closed = threading.Event()

    @contextmanager
    def read_messages(self, handler: PipesMessageHandler) -> Iterator[PipesParams]:
        #  This method should start a thread to continuously read messages from some location
        self._handler = handler

        try:
            yield {PipesDefaultMessageWriter.STDIO_KEY: PipesDefaultMessageWriter.STDOUT}
        finally:
            self.terminate()

    @property
    def handler(self) -> PipesMessageHandler:
        if self._handler is None:
            raise Exception("PipesMessageHandler is only available while reading messages in open_pipes_session")

        return self._handler

    def terminate(self) -> None:
        self.session_closed.set()

        if self._thread is not None:
            _join_thread(self._thread, "ray job logs tailing")

        self._handler = None

    # TODO: call this method as part of self.read_messages
    def consume_job_logs(self, client: JobSubmissionClient, job_id: str, blocking: bool = False) -> None:
        if blocking:
            handle_job_logs(handler=self.handler, client=client, job_id=job_id, session_closed=None)
        else:
            self._thread = threading.Thread(
                target=handle_job_logs,
                kwargs={
                    "handler": self.handler,
                    "client": client,
                    "job_id": job_id,
                    "session_closed": self.session_closed,
                },
                daemon=True,
            )
            self._thread.start()

    def no_messages_debug_text(self) -> str:
        return "Tried to read messages from a Ray job logs, but no messages were received."


def handle_job_logs(
    handler: PipesMessageHandler, client: JobSubmissionClient, job_id: str, session_closed: threading.Event | None
):
    import asyncio

    async_tailer = client.tail_job_logs(job_id=job_id)

    # Backward compatible sync generator
    def tail_logs() -> Generator[str, None, None]:
        while True:
            try:
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                yield loop.run_until_complete(async_tailer.__anext__())  # type: ignore
            except (StopAsyncIteration, ValueError):
                break

    session_closed_at = None

    for chunk in tail_logs():
        for log_line in chunk.split("\n"):
            if log_line:
                extract_message_or_forward_to_stdout(handler, log_line)

        if session_closed is not None and session_closed.is_set():
            if session_closed_at is None:
                session_closed_at = time.time()

            if time.time() - session_closed_at > 30:  # wait for 30 seconds to flush all logs
                session_closed.wait()
                return

    if session_closed is not None:
        session_closed.wait()

    return


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

            job_id = self._start(context, enriched_submit_job_params)

            try:
                self._read_messages(context, job_id)
                self._wait_for_completion(context, job_id)
                return PipesClientCompletedInvocation(session)

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

    def _start(self, context: OpOrAssetExecutionContext, submit_job_params: EnrichedSubmitJobParams) -> str:
        submission_id = submit_job_params["submission_id"]

        context.log.info(f"[pipes] Starting Ray job {submission_id}...")

        job_id = self.client.submit_job(**submit_job_params)

        context.log.info(f"[pipes] Ray job {job_id} started.")

        return job_id

    def _read_messages(self, context: OpOrAssetExecutionContext, job_id: str) -> None:
        if isinstance(self._message_reader, PipesRayJobMessageReader):
            # starts a thread
            self._message_reader.consume_job_logs(
                # TODO: investigate why some messages aren't being handled with blocking=False
                client=self.client,
                job_id=job_id,
                blocking=True,
            )

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
