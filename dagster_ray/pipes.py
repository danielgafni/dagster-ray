import threading
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Generator, Iterator, Optional

from dagster._annotations import experimental
from dagster._core.pipes.client import PipesMessageReader
from dagster._core.pipes.context import PipesMessageHandler
from dagster._core.pipes.utils import (
    _join_thread,
    extract_message_or_forward_to_stdout,
)
from dagster_pipes import PipesDefaultMessageWriter, PipesParams

if TYPE_CHECKING:
    from ray.job_submission import JobSubmissionClient


@experimental
class PipesRayJobSubmissionClientMessageReader(PipesMessageReader):
    """
    Dagster Pipes message reader for recieving messages from a Ray job.
    Will extract Dagster events and forward the rest to stdout.
    """

    def __init__(self):
        self._handler: Optional["PipesMessageHandler"] = None
        self._thread: Optional[threading.Thread] = None
        self.session_closed = threading.Event()

    @contextmanager
    def read_messages(self, handler: "PipesMessageHandler") -> Iterator[PipesParams]:
        #  This method should start a thread to continuously read messages from some location
        self._handler = handler

        try:
            yield {PipesDefaultMessageWriter.STDIO_KEY: PipesDefaultMessageWriter.STDOUT}
        finally:
            self.terminate()

    @property
    def handler(self) -> "PipesMessageHandler":
        if self._handler is None:
            raise Exception("PipesMessageHandler is only available while reading messages in open_pipes_session")

        return self._handler

    def terminate(self) -> None:
        self.session_closed.set()

        if self._thread is not None:
            _join_thread(self._thread, "ray job logs tailing")

        self._handler = None

    def consume_job_logs(self, client: "JobSubmissionClient", job_id: str, blocking: bool = False) -> None:
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
    handler: PipesMessageHandler, client: "JobSubmissionClient", job_id: str, session_closed: Optional[threading.Event]
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
