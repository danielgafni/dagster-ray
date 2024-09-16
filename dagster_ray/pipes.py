import threading
from contextlib import contextmanager
from typing import TYPE_CHECKING, Iterator, Optional

from dagster._annotations import experimental
from dagster._core.pipes.client import PipesMessageReader
from dagster._core.pipes.context import PipesMessageHandler
from dagster._core.pipes.utils import extract_message_or_forward_to_stdout
from dagster_pipes import PipesParams

if TYPE_CHECKING:
    from ray.job_submission import JobSubmissionClient


@experimental
class PipesRayJobSubmissionClientMessageReader(PipesMessageReader):
    """
    Dagster Pipes message reader for recieving messages from a Ray job.
    Will extract Dagster events and forward the rest to stdout.
    """

    def __init__(self, client: "JobSubmissionClient"):
        self.client = client
        self._handler: Optional["PipesMessageHandler"] = None
        self._thread: Optional[threading.Thread] = None
        self._terminate_reading = threading.Event()

    @property
    def handler(self) -> "PipesMessageHandler":
        if self._handler is None:
            raise Exception("PipesMessageHandler is only available while reading messages in open_pipes_session")

        return self._handler

    @property
    def thread(self) -> threading.Thread:
        if self._thread is None:
            raise Exception(
                "Message reading thread is not available. "
                "Did you call tail_job_logs with blocking=False inside open_pipes_session?"
            )

        return self._thread

    def terminate(self) -> None:
        self._terminate_reading.set()

    @contextmanager
    def read_messages(self, handler: "PipesMessageHandler") -> Iterator[PipesParams]:
        #  This method should start a thread to continuously read messages from some location
        self._handler = handler

        try:
            yield {}
        finally:
            self._handler = None
            if self._thread is not None:
                self.terminate()
                self.thread.join()

    def tail_job_logs(self, client: "JobSubmissionClient", job_id: str, blocking: bool = False) -> None:
        def _thread():
            import asyncio

            async_tailer = client.tail_job_logs(job_id=job_id)

            # Backward compatible sync generator
            def tail_logs() -> Iterator[str]:
                while True:
                    try:
                        yield asyncio.get_event_loop().run_until_complete(async_tailer.__anext__())  # type: ignore
                    except StopAsyncIteration:
                        break

            for log_line in tail_logs():
                if self._terminate_reading.is_set():
                    break
                else:
                    extract_message_or_forward_to_stdout(handler=self.handler, log_line=log_line)

        if blocking:
            _thread()
        else:
            self._thread = threading.Thread(target=_thread, daemon=True)
            self.thread.start()
