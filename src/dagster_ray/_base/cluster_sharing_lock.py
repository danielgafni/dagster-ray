import logging
import threading
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta

from pydantic import BaseModel, Field, ValidationError
from typing_extensions import Self

logger = logging.getLogger(__name__)


class ClusterSharingLock(BaseModel):
    run_id: str = Field(description="The ID of the Dagster run that placed the lock.")
    key: str = Field(description="The key of the Dagster step that placed the lock, any unique identifier.")
    created_at: datetime = Field(description="The time at which the lock was created.")
    ttl_seconds: float = Field(
        description="Time to live for the lock after which it's considered expired. Counted from the last sign of life (`heartbeat_at` if present, otherwise `created_at`)."
    )
    heartbeat_at: datetime | None = Field(
        default=None,
        description="The time of the last heartbeat renewal. `None` if the lock has never been renewed, for example when the heartbeat is disabled or the lock was placed by an older version of `dagster-ray`.",
    )

    @property
    def identifier(self) -> str:
        return f"{self.run_id}-{self.key}"

    @property
    def tag(self) -> str:
        return f"dagster/lock-{self.identifier}"[:63]  # 63 is the limit for k8s

    @property
    def alive_at(self) -> datetime:
        """The last sign of life: the last heartbeat, or the lock creation time."""
        return self.heartbeat_at or self.created_at

    @property
    def expired_at(self) -> datetime:
        return self.alive_at + timedelta(seconds=self.ttl_seconds)

    @property
    def is_expired(self) -> bool:
        return self.expired_at < datetime.now()

    def renewed(self) -> Self:
        """A copy of the lock with a fresh `heartbeat_at`. `created_at` is preserved."""
        return self.model_copy(update={"heartbeat_at": datetime.now()})

    @classmethod
    def parse_all_locks(cls, data: dict[str, str]) -> Sequence[Self]:
        locks: list[Self] = []
        if data:
            for key, value in data.items():
                if key.startswith("dagster/lock-"):
                    try:
                        lock = cls.model_validate_json(value)
                        locks.append(lock)
                    except ValidationError:
                        logger.exception(
                            f"Invalid cluster sharing lock: {key}={value}. Consider updating `dagster-ray`."
                        )
        return locks

    @classmethod
    def get_alive_locks(cls, locks: Sequence[Self]) -> Sequence[Self]:
        """Locks that have not expired.

        A lock expires `ttl_seconds` after its last sign of life (`heartbeat_at` or `created_at`),
        so a lock with an actively heartbeating step never expires.
        """
        return [lock for lock in locks if not lock.is_expired]


class ClusterSharingLockHeartbeat:
    """Renews a cluster sharing lock in a background thread while the Dagster step is running.

    Without renewal, the lock expires `ttl_seconds` after step start — even if the step is still
    using the cluster — and the garbage collection sensor may delete the cluster mid-step. The
    heartbeat re-stamps the lock's `heartbeat_at` on a timer for as long as the step is alive.
    This keeps `ttl_seconds` short — idle clusters are still reaped promptly — while long active
    steps survive.

    The first renewal happens immediately: by the time the heartbeat starts, the lock may already
    be almost as old as the cluster provisioning wait, so a first renewal delayed by a full
    interval could land after the lock has expired.

    Renewal failures (e.g. the cluster was already deleted, or a transient API error) are logged
    and do not stop the loop. The thread is a daemon: if the step process crashes, renewals stop
    with it and the lock expires within one TTL.

    This class is backend-agnostic: the actual renewal is performed by the `renew` callable
    (for example, a Kubernetes annotation patch for KubeRay).
    """

    def __init__(
        self,
        *,
        renew: Callable[[], None],
        interval_seconds: float,
        logger: logging.Logger = logger,
    ):
        self.renew = renew
        self.interval_seconds = interval_seconds
        self.logger = logger
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run,
            name="dagster-ray-lock-heartbeat",
            daemon=True,
        )
        self._thread.start()

    def stop(self, join_timeout: float = 10.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=join_timeout)

    def _run(self) -> None:
        while True:
            try:
                self.renew()
            except Exception as e:
                self.logger.warning(f"Failed to renew the cluster sharing lock: {e}")
            if self._stop.wait(self.interval_seconds):
                return
