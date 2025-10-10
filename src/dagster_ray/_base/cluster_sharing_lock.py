import logging
from collections.abc import Sequence
from datetime import datetime, timedelta

from pydantic import BaseModel, Field, ValidationError
from typing_extensions import Self

logger = logging.getLogger(__name__)


class ClusterSharingLock(BaseModel):
    run_id: str = Field(description="The ID of the Dagster run that placed the lock.")
    key: str = Field(description="The key of the Dagster step that placed the lock, any unique identifier.")
    created_at: datetime = Field(description="The time at which the lock was created.")
    ttl_seconds: float = Field(description="Time to live for the lock after which it's considered expired.")

    @property
    def identifier(self) -> str:
        return f"{self.run_id}-{self.key}"

    @property
    def tag(self) -> str:
        return f"dagster/lock-{self.identifier}"[:63]  # 63 is the limit for k8s

    @property
    def expired_at(self) -> datetime:
        return self.created_at + timedelta(seconds=self.ttl_seconds)

    @property
    def is_expired(self) -> bool:
        return self.created_at + timedelta(seconds=self.ttl_seconds) < datetime.now()

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
        return [lock for lock in locks if not lock.is_expired]
