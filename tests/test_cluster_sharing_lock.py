import threading
import time
from datetime import datetime, timedelta
from unittest.mock import MagicMock

from dagster_ray._base.cluster_sharing_lock import ClusterSharingLock, ClusterSharingLockHeartbeat


def make_lock(**kwargs) -> ClusterSharingLock:
    defaults = {
        "run_id": "run123",
        "key": "stepA",
        "created_at": datetime.now(),
        "ttl_seconds": 60.0,
    }
    defaults.update(kwargs)
    return ClusterSharingLock(**defaults)


def test_lock_expiry_counts_from_last_heartbeat():
    lock = make_lock(created_at=datetime.now() - timedelta(seconds=100), ttl_seconds=10.0)
    assert lock.is_expired

    renewed = lock.renewed()
    assert not renewed.is_expired
    assert renewed.heartbeat_at is not None
    # created_at is static: renewals never change it
    assert renewed.created_at == lock.created_at


def test_lock_without_heartbeat_behaves_as_before():
    """Locks placed by older dagster-ray versions have no `heartbeat_at` and must keep the
    original expiry semantics: `created_at + ttl_seconds`."""
    old_json = '{"run_id": "run123", "key": "stepA", "created_at": "2020-01-01T10:00:00", "ttl_seconds": 60.0}'
    lock = ClusterSharingLock.model_validate_json(old_json)
    assert lock.heartbeat_at is None
    assert lock.alive_at == lock.created_at
    assert lock.is_expired


def test_get_alive_locks_respects_heartbeats():
    """An old but actively heartbeating lock is alive; the same lock without a heartbeat is not."""
    stale = make_lock(created_at=datetime.now() - timedelta(seconds=1000), ttl_seconds=10.0)
    fresh = stale.renewed()

    assert list(ClusterSharingLock.get_alive_locks([stale])) == []
    assert list(ClusterSharingLock.get_alive_locks([fresh])) == [fresh]


def test_heartbeat_renews_immediately():
    """The first renewal must happen at startup, not after the first interval: by the time the
    heartbeat starts, the lock may already be almost as old as the cluster provisioning wait."""
    renewed = threading.Event()
    heartbeat = ClusterSharingLockHeartbeat(renew=renewed.set, interval_seconds=100.0)
    heartbeat.start()
    try:
        assert renewed.wait(timeout=2), "expected an immediate renewal, not one delayed by the interval"
    finally:
        heartbeat.stop()
    assert not heartbeat.is_running


def test_heartbeat_renews_until_stopped():
    renewals: list[int] = []
    heartbeat = ClusterSharingLockHeartbeat(renew=lambda: renewals.append(1), interval_seconds=0.02)
    heartbeat.start()
    time.sleep(0.15)
    heartbeat.stop()

    assert not heartbeat.is_running
    assert len(renewals) >= 2


def test_heartbeat_survives_renewal_errors():
    """A failing renewal (e.g. the cluster was already deleted, or a transient API error) is
    logged, not raised, and the loop keeps going until stopped."""
    renew = MagicMock(side_effect=RuntimeError("boom"))
    logger = MagicMock()
    heartbeat = ClusterSharingLockHeartbeat(renew=renew, interval_seconds=0.02, logger=logger)
    heartbeat.start()
    time.sleep(0.1)
    heartbeat.stop()

    assert not heartbeat.is_running
    assert renew.call_count >= 2
    assert logger.warning.called


def test_heartbeat_start_is_idempotent():
    heartbeat = ClusterSharingLockHeartbeat(renew=MagicMock(), interval_seconds=100.0)
    heartbeat.start()
    thread = heartbeat._thread
    heartbeat.start()
    assert heartbeat._thread is thread
    heartbeat.stop()
    assert not heartbeat.is_running
