"""Leader election via Kubernetes Lease for coordinating shared cluster creation.

This module provides a Kubernetes Lease-based leader election mechanism to ensure
only one step creates a shared RayCluster when multiple parallel steps start simultaneously.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kubernetes.client import ApiClient

logger = logging.getLogger(__name__)


@dataclass
class LeaderElectionResult:
    """Result of a leader election attempt."""

    is_leader: bool
    lease_name: str
    holder_identity: str


class LeaderElection:
    """Kubernetes Lease-based leader election for coordinating shared cluster creation.

    Uses the `coordination.k8s.io/v1` `Lease` API to implement distributed leader election.
    """

    def __init__(
        self,
        holder_identity: str,
        api_client: ApiClient,
        lease_duration_seconds: int = 60,
        retry_period_seconds: float = 1.0,
        acquire_timeout_seconds: float = 60.0,
    ):
        """Initialize leader election.

        Args:
            holder_identity: Unique identifier for this process
            api_client: Kubernetes API client
            lease_duration_seconds: How long the lease is held before expiring
            retry_period_seconds: How often to retry acquiring the lease
            acquire_timeout_seconds: Total time to wait for leader election
        """
        from kubernetes import client

        self.api_client = api_client
        self._coordination_api = client.CoordinationV1Api(api_client=api_client)
        self._holder_identity = holder_identity
        self.lease_duration_seconds = lease_duration_seconds
        self.retry_period_seconds = retry_period_seconds
        self.acquire_timeout_seconds = acquire_timeout_seconds

    def acquire_or_wait(
        self,
        lease_name: str,
        namespace: str,
        resource_exists_check: Callable[[], bool],
    ) -> LeaderElectionResult:
        """Acquire leadership or wait for the resource to be created by the leader.

        Args:
            lease_name: Name of the lease to use for coordination
            namespace: Kubernetes namespace
            resource_exists_check: Callable that returns True if the shared resource exists

        Returns:
            LeaderElectionResult indicating whether this process should create the resource
        """
        start_time = time.time()

        while time.time() - start_time < self.acquire_timeout_seconds:
            # Check if resource already exists
            if resource_exists_check():
                logger.debug("Shared resource already exists, not acquiring leadership")
                # Try to get the current lease holder for logging purposes
                current_holder = self._get_lease_holder(lease_name, namespace)
                return LeaderElectionResult(
                    is_leader=False,
                    lease_name=lease_name,
                    holder_identity=current_holder or "unknown",
                )

            # Try to become the leader
            if self._try_create_lease(lease_name, namespace):
                logger.info(f"Acquired leadership by creating lease {namespace}/{lease_name}")
                return LeaderElectionResult(
                    is_leader=True,
                    lease_name=lease_name,
                    holder_identity=self._holder_identity,
                )

            # Someone else is the leader, wait for resource to appear
            current_holder = self._get_lease_holder(lease_name, namespace)
            logger.debug(f"Lease {namespace}/{lease_name} held by {current_holder}, waiting for resource")
            time.sleep(self.retry_period_seconds)

        # Timeout - assume leadership to avoid deadlock
        logger.warning(f"Leader election timed out after {self.acquire_timeout_seconds}s, assuming leadership")
        return LeaderElectionResult(
            is_leader=True,
            lease_name=lease_name,
            holder_identity=self._holder_identity,
        )

    def _try_create_lease(self, name: str, namespace: str) -> bool:
        """Attempt to create a new lease. Returns True if successful (we're the leader)."""
        from kubernetes.client import ApiException
        from kubernetes.client.models import V1Lease, V1LeaseSpec, V1ObjectMeta

        lease = V1Lease(
            metadata=V1ObjectMeta(name=name, namespace=namespace),
            spec=V1LeaseSpec(
                holder_identity=self._holder_identity,
                lease_duration_seconds=self.lease_duration_seconds,
                acquire_time=datetime.now(timezone.utc),
            ),
        )

        try:
            self._coordination_api.create_namespaced_lease(namespace, lease)
            return True
        except ApiException as e:
            if e.status == 409:  # Already exists - someone else is the leader
                return False
            raise

    def _get_lease_holder(self, name: str, namespace: str) -> str | None:
        """Get the current holder identity from an existing lease."""
        from typing import cast

        from kubernetes.client import ApiException
        from kubernetes.client.models import V1Lease

        try:
            lease = cast(V1Lease, self._coordination_api.read_namespaced_lease(name=name, namespace=namespace))
            return lease.spec.holder_identity if lease.spec else None
        except ApiException as e:
            if e.status == 404:
                return None
            raise

    def _delete_lease(self, name: str, namespace: str) -> None:
        """Delete a lease."""
        from kubernetes.client import ApiException

        try:
            self._coordination_api.delete_namespaced_lease(name=name, namespace=namespace)
        except ApiException as e:
            if e.status != 404:
                raise

    def release(self, lease_name: str, namespace: str) -> None:
        """Release leadership by deleting the lease."""
        self._delete_lease(lease_name, namespace)
        logger.debug(f"Released lease {namespace}/{lease_name}")
