from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from typing import TYPE_CHECKING, Literal, TypedDict

from typing_extensions import NotRequired

from dagster_ray.kuberay.client.base import BaseKubeRayClient, load_kubeconfig
from dagster_ray.kuberay.client.raycluster import RayClusterClient, RayClusterStatus

if TYPE_CHECKING:
    from kubernetes.client import ApiClient

GROUP = "ray.io"
VERSION = "v1"
PLURAL = "rayjobs"
KIND = "RayJob"

logger = logging.getLogger(__name__)


class RayJobStatus(TypedDict):
    jobId: str
    jobDeploymentStatus: str
    rayClusterName: str
    rayClusterStatus: RayClusterStatus
    startTime: str

    dashboardURL: NotRequired[str]
    endTime: NotRequired[str]
    jobStatus: NotRequired[Literal["PENDING", "RUNNING", "SUCCEEDED", "FAILED", "STOPPED"]]
    message: NotRequired[str]


class RayJobClient(BaseKubeRayClient[RayJobStatus]):
    def __init__(
        self,
        config_file: str | None = None,
        context: str | None = None,
        api_client: ApiClient | None = None,
    ) -> None:
        # this call must happen BEFORE creating K8s apis
        load_kubeconfig(config_file=config_file, context=context)

        self.config_file = config_file
        self.context = context

        super().__init__(group=GROUP, version=VERSION, kind=KIND, plural=PLURAL, api_client=api_client)

    def get_ray_cluster_name(self, name: str, namespace: str) -> str:
        return self.get_status(name, namespace)["rayClusterName"]

    def get_job_sumission_id(self, name: str, namespace: str) -> str:
        return self.get_status(name, namespace)["jobId"]

    @property
    def ray_cluster_client(self) -> RayClusterClient:
        return RayClusterClient(config_file=self.config_file, context=self.context)

    def wait_until_running(
        self,
        name: str,
        namespace: str,
        timeout: int = 600,
        poll_interval: int = 5,
        terminate_on_timeout: bool = True,
        port_forward: bool = False,
    ) -> bool:
        start_time = time.time()

        while True:
            status = self.get_status(name, namespace, timeout, poll_interval).get("jobDeploymentStatus")

            if status in ["Running", "Complete"]:
                break
            elif status == "Failed":
                raise RuntimeError(f"RayJob {namespace}/{name} deployment failed. Status:\n{status}")

            if time.time() - start_time > timeout:
                if terminate_on_timeout:
                    logger.warning(f"Terminating RayJob {namespace}/{name} because of timeout {timeout}s")
                    try:
                        self.terminate(name, namespace, port_forward=port_forward)
                    except Exception as e:
                        logger.warning(
                            f"Failed to gracefully terminate RayJob {namespace}/{name}: {e}, will delete it instead."
                        )
                        self.delete(name, namespace)

                raise TimeoutError(f"Timed out waiting for RayJob {namespace}/{name} to start. Status:\n{status}")

            time.sleep(poll_interval)

        while True:
            status = self.get_status(name, namespace, timeout, poll_interval).get("jobStatus")

            if status:
                break

            if time.time() - start_time > timeout:
                raise TimeoutError(f"Timed out waiting for RayJob {namespace}/{name} to start. Status:\n{status}")

            time.sleep(poll_interval)

        return True

    def _wait_for_job_submission(
        self,
        name: str,
        namespace: str,
        timeout: int = 600,
        poll_interval: int = 10,
    ):
        start_time = time.time()

        while True:
            status = self.get_status(name, namespace)
            if status.get("jobDeploymentStatus") in ["Complete", "Failed"]:
                return

            if (job_status := status.get("jobStatus")) is not None:
                if job_status != "PENDING":
                    return

            if time.time() - start_time > timeout:
                raise TimeoutError(f"Timed out waiting for job {name} to start")

            logger.debug(f"RayJob {namespace}/{name} deployment status is {job_status}, waiting for it to start...")

            time.sleep(poll_interval)

    def get_job_logs(self, name: str, namespace: str, timeout: int = 60 * 60, port_forward: bool = False) -> str:
        self._wait_for_job_submission(name, namespace, timeout=timeout)
        with self.ray_cluster_client.job_submission_client(
            name=self.get_ray_cluster_name(name, namespace), namespace=namespace, port_forward=port_forward
        ) as job_submission_client:
            return job_submission_client.get_job_logs(job_id=self.get_job_sumission_id(name, namespace))

    def tail_job_logs(
        self, name: str, namespace: str, timeout: int = 60 * 60, port_forward: bool = False
    ) -> Iterator[str]:
        import asyncio

        self._wait_for_job_submission(name, namespace, timeout=timeout)
        with self.ray_cluster_client.job_submission_client(
            name=self.get_ray_cluster_name(name, namespace), namespace=namespace, port_forward=port_forward
        ) as job_submission_client:
            async_tailer = job_submission_client.tail_job_logs(job_id=self.get_job_sumission_id(name, namespace))

            # Backward compatible sync generator
            def tail_logs() -> Iterator[str]:
                while True:
                    try:
                        yield asyncio.get_event_loop().run_until_complete(async_tailer.__anext__())  # type: ignore
                    except StopAsyncIteration:
                        break

            yield from tail_logs()

    def terminate(self, name: str, namespace: str, port_forward: bool = False) -> bool:
        """
        Unlike the .delete method, this won't remove the Kubernetes object, but will instead stop the Ray Job.
        """
        with self.ray_cluster_client.job_submission_client(
            name=self.get_ray_cluster_name(name, namespace), namespace=namespace, port_forward=port_forward
        ) as job_submission_client:
            job_id = self.get_job_sumission_id(name, namespace)

            job_submitted = False

            while not job_submitted:
                jobs = job_submission_client.list_jobs()

                for job in jobs:
                    if job.submission_id == job_id:
                        job_submitted = True
                        break

                logger.debug(
                    f"Trying to terminate job {name}, but it wasn't submitted yet. Waiting for it to be submitted..."
                )
                time.sleep(10)

            return job_submission_client.stop_job(job_id=job_id)
