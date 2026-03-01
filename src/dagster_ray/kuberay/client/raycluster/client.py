from __future__ import annotations

import logging
import socket
import subprocess
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from io import FileIO
from queue import Queue
from typing import TYPE_CHECKING, Any, TypedDict, cast

import urllib3
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed
from typing_extensions import NotRequired

from dagster_ray.kuberay.client.base import BaseKubeRayClient, load_kubeconfig

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from kubernetes.client import ApiClient
    from ray.job_submission import JobSubmissionClient


GROUP = "ray.io"
VERSION = "v1"
PLURAL = "rayclusters"
KIND = "RayCluster"


def enqueue_output(out: FileIO, queue: Queue, should_stop: Callable[[], bool]):
    import select

    try:
        while True:
            if should_stop():
                break
            rlist, _, _ = select.select([out], [], [], 1.0)
            if not rlist:
                continue
            line = out.readline()
            if not line:
                break
            queue.put(line)
    except Exception:
        logger.exception("Exception encountered while reading port-forwarded RayCluster logs")
    finally:
        try:
            out.close()
        except Exception:
            logger.exception("Exception encountered while closing RayCluster logs during port-forwarding")


def get_random_available_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))  # Bind to an available port chosen by the OS
        _, port = s.getsockname()  # Get the port number assigned
        return port


class RayClusterEndpoints(TypedDict):  # these are ports
    client: str
    dashboard: str
    metrics: str
    redis: str
    serve: str


class RayClusterHead(TypedDict):
    podIP: NotRequired[str]
    podName: NotRequired[str]
    serviceIP: NotRequired[str]
    serviceName: NotRequired[str]


class RayClusterStatus(TypedDict):
    desiredCPU: str
    desiredGPU: str
    desiredMemory: str
    desiredTPU: str

    lastUpdateTime: str  # "2024-09-16T09:34:29Z"
    maxWorkerReplicas: int
    observedGeneration: int

    head: NotRequired[RayClusterHead]
    endpoints: NotRequired[RayClusterEndpoints]
    state: NotRequired[str]
    conditions: NotRequired[list[dict[str, Any]]]


class RayClusterClient(BaseKubeRayClient[RayClusterStatus]):
    def __init__(
        self,
        kube_config: str | None = None,
        kube_context: str | None = None,
        api_client: ApiClient | None = None,
    ) -> None:
        self.kube_config = kube_config
        self.kube_context = kube_context

        # note: this call must happen BEFORE creating the api clients
        if api_client is None:
            load_kubeconfig(context=self.kube_context, config_file=self.kube_config)

        super().__init__(group=GROUP, version=VERSION, kind=KIND, plural=PLURAL, api_client=api_client)

    def wait_until_ready(
        self,
        name: str,
        namespace: str,
        timeout: float,
        failure_tolerance_timeout: float = 0.0,
        poll_interval: float = 5.0,
        log_cluster_conditions: bool = False,
    ) -> tuple[str, RayClusterEndpoints]:
        """
        If ready, returns service ip address and a dictionary of ports.

        Parameters:
            name (str): The name of the `RayCluster` resource
            namespace (str): The namespace of the `RayCluster` resource
            timeout (float): The timeout in seconds to wait for the cluster to become ready.
            failure_tolerance_timeout (float): The period in seconds to wait for the cluster to transition out of `failed` state if it reaches it. This state can be transient under certain conditions. With the default value of 0, the first `failed` state appearance will raise an exception immediately.
            poll_interval (float): The interval in seconds to poll the cluster status.
            log_cluster_conditions (bool): Whether to log cluster conditions. See [KubeRay docs](https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/observability.html#raycluster-status-conditions)

        Returns:
            tuple[str, RayClusterEndpoints]: The service ip address and a dictionary of ports.
        """
        start_time = time.time()

        condition_index_to_log = 0

        @retry(
            stop=stop_after_attempt(30),
            wait=wait_fixed(2),
            # ignore a very specific error which happens rarely under certain conditions
            retry=retry_if_exception_type(urllib3.exceptions.ProtocolError),
            reraise=True,
        )
        def get_status_with_retry() -> RayClusterStatus:
            return self.get_status(name=name, namespace=namespace, timeout=timeout)

        status = get_status_with_retry()

        while time.time() - start_time < timeout:
            status = get_status_with_retry()
            state = status.get("state")

            if not state:
                continue

            # https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/observability.html#raycluster-status-conditions
            conditions = list(reversed(status.get("conditions", [])))

            if log_cluster_conditions:
                while len(conditions) > condition_index_to_log:
                    logger.info(f"RayCluster {namespace}/{name} condition: {conditions[condition_index_to_log]}")
                    condition_index_to_log += 1

            if state == "failed" and (time.time() - start_time > failure_tolerance_timeout):
                error_msg = f"RayCluster {namespace}/{name} status transitioned into `failed`. Check the logs for more info. Consider increasing `failure_tolerance_timeout` ({failure_tolerance_timeout:.1f}s). Status:\n\n{status}\n\nMore details: `kubectl -n {namespace} describe RayCluster {name}`"

                logs = self._read_head_pod_logs(status=status, namespace=namespace, tail_lines=500)
                if logs:
                    logger.error(logs)

                raise RuntimeError(error_msg)

            if (
                state == "ready"
                and status.get("endpoints", {}).get("dashboard")
                and (head := status.get("head"))
                and head.get("serviceIP")
                and head.get("serviceName")
            ):
                # TODO: this should return serviceName instead
                # to support multi-cluster networking
                logger.debug(f"RayCluster {namespace}/{name} is ready!")
                return head["serviceIP"], status["endpoints"]  # pyright: ignore[reportTypedDictNotRequiredAccess]

            time.sleep(poll_interval)
        else:
            # Try to fetch head pod logs for better debugging
            error_msg = f"Timed out ({timeout:.1f}s) waiting for RayCluster {namespace}/{name} to be ready. Check the logs for more info. Status: {status}"

            logs = self._read_head_pod_logs(status=status, namespace=namespace, tail_lines=500)
            if logs:
                logger.error(logs)

            raise TimeoutError(error_msg)

    @contextmanager
    def port_forward(
        self,
        name: str,
        namespace: str,
        local_dashboard_port: int = 8265,
        local_gcs_port: int = 10001,
    ) -> Iterator[tuple[int, int]]:
        """
        Port forwards the Ray dashboard and GCS ports to localhost.
        Use 0 for local_dashboard_port and local_gcs_port to get random available ports.
        Returns the ports that the dashboard and GCS are forwarded to.
        """
        if local_dashboard_port == 0:
            local_dashboard_port = get_random_available_port()

        if local_gcs_port == 0:
            local_gcs_port = get_random_available_port()

        service = f"{name}-head-svc"

        if not self.get_status(name, namespace):
            raise RuntimeError(f"RayCluster {name} does not exist in namespace {namespace}")

        self.wait_for_service_endpoints(service_name=service, namespace=namespace)

        cmd = [
            "kubectl",
            "port-forward",
            "-n",
            namespace,
            f"svc/{service}",
            f"{local_dashboard_port}:8265",
            f"{local_gcs_port}:10001",
        ]

        if self.kube_context:
            cmd.extend(["--context", self.kube_context])

        if self.kube_config:
            cmd.extend(["--kubeconfig", self.kube_config])

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        queue = Queue()
        SHOULD_STOP = False

        def should_stop():
            return SHOULD_STOP

        t = threading.Thread(target=enqueue_output, args=(process.stdout, queue, should_stop))
        t.daemon = True  # thread dies with the program
        t.start()

        time.sleep(0.01)

        try:
            # inspect stdout to avoid connecting too early
            # we must check for "Forwarding from" in stdout
            #

            while True:
                if process.poll() is not None:
                    raise RuntimeError(
                        f"port-forwarding command: `{' '.join(cmd)}` failed. Most likely ports {local_dashboard_port} or {local_gcs_port} are  already in use, or Kubernetes service {name}-head-svc does not exist in namespace {namespace}."
                    )

                line = queue.get()

                if "Forwarding from" in line:
                    break

            logger.info(
                f"Connecting to {namespace}/{name} via port-forwarding for ports {local_dashboard_port} and {local_gcs_port}..."
            )

            yield local_dashboard_port, local_gcs_port
        finally:
            # terminate the thread
            SHOULD_STOP = True
            process.kill()
            process.wait()
            t.join()
            logger.info(f"Port-forwarding for ports {local_dashboard_port} and {local_gcs_port} has been stopped.")

    @contextmanager
    def job_submission_client(
        self,
        name: str,
        namespace: str,
        port_forward: bool = False,
        timeout: float = 60,
        address: str | None = None,
        headers: dict[str, Any] | None = None,
        verify: str | bool | None = None,
        cookies: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Iterator[JobSubmissionClient]:
        """
        Returns a JobSubmissionClient object that can be used to interact with Ray jobs running in the KubeRay cluster.

        Args:
            name: Name of the RayCluster resource
            namespace: Namespace of the RayCluster resource
            port_forward: If True, port forward the dashboard to localhost
            timeout: Timeout for various operations
            address: Custom Ray dashboard address (e.g., "https://ray-cluster.example.com").
                When provided, connects directly to this address instead of using port-forwarding or in-cluster service IPs.
            headers: HTTP headers for dashboard authentication (e.g., {"Authorization": "Bearer token"}).
                Only used when custom address is provided.
            verify: TLS certificate verification for custom address. Can be True, False, or path to CA bundle.
                Only used when custom address is provided. Defaults to True when address is set.
            cookies: Cookies to use when sending requests to the HTTP job server.
                Only used when custom address is provided.
            metadata: Arbitrary metadata to store along with all jobs. Will be merged with per-job metadata.
                Only used when custom address is provided.
        """

        from ray.job_submission import JobSubmissionClient

        if address:
            yield JobSubmissionClient(
                address=address,
                headers=headers,
                verify=verify if verify is not None else True,
                cookies=cookies,
                metadata=metadata,
            )
        elif port_forward:
            self.wait_for_service_endpoints(service_name=f"{name}-head-svc", namespace=namespace, timeout=timeout)
            with self.port_forward(name=name, namespace=namespace, local_dashboard_port=0, local_gcs_port=0) as (
                local_dashboard_port,
                _,
            ):
                yield JobSubmissionClient(address=f"http://localhost:{local_dashboard_port}")
        else:
            # TODO: revisit the decision to use this as context-manager in this case
            status = self.get_status(name, namespace)

            host = status["head"]["serviceIP"]  # type: ignore
            dashboard_port = status["endpoints"]["dashboard"]  # type: ignore

            yield JobSubmissionClient(address=f"http://{host}:{dashboard_port}")

    def _read_head_pod_logs(self, status: RayClusterStatus, namespace: str, tail_lines: int = 500) -> str | None:
        if not ((head := status.get("head")) and (pod_name := head.get("podName"))):
            return None

        logs_output = []

        try:
            # First, get the pod to list all containers
            pod = self._core_v1_api.read_namespaced_pod(name=pod_name, namespace=namespace)

            if pod and pod.spec:  # pyright: ignore
                # Fetch logs from all containers except autoscaler
                for container in pod.spec.containers:  # pyright: ignore
                    container_name = container.name

                    # Skip autoscaler container - we only care about the main Ray head container
                    if container_name == "autoscaler":
                        continue

                    try:
                        container_logs = cast(
                            str,
                            self._core_v1_api.read_namespaced_pod_log(
                                name=pod_name,
                                namespace=namespace,
                                container=container_name,
                                tail_lines=tail_lines,
                            ),
                        )
                        logs_output.append(f"=== Container: {container_name} (last {tail_lines} lines) ===")
                        logs_output.append(container_logs)
                        logs_output.append("")  # Empty line between containers
                    except Exception:
                        logger.exception(
                            f"Failed to fetch logs for container {container_name} in pod {namespace}/{pod_name}"
                        )

            return "\n".join(logs_output) if logs_output else ""

        except Exception:
            logger.exception(f"Failed to fetch head pod {namespace}/{pod_name} metadata or logs")
            return None
