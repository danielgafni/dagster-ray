import logging
import socket
import subprocess
import threading
import time
from contextlib import contextmanager
from io import FileIO
from queue import Queue
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    Optional,
    Tuple,
    TypedDict,
    cast,
)

from kubernetes import watch
from typing_extensions import NotRequired

from dagster_ray.kuberay.client.base import BaseKubeRayClient

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ray.job_submission import JobSubmissionClient


GROUP = "ray.io"
VERSION = "v1"
PLURAL = "rayclusters"
KIND = "RayCluster"


def enqueue_output(out: FileIO, queue: Queue, should_stop):
    for line in iter(out.readline, b""):
        if should_stop():
            return
        queue.put(line)
    out.close()


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
    podIP: str
    serviceIP: str


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


class RayClusterClient(BaseKubeRayClient):
    def __init__(self, config_file: Optional[str] = None, context: Optional[str] = None) -> None:
        super().__init__(
            group=GROUP,
            version=VERSION,
            kind=KIND,
            plural=PLURAL,
        )

        # these are only used because of kubectl port-forward CLI command
        # TODO: remove kubectl usage and remove these attributes
        self.config_file = config_file
        self.context = context

    def get_status(self, name: str, namespace: str, timeout: int = 60, poll_interval: int = 5) -> RayClusterStatus:  # type: ignore
        return cast(
            RayClusterStatus,
            super().get_status(name=name, namespace=namespace, timeout=timeout, poll_interval=poll_interval),
        )

    def wait_until_ready(
        self,
        name: str,
        namespace: str,
        timeout: int,
        image: Optional[str] = None,
    ) -> Tuple[str, Dict[str, str]]:
        """
        If ready, returns service ip address and a dictionary of ports.
        Dictionary keys: ["client", "dashboard", "metrics", "redis", "serve"]
        """

        w = watch.Watch()

        start_time = time.time()

        # TODO: use get_namespaced_custom_object instead
        # once https://github.com/kubernetes-client/python/issues/1679
        # is solved

        for event in w.stream(
            self._api.list_namespaced_custom_object,
            self.group,
            self.version,
            namespace,
            self.plural,
        ):
            item = cast(Dict[str, Any], event["raw_object"])  # type: ignore

            if "status" not in item:
                continue

            status: RayClusterStatus = item["status"]

            if status.get("state") == "failed":
                raise Exception(
                    f"RayCluster {namespace}/{name} failed to start. Reason:\n{status.get('reason')}\nMore details: `kubectl -n {namespace} describe RayCluster {name}`"
                )

            if (
                item.get("metadata")  # type: ignore
                and item["metadata"].get("name") == name  # type: ignore
                and status.get("state") == "ready"
                and status.get("head")
            ):
                if image is not None:
                    if (
                        item.get("spec")  # type: ignore
                        and item["spec"]["headGroupSpec"]["template"]["spec"]["containers"][0]["image"]  # type: ignore
                        != image
                    ):
                        continue
                w.stop()
                logger.debug(f"RayCluster {namespace}/{name} is ready!")
                return status["head"]["serviceIP"], status["endpoints"]  # type: ignore

            if time.time() - start_time > timeout:
                w.stop()
                raise TimeoutError(
                    f"Timed out waiting for RayCluster {namespace}/{name} to be ready. " f"Status: {status}"
                )

        raise Exception("This code should be unreachable")

    @contextmanager
    def port_forward(
        self,
        name: str,
        namespace: str,
        local_dashboard_port: int = 8265,
        local_gcs_port: int = 10001,
    ) -> Iterator[Tuple[int, int]]:
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

        if self.context:
            cmd.extend(["--context", self.context])

        if self.config_file:
            cmd.extend(["--kubeconfig", self.config_file])

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
        self, name: str, namespace: str, port_forward: bool = False, timeout: int = 60
    ) -> Iterator["JobSubmissionClient"]:
        """
        Returns a JobSubmissionClient object that can be used to interact with Ray jobs running in the KubeRay cluster.
        If port_forward is True, it will port forward the dashboard and GCS ports to localhost, and should be used in a context manager.
        If port_forward is False, the client will connect to the dashboard directly (assuming the dashboard is accessible from the host).
        """

        from ray.job_submission import JobSubmissionClient

        if not port_forward:
            # TODO: revisit the decision to use this as context-manager in this case
            #
            status = self.get_status(name, namespace)

            host = status["head"]["serviceIP"]  # type: ignore
            dashboard_port = status["endpoints"]["dashboard"]  # type: ignore

            yield JobSubmissionClient(address=f"http://{host}:{dashboard_port}")
        else:
            self.wait_for_service_endpoints(service_name=f"{name}-head-svc", namespace=namespace, timeout=timeout)
            with self.port_forward(name=name, namespace=namespace, local_dashboard_port=0, local_gcs_port=0) as (
                local_dashboard_port,
                _,
            ):
                yield JobSubmissionClient(address=f"http://localhost:{local_dashboard_port}")
