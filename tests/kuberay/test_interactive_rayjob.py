import socket
import sys
import time
from pathlib import Path
from typing import Any, Literal, cast

import dagster as dg
import pytest
import ray  # noqa: TID253
from packaging.version import Version
from pytest_kubernetes.providers import AClusterManager

from dagster_ray import RayResource
from dagster_ray._base.resources import Lifecycle
from dagster_ray.kuberay import (
    KubeRayCluster,
    KubeRayInteractiveJob,
)
from dagster_ray.kuberay.client.rayjob.client import RayJobClient
from dagster_ray.kuberay.configs import RayClusterSpec
from dagster_ray.kuberay.resources.rayjob import InteractiveRayJobConfig, InteractiveRayJobSpec
from tests.kuberay.conftest import KUBERNETES_CONTEXT, RAYJOB_TIMEOUT
from tests.kuberay.utils import NAMESPACE, get_random_free_port

MIN_KUBERAY_VERSION = "1.3.0"


@pytest.fixture(scope="session")
def rayjob_client(k8s_with_kuberay: AClusterManager) -> RayJobClient:
    return RayJobClient(kube_config=str(k8s_with_kuberay.kubeconfig), kube_context=KUBERNETES_CONTEXT)


def test_instantiate_defaults():
    _ = KubeRayInteractiveJob()


def test_env_vars_injection():
    interactive_rayjob = KubeRayInteractiveJob(
        image="test-image",
        env_vars={
            "RAY_LOG_TO_STDERR": "1",
            "RAY_LOGGING_CONFIG_ENCODING": "JSON",
        },
    )
    interactive_rayjob._name = "test-rayjob"
    context = dg.build_init_resource_context()

    k8s_manifest = interactive_rayjob.ray_job.to_k8s(
        context,
        image="test-image",
        env_vars=interactive_rayjob.get_env_vars_to_inject(),
    )

    # Check that env_vars are injected into head and worker group specs
    ray_cluster_spec = k8s_manifest["spec"]["rayClusterSpec"]
    head_group_spec = ray_cluster_spec["headGroupSpec"]
    worker_group_specs = ray_cluster_spec.get("workerGroupSpecs", [])

    for group_spec in [head_group_spec, *worker_group_specs]:
        for container in group_spec["template"]["spec"]["containers"]:
            assert {"name": "RAY_LOG_TO_STDERR", "value": "1"} in container["env"], container
            assert {"name": "RAY_LOGGING_CONFIG_ENCODING", "value": "JSON"} in container["env"], container


def test_env_vars_with_debug_flags():
    interactive_rayjob = KubeRayInteractiveJob(
        image="test-image",
        enable_debug_post_mortem=True,
        enable_tracing=True,
        enable_actor_task_logging=True,
    )
    interactive_rayjob._name = "test-rayjob"
    context = dg.build_init_resource_context()

    k8s_manifest = interactive_rayjob.ray_job.to_k8s(
        context,
        image="test-image",
        env_vars=interactive_rayjob.get_env_vars_to_inject(),
    )

    ray_cluster_spec = k8s_manifest["spec"]["rayClusterSpec"]
    head_group_spec = ray_cluster_spec["headGroupSpec"]
    worker_group_specs = ray_cluster_spec.get("workerGroupSpecs", [])

    for group_spec in [head_group_spec, *worker_group_specs]:
        for container in group_spec["template"]["spec"]["containers"]:
            assert {"name": "RAY_DEBUG_POST_MORTEM", "value": "1"} in container["env"], container
            assert {"name": "RAY_PROFILING", "value": "1"} in container["env"], container
            assert {"name": "RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING", "value": "1"} in container["env"], container


def test_no_lifecycle(dagster_instance: dg.DagsterInstance, rayjob_client: RayJobClient):
    interactive_rayjob = KubeRayInteractiveJob(
        client=rayjob_client, lifecycle=Lifecycle(create=False, wait=False, connect=False)
    )

    @dg.asset
    def my_asset(interactive_rayjob: KubeRayInteractiveJob) -> None:
        return

    dg.materialize(assets=[my_asset], resources={"interactive_rayjob": interactive_rayjob}, instance=dagster_instance)


@pytest.mark.parametrize("cleanup", ["always", "never"])
@pytest.mark.parametrize("wait", [True, False])
def test_cleanup(
    head_group_spec: dict[str, Any],
    worker_group_specs: list[dict[str, Any]],
    k8s_with_kuberay: AClusterManager,
    kuberay_version: str,
    dagster_instance: dg.DagsterInstance,
    rayjob_client: RayJobClient,
    dagster_ray_image: str,
    wait: bool,
    cleanup: Literal["always", "never", "on_exception"],
):
    if Version(kuberay_version) < Version(MIN_KUBERAY_VERSION):
        pytest.skip(f"KubeRay {MIN_KUBERAY_VERSION} is required to use interactive mode with RayJob")

    interactive_rayjob = KubeRayInteractiveJob(
        image=dagster_ray_image,
        client=rayjob_client,
        redis_port=get_random_free_port(),
        ray_job=InteractiveRayJobConfig(
            metadata={"namespace": NAMESPACE},
            spec=InteractiveRayJobSpec(
                ray_cluster_spec=RayClusterSpec(head_group_spec=head_group_spec, worker_group_specs=worker_group_specs),
            ),
        ),
        lifecycle=Lifecycle(create=True, wait=wait, connect=False, cleanup=cleanup),
        timeout=RAYJOB_TIMEOUT,
    )

    @dg.asset
    def my_asset(interactive_rayjob: KubeRayInteractiveJob) -> None:
        return

    res = dg.materialize(
        assets=[my_asset],
        resources={"interactive_rayjob": interactive_rayjob},
        instance=dagster_instance,
        raise_on_error=False,
    )

    # Wait for cleanup to complete with retry mechanism
    max_wait_time = 30  # seconds
    wait_interval = 1  # seconds
    start_time = time.time()

    while time.time() - start_time < max_wait_time:
        jobs = rayjob_client.list(
            namespace=interactive_rayjob.namespace, label_selector=f"dagster/run-id={res.run_id}"
        )["items"]

        # Filter out jobs that are being deleted (have deletionTimestamp)
        active_jobs = [job for job in jobs if job.get("metadata", {}).get("deletionTimestamp") is None]

        if cleanup == "always" and len(active_jobs) == 0:
            break
        elif cleanup == "never" and len(active_jobs) == 1:
            break

        time.sleep(wait_interval)

    # Final check
    jobs = rayjob_client.list(namespace=interactive_rayjob.namespace, label_selector=f"dagster/run-id={res.run_id}")[
        "items"
    ]
    active_jobs = [job for job in jobs if job.get("metadata", {}).get("deletionTimestamp") is None]

    if cleanup == "always":
        assert len(active_jobs) == 0, (
            f"Expected 0 active jobs after cleanup, but found {len(active_jobs)} active jobs out of {len(jobs)} total jobs"
        )
    elif cleanup == "never":
        assert len(active_jobs) == 1, (
            f"Expected 1 active job when cleanup is disabled, but found {len(active_jobs)} active jobs out of {len(jobs)} total jobs"
        )


@pytest.mark.parametrize("cleanup", ["always", "on_exception"])
def test_cleanup_on_interrupt(
    head_group_spec: dict[str, Any],
    worker_group_specs: list[dict[str, Any]],
    k8s_with_kuberay: AClusterManager,
    kuberay_version: str,
    dagster_instance: dg.DagsterInstance,
    rayjob_client: RayJobClient,
    cleanup: str,
):
    import json
    import signal
    import subprocess

    if Version(kuberay_version) < Version(MIN_KUBERAY_VERSION):
        pytest.skip(f"KubeRay {MIN_KUBERAY_VERSION} is required to use interactive mode with RayJob")

    # Path to the separate test script
    script_path = str(Path(__file__).parents[1] / "scripts/launch_dagster_run_with_kuberay_interactive_job.py")

    assert Path(script_path).exists()

    # Build command line arguments
    cmd_args = [
        sys.executable,
        script_path,
        "--config-file",
        str(k8s_with_kuberay.kubeconfig),
        "--context",
        KUBERNETES_CONTEXT,
        "--image",
        "invalid-image:nonexistent",  # this will cause the RayJob to hang, we need it to test the interrupt cleanup functionality
        "--redis-port",
        str(get_random_free_port()),
        "--namespace",
        NAMESPACE,
        "--head-group-spec",
        json.dumps(head_group_spec),
        "--worker-group-specs",
        json.dumps(worker_group_specs),
        "--cleanup",
        cleanup,
    ]

    # Start the subprocess with stdout capture
    process = subprocess.Popen(cmd_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Wait a bit to let the RayJob get created
    time.sleep(10)

    # Interrupt the process
    process.send_signal(signal.SIGTERM)

    # Wait for process to terminate and capture output
    try:
        stdout, stderr = process.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate()

    # Extract run ID from stdout (last line should be the run ID)
    assert stdout, "Expected to get stdout from the subprocess"
    lines = stdout.strip().split("\n")
    run_id = None
    for line in reversed(lines):
        if line.strip() and "-" in line.strip():  # Run IDs have dashes
            run_id = line.strip()
            break

    assert run_id, f"Expected to get a run ID from subprocess output, got: {stdout}"

    # Wait for cleanup to complete with retry mechanism
    max_wait_time = 30  # seconds
    wait_interval = 1  # seconds
    start_time = time.time()

    while time.time() - start_time < max_wait_time:
        jobs = rayjob_client.list(namespace=NAMESPACE, label_selector=f"dagster/run-id={run_id}")["items"]

        # Filter out jobs that are being deleted (have deletionTimestamp)
        active_jobs = [job for job in jobs if job.get("metadata", {}).get("deletionTimestamp") is None]

        if len(active_jobs) == 0:
            break

        time.sleep(wait_interval)

    # Final verification that RayJob has been cleaned up for this specific run
    final_jobs = rayjob_client.list(namespace=NAMESPACE, label_selector=f"dagster/run-id={run_id}")["items"]
    active_final_jobs = [job for job in final_jobs if job.get("metadata", {}).get("deletionTimestamp") is None]

    # The cleanup should have removed the RayJob created by this test
    assert len(active_final_jobs) == 0, (
        f"RayJob with run-id {run_id} should have been cleaned up, but found {len(active_final_jobs)} active jobs out of {len(final_jobs)} total jobs"
    )

    # Ensure no leftover jobs in case test failed
    try:
        jobs = rayjob_client.list(namespace=NAMESPACE)["items"]
        for job in jobs:
            rayjob_client.delete(job["metadata"]["name"], namespace=NAMESPACE)
    except Exception:
        pass  # Best effort cleanup


@pytest.fixture(scope="session")
def interactive_rayjob_resource(
    rayjob_client: RayJobClient,
    dagster_ray_image: str,
    head_group_spec: dict[str, Any],
    worker_group_specs: list[dict[str, Any]],
    kuberay_version: str,
) -> KubeRayInteractiveJob:
    if Version(kuberay_version) < Version(MIN_KUBERAY_VERSION):
        pytest.skip(f"KubeRay {MIN_KUBERAY_VERSION} is required to use interactive mode with RayJob")

    return KubeRayInteractiveJob(
        image=dagster_ray_image,
        client=rayjob_client,
        ray_job=InteractiveRayJobConfig(
            metadata={"namespace": NAMESPACE},
            spec=InteractiveRayJobSpec(
                ray_cluster_spec=RayClusterSpec(head_group_spec=head_group_spec, worker_group_specs=worker_group_specs),
            ),
        ),
        redis_port=get_random_free_port(),
        lifecycle=Lifecycle(connect=False),
        timeout=RAYJOB_TIMEOUT,
    )


@ray.remote
def get_hostname():
    return socket.gethostname()


def ensure_interactive_rayjob_correctness(
    rayjob: KubeRayInteractiveJob,
    k8s_with_kuberay: AClusterManager,
    context: dg.AssetExecutionContext,
):
    with k8s_with_kuberay.port_forwarding(
        target=f"svc/{rayjob.cluster_name}-head-svc",
        source_port=cast(int, rayjob.redis_port),
        target_port=10001,
        namespace=rayjob.namespace,
    ):
        assert rayjob.client.get_status(rayjob.name, rayjob.namespace)["jobDeploymentStatus"] == "Waiting"  # pyright: ignore[reportTypedDictNotRequiredAccess]

        # now we can access the head node
        # hack the _host attribute to point to the port-forwarded address
        rayjob._host = "127.0.0.1"
        rayjob.connect(context)  # normally this would happen automatically during resource setup
        assert rayjob.context is not None

        time.sleep(1)

        job_status = rayjob.client.get_status(rayjob.name, rayjob.namespace).get("jobStatus")
        assert job_status == "RUNNING", job_status

        # make sure a @remote function runs inside the cluster
        # not in localhost
        assert rayjob.cluster_name in ray.get(get_hostname.remote())

        rayjob_description = rayjob.client.get(rayjob.name, namespace=rayjob.namespace)
        assert rayjob_description["metadata"]["labels"]["dagster/run-id"] == context.run_id


def test_interactive_rayjob(
    interactive_rayjob_resource: KubeRayCluster,
    k8s_with_kuberay: AClusterManager,
):
    @dg.asset
    # testing RayResource type annotation too!
    def my_asset(context: dg.AssetExecutionContext, interactive_rayjob: RayResource) -> None:
        # port-forward to the head node
        # because it's not possible to access it otherwise

        assert isinstance(interactive_rayjob, KubeRayInteractiveJob)

        ensure_interactive_rayjob_correctness(
            interactive_rayjob,
            k8s_with_kuberay,
            context,
        )

    dg.materialize_to_memory(
        [my_asset],
        resources={"interactive_rayjob": interactive_rayjob_resource},
    )
