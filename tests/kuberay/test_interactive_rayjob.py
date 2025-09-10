import socket
import time
from typing import Any, Literal, cast

import dagster as dg
import pytest
import ray  # noqa: TID253
from dagster import AssetExecutionContext, asset, materialize_to_memory
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
from tests.kuberay.utils import NAMESPACE, get_random_free_port

MIN_KUBERAY_VERSION = "1.3.0"


@pytest.fixture(scope="session")
def rayjob_client(k8s_with_kuberay: AClusterManager) -> RayJobClient:
    return RayJobClient(config_file=str(k8s_with_kuberay.kubeconfig))


def test_instantiate_defaults():
    _ = KubeRayInteractiveJob()


def test_no_lifecycle(dagster_instance: dg.DagsterInstance, rayjob_client: RayJobClient):
    interactive_rayjob = KubeRayInteractiveJob(
        client=rayjob_client, lifecycle=Lifecycle(create=False, wait=False, connect=False)
    )

    @dg.asset
    def my_asset(interactive_rayjob: KubeRayInteractiveJob) -> None:
        return

    dg.materialize(assets=[my_asset], resources={"interactive_rayjob": interactive_rayjob}, instance=dagster_instance)


@pytest.mark.parametrize("cleanup", ["always", "never", "except_failure", "on_interrupt"])
@pytest.mark.parametrize("wait", [True, False])
@pytest.mark.parametrize("interrupt", [True, False])
@pytest.mark.slow
def test_cleanup(
    kuberay_version: str,
    dagster_instance: dg.DagsterInstance,
    rayjob_client: RayJobClient,
    dagster_ray_image: str,
    wait: bool,
    cleanup: Literal["always", "never", "except_failure", "on_interrupt"],
    interrupt: bool,
):
    if Version(kuberay_version) < Version(MIN_KUBERAY_VERSION):
        pytest.skip(f"KubeRay {MIN_KUBERAY_VERSION} is required to use interactive mode with RayJob")

    interactive_rayjob = KubeRayInteractiveJob(
        image=dagster_ray_image,
        client=rayjob_client,
        redis_port=get_random_free_port(),
        ray_job=InteractiveRayJobConfig(
            metadata={"namespace": NAMESPACE},
        ),
        lifecycle=Lifecycle(create=True, wait=wait, connect=False, cleanup=cleanup),
    )

    @dg.asset
    def my_asset(interactive_rayjob: KubeRayInteractiveJob) -> None:
        if interrupt:
            raise KeyboardInterrupt("Intentional interruption")

    res = dg.materialize(
        assets=[my_asset],
        resources={"interactive_rayjob": interactive_rayjob},
        instance=dagster_instance,
        raise_on_error=False,
    )

    jobs = rayjob_client.list(namespace=interactive_rayjob.namespace, label_selector=f"dagster/run-id={res.run_id}")[
        "items"
    ]

    if cleanup == "always":
        assert len(jobs) == 0
    elif cleanup == "never":
        assert len(jobs) == 1
    elif cleanup == "except_failure":
        if res.success:
            assert len(jobs) == 0
        else:
            assert len(jobs) == 1
    elif cleanup == "on_interrupt":
        if interrupt:
            assert len(jobs) == 0
        else:
            assert len(jobs) == 1


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
    )


@ray.remote
def get_hostname():
    return socket.gethostname()


def ensure_interactive_rayjob_correctness(
    rayjob: KubeRayInteractiveJob,
    k8s_with_kuberay: AClusterManager,
    context: AssetExecutionContext,
):
    with k8s_with_kuberay.port_forwarding(
        target=f"svc/{rayjob.cluster_name}-head-svc",
        source_port=cast(int, rayjob.redis_port),
        target_port=10001,
        namespace=rayjob.namespace,
    ):
        assert rayjob.client.get_status(rayjob.job_name, rayjob.namespace)["jobDeploymentStatus"] == "Waiting"  # pyright: ignore[reportTypedDictNotRequiredAccess]

        # now we can access the head node
        # hack the _host attribute to point to the port-forwarded address
        rayjob._host = "127.0.0.1"
        rayjob.connect(context)  # normally this would happen automatically during resource setup
        assert rayjob.context is not None

        time.sleep(1)

        job_status = rayjob.client.get_status(rayjob.job_name, rayjob.namespace).get("jobStatus")
        assert job_status == "RUNNING", job_status

        # make sure a @remote function runs inside the cluster
        # not in localhost
        assert rayjob.cluster_name in ray.get(get_hostname.remote())

        rayjob_description = rayjob.client.get(rayjob.job_name, namespace=rayjob.namespace)
        assert rayjob_description["metadata"]["labels"]["dagster/run-id"] == context.run_id


def test_interactive_rayjob(
    interactive_rayjob_resource: KubeRayCluster,
    k8s_with_kuberay: AClusterManager,
):
    @asset
    # testing RayResource type annotation too!
    def my_asset(context: AssetExecutionContext, interactive_rayjob: RayResource) -> None:
        # port-forward to the head node
        # because it's not possible to access it otherwise

        assert isinstance(interactive_rayjob, KubeRayInteractiveJob)

        ensure_interactive_rayjob_correctness(
            interactive_rayjob,
            k8s_with_kuberay,
            context,
        )

    materialize_to_memory(
        [my_asset],
        resources={"interactive_rayjob": interactive_rayjob_resource},
    )
