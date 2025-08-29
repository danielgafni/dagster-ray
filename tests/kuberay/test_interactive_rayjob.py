import socket
from typing import Any, cast

import pytest
import ray  # noqa: TID253
from dagster import AssetExecutionContext, asset, materialize_to_memory
from packaging.version import Version
from pytest_kubernetes.providers import AClusterManager

from dagster_ray import RayResource
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
def interative_rayjob_resource(
    k8s_with_kuberay: AClusterManager,
    dagster_ray_image: str,
    head_group_spec: dict[str, Any],
    worker_group_specs: list[dict[str, Any]],
    kuberay_version: str,
) -> KubeRayInteractiveJob:
    if Version(kuberay_version) < Version(MIN_KUBERAY_VERSION):
        pytest.skip(f"KubeRay {MIN_KUBERAY_VERSION} is required to use interactive mode with RayJob")

    return KubeRayInteractiveJob(
        image=dagster_ray_image,
        client=RayJobClient(config_file=str(k8s_with_kuberay.kubeconfig)),
        ray_job=InteractiveRayJobConfig(
            metadata={"namespace": NAMESPACE},
            spec=InteractiveRayJobSpec(
                ray_cluster_spec=RayClusterSpec(head_group_spec=head_group_spec, worker_group_specs=worker_group_specs),
            ),
        ),
        redis_port=get_random_free_port(),
        skip_init=True,
    )


@ray.remote
def get_hostname():
    return socket.gethostname()


def ensure_rayjob_correctness(
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
        # now we can access the head node
        # hack the _host attribute to point to the port-forwarded address
        rayjob._host = "127.0.0.1"
        rayjob.init_ray(context)  # normally this would happen automatically during resource setup
        assert rayjob.context is not None

        # make sure a @remote function runs inside the cluster
        # not in localhost
        assert rayjob.cluster_name in ray.get(get_hostname.remote())

        rayjob_description = rayjob.client.get(rayjob.job_name, namespace=rayjob.namespace)
        assert rayjob_description["metadata"]["labels"]["dagster.io/run-id"] == context.run_id


def test_interactive_rayjob(
    interative_rayjob_resource: KubeRayCluster,
    k8s_with_kuberay: AClusterManager,
):
    @asset
    # testing RayResource type annotation too!
    def my_asset(context: AssetExecutionContext, interactive_rayjob: RayResource) -> None:
        # port-forward to the head node
        # because it's not possible to access it otherwise

        assert isinstance(interactive_rayjob, KubeRayInteractiveJob)

        ensure_rayjob_correctness(
            interactive_rayjob,
            k8s_with_kuberay,
            context,
        )

    materialize_to_memory(
        [my_asset],
        resources={"interactive_rayjob": interative_rayjob_resource},
    )
