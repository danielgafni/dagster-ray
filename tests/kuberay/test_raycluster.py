import socket
from typing import Any, cast

import dagster as dg
import pytest
import ray  # noqa: TID253
from dagster import AssetExecutionContext, RunConfig, asset, materialize_to_memory
from pytest_kubernetes.providers import AClusterManager

from dagster_ray import RayResource
from dagster_ray.kuberay import KubeRayCluster, RayClusterClientResource, RayClusterConfig, cleanup_kuberay_clusters
from dagster_ray.kuberay.client import RayClusterClient
from dagster_ray.kuberay.ops import CleanupKuberayClustersConfig
from tests.kuberay.utils import NAMESPACE, get_random_free_port


@pytest.fixture(scope="session")
def ray_cluster_resource(
    k8s_with_kuberay: AClusterManager,
    dagster_ray_image: str,
    head_group_spec: dict[str, Any],
    worker_group_specs: list[dict[str, Any]],
) -> KubeRayCluster:
    redis_port = get_random_free_port()

    return KubeRayCluster(
        # have have to first run port-forwarding with minikube
        # we can only init ray after that
        skip_init=True,
        client=RayClusterClientResource(kubeconfig_file=str(k8s_with_kuberay.kubeconfig)),
        ray_cluster=RayClusterConfig(
            image=dagster_ray_image,
            namespace=NAMESPACE,
            head_group_spec=head_group_spec,
            worker_group_specs=worker_group_specs,
        ),
        redis_port=redis_port,
    )


@pytest.fixture(scope="session")
def ray_cluster_resource_skip_cleanup(
    k8s_with_kuberay: AClusterManager,
    dagster_ray_image: str,
    head_group_spec: dict[str, Any],
    worker_group_specs: list[dict[str, Any]],
) -> KubeRayCluster:
    redis_port = get_random_free_port()

    return KubeRayCluster(
        # have have to first run port-forwarding with minikube
        # we can only init ray after that
        skip_init=True,
        skip_cleanup=True,
        client=RayClusterClientResource(kubeconfig_file=str(k8s_with_kuberay.kubeconfig)),
        ray_cluster=RayClusterConfig(
            image=dagster_ray_image,
            namespace=NAMESPACE,
            head_group_spec=head_group_spec,
            worker_group_specs=worker_group_specs,
        ),
        redis_port=redis_port,
    )


@ray.remote
def get_hostname():
    return socket.gethostname()


def test_kuberay_cluster_resource(
    ray_cluster_resource: KubeRayCluster,
    k8s_with_kuberay: AClusterManager,
):
    @asset
    # testing RayResource type annotation too!
    def my_asset(context: AssetExecutionContext, ray_cluster: RayResource) -> None:
        # port-forward to the head node
        # because it's not possible to access it otherwise

        assert isinstance(ray_cluster, KubeRayCluster)

        with k8s_with_kuberay.port_forwarding(
            target=f"svc/{ray_cluster.cluster_name}-head-svc",
            source_port=cast(int, ray_cluster.redis_port),
            target_port=10001,
            namespace=ray_cluster.namespace,
        ):
            # now we can access the head node
            # hack the _host attribute to point to the port-forwarded address
            ray_cluster._host = "127.0.0.1"
            ray_cluster.init_ray(context)  # normally this would happen automatically during resource setup
            assert ray_cluster.context is not None

            # make sure a @remote function runs inside the cluster
            # not in localhost
            assert ray_cluster.cluster_name in ray.get(get_hostname.remote())

            ray_cluster_description = ray_cluster.client.client.get(
                ray_cluster.cluster_name, namespace=ray_cluster.namespace
            )
            assert ray_cluster_description["metadata"]["labels"]["dagster.io/run_id"] == context.run_id
            assert ray_cluster_description["metadata"]["labels"]["dagster.io/cluster"] == ray_cluster.cluster_name

    result = materialize_to_memory(
        [my_asset],
        resources={"ray_cluster": ray_cluster_resource},
    )

    kuberay_client = RayClusterClient(config_file=str(k8s_with_kuberay.kubeconfig))

    # make sure the RayCluster is cleaned up

    assert (
        len(
            kuberay_client.list(
                namespace=ray_cluster_resource.namespace, label_selector=f"dagster.io/run_id={result.run_id}"
            )["items"]
        )
        == 0
    )


def test_kuberay_cleanup_job(
    ray_cluster_resource_skip_cleanup: KubeRayCluster,
    k8s_with_kuberay: AClusterManager,
):
    @asset
    def my_asset(ray_cluster: RayResource) -> None:
        assert isinstance(ray_cluster, KubeRayCluster)

    result = materialize_to_memory(
        [my_asset],
        resources={"ray_cluster": ray_cluster_resource_skip_cleanup},
    )

    kuberay_client = RayClusterClient(config_file=str(k8s_with_kuberay.kubeconfig))

    assert (
        len(
            kuberay_client.list(
                namespace=ray_cluster_resource_skip_cleanup.namespace,
                label_selector=f"dagster.io/run_id={result.run_id}",
            )["items"]
        )
        > 0
    )

    cleanup_kuberay_clusters.execute_in_process(
        resources={
            "kuberay_client": RayClusterClientResource(kubeconfig_file=str(k8s_with_kuberay.kubeconfig)),
        },
        run_config=RunConfig(
            ops={
                "cleanup_kuberay_clusters": CleanupKuberayClustersConfig(
                    namespace=ray_cluster_resource_skip_cleanup.namespace,
                )
            }
        ),
    )

    assert not kuberay_client.list(
        namespace=ray_cluster_resource_skip_cleanup.namespace, label_selector=f"dagster.io/run_id={result.run_id}"
    )["items"]


def test_ray_cluster_builder_debug():
    kuberay_cluster = KubeRayCluster(enable_debug_post_mortem=True)
    kuberay_cluster._cluster_name = "test-cluster"
    context = dg.build_init_resource_context()

    ray_cluster_config = kuberay_cluster._build_raycluster(context)
    for group_spec in [ray_cluster_config["spec"]["headGroupSpec"], *ray_cluster_config["spec"]["workerGroupSpecs"]]:
        for container in group_spec["template"]["spec"]["containers"]:
            assert {"name": "RAY_DEBUG_POST_MORTEM", "value": "1"} in container["env"], container

    kuberay_cluster = KubeRayCluster(enable_tracing=True)
    kuberay_cluster._cluster_name = "test-cluster"
    ray_cluster_config = kuberay_cluster._build_raycluster(context)
    for group_spec in [ray_cluster_config["spec"]["headGroupSpec"], *ray_cluster_config["spec"]["workerGroupSpecs"]]:
        for container in group_spec["template"]["spec"]["containers"]:
            assert {"name": "RAY_PROFILING", "value": "1"} in container["env"], container

    kuberay_cluster = KubeRayCluster(enable_actor_task_logging=True)
    kuberay_cluster._cluster_name = "test-cluster"
    ray_cluster_config = kuberay_cluster._build_raycluster(context)
    kuberay_cluster._cluster_name = "test-cluster"
    for group_spec in [ray_cluster_config["spec"]["headGroupSpec"], *ray_cluster_config["spec"]["workerGroupSpecs"]]:
        for container in group_spec["template"]["spec"]["containers"]:
            assert {"name": "RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING", "value": "1"} in container["env"], container
