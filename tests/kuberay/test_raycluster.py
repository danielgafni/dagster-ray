import socket
import time
from datetime import datetime, timedelta
from typing import Any, cast

import dagster as dg
import pytest
import ray  # noqa: TID253
from pytest_kubernetes.providers import AClusterManager

from dagster_ray import Lifecycle, RayResource
from dagster_ray._base.cluster_sharing_lock import ClusterSharingLock
from dagster_ray.kuberay import (
    KubeRayCluster,
    KubeRayClusterClientResource,
    RayClusterConfig,
    cleanup_expired_kuberay_clusters,
)
from dagster_ray.kuberay.client import RayClusterClient
from dagster_ray.kuberay.configs import ClusterSharing, RayClusterSpec
from dagster_ray.kuberay.jobs import delete_kuberay_clusters
from dagster_ray.kuberay.ops import DeleteKubeRayClustersConfig, RayClusterRef
from tests.kuberay.conftest import KUBERNETES_CONTEXT
from tests.kuberay.utils import NAMESPACE, get_random_free_port
from tests.utils import CodeLocationOrigin  # pyright: ignore[reportAttributeAccessIssue]


def delete_shared_rayclusters(raycluster_client: RayClusterClient | KubeRayClusterClientResource):
    raycluster_client = (
        raycluster_client
        if isinstance(raycluster_client, RayClusterClient)
        else RayClusterClient(kube_config=raycluster_client.kube_config, kube_context=raycluster_client.kube_context)
    )
    clusters = raycluster_client.list(namespace=NAMESPACE).get("items", [])
    for cluster in clusters:
        if cluster["metadata"].get("labels", {}).get("dagster/cluster-sharing") == "true":
            raycluster_client.delete(name=cluster["metadata"]["name"], namespace=cluster["metadata"]["namespace"])


@pytest.fixture(scope="session")
def raycluster_client_resource(k8s_with_kuberay: AClusterManager):
    return KubeRayClusterClientResource(kube_config=str(k8s_with_kuberay.kubeconfig), kube_context=KUBERNETES_CONTEXT)


@pytest.fixture(scope="session")
def raycluster_client(k8s_with_kuberay: AClusterManager):
    return RayClusterClient(kube_config=str(k8s_with_kuberay.kubeconfig), kube_context=KUBERNETES_CONTEXT)


@pytest.fixture
def shared_clusters_cleanup(raycluster_client: RayClusterClient):
    delete_shared_rayclusters(raycluster_client)
    yield
    delete_shared_rayclusters(raycluster_client)


def test_instantiate_defaults():
    _ = KubeRayCluster()


@pytest.fixture(scope="session")
def ray_cluster_resource(
    k8s_with_kuberay: AClusterManager,
    raycluster_client_resource: KubeRayClusterClientResource,
    dagster_ray_image: str,
    head_group_spec: dict[str, Any],
    worker_group_specs: list[dict[str, Any]],
) -> KubeRayCluster:
    redis_port = get_random_free_port()

    return KubeRayCluster(
        image=dagster_ray_image,
        # have have to first run port-forwarding with minikube
        # we can only init ray after that
        lifecycle=Lifecycle(connect=False),
        client=raycluster_client_resource,
        ray_cluster=RayClusterConfig(
            metadata={"namespace": NAMESPACE},
            spec=RayClusterSpec(head_group_spec=head_group_spec, worker_group_specs=worker_group_specs),
        ),
        redis_port=redis_port,
    )


@pytest.fixture(scope="session")
def ray_cluster_resource_skip_cleanup(
    k8s_with_kuberay: AClusterManager,
    dagster_ray_image: str,
    head_group_spec: dict[str, Any],
    worker_group_specs: list[dict[str, Any]],
    raycluster_client_resource: KubeRayClusterClientResource,
) -> KubeRayCluster:
    redis_port = get_random_free_port()

    return KubeRayCluster(
        image=dagster_ray_image,
        # have have to first run port-forwarding with minikube
        # we can only init ray after that
        lifecycle=Lifecycle(
            connect=False,
            cleanup="never",
        ),
        client=raycluster_client_resource,
        ray_cluster=RayClusterConfig(
            metadata={"namespace": NAMESPACE},
            spec=RayClusterSpec(head_group_spec=head_group_spec, worker_group_specs=worker_group_specs),
        ),
        redis_port=redis_port,
    )


@pytest.fixture(scope="session")
def ray_cluster_resource_skip_create(
    k8s_with_kuberay: AClusterManager,
    dagster_ray_image: str,
    head_group_spec: dict[str, Any],
    worker_group_specs: list[dict[str, Any]],
    raycluster_client_resource: KubeRayClusterClientResource,
) -> KubeRayCluster:
    redis_port = get_random_free_port()

    return KubeRayCluster(
        image=dagster_ray_image,
        # have have to first run port-forwarding with minikube
        # we can only init ray after that
        lifecycle=Lifecycle(create=False),
        client=raycluster_client_resource,
        ray_cluster=RayClusterConfig(
            metadata={"namespace": NAMESPACE},
            spec=RayClusterSpec(head_group_spec=head_group_spec, worker_group_specs=worker_group_specs),
        ),
        redis_port=redis_port,
    )


@pytest.fixture(scope="session")
def ray_cluster_resource_skip_wait(
    k8s_with_kuberay: AClusterManager,
    dagster_ray_image: str,
    head_group_spec: dict[str, Any],
    worker_group_specs: list[dict[str, Any]],
    raycluster_client_resource: KubeRayClusterClientResource,
) -> KubeRayCluster:
    redis_port = get_random_free_port()

    return KubeRayCluster(
        image=dagster_ray_image,
        # have have to first run port-forwarding with minikube
        # we can only init ray after that
        lifecycle=Lifecycle(wait=False),
        client=raycluster_client_resource,
        ray_cluster=RayClusterConfig(
            metadata={"namespace": NAMESPACE},
            spec=RayClusterSpec(head_group_spec=head_group_spec, worker_group_specs=worker_group_specs),
        ),
        redis_port=redis_port,
    )


@ray.remote
def get_hostname():
    return socket.gethostname()


def ensure_kuberay_cluster_correctness(
    ray_cluster: KubeRayCluster,
    k8s_with_kuberay: AClusterManager,
    context: dg.AssetExecutionContext,
):
    with k8s_with_kuberay.port_forwarding(
        target=f"svc/{ray_cluster.name}-head-svc",
        source_port=cast(int, ray_cluster.redis_port),
        target_port=10001,
        namespace=ray_cluster.namespace,
    ):
        # now we can access the head node
        # hack the _host attribute to point to the port-forwarded address
        ray_cluster._host = "127.0.0.1"
        ray_cluster.connect(context)  # normally this would happen automatically during resource setup
        assert ray_cluster.context is not None

        # make sure a @remote function runs inside the cluster
        # not in localhost
        assert ray_cluster.name in ray.get(get_hostname.remote())

        ray_cluster_description = ray_cluster.client.get(ray_cluster.name, namespace=ray_cluster.namespace)
        assert ray_cluster_description["metadata"]["labels"]["dagster/run-id"] == context.run_id


def test_kuberay_cluster_resource(
    ray_cluster_resource: KubeRayCluster,
    k8s_with_kuberay: AClusterManager,
    raycluster_client: RayClusterClient,
):
    @dg.asset
    # testing RayResource type annotation too!
    def my_asset(context: dg.AssetExecutionContext, ray_cluster: RayResource) -> None:
        # port-forward to the head node
        # because it's not possible to access it otherwise

        assert isinstance(ray_cluster, KubeRayCluster)

        ensure_kuberay_cluster_correctness(
            ray_cluster,
            k8s_with_kuberay,
            context,
        )

    result = dg.materialize_to_memory(
        [my_asset],
        resources={"ray_cluster": ray_cluster_resource},
    )

    # make sure the RayCluster is cleaned up

    assert (
        len(
            raycluster_client.list(
                namespace=ray_cluster_resource.namespace, label_selector=f"dagster/run-id={result.run_id}"
            )["items"]
        )
        == 0
    )


def test_kuberay_cluster_resource_skip_create(
    ray_cluster_resource_skip_create: KubeRayCluster,
    k8s_with_kuberay: AClusterManager,
):
    @dg.asset
    def my_asset(context: dg.AssetExecutionContext, ray_cluster: RayResource) -> None:
        assert isinstance(ray_cluster, KubeRayCluster)

        # call create and wait manually
        ray_cluster.create(context)
        ray_cluster.wait(context)

        ensure_kuberay_cluster_correctness(
            ray_cluster,
            k8s_with_kuberay,
            context,
        )

    dg.materialize_to_memory(
        [my_asset],
        resources={"ray_cluster": ray_cluster_resource_skip_create},
    )


def test_kuberay_cluster_resource_skip_wait(
    ray_cluster_resource_skip_wait: KubeRayCluster,
    k8s_with_kuberay: AClusterManager,
):
    @dg.asset
    def my_asset(context: dg.AssetExecutionContext, ray_cluster: RayResource) -> None:
        assert isinstance(ray_cluster, KubeRayCluster)

        # call wait manually
        ray_cluster.wait(context)

        ensure_kuberay_cluster_correctness(
            ray_cluster,
            k8s_with_kuberay,
            context,
        )

    dg.materialize_to_memory(
        [my_asset],
        resources={"ray_cluster": ray_cluster_resource_skip_wait},
    )


def test_ray_cluster_builder_debug():
    kuberay_cluster = KubeRayCluster(enable_debug_post_mortem=True, image="test")
    kuberay_cluster._cluster_name = "test-cluster"
    context = dg.build_init_resource_context()

    ray_cluster_config = kuberay_cluster.ray_cluster.to_k8s(
        context, env_vars=kuberay_cluster.get_env_vars_to_inject(), labels={"foo": "bar"}, image="test"
    )
    assert ray_cluster_config["metadata"]["labels"]["foo"] == "bar"
    for group_spec in [ray_cluster_config["spec"]["headGroupSpec"], *ray_cluster_config["spec"]["workerGroupSpecs"]]:
        for container in group_spec["template"]["spec"]["containers"]:
            assert {"name": "RAY_DEBUG_POST_MORTEM", "value": "1"} in container["env"], container

    kuberay_cluster = KubeRayCluster(enable_tracing=True)
    kuberay_cluster._cluster_name = "test-cluster"
    ray_cluster_config = kuberay_cluster.ray_cluster.to_k8s(
        context, env_vars=kuberay_cluster.get_env_vars_to_inject(), image="test"
    )
    for group_spec in [ray_cluster_config["spec"]["headGroupSpec"], *ray_cluster_config["spec"]["workerGroupSpecs"]]:
        for container in group_spec["template"]["spec"]["containers"]:
            assert {"name": "RAY_PROFILING", "value": "1"} in container["env"], container

    kuberay_cluster = KubeRayCluster(enable_actor_task_logging=True)
    kuberay_cluster._cluster_name = "test-cluster"
    ray_cluster_config = kuberay_cluster.ray_cluster.to_k8s(
        context, env_vars=kuberay_cluster.get_env_vars_to_inject(), image="test"
    )
    kuberay_cluster._cluster_name = "test-cluster"
    for group_spec in [ray_cluster_config["spec"]["headGroupSpec"], *ray_cluster_config["spec"]["workerGroupSpecs"]]:
        for container in group_spec["template"]["spec"]["containers"]:
            assert {"name": "RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING", "value": "1"} in container["env"], container


def test_cleanup_expired_kuberay_clusters_sensor_skip(
    code_location_origin: CodeLocationOrigin,
    dagster_instance: dg.DagsterInstance,
    raycluster_client: RayClusterClient,
    shared_clusters_cleanup,
):
    # run the sensor - it shouldn't request any runs
    context = dg.build_sensor_context(instance=dagster_instance)
    context._code_location_origin = code_location_origin
    for item in cleanup_expired_kuberay_clusters(  # pyright: ignore[reportGeneralTypeIssues,reportOptionalIterable]
        context, raycluster_client=raycluster_client
    ):
        assert isinstance(item, dg.SkipReason)


def test_cleanup_expired_kuberay_clusters_sensor_request(
    k8s_with_kuberay: AClusterManager,
    dagster_ray_image: str,
    code_location_origin: CodeLocationOrigin,
    dagster_instance: dg.DagsterInstance,
    raycluster_client_resource: KubeRayClusterClientResource,
    raycluster_client: RayClusterClient,
    head_group_spec: dict[str, Any],
    worker_group_specs: list[dict[str, Any]],
    shared_clusters_cleanup,
):
    # run the sensor - it shouldn't request any runs
    context = dg.build_sensor_context(instance=dagster_instance)
    context._code_location_origin = code_location_origin
    for item in cleanup_expired_kuberay_clusters(  # pyright: ignore[reportGeneralTypeIssues,reportOptionalIterable]
        context, raycluster_client=raycluster_client
    ):
        assert isinstance(item, dg.SkipReason)

    ray_cluster = KubeRayCluster(
        image=dagster_ray_image,
        # have have to first run port-forwarding with minikube
        # we can only init ray after that
        lifecycle=Lifecycle(wait=False),
        client=raycluster_client_resource,
        ray_cluster=RayClusterConfig(
            # dagster/code-location should be added manually since there is no (normal) way to set the code location name for `dg.materialize`
            metadata={"namespace": NAMESPACE, "labels": {"dagster/code-location": "test_location"}},
            spec=RayClusterSpec(head_group_spec=head_group_spec, worker_group_specs=worker_group_specs),
        ),
        redis_port=get_random_free_port(),
        cluster_sharing=ClusterSharing(enabled=True, ttl_seconds=10),
    )

    @dg.asset
    def my_asset(context: dg.AssetExecutionContext, ray_cluster: RayResource):
        return "Hello, World!"

    res = dg.materialize(assets=[my_asset], resources={"ray_cluster": ray_cluster})
    clusters = raycluster_client.list(
        namespace=ray_cluster.namespace,
        label_selector=f"dagster/run-id={res.run_id}",
    )["items"]

    # cluster should not be cleaned up yet since it has a TTL of 10 seconds
    assert len(clusters) == 1
    cluster = clusters[0]

    assert cluster["metadata"]["labels"].get("dagster/cluster-sharing") == "true"
    assert cluster["metadata"]["labels"].get("dagster/code-location") == "test_location"

    locks = ClusterSharingLock.parse_all_locks(cluster["metadata"]["annotations"])
    assert len(locks) == 1
    lock = locks[0]
    assert lock.ttl_seconds == 10.0
    assert not lock.is_expired

    # wait until the lock expires
    time.sleep((lock.expired_at - datetime.now()).total_seconds() + 0.5)

    assert lock.is_expired

    # run the sensor - it should request a run
    context = dg.build_sensor_context(instance=dagster_instance)
    context._code_location_origin = code_location_origin
    for item in cleanup_expired_kuberay_clusters(  # pyright: ignore[reportGeneralTypeIssues,reportOptionalIterable]
        context, raycluster_client=raycluster_client
    ):
        assert isinstance(item, dg.RunRequest)
        assert (
            item.run_config["ops"]["delete_kuberay_clusters_op"]["config"]["clusters"][0]["name"]
            == cluster["metadata"]["name"]
        )


def test_cluster_sharing(
    k8s_with_kuberay: AClusterManager,
    dagster_ray_image: str,
    raycluster_client_resource: KubeRayClusterClientResource,
    raycluster_client: RayClusterClient,
    head_group_spec: dict[str, Any],
    worker_group_specs: list[dict[str, Any]],
    dagster_instance: dg.DagsterInstance,
    code_location_origin: CodeLocationOrigin,
    shared_clusters_cleanup,
):
    ray_cluster = KubeRayCluster(
        image=dagster_ray_image,
        # have have to first run port-forwarding with minikube
        # we can only init ray after that
        lifecycle=Lifecycle(wait=False),
        client=raycluster_client_resource,
        ray_cluster=RayClusterConfig(
            metadata={"namespace": NAMESPACE},
            spec=RayClusterSpec(head_group_spec=head_group_spec, worker_group_specs=worker_group_specs),
        ),
        redis_port=get_random_free_port(),
        cluster_sharing=ClusterSharing(enabled=True),
    )

    @dg.asset
    def my_asset(context: dg.AssetExecutionContext, ray_cluster: RayResource):
        annotations = ray_cluster.get_sharing_lock_annotations(context)
        assert len(list(filter(lambda x: x.startswith(f"dagster/lock-{context.run_id}"), annotations.keys()))) == 1
        return "Hello, World!"

    res = dg.materialize(assets=[my_asset], resources={"ray_cluster": ray_cluster})

    # with cluster sharing enabled the cluster should not be cleaned up
    clusters = raycluster_client.list(
        namespace=ray_cluster.namespace,
        label_selector=f"dagster/run-id={res.run_id}",
    )["items"]
    assert len(clusters) == 1

    cluster = clusters[0]

    assert cluster["metadata"]["labels"].get("dagster/cluster-sharing") == "true"

    locks = ClusterSharingLock.parse_all_locks(cluster["metadata"]["annotations"])
    assert len(locks) == 1
    lock = locks[0]
    assert lock.run_id == res.run_id
    assert lock.created_at + timedelta(seconds=cast(float, lock.ttl_seconds)) > datetime.now()
    assert not lock.is_expired

    # run the sensor - it shouldn't request any runs

    context = dg.build_sensor_context(instance=dagster_instance)
    context._code_location_origin = code_location_origin

    for item in cleanup_expired_kuberay_clusters(context, raycluster_client=raycluster_client):  # pyright: ignore[reportGeneralTypeIssues,reportOptionalIterable]
        assert isinstance(item, dg.SkipReason)

    # run the asset again, it shouldn't create a new RayCluster this time

    res_2 = dg.materialize(assets=[my_asset], resources={"ray_cluster": ray_cluster})

    # the old cluster should still be around and it should contain the new run in one of the lock annotations
    updated_cluster = raycluster_client.list(
        namespace=ray_cluster.namespace,
        label_selector=f"dagster/run-id={res.run_id}",
    )["items"][0]

    assert cluster["metadata"]["labels"] == updated_cluster["metadata"]["labels"]
    locks = ClusterSharingLock.parse_all_locks(updated_cluster["metadata"]["annotations"])
    assert len(locks) == 2
    assert locks[0].run_id != locks[1].run_id

    # the new run should not have any clusters associated with it
    assert (
        len(
            raycluster_client.list(
                namespace=ray_cluster.namespace,
                label_selector=f"dagster/run-id={res_2.run_id}",
            )["items"]
        )
        == 0
    )


def test_cluster_sharing_cleanup_expired(
    dagster_ray_image: str,
    raycluster_client_resource: KubeRayClusterClientResource,
    raycluster_client: RayClusterClient,
    head_group_spec: dict[str, Any],
    worker_group_specs: list[dict[str, Any]],
    shared_clusters_cleanup,
):
    ray_cluster = KubeRayCluster(
        image=dagster_ray_image,
        # have have to first run port-forwarding with minikube
        # we can only init ray after that
        lifecycle=Lifecycle(wait=False),
        client=raycluster_client_resource,
        ray_cluster=RayClusterConfig(
            metadata={"namespace": NAMESPACE},
            spec=RayClusterSpec(head_group_spec=head_group_spec, worker_group_specs=worker_group_specs),
        ),
        redis_port=get_random_free_port(),
        cluster_sharing=ClusterSharing(enabled=True, ttl_seconds=0.1),
    )

    @dg.asset
    def my_asset(context: dg.AssetExecutionContext, ray_cluster: RayResource):
        time.sleep(0.1)
        return "Hello, World!"

    res = dg.materialize(assets=[my_asset], resources={"ray_cluster": ray_cluster})
    clusters = raycluster_client.list(
        namespace=ray_cluster.namespace,
        label_selector=f"dagster/run-id={res.run_id}",
    )["items"]

    # cluster should be cleaned up since it's only lock has already expired
    assert len(clusters) == 0


def test_delete_kuberay_clusters_job(
    ray_cluster_resource_skip_cleanup: KubeRayCluster,
    k8s_with_kuberay: AClusterManager,
    raycluster_client: RayClusterClient,
):
    @dg.asset
    def my_asset(ray_cluster: RayResource):
        return 42

    result = dg.materialize(
        assets=[my_asset],
        resources={
            "ray_cluster": ray_cluster_resource_skip_cleanup,
        },
    )

    clusters = raycluster_client.list(
        namespace=ray_cluster_resource_skip_cleanup.namespace,
        label_selector=f"dagster/run-id={result.run_id}",
    )["items"]

    assert len(clusters) == 1

    delete_kuberay_clusters.execute_in_process(
        resources={
            "raycluster_client": raycluster_client,
        },
        run_config=dg.RunConfig(
            ops={
                "delete_kuberay_clusters_op": DeleteKubeRayClustersConfig(
                    namespace=ray_cluster_resource_skip_cleanup.namespace,
                    clusters=[
                        RayClusterRef(
                            name=clusters[0]["metadata"]["name"],
                        )
                    ],
                )
            }
        ),
    )

    assert not raycluster_client.list(
        namespace=ray_cluster_resource_skip_cleanup.namespace, label_selector=f"dagster/run-id={result.run_id}"
    )["items"]
