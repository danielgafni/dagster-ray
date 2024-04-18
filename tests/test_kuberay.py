import os
import socket
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, Generator, List, cast

import pytest
import ray
from dagster import asset, materialize_to_memory
from pytest_kubernetes.options import ClusterOptions
from pytest_kubernetes.providers import AClusterManager, select_provider_manager

from dagster_ray.kuberay import KubeRayCluster
from dagster_ray.kuberay.resources import DEFAULT_HEAD_GROUP_SPEC, DEFAULT_WORKER_GROUP_SPECS, RayClusterConfig
from tests import ROOT_DIR


def get_random_free_port():
    sock = socket.socket()
    sock.bind(("", 0))
    return sock.getsockname()[1]


@pytest.fixture(scope="session")
def kuberay_helm_repo():
    subprocess.run(["helm", "repo", "add", "kuberay", "https://ray-project.github.io/kuberay-helm/"], check=True)
    subprocess.run(["helm", "repo", "update", "kuberay"], check=True)


LOCAL_IMAGE = "docker.io/local/dagster-ray:latest"


@pytest.fixture(scope="session")
def dagster_ray_image():
    """
    Either returns the image name from the environment variable PYTEST_DAGSTER_RAY_IMAGE
    or builds the image and returns it
    """
    image = os.getenv("PYTEST_DAGSTER_RAY_IMAGE", LOCAL_IMAGE)

    if image == LOCAL_IMAGE:
        # build the local image
        subprocess.run(
            [
                "docker",
                "build",
                "-f",
                str(ROOT_DIR / "Dockerfile"),
                "--build-arg",
                "BUILD_DEPENDENCIES=dev",
                "-t",
                image,
                str(ROOT_DIR),
            ],
            check=True,
        )

    return image


# TODO: it's easy to parametrize over different versions of k8s
# but it would take quite some time to test all of them!
# probably should only do it in CI
KUBERNETES_VERSION = "1.25.3"

# TODO: same as above
KUBERAY_VERSION = "1.1.0"


@pytest.fixture(scope="session")
def k8s_with_raycluster(request, kuberay_helm_repo, dagster_ray_image: str) -> Generator[AClusterManager, None, None]:
    k8s = select_provider_manager("minikube")("dagster-ray")
    k8s.create(ClusterOptions(api_version=KUBERNETES_VERSION))
    # load images in advance to avoid possible timeouts later on
    k8s.load_image(f"quay.io/kuberay/operator:v{KUBERAY_VERSION}")

    # warning: minikube fails to load the image directly because of
    # https://github.com/kubernetes/minikube/issues/18021
    # so we export it to .tar first
    # TODO: load image without .tar export once the issue is resolved
    with tempfile.TemporaryDirectory() as tmpdir:
        image_tar = Path(tmpdir) / "dagster-ray.tar"
        subprocess.run(["docker", "image", "save", "-o", str(image_tar), dagster_ray_image], check=True)
        k8s.load_image(str(image_tar))

    # init the cluster with a workload
    subprocess.run(
        [
            "helm",
            "--kubeconfig",
            str(k8s.kubeconfig),
            "upgrade",
            "--install",
            "--create-namespace",
            "--namespace",
            "kuberay-operator",
            "kuberay-operator",
            "kuberay/kuberay-operator",
            "--version",
            KUBERAY_VERSION,
        ],
        check=True,
    )

    k8s.wait("deployment/kuberay-operator", "condition=Available=True", namespace="kuberay-operator")
    # namespace to create RayClusters in
    k8s.kubectl(["create", "namespace", "kuberay"])
    yield k8s
    k8s.delete()


@pytest.fixture(scope="session")
def head_group_spec(dagster_ray_image: str) -> Dict[str, Any]:
    head_group_spec = DEFAULT_HEAD_GROUP_SPEC.copy()
    head_group_spec["serviceType"] = "LoadBalancer"
    head_group_spec["template"]["spec"]["containers"][0]["image"] = dagster_ray_image
    head_group_spec["template"]["spec"]["containers"][0]["imagePullPolicy"] = "IfNotPresent"
    return head_group_spec


@pytest.fixture(scope="session")
def worker_group_specs(dagster_ray_image: str) -> List[Dict[str, Any]]:
    worker_group_specs = DEFAULT_WORKER_GROUP_SPECS.copy()
    worker_group_specs[0]["template"]["spec"]["containers"][0]["image"] = dagster_ray_image
    worker_group_specs[0]["template"]["spec"]["containers"][0]["imagePullPolicy"] = "IfNotPresent"
    return worker_group_specs


@pytest.fixture(scope="session")
def ray_cluster_resource(
    k8s_with_raycluster: AClusterManager,
    dagster_ray_image: str,
    head_group_spec: Dict[str, Any],
    worker_group_specs: List[Dict[str, Any]],
) -> KubeRayCluster:
    redis_port = get_random_free_port()

    return KubeRayCluster(
        # have have to first run port-forwarding with minikube
        # we can only init ray after that
        skip_init=True,
        kubeconfig_file=str(k8s_with_raycluster.kubeconfig),
        ray_cluster=RayClusterConfig(
            image=dagster_ray_image,
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
    k8s_with_raycluster: AClusterManager,
):
    @asset
    def my_asset(ray_cluster: KubeRayCluster) -> None:
        # port-forward to the head node
        # because it's not possible to access it otherwise

        with k8s_with_raycluster.port_forwarding(
            target=f"svc/{ray_cluster.cluster_name}-head-svc",
            source_port=cast(int, ray_cluster.redis_port),
            target_port=10001,
            namespace=ray_cluster.namespace,
        ):
            # now we can access the head node
            # hack the _host attribute to point to the port-forwarded address
            ray_cluster._host = "127.0.0.1"
            ray_cluster.init_ray()  # normally this would happen automatically during resource setup
            assert ray_cluster.context is not None

            # make sure a @remote function runs inside the cluster
            # not in localhost
            assert ray_cluster.cluster_name in ray.get(get_hostname.remote())

    materialize_to_memory(
        [my_asset],
        resources={"ray_cluster": ray_cluster_resource},
    )
