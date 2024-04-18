import os
import subprocess
from typing import Generator

import pytest
from dagster import asset, materialize_to_memory
from pytest_kubernetes.options import ClusterOptions
from pytest_kubernetes.providers import AClusterManager, select_provider_manager

from dagster_ray.kuberay import KubeRayCluster
from dagster_ray.kuberay.resources import RayClusterConfig
from tests import ROOT_DIR


@pytest.fixture(scope="session")
def kuberay_helm_repo():
    subprocess.run(["helm", "repo", "add", "kuberay", "https://ray-project.github.io/kuberay-helm/"], check=True)
    subprocess.run(["helm", "repo", "update", "kuberay"], check=True)


@pytest.fixture(scope="session")
def dagster_ray_image():
    """
    Either returns the image name from the environment variable PYTEST_DAGSTER_RAY_IMAGE
    or builds the image and returns it
    """
    image = os.getenv("PYTEST_DAGSTER_RAY_IMAGE", "local/dagster-ray")

    if image == "local/dagster-ray":
        # build the local image
        subprocess.run(["docker", "build", "-f", str(ROOT_DIR / "Dockerfile"), "-t", image, str(ROOT_DIR)], check=True)

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
    k8s.load_image(dagster_ray_image)
    # init the cluster with a workload
    subprocess.run(
        [
            "helm",
            "--kubeconfig",
            str(k8s.kubeconfig),
            "upgrade",
            "--install",
            "kuberay-operator",
            "kuberay/kuberay-operator",
            "--version",
            KUBERAY_VERSION,
        ],
        check=True,
    )
    # k8s.load_image(dagster_ray_image)
    k8s.wait("deployment/kuberay-operator", "condition=Available=True")
    yield k8s
    k8s.delete()


def test_kuberay_cluster_resource(k8s_with_raycluster: AClusterManager, dagster_ray_image: str):
    @asset
    def my_asset(ray_cluster: KubeRayCluster) -> str:
        return ray_cluster.host

    materialize_to_memory(
        [my_asset],
        resources={
            "ray_cluster": KubeRayCluster(
                kubeconfig_file=str(k8s_with_raycluster.kubeconfig),
                ray_cluster=RayClusterConfig(image=dagster_ray_image),
            )
        },
    )
