import os
import socket
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Generator

import pytest
import pytest_cases
from pytest_kubernetes.options import ClusterOptions
from pytest_kubernetes.providers import AClusterManager, select_provider_manager

from tests import ROOT_DIR


def get_random_free_port():
    sock = socket.socket()
    sock.bind(("", 0))
    return sock.getsockname()[1]


@pytest.fixture(scope="session")
def kuberay_helm_repo():
    subprocess.run(["helm", "repo", "add", "kuberay", "https://ray-project.github.io/kuberay-helm/"], check=True)
    subprocess.run(["helm", "repo", "update", "kuberay"], check=True)


PYTEST_DAGSTER_RAY_IMAGE = os.getenv("PYTEST_DAGSTER_RAY_IMAGE")


@pytest.fixture(scope="session")
def dagster_ray_image():
    """
    Either returns the image name from the environment variable PYTEST_DAGSTER_RAY_IMAGE
    or builds the image and returns it
    """

    if PYTEST_DAGSTER_RAY_IMAGE is None:
        # build the local image
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        image = f"local/dagster-ray:py-{python_version}"
        subprocess.run(
            [
                "docker",
                "build",
                "-f",
                str(ROOT_DIR / "Dockerfile"),
                "--build-arg",
                "BUILD_DEPENDENCIES=dev",
                "--build-arg",
                f"PYTHON_VERSION={python_version}",
                "-t",
                image,
                str(ROOT_DIR),
            ],
            env={
                "DOCKER_BUILDKIT": "1",
            },
            check=True,
        )
    else:
        image = PYTEST_DAGSTER_RAY_IMAGE

    return image


# TODO: it's easy to parametrize over different versions of k8s
# but it would take quite some time to test all of them!
# probably should only do it in CI
KUBERNETES_VERSION = os.getenv("PYTEST_KUBERNETES_VERSION", "1.31.0")

KUBERAY_VERSIONS = os.getenv("PYTEST_KUBERAY_VERSIONS", "1.2.2").split(",")

NAMESPACE = "ray"


@pytest_cases.fixture(scope="session")
@pytest.mark.parametrize("kuberay_version", KUBERAY_VERSIONS)
def k8s_with_kuberay(
    request, kuberay_helm_repo, dagster_ray_image: str, kuberay_version: str
) -> Generator[AClusterManager, None, None]:
    k8s = select_provider_manager("minikube")("dagster-ray")
    k8s.create(ClusterOptions(api_version=KUBERNETES_VERSION))
    # load images in advance to avoid possible timeouts later on
    k8s.load_image(f"quay.io/kuberay/operator:v{kuberay_version}")

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
            kuberay_version,
        ],
        check=True,
    )

    k8s.wait("deployment/kuberay-operator", "condition=Available=True", namespace="kuberay-operator")
    # namespace to create RayClusters in
    try:
        k8s.kubectl(["create", "namespace", NAMESPACE])
    except RuntimeError as e:
        if "AlreadyExists" not in str(e):
            raise

    yield k8s
    k8s.delete()
