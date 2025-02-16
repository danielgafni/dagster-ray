import os
import subprocess
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import pytest_cases
from kubernetes import config  # noqa: TID253
from pytest_kubernetes.options import ClusterOptions
from pytest_kubernetes.providers import AClusterManager, select_provider_manager
from tenacity import retry, stop_after_attempt, wait_random_exponential

from dagster_ray.kuberay.client import RayClusterClient
from dagster_ray.kuberay.configs import DEFAULT_HEAD_GROUP_SPEC, DEFAULT_WORKER_GROUP_SPECS
from tests import ROOT_DIR
from tests.kuberay.utils import NAMESPACE


@pytest.fixture(scope="session")
def kuberay_helm_repo():
    subprocess.run(["helm", "repo", "add", "kuberay", "https://ray-project.github.io/kuberay-helm/"], check=True)
    subprocess.run(["helm", "repo", "update", "kuberay"], check=True)


PYTEST_DAGSTER_RAY_IMAGE = os.getenv("PYTEST_DAGSTER_RAY_IMAGE")


@pytest.fixture(scope="session")
@retry(
    stop=stop_after_attempt(3),
    wait=wait_random_exponential(multiplier=1, max=10),
)
def dagster_ray_image():
    import dagster
    import ray

    """
    Either returns the image name from the environment variable PYTEST_DAGSTER_RAY_IMAGE
    or builds the image and returns it
    """

    if PYTEST_DAGSTER_RAY_IMAGE is None:
        # build the local image
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        ray_version = ray.__version__
        dagster_version = dagster.__version__

        image = f"local/dagster-ray:py-{python_version}-{ray_version}-{dagster_version}"

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
                "--build-arg",
                f"RAY_VERSION={ray_version}",
                "--build-arg",
                f"DAGSTER_VERSION={dagster_version}",
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


@pytest_cases.fixture(scope="session")  # type: ignore
@pytest.mark.parametrize("kuberay_version", KUBERAY_VERSIONS)
def k8s_with_kuberay(
    request, kuberay_helm_repo, dagster_ray_image: str, kuberay_version: str
) -> Iterator[AClusterManager]:
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


@pytest.fixture(scope="session")
def head_group_spec(dagster_ray_image: str) -> dict[str, Any]:
    head_group_spec = DEFAULT_HEAD_GROUP_SPEC.copy()
    head_group_spec["serviceType"] = "LoadBalancer"
    head_group_spec["template"]["spec"]["containers"][0]["image"] = dagster_ray_image
    head_group_spec["template"]["spec"]["containers"][0]["imagePullPolicy"] = "IfNotPresent"
    return head_group_spec


@pytest.fixture(scope="session")
def worker_group_specs(dagster_ray_image: str) -> list[dict[str, Any]]:
    worker_group_specs = DEFAULT_WORKER_GROUP_SPECS.copy()
    worker_group_specs[0]["template"]["spec"]["containers"][0]["image"] = dagster_ray_image
    worker_group_specs[0]["template"]["spec"]["containers"][0]["imagePullPolicy"] = "IfNotPresent"
    return worker_group_specs


PERSISTENT_RAY_CLUSTER_NAME = "persistent-ray-cluster"


@pytest.fixture(scope="session")
def k8s_with_raycluster(
    k8s_with_kuberay: AClusterManager,
    head_group_spec: dict[str, Any],
    worker_group_specs: list[dict[str, Any]],
) -> Iterator[tuple[dict[str, str], AClusterManager]]:
    # create a RayCluster
    config.load_kube_config(str(k8s_with_kuberay.kubeconfig))

    client = RayClusterClient(
        config_file=str(k8s_with_kuberay.kubeconfig),
    )

    client.create(
        body={
            "kind": "RayCluster",
            "apiVersion": "ray.io/v1",
            "metadata": {"name": PERSISTENT_RAY_CLUSTER_NAME},
            "spec": {
                "headGroupSpec": head_group_spec,
                "workerGroupSpecs": worker_group_specs,
            },
        },
        namespace=NAMESPACE,
    )

    client.wait_until_ready(
        name=PERSISTENT_RAY_CLUSTER_NAME,
        namespace=NAMESPACE,
        timeout=600,
    )

    with client.port_forward(
        name=PERSISTENT_RAY_CLUSTER_NAME,
        namespace=NAMESPACE,
        local_dashboard_port=0,
        local_gcs_port=0,
    ) as (dashboard_port, redis_port):
        yield (
            {"gcs": f"ray://localhost:{redis_port}", "dashboard": f"http://localhost:{dashboard_port}"},
            k8s_with_kuberay,
        )

    client.delete(
        name=PERSISTENT_RAY_CLUSTER_NAME,
        namespace=NAMESPACE,
    )
