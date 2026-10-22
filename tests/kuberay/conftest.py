import copy
import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
import pytest_cases
from kubernetes import config  # noqa: TID253
from packaging.version import Version
from pytest_kubernetes.options import ClusterOptions
from pytest_kubernetes.providers import AClusterManager

from dagster_ray.kuberay.client import RayClusterClient
from dagster_ray.kuberay.configs import DEFAULT_HEAD_GROUP_SPEC, DEFAULT_WORKER_GROUP_SPECS, AuthOptions
from tests.kuberay.utils import HEAD_READINESS_PROBE, NAMESPACE, RAY_POD_RESOURCES, RAY_START_PARAMS

logger = logging.getLogger(__name__)


# the cluster and the image come from the Dagger `test-kuberay*` checks, see .dagger/README.md
PYTEST_DAGSTER_RAY_IMAGE = os.getenv("PYTEST_DAGSTER_RAY_IMAGE")
PYTEST_KUBECONFIG = os.getenv("PYTEST_KUBECONFIG")
SKIP_WITHOUT_DAGGER = "KubeRay tests run in Dagger: `dagger check 'test-kuberay*'`"


@pytest.fixture(scope="session")
def dagster_ray_image() -> str:
    if PYTEST_DAGSTER_RAY_IMAGE is None:
        pytest.skip(SKIP_WITHOUT_DAGGER)
    return PYTEST_DAGSTER_RAY_IMAGE


class ExistingClusterManager(AClusterManager):
    """A cluster that outlives the test session: creating it waits for readiness, deleting it is a no-op."""

    @classmethod
    def get_binary_name(cls) -> str:
        return "kubectl"

    def _on_create(self, cluster_options: ClusterOptions, **kwargs) -> None:
        pass

    def _on_delete(self) -> None:
        pass

    def load_image(self, image: str) -> None:
        raise NotImplementedError("images have to be loaded by whatever created the cluster")


KUBERAY_VERSIONS = os.getenv("PYTEST_KUBERAY_VERSIONS", "1.4.0").split(",")
KUBERNETES_CONTEXT = "pytest-dagster-ray"


@pytest_cases.fixture(scope="session")  # type: ignore
@pytest.mark.parametrize("kuberay_version_param", KUBERAY_VERSIONS)
def kuberay_version(kuberay_version_param: str):
    return kuberay_version_param


@pytest.fixture(scope="session")
def ray_version() -> str:
    """Get the Ray version from the installed ray package."""
    import ray

    return ray.__version__


@pytest.fixture(scope="session")  # type: ignore
def k8s_with_kuberay(dagster_ray_image: str, kuberay_version: str) -> Iterator[AClusterManager]:
    if PYTEST_KUBECONFIG is None:
        pytest.skip(SKIP_WITHOUT_DAGGER)
    k8s: AClusterManager = ExistingClusterManager(KUBERNETES_CONTEXT[7:])  # strip pytest-
    k8s.create(ClusterOptions(kubeconfig_path=Path(PYTEST_KUBECONFIG)))

    k8s.wait("deployment/kuberay-operator", "condition=Available=True", namespace="kuberay-operator")
    # namespace to create RayClusters in
    try:
        k8s.kubectl(["create", "namespace", NAMESPACE])
    except RuntimeError as e:
        if "AlreadyExists" not in str(e):
            raise

    yield k8s


@pytest.fixture(scope="session")
def head_group_spec(dagster_ray_image: str) -> dict[str, Any]:
    head_group_spec = DEFAULT_HEAD_GROUP_SPEC.copy()
    head_group_spec["serviceType"] = "LoadBalancer"
    head_group_spec["template"]["spec"]["containers"][0]["image"] = dagster_ray_image
    head_group_spec["template"]["spec"]["containers"][0]["imagePullPolicy"] = "IfNotPresent"
    head_group_spec["template"]["spec"]["containers"][0]["env"] = head_group_spec["template"]["spec"]["containers"][
        0
    ].get("env", []) + [
        # to prevent timeout errors in low-resource CI environments
        {"name": "RAY_DASHBOARD_SUBPROCESS_MODULE_WAIT_READY_TIMEOUT", "value": "120"},
    ]
    head_group_spec["template"]["spec"]["containers"][0]["resources"] = copy.deepcopy(RAY_POD_RESOURCES)
    head_group_spec["template"]["spec"]["containers"][0]["readinessProbe"] = copy.deepcopy(HEAD_READINESS_PROBE)
    head_group_spec["rayStartParams"] = {**head_group_spec.get("rayStartParams", {}), **RAY_START_PARAMS}
    return head_group_spec


@pytest.fixture(scope="session")
def worker_group_specs(dagster_ray_image: str) -> list[dict[str, Any]]:
    worker_group_specs = DEFAULT_WORKER_GROUP_SPECS.copy()
    worker_group_specs[0]["template"]["spec"]["containers"][0]["image"] = dagster_ray_image
    worker_group_specs[0]["template"]["spec"]["containers"][0]["imagePullPolicy"] = "IfNotPresent"
    worker_group_specs[0]["template"]["spec"]["containers"][0]["resources"] = copy.deepcopy(RAY_POD_RESOURCES)
    worker_group_specs[0]["rayStartParams"] = {**worker_group_specs[0].get("rayStartParams", {}), **RAY_START_PARAMS}
    return worker_group_specs


PERSISTENT_RAY_CLUSTER_NAME = "persistent-ray-cluster"
# generous: the Ray head starts slowly under its CPU limit on a busy CI node
RAYJOB_TIMEOUT = 120

# Auth-enabled cluster constants
AUTH_RAY_CLUSTER_NAME = "persistent-ray-cluster-with-auth"
AUTH_TOKEN = "test-auth-token-12345"
AUTH_SECRET_NAME = "ray-auth-token-secret"
MIN_RAY_VERSION_FOR_AUTH = "2.52.0"  # Ray token auth requires 2.52.0+
# `authOptions` appeared in KubeRay 1.5.2 with only `mode`, and gained `secretName` in 1.6.0.
# Without `secretName` KubeRay generates its own token Secret, whose value the test can't know,
# so below 1.6.0 the auth env vars have to be injected by hand instead.
MIN_KUBERAY_VERSION_FOR_AUTH_OPTIONS = "1.6.0"


@contextmanager
def _auth_secret(k8s: AClusterManager, namespace: str, secret_name: str, token: str) -> Iterator[None]:
    """Create a Kubernetes secret for Ray auth token and delete it on exit."""
    k8s.kubectl(["create", "secret", "generic", secret_name, f"--from-literal=auth_token={token}", "-n", namespace])
    try:
        yield
    finally:
        try:
            k8s.kubectl(["delete", "secret", secret_name, "-n", namespace, "--ignore-not-found"])
        except RuntimeError:
            pass


@pytest.fixture(scope="session")
def k8s_with_raycluster(
    k8s_with_kuberay: AClusterManager,
    head_group_spec: dict[str, Any],
    worker_group_specs: list[dict[str, Any]],
) -> Iterator[tuple[dict[str, str], AClusterManager]]:
    # create a RayCluster
    config.load_kube_config(str(k8s_with_kuberay.kubeconfig), context=KUBERNETES_CONTEXT)

    client = RayClusterClient(kube_config=str(k8s_with_kuberay.kubeconfig), kube_context=KUBERNETES_CONTEXT)

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


@pytest.fixture(scope="session")
def k8s_with_raycluster_with_auth(
    k8s_with_kuberay: AClusterManager,
    head_group_spec: dict[str, Any],
    ray_version: str,
    kuberay_version: str,
) -> Iterator[tuple[dict[str, str], str, AClusterManager]]:
    """Create a RayCluster with token authentication enabled.

    Requires Ray 2.52.0+ for token authentication support.
    Uses no workers to minimize resource usage alongside other test clusters.

    From KubeRay 1.6.0 the auth env vars come from `spec.authOptions`, built by
    [`AuthOptions`][dagster_ray.kuberay.configs.AuthOptions], so this exercises the config class
    users actually configure. On older operators they are injected into the head container
    directly, which is what KubeRay does for us otherwise.

    Returns:
        tuple: (addresses_dict, auth_token, cluster_manager)
    """
    if Version(ray_version) < Version(MIN_RAY_VERSION_FOR_AUTH):
        pytest.skip(f"Ray {MIN_RAY_VERSION_FOR_AUTH}+ is required for token authentication (current: {ray_version})")

    config.load_kube_config(str(k8s_with_kuberay.kubeconfig), context=KUBERNETES_CONTEXT)

    client = RayClusterClient(kube_config=str(k8s_with_kuberay.kubeconfig), kube_context=KUBERNETES_CONTEXT)

    head_spec = copy.deepcopy(head_group_spec)

    cluster_spec: dict[str, Any] = {
        "headGroupSpec": head_spec,
        "workerGroupSpecs": [],
    }

    if Version(kuberay_version) >= Version(MIN_KUBERAY_VERSION_FOR_AUTH_OPTIONS):
        # KubeRay injects RAY_AUTH_MODE and RAY_AUTH_TOKEN (from our Secret's `auth_token` key)
        # into the Ray container itself once authOptions.mode is `token`.
        #
        # `rayVersion` is mandatory with token auth: without it KubeRay marks the RayCluster spec
        # invalid and never reconciles it, so the cluster never reports a status at all.
        cluster_spec["rayVersion"] = ray_version
        cluster_spec["authOptions"] = AuthOptions(mode="token", secret_name=AUTH_SECRET_NAME).to_k8s()
    else:
        head_container = head_spec["template"]["spec"]["containers"][0]
        if "env" not in head_container:
            head_container["env"] = []
        head_container["env"].extend(
            [
                {"name": "RAY_AUTH_MODE", "value": "token"},
                {
                    "name": "RAY_AUTH_TOKEN",
                    "valueFrom": {"secretKeyRef": {"name": AUTH_SECRET_NAME, "key": "auth_token"}},
                },
            ]
        )

    manifest = {
        "kind": "RayCluster",
        "apiVersion": "ray.io/v1",
        "metadata": {"name": AUTH_RAY_CLUSTER_NAME},
        "spec": cluster_spec,
    }

    with _auth_secret(k8s_with_kuberay, NAMESPACE, AUTH_SECRET_NAME, AUTH_TOKEN):
        client.create(
            body=manifest,
            namespace=NAMESPACE,
        )
        logger.info(f"Manifest: {manifest}")

        client.wait_until_ready(
            name=AUTH_RAY_CLUSTER_NAME,
            namespace=NAMESPACE,
            timeout=600,
        )

        with client.port_forward(
            name=AUTH_RAY_CLUSTER_NAME,
            namespace=NAMESPACE,
            local_dashboard_port=0,
            local_gcs_port=0,
        ) as (dashboard_port, redis_port):
            yield (
                {"gcs": f"ray://localhost:{redis_port}", "dashboard": f"http://localhost:{dashboard_port}"},
                AUTH_TOKEN,
                k8s_with_kuberay,
            )

        client.delete(
            name=AUTH_RAY_CLUSTER_NAME,
            namespace=NAMESPACE,
        )


@pytest.fixture(autouse=True)
def ray_shutdown():
    """Ensure there is not existing Ray connection both before and after each test,

    since all the tests in this module are using remote Ray clusters."""
    try:
        import ray

        if ray.is_initialized():
            ray.shutdown()
    except ImportError:
        pass

    yield
    try:
        import ray

        if ray.is_initialized():
            ray.shutdown()
    except ImportError:
        pass
