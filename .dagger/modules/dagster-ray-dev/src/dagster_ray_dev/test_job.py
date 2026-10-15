import asyncio
from collections.abc import Awaitable

from dagger import field, function, object_type
from dagger.telemetry import get_tracer
from dagger_clients.core import Container, Directory, File, core

from dagster_ray_dev.image import Image
from dagster_ray_dev.kuberay import KuberayCluster
from dagster_ray_dev.registry import registry

KUBECTL_VERSION = "v1.34.1"
# the tests address the cluster by this context name
TEST_KUBE_CONTEXT = "pytest-dagster-ray"
GIB = 2**30


@object_type
class TestJob:
    """The test suite for one Python, Ray and Dagster version against one KubeRay version."""

    source: Directory
    python_version: str = field()
    ray_version: str = field()
    dagster_version: str = field()
    kuberay_version: str = field()
    kubernetes_version: str = field()

    @function
    def key(self) -> str:
        return (
            f"py{self.python_version}-ray{self.ray_version}-dagster{self.dagster_version}-kuberay{self.kuberay_version}"
        )

    @function
    def label(self) -> str:
        return (
            f"Python {self.python_version}, Ray {self.ray_version}, "
            f"Dagster {self.dagster_version}, KubeRay {self.kuberay_version}"
        )

    @function
    def namespace(self) -> str:
        """The Kubernetes namespace for this job's resources, so jobs can share a cluster."""
        return self.key().replace(".", "-")

    @function
    def image(self) -> Image:
        return Image(
            source=self.source,
            python_version=self.python_version,
            ray_version=self.ray_version,
            dagster_version=self.dagster_version,
        )

    @function
    def kuberay(self) -> KuberayCluster:
        return KuberayCluster(
            version=self.kuberay_version, kubernetes_version=self.kubernetes_version, instance=self.key()
        )

    def _runner(self) -> Container:
        """The image as a test runner, with room for a local Ray object store in /dev/shm."""
        return (
            self.image()
            .container()
            # containers get a 64 MiB /dev/shm, too small for Ray's object store, which then falls back to /tmp
            .with_mounted_temp("/dev/shm", size=3 * GIB)
            .with_env_variable("RAY_DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES", str(2 * GIB))
        )

    @function
    async def run_core(self) -> None:
        """Run the tests that don't need Kubernetes."""
        with get_tracer().start_as_current_span("pytest without KubeRay"):
            await self._runner().with_exec(["pytest", "-v", "--ignore=tests/kuberay"]).sync()

    @function
    async def run_kuberay(self, kubeconfig: File | None = None) -> None:
        """Run the KubeRay tests, then the `kuberay_auth` tests.

        Without `kubeconfig`, starts a cluster for this job. With it, uses that cluster,
        which must come from `kuberay` (`TEST_KUBE_CONTEXT` context) and pull from the registry service.
        """
        image = self.image()
        # started explicitly so the push and the cluster use one registry instance; a service that is only
        # bound can stop between the push and the cluster starting, and then the cluster sees an empty registry
        ref = await image.push(await registry().start())
        if kubeconfig is None:
            kubeconfig = await self.kuberay().kubeconfig(context=TEST_KUBE_CONTEXT)

        os_arch = "/".join((await core().default_platform()).split("/")[:2])
        runner = (
            self._runner()
            .with_file(
                "/usr/local/bin/kubectl",
                core().http(f"https://dl.k8s.io/release/{KUBECTL_VERSION}/bin/{os_arch}/kubectl"),
                permissions=0o755,
            )
            .with_file("/root/.kube/config", kubeconfig)
            .with_env_variable("PYTEST_KUBECONFIG", "/root/.kube/config")
            .with_env_variable("PYTEST_DAGSTER_RAY_IMAGE", ref)
            .with_env_variable("PYTEST_KUBERAY_VERSIONS", self.kuberay_version)
            .with_env_variable("PYTEST_KUBERAY_NAMESPACE", self.namespace())
            # namespaces watched by the cleanup sensor under test
            .with_env_variable("DAGSTER_RAY_NAMESPACES", self.namespace())
        )
        for args in (["-v", "tests/kuberay"], ["-v", "-m", "kuberay_auth", "tests/kuberay"]):
            with get_tracer().start_as_current_span(f"pytest {' '.join(args)}"):
                runner = await runner.with_exec(["pytest", *args]).sync()

    async def run_with(self, kubeconfig: Awaitable[File] | None) -> None:
        """Run the core and KubeRay tests concurrently; only the KubeRay tests wait for `kubeconfig`."""

        async def kuberay() -> None:
            await self.run_kuberay(None if kubeconfig is None else await kubeconfig)

        results = await asyncio.gather(self.run_core(), kuberay(), return_exceptions=True)
        errors = [result for result in results if isinstance(result, BaseException)]
        if errors:
            raise errors[0]

    @function
    async def run(self) -> None:
        """Run the core and KubeRay tests concurrently, on a cluster started for this job."""
        await self.run_with(None)
