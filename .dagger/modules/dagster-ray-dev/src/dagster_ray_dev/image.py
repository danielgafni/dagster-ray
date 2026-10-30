import time

from dagger import field, function, object_type
from dagger.telemetry import get_tracer
from dagger_clients.core import Container, Directory, Service, core
from dagger_clients.uv import uv

from dagster_ray_dev.registry import REGISTRY_HOST

UV_IMAGE = "ghcr.io/astral-sh/uv:0.11.31"
SKOPEO_IMAGE = "quay.io/skopeo/stable:v1.20.0"
# the tests expect the repository at /src inside Ray pods too
APP_DIR = "/src"
VENV_DIR = "/opt/venv"


@object_type
class Image:
    """dagster-ray with all extras and dev dependencies for one Python, Ray and Dagster version.

    Used for both Ray pods and the test runner.
    """

    source: Directory
    python_version: str = field()
    ray_version: str = field()
    dagster_version: str = field()

    @function
    def tag(self) -> str:
        return f"py{self.python_version}-ray{self.ray_version}-dagster{self.dagster_version}"

    @function
    def pinned_source(self) -> Directory:
        """The source with Python, Ray and Dagster pinned in .python-version, pyproject.toml and uv.lock."""
        # scoped to this Python so the lock still resolves for the rest of requires-python
        marker = f"python_full_version == '{self.python_version}.*'"
        return (
            core()
            .container()
            .from_(f"python:{self.python_version}-slim")
            # resolving the lock fetches git sources
            .with_exec(["sh", "-c", "apt-get update && apt-get install -y --no-install-recommends git"])
            .with_file("/usr/local/bin/uv", core().container().from_(UV_IMAGE).file("/uv"))
            .with_directory("/src", self.source)
            .with_workdir("/src")
            .with_new_file(".python-version", f"{self.python_version}\n")
            .with_exec(
                [
                    "uv",
                    "add",
                    "--no-sync",
                    f"ray[all]=={self.ray_version} ; {marker}",
                    f"dagster=={self.dagster_version} ; {marker}",
                ]
            )
            .directory("/src")
        )

    @function
    def container(self) -> Container:
        base = (
            core()
            .container()
            .from_(f"python:{self.python_version}-slim")
            # KubeRay's probes call wget
            .with_exec(
                [
                    "sh",
                    "-c",
                    "apt-get update && apt-get install -y --no-install-recommends wget && rm -rf /var/lib/apt/lists/*",
                ]
            )
            # Ray pods find the `ray` CLI on PATH
            .with_env_variable("UV_PROJECT_ENVIRONMENT", VENV_DIR)
            .with_env_variable("PATH", f"{VENV_DIR}/bin:$PATH", expand=True)
            # KubeRay before 1.4 starts Ray with `bash -l`, and /etc/profile resets PATH
            .with_new_file("/etc/profile.d/venv.sh", f'export PATH="{VENV_DIR}/bin:$PATH"\n')
            # the uv module installs the project into the working directory
            .with_workdir(APP_DIR)
        )
        source = self.pinned_source()
        return (
            uv(source.as_workspace())
            .workspace(".")
            .builder(base_container=base)
            .with_all_extras()
            .with_dependencies()
            .container()
            # Ray pods run scripts from tests/
            .with_directory(APP_DIR, source)
        )

    @function
    async def push(self, registry: Service) -> str:
        """Build the image and push it to `registry`. Returns the image reference."""
        ref = f"{REGISTRY_HOST}/dagster-ray:{self.tag()}"
        with get_tracer().start_as_current_span(f"build image {self.tag()}"):
            container = await self.container().sync()
        with get_tracer().start_as_current_span(f"push image {ref}"):
            await (
                core()
                .container()
                .from_(SKOPEO_IMAGE)
                .with_service_binding("registry", registry)
                .with_file("/image.tar", container.as_tarball())
                # the registry starts empty on every call, so the push must not be cached
                .with_env_variable("CACHEBUST", str(time.time_ns()))
                .with_exec(
                    ["skopeo", "copy", "--dest-tls-verify=false", "docker-archive:/image.tar", f"docker://{ref}"]
                )
                .sync()
            )
        return ref
