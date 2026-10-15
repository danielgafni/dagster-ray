from typing import Annotated

from dagger import Doc, check, function, generate, object_type
from dagger_clients.core import Changeset, Directory, Secret, Workspace, core
from dagger_clients.github import github

from dagster_ray_dev.compatibility import COMPATIBILITY_MATRIX_PATH, compatibility_matrix
from dagster_ray_dev.dev_env import REPO_DIR, dev_env, pyright_python_version, ruff_version
from dagster_ray_dev.image import Image
from dagster_ray_dev.kuberay import KuberayCluster
from dagster_ray_dev.test_job import TestJob
from dagster_ray_dev.test_matrix import MATRIX, add_test_checks

# what the image and the tests need, so unrelated changes (docs, CI, editor config) keep the build cached
SOURCE_INCLUDE = ["src", "tests", "examples", "pyproject.toml", "uv.lock", "README.md", ".python-version"]


@object_type
@add_test_checks
class DagsterRayDev:
    source: Directory
    repo: Directory

    @classmethod
    def create(cls, ws: Workspace) -> DagsterRayDev:
        return cls(
            source=ws.directory("/", include=SOURCE_INCLUDE, gitignore=True),
            repo=ws.directory("/", exclude=[".dagger", ".git"], gitignore=True),
        )

    async def _ruff(self, *args: str) -> None:
        await (
            core()
            .container()
            .from_(f"ghcr.io/astral-sh/ruff:{await ruff_version(self.repo)}")
            .with_directory(REPO_DIR, self.repo)
            .with_workdir(REPO_DIR)
            .with_exec(["/ruff", *args])
            .sync()
        )

    @function
    @check
    async def ruff_check(self) -> None:
        """Lint with `ruff check`."""
        await self._ruff("check")

    @function
    @check
    async def ruff_format(self) -> None:
        """Check formatting with `ruff format --check`."""
        await self._ruff("format", "--check")

    @function
    @check
    async def typecheck(self) -> None:
        """Type check with basedpyright."""
        python_version = await pyright_python_version(self.repo)
        await dev_env(self.repo, python_version).with_exec(["basedpyright", "--level", "error"]).sync()

    @function
    async def docs(self) -> Directory:
        """The built documentation site."""
        python_version = await pyright_python_version(self.repo)
        return (
            dev_env(self.repo, python_version, groups=["docs"])
            .with_exec(["zensical", "build", "--strict", "--clean"])
            .directory(f"{REPO_DIR}/site")
        )

    @function
    @check
    async def build_docs(self) -> None:
        """Build the documentation with zensical in strict mode."""
        await (await self.docs()).sync()

    @function
    @generate
    async def gen_docs(self) -> Changeset:
        """Generate docs/compatibility_matrix.md from pyproject.toml and test-matrix.toml."""
        page = compatibility_matrix(await self.repo.file("pyproject.toml").contents(), MATRIX)
        return self.repo.with_new_file(COMPATIBILITY_MATRIX_PATH, page).changes(self.repo)

    @function
    async def wait_for_cloud_checks(
        self,
        ws: Workspace,
        repo: Annotated[str, Doc("GitHub repository as `owner/name`")],
        ref: Annotated[str, Doc("Commit SHA")],
        token: Annotated[Secret, Doc("GitHub token that can read commit statuses")],
    ) -> str:
        """Wait until every check of this workspace passes on `ref`. GitHub CI runs this.

        Dagger Cloud reports each check as a GitHub commit status named after the check.
        """
        # the github module's wait-for-dagger-checks lists checks with an API engine 1.0 no longer has
        checks = [
            "/".join(await artifact.path()) for artifact in await ws.artifacts().filter_directives(["check"]).items()
        ]
        return await (
            github().status_monitor().wait_for_statuses(repo=repo, ref=ref, token=token, checks=checks, timeout=10800)
        )

    @function
    def image(
        self,
        python_version: str = "3.13",
        ray_version: str = "2.59.0",
        dagster_version: str = "1.13.15",
    ) -> Image:
        """dagster-ray for one Python, Ray and Dagster version."""
        return Image(
            source=self.source,
            python_version=python_version,
            ray_version=ray_version,
            dagster_version=dagster_version,
        )

    @function
    def kuberay(self, version: str = "1.7.0", kubernetes_version: str = "1.31.9") -> KuberayCluster:
        """A k3s cluster with the KubeRay operator."""
        return KuberayCluster(version=version, kubernetes_version=kubernetes_version)

    @function
    def test_job(
        self,
        python_version: str = "3.13",
        ray_version: str = "2.59.0",
        dagster_version: str = "1.13.15",
        kuberay_version: str = "1.7.0",
        kubernetes_version: str = "1.31.9",
    ) -> TestJob:
        """The test suite for one Python, Ray and Dagster version against one KubeRay version."""
        return TestJob(
            source=self.source,
            python_version=python_version,
            ray_version=ray_version,
            dagster_version=dagster_version,
            kuberay_version=kuberay_version,
            kubernetes_version=kubernetes_version,
        )

    @function
    async def test(
        self,
        python_version: str = "3.13",
        ray_version: str = "2.59.0",
        dagster_version: str = "1.13.15",
        kuberay_version: str = "1.7.0",
    ) -> None:
        """Run the tests for one version combination against KubeRay in k3s. See `test-job run`."""
        await self.test_job(python_version, ray_version, dagster_version, kuberay_version).run()
