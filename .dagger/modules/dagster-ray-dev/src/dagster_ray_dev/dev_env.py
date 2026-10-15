import tomllib

from dagger_clients.core import Container, Directory, core
from dagger_clients.uv import uv

REPO_DIR = "/src"
VENV_DIR = "/opt/venv"


async def pyright_python_version(repo: Directory) -> str:
    """The Python version basedpyright checks against, the oldest one dagster-ray supports."""
    pyproject = tomllib.loads(await repo.file("pyproject.toml").contents())
    return pyproject["tool"]["pyright"]["pythonVersion"]


async def ruff_version(repo: Directory) -> str:
    """The ruff version pinned in uv.lock."""
    lock = tomllib.loads(await repo.file("uv.lock").contents())
    return next(package["version"] for package in lock["package"] if package["name"] == "ruff")


def dev_env(repo: Directory, python_version: str, groups: list[str] | None = None) -> Container:
    """The repository with all extras, the dev group and `groups` installed."""
    base = (
        core()
        .container()
        .from_(f"python:{python_version}-slim")
        # some dependency groups have git sources
        .with_exec(["sh", "-c", "apt-get update && apt-get install -y --no-install-recommends git"])
        .with_env_variable("UV_PROJECT_ENVIRONMENT", VENV_DIR)
        .with_env_variable("PATH", f"{VENV_DIR}/bin:$PATH", expand=True)
        .with_workdir(REPO_DIR)
    )
    return (
        uv(source=repo)
        .workspace()
        .install(base_container=base, all_extras=True, group=groups)
        .with_directory(REPO_DIR, repo)
    )
