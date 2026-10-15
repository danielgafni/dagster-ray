from pathlib import Path
from typing import Any

from dagger import check, function

from dagster_ray_dev.matrix import expand

# read at import, so the checks are part of the module's schema
MATRIX = expand((Path(__file__).parent / "test-matrix.toml").read_text())


def _add_check(cls: type, name: str, doc: str, run: str, **versions: str) -> None:
    async def test(self: Any) -> None:
        await getattr(self.test_job(**versions), run)()

    test.__name__ = test.__qualname__ = name.replace(".", "_")
    test.__doc__ = doc
    setattr(cls, test.__name__, function(check(test)))


def add_test_checks[T: type](cls: T) -> T:
    """Add a check per Python, Ray and Dagster combination in test-matrix.toml for the tests outside
    tests/kuberay, and a check per matrix entry for tests/kuberay.

    Separate checks run on separate Dagger Cloud engines. Run `dagger generate` after changing test-matrix.toml.
    """
    for py, ray, dagster in dict.fromkeys((v.python, v.ray, v.dagster) for v in MATRIX.versions):
        _add_check(
            cls,
            f"test_core_py{py}_ray{ray}_dagster{dagster}",
            f"Run the tests outside tests/kuberay with Python {py}, Ray {ray} and Dagster {dagster}.",
            "run_core",
            python_version=py,
            ray_version=ray,
            dagster_version=dagster,
        )
    for v in MATRIX.versions:
        _add_check(
            cls,
            f"test_kuberay{v.kuberay}_py{v.python}_ray{v.ray}_dagster{v.dagster}",
            f"Run tests/kuberay against KubeRay {v.kuberay}, with Python {v.python}, Ray {v.ray}, Dagster {v.dagster}.",
            "run_kuberay",
            python_version=v.python,
            ray_version=v.ray,
            dagster_version=v.dagster,
            kuberay_version=v.kuberay,
            kubernetes_version=MATRIX.kubernetes,
        )
    return cls
