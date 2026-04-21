from __future__ import annotations

import sys
import warnings
from types import ModuleType, SimpleNamespace

import pytest
from pydantic import ValidationError

from dagster_ray.configs import RayDataExecutionOptions


class _FakeExecutionResources:
    @staticmethod
    def for_limits(
        cpu: int | None,
        gpu: int | None,
        object_store_memory: int | None,
    ) -> tuple[int | None, int | None, int | None]:
        return cpu, gpu, object_store_memory


def _install_fake_ray(monkeypatch: pytest.MonkeyPatch, ray_version: str) -> SimpleNamespace:
    context = SimpleNamespace(execution_options=SimpleNamespace(resource_limits=None), verbose_progress=None)

    ray_module = ModuleType("ray")
    ray_data_module = ModuleType("ray.data")

    def get_current() -> SimpleNamespace:
        return context

    setattr(ray_data_module, "DatasetContext", SimpleNamespace(get_current=get_current))
    setattr(ray_data_module, "ExecutionResources", _FakeExecutionResources)
    setattr(ray_module, "__version__", ray_version)
    setattr(ray_module, "data", ray_data_module)

    monkeypatch.setitem(sys.modules, "ray", ray_module)
    monkeypatch.setitem(sys.modules, "ray.data", ray_data_module)

    return context


def test_use_polars_sort_defaults_to_true():
    opts = RayDataExecutionOptions()
    assert opts.use_polars_sort is True
    # accessing the deprecated field emits a warning, so silence it for this assertion
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        assert opts.use_polars is None


def test_deprecated_use_polars_is_forwarded_to_use_polars_sort():
    opts = RayDataExecutionOptions(use_polars=False)
    assert opts.use_polars_sort is False
    # reading the deprecated attribute warns and preserves backward compatibility
    with pytest.warns(DeprecationWarning, match="use_polars_sort"):
        assert opts.use_polars is False


def test_use_polars_and_use_polars_sort_in_agreement_is_accepted():
    opts = RayDataExecutionOptions(use_polars=True, use_polars_sort=True)
    assert opts.use_polars_sort is True


def test_use_polars_and_use_polars_sort_contradicting_raises():
    with pytest.raises(ValidationError, match="contradicting values"):
        RayDataExecutionOptions(use_polars=True, use_polars_sort=False)
    with pytest.raises(ValidationError, match="contradicting values"):
        RayDataExecutionOptions(use_polars=False, use_polars_sort=True)


def test_use_polars_sort_alone_does_not_warn_on_construction():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        opts = RayDataExecutionOptions(use_polars_sort=False)
    ours = [w for w in caught if "use_polars" in str(w.message)]
    assert ours == []
    assert opts.use_polars_sort is False


@pytest.mark.parametrize(
    ("ray_version", "expected_polars_attr"),
    [
        ("2.55.0", "use_polars_sort"),
        ("2.49.0", "use_polars"),
    ],
)
def test_apply_writes_forwarded_legacy_value_to_ray_version_specific_context_attr(
    monkeypatch: pytest.MonkeyPatch,
    ray_version: str,
    expected_polars_attr: str,
):
    context = _install_fake_ray(monkeypatch, ray_version)

    RayDataExecutionOptions(use_polars=False).apply()

    assert context.execution_options.resource_limits == (None, None, None)
    assert context.verbose_progress is True
    assert getattr(context, expected_polars_attr) is False
