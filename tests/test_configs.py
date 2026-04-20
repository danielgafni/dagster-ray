from __future__ import annotations

import warnings

import pytest

from dagster_ray.configs import RayDataExecutionOptions


def test_use_polars_sort_defaults_to_true():
    opts = RayDataExecutionOptions()
    assert opts.use_polars_sort is True
    assert opts.use_polars is None


def test_deprecated_use_polars_is_forwarded_to_use_polars_sort():
    with pytest.warns(DeprecationWarning, match="use_polars_sort"):
        opts = RayDataExecutionOptions(use_polars=False)
    assert opts.use_polars_sort is False
    # read-back of the deprecated attribute is preserved for backward compatibility
    assert opts.use_polars is False


def test_explicit_use_polars_sort_wins_over_deprecated_alias():
    with pytest.warns(DeprecationWarning):
        opts = RayDataExecutionOptions(use_polars=True, use_polars_sort=False)
    assert opts.use_polars_sort is False


def test_use_polars_sort_alone_does_not_warn_on_construction():
    # Construction with only the new field must not emit our deprecation warning.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        opts = RayDataExecutionOptions(use_polars_sort=False)
    ours = [w for w in caught if "use_polars_sort" in str(w.message) and "deprecated" in str(w.message).lower()]
    assert ours == []
    assert opts.use_polars_sort is False
