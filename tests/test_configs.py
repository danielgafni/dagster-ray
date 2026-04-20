from __future__ import annotations

import warnings

import pytest
from pydantic import ValidationError

from dagster_ray.configs import RayDataExecutionOptions


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
