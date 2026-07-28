import re

import pytest

from dagster_ray.kuberay.utils import (
    merge_extra_k8s_fields,
    normalize_k8s_label_values,
    to_camel_case,
)

# From the K8s label-value spec:
# https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
_K8S_LABEL_VALUE = re.compile(r"^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$")


def _assert_all_k8s_valid(labels: dict[str, str]) -> None:
    for k, v in labels.items():
        assert len(v) <= 63, f"{k!r}: value longer than 63 chars: {v!r}"
        assert _K8S_LABEL_VALUE.match(v), f"{k!r}: value violates K8s label regex: {v!r}"


def test_normalize_k8s_label_values(snapshot):
    out = normalize_k8s_label_values(
        {
            "foo": "bar",
            "my/label": "my/value",
            "user": "daniel@my.org",
            "user-dirty": "daniel!`~@my.org",
            "alphanumeric": "abc123",
            "long": 64 * "a",
            "badstart": "-foo",
            "badstart_after_initial_replace": "@foo",
        }
    )
    assert snapshot == out
    _assert_all_k8s_valid(out)


def test_normalize_k8s_label_values_important_labels():
    out = normalize_k8s_label_values({"dagster/run-id": "12345"})
    assert out == {"dagster/run-id": "12345"}
    _assert_all_k8s_valid(out)


@pytest.mark.parametrize(
    "value",
    [
        # Git branch name > 63 chars whose 63rd char is `_`. Regression for
        # KubeRay RayCluster creation failing with
        # `metadata.labels: Invalid value … must start and end with an
        # alphanumeric character` when a PR's long branch name was being
        # stuck into a `dagster/git-branch`-style label.
        "04-22-datastack_reshape_metaxy_map-entry_columns_on_inputmodel_"
        "to_silence_spurious_pydantic_serializer_warnings_in_ray_workers",
        # Truncation landing on `.`
        "a" * 63 + "." + "b" * 10,
        # Truncation landing on `-`
        "a" * 63 + "-" + "b" * 10,
        # Collapsed-hyphen run exactly at position 63.
        "a" * 62 + "--" + "b" * 10,
        # Leading non-alnum run that does NOT collapse (`_` / `.` are not
        # collapsed like `-+` is). If we truncated first, these would eat the
        # 63-char budget and we'd strip the whole thing to empty. Stripping
        # leading first preserves the useful tail.
        "_" * 40 + "useful-and-specific-branch-name-that-should-survive",
        "." * 40 + "useful-and-specific-branch-name-that-should-survive",
        # Daniel's case: leading garbage of chars that the invalid-char
        # replacement turns into `-` (and then collapses), followed by a
        # useful tail that we want to keep.
        "§" * 39 + "-finally-something-useful-and-specific-that-should-survive",
    ],
)
def test_normalize_k8s_label_values_truncated_tail_is_alphanumeric(value: str) -> None:
    """Truncation to 63 chars must never leave a trailing non-alphanumeric
    character; doing so produces a label value that K8s rejects with HTTP 422."""
    out = normalize_k8s_label_values({"k": value})["k"]

    assert len(out) <= 63
    assert out == "" or out[-1].isalnum(), f"label ends non-alphanumeric: {out!r}"
    assert _K8S_LABEL_VALUE.match(out), f"value violates K8s label regex: {out!r}"


def test_normalize_k8s_label_values_preserves_useful_tail_after_leading_garbage() -> None:
    """Stripping leading non-alphanumerics before truncation preserves useful
    characters when the input has a long leading non-alnum run. Truncating
    first would eat the budget on characters we'd immediately strip."""
    useful = "useful-and-specific-branch-name-that-should-survive"
    out = normalize_k8s_label_values({"k": "_" * 40 + useful})
    assert out["k"].startswith("useful"), out
    _assert_all_k8s_valid(out)


def test_normalize_k8s_label_values_rejects_unexpectedly_invalid_output(monkeypatch) -> None:
    """The final K8s-regex check at label creation time should raise if
    normalization ever produces an invalid value."""
    import dagster_ray.kuberay.utils as utils_mod

    # Force the trailing-strip to be a no-op to simulate a normalization bug.
    # Input: 62 `a`s + `_` + 10 `b`s — truncation to 63 leaves `_` at the tail.
    monkeypatch.setattr(utils_mod, "_TRAILING_NON_ALNUM_PATTERN", re.compile(r"(?!)"))
    with pytest.raises(ValueError, match="K8s label-value regex"):
        utils_mod.normalize_k8s_label_values({"k": "a" * 62 + "_" + "b" * 10})


def test_to_camel_case() -> None:
    assert to_camel_case("pre_running_deadline_seconds") == "preRunningDeadlineSeconds"
    assert to_camel_case("entrypoint_num_cpus") == "entrypointNumCpus"
    # already camelCase -> unchanged, so specs copy-pasted from the KubeRay docs work as-is
    assert to_camel_case("preRunningDeadlineSeconds") == "preRunningDeadlineSeconds"
    assert to_camel_case("suspend") == "suspend"
    # interior capitalization is preserved (`str.capitalize` would give `runtimeEnvYaml`)
    assert to_camel_case("runtime_env_YAML") == "runtimeEnvYAML"


def test_merge_extra_k8s_fields_merges_and_converts_case() -> None:
    merged = merge_extra_k8s_fields(
        {"suspend": False},
        {"pre_running_deadline_seconds": 300, "someBrandNewField": "hello"},
    )
    assert merged == {
        "suspend": False,
        "preRunningDeadlineSeconds": 300,
        "someBrandNewField": "hello",
    }


def test_merge_extra_k8s_fields_passes_values_through_untouched() -> None:
    """Only the top-level key is converted; values are never recursed into. Declared dict
    fields already work this way (DEFAULT_HEAD_GROUP_SPEC holds raw camelCase)."""
    merged = merge_extra_k8s_fields({}, {"some_new_field": {"nested_key": 1}})
    assert merged == {"someNewField": {"nested_key": 1}}


def test_merge_extra_k8s_fields_is_a_noop_without_extras() -> None:
    manifest = {"suspend": False}
    assert merge_extra_k8s_fields(manifest, None) == manifest
    assert merge_extra_k8s_fields(manifest, {}) == manifest


def test_merge_extra_k8s_fields_does_not_mutate_input() -> None:
    manifest = {"suspend": False}
    merge_extra_k8s_fields(manifest, {"new_field": 1})
    assert manifest == {"suspend": False}


def test_merge_extra_k8s_fields_drops_none_values() -> None:
    """Consistent with remove_none_from_dict, which already prunes None declared fields."""
    assert merge_extra_k8s_fields({}, {"new_field": None}) == {}


def test_merge_extra_k8s_fields_raises_on_collision_with_manifest_key() -> None:
    with pytest.raises(ValueError, match="collides with the 'activeDeadlineSeconds' key"):
        merge_extra_k8s_fields({"activeDeadlineSeconds": 86400}, {"activeDeadlineSeconds": 5})


def test_merge_extra_k8s_fields_raises_when_two_extras_converge() -> None:
    """Both spellings resolve to one key; dict ordering must not silently decide."""
    with pytest.raises(ValueError, match="both resolve to 'preRunningDeadlineSeconds'"):
        merge_extra_k8s_fields(
            {},
            {"pre_running_deadline_seconds": 1, "preRunningDeadlineSeconds": 2},
        )
