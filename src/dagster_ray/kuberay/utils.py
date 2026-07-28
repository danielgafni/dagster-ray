from __future__ import annotations

import hashlib
import random
import re
import string
from collections.abc import Mapping
from typing import Any

import dagster._check as check

RETRYABLE_K8S_STATUSES = {
    404,  # Not Found — resource not yet created (e.g. RayCluster during RayJob startup)
    409,  # Conflict — concurrent update to the resource
    429,  # Too Many Requests — API server rate limiting
    500,  # Internal Server Error — transient etcd or apiserver issue
    502,  # Bad Gateway — apiserver restarting behind a load balancer
    503,  # Service Unavailable — apiserver overloaded or restarting
    504,  # Gateway Timeout — apiserver didn't respond in time
}


def is_retryable_k8s_api_exception(e: BaseException) -> bool:
    """Check if a Kubernetes API exception is transient and safe to retry."""
    from kubernetes.client import ApiException

    return isinstance(e, ApiException) and e.status in RETRYABLE_K8S_STATUSES


# K8s label-value character set and regex are defined at:
# https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
# Allowed interior characters: alphanumerics, `-`, `_`, `.`.
_INVALID_CHARS_PATTERN = re.compile(r"[^a-zA-Z0-9\-_.]")
# Leading/trailing characters must be alphanumeric; these patterns strip
# anything else on each end.
_LEADING_NON_ALNUM_PATTERN = re.compile(r"^[^a-zA-Z0-9]+")
_TRAILING_NON_ALNUM_PATTERN = re.compile(r"[^a-zA-Z0-9]+$")
# Full K8s label-value regex (must also satisfy <=63 chars).
_K8S_LABEL_VALUE_PATTERN = re.compile(r"^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$")


def normalize_k8s_label_values(labels: dict[str, str]) -> dict[str, str]:
    """Normalize label values to comply with Kubernetes requirements.

    Per the K8s label spec
    (https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set),
    label values must match: ``(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?``
    - Start and end with alphanumeric
    - Middle can contain alphanumeric, -, _, .
    - Max 63 characters
    - Empty string is valid

    Additionally, key starting with `dagster.io/` are replaced with `dagster/`
    """
    normalized = {}

    for key, value in labels.items():
        if not value:  # Empty string is valid
            normalized[key] = value
            continue

        # Replace common email symbols for better readability
        value = value.replace("@", "-at-")
        value = value.replace("+", "-plus-")

        # Replace remaining invalid characters with hyphen
        value = _INVALID_CHARS_PATTERN.sub("-", value)

        # Collapse multiple consecutive hyphens into a single hyphen
        value = re.sub(r"-+", "-", value)

        # Strip leading non-alphanumerics first so the 63-char budget is
        # spent on useful content (e.g. `___useful-tail` keeps `useful-tail`
        # rather than truncating into a run of `_`).
        value = _LEADING_NON_ALNUM_PATTERN.sub("", value)
        # Truncate to 63 characters.
        value = value[:63]
        # Strip trailing non-alphanumerics — the character at position 63 may
        # be `_`, `-`, or `.`, which would violate the K8s label-value regex.
        value = _TRAILING_NON_ALNUM_PATTERN.sub("", value)

        if not _K8S_LABEL_VALUE_PATTERN.match(value):
            raise ValueError(
                f"Normalized label value for key {key!r} does not match the "
                f"K8s label-value regex {_K8S_LABEL_VALUE_PATTERN.pattern!r}: "
                f"{value!r}"
            )

        normalized[key] = value

    # replace keys starting with `dagster.io/` with `dagster/`
    normalized_with_fixed_dagster_keys = {}
    for key, value in normalized.items():
        if key.startswith("dagster.io/"):
            normalized_with_fixed_dagster_keys[f"dagster/{key[11:]}"] = value
        else:
            normalized_with_fixed_dagster_keys[key] = value
    return normalized_with_fixed_dagster_keys


def remove_none_from_dict(d: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None}


def to_camel_case(name: str) -> str:
    """Convert a snake_case field name to the camelCase used by Kubernetes manifests.

    Names without underscores are returned unchanged, so already-camelCase keys pass through.
    Interior capitalization is preserved: `runtime_env_YAML` becomes `runtimeEnvYAML`, not
    `runtimeEnvYaml`.
    """
    head, *rest = name.split("_")
    return head + "".join(part[:1].upper() + part[1:] for part in rest)


def merge_extra_k8s_fields(manifest: dict[str, Any], extras: Mapping[str, Any] | None) -> dict[str, Any]:
    """Merge extra fields from a `PermissiveConfig` into an already-built manifest dict.

    `RayClusterSpec` and `RayJobSpec` are permissive, so Pydantic accepts fields they don't
    declare. Those land in `model_extra` and would otherwise be silently dropped, letting a
    config validate and run while never reaching Kubernetes.

    Keys are converted from snake_case to camelCase so extras behave like declared fields.
    Values are passed through untouched — only the top-level key is converted, matching how
    declared dict fields already work.

    `None` values are dropped, consistent with `remove_none_from_dict`.

    Raises:
        ValueError: if an extra resolves to a key `manifest` already contains, or if two
            extras resolve to the same key. Both spellings of one field is ambiguous config,
            and Pydantic doesn't catch it: without an alias generator, `activeDeadlineSeconds`
            is just a different key from `active_deadline_seconds` and is kept alongside it.
    """
    if not extras:
        return manifest

    merged = dict(manifest)
    # Maps a resolved manifest key back to the extra that produced it, to detect two extras
    # converging on one key (e.g. `pre_running_deadline_seconds` and `preRunningDeadlineSeconds`).
    resolved_by_extra: dict[str, str] = {}

    for name, value in extras.items():
        key = to_camel_case(name)

        if key in manifest:
            raise ValueError(
                f"extra field {name!r} collides with the {key!r} key already set by this spec. "
                f"Set the declared field instead of passing {name!r} as an extra field."
            )

        if key in resolved_by_extra:
            raise ValueError(
                f"extra fields {resolved_by_extra[key]!r} and {name!r} both resolve to {key!r}. Pass only one of them."
            )

        resolved_by_extra[key] = name

        if value is not None:
            merged[key] = value

    return merged


def k8s_service_fqdn(service_name: str, namespace: str) -> str:
    """Returns the fully-qualified domain name (FQDN) for a Kubernetes Service.

    Using the FQDN instead of a bare serviceIP ensures the address is resolvable
    across clusters (e.g. via Cilium cluster mesh).
    """
    return f"{service_name}.{namespace}.svc.cluster.local"


def get_k8s_object_name(run_id: str, step_key: str | None = None):
    """Creates a unique (short!) identifier to name k8s objects based on run ID and step key(s).

    K8s Job names are limited to 63 characters, because they are used as labels. For more info, see:

    https://kubernetes.io/docs/concepts/overview/working-with-objects/names/
    """
    check.str_param(run_id, "run_id")
    check.opt_str_param(step_key, "step_key")
    if not step_key:
        letters = string.ascii_lowercase
        step_key = "".join(random.choice(letters) for i in range(20))

    # Creates 32-bit signed int, so could be negative
    name_hash = hashlib.md5((run_id + step_key).encode("utf-8"))

    return name_hash.hexdigest()[:8]
