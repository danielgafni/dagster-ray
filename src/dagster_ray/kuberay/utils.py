from __future__ import annotations

import hashlib
import random
import re
import string
from typing import Any

import dagster._check as check

_INVALID_CHARS_PATTERN = re.compile(r"[^a-zA-Z0-9\-_.]")
_LEADING_TRAILING_NON_ALNUM_PATTERN = re.compile(r"^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$")


def normalize_k8s_label_values(labels: dict[str, str]) -> dict[str, str]:
    """Normalize label values to comply with Kubernetes requirements.

    K8s label values must match: (([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?
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

        # Remove leading/trailing non-alphanumeric characters
        value = _LEADING_TRAILING_NON_ALNUM_PATTERN.sub("", value)

        # Truncate to 63 characters
        normalized[key] = value[:63]

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
