from __future__ import annotations

import hashlib
import random
import re
import string
from typing import Any

import dagster._check as check


def normalize_k8s_label_values(labels: dict[str, str]) -> dict[str, str]:
    # prevent errors like:
    # Invalid value: \"daniel@anam.ai\": a valid label must be an empty string or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyValue',  or 'my_value',  or '12345', regex used for validation is '(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?')
    # and comply with the 63 character limit

    cleanup_regex = re.compile(r"[^a-zA-Z0-9-_.]+")

    banned_starting_characters = ["-", "_", "."]

    for key, value in labels.items():
        # -daniel~!@my.domain -> daniel-my-domain
        with_maybe_bad_start = cleanup_regex.sub("", value.replace("@", "-").replace(".", "-"))
        while with_maybe_bad_start and with_maybe_bad_start[0] in banned_starting_characters:
            with_maybe_bad_start = with_maybe_bad_start[1:]
        labels[key] = with_maybe_bad_start[:63]

    return labels


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
