from __future__ import annotations

import hashlib
import os
import random
import string
from collections.abc import Mapping, Sequence
from typing import Any

import dagster as dg
import dagster._check as check
from dagster._config.field_utils import IntEnvVar

# yes, `python-client` is actually the KubeRay package name
# https://github.com/ray-project/kuberay/issues/2078


def resolve_env_vars_list(env_vars: list[str] | None) -> dict[str, str]:
    res = {}

    if env_vars is not None:
        for env_var in env_vars:
            if "=" in env_var:
                var, value = env_var.split("=", 1)
                res[var] = value
            else:
                if value := os.getenv(env_var):
                    res[env_var] = value

    return res


def _process_dagster_env_vars(config: Any) -> Any:
    # If it's already one of the EnvVar types, return its value
    if isinstance(config, dg.EnvVar | IntEnvVar):
        return config.get_value()

    # If it's exactly {"env": "FOO"}, fetch the value
    if isinstance(config, Mapping) and len(config) == 1 and "env" in config:
        return dg.EnvVar(config["env"]).get_value()

    # If it's a dictionary, recurse into its values
    if isinstance(config, Mapping):
        return {k: _process_dagster_env_vars(v) for k, v in config.items()}

    # If it's a list/tuple, recurse into each element
    if isinstance(config, Sequence) and not isinstance(config, str):
        return [_process_dagster_env_vars(v) for v in config]

    # Otherwise, just return as-is
    return config


def get_k8s_object_name(run_id: str, step_key: str | None = None) -> str:
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


def get_current_job_id() -> str:
    import ray

    return ray.get_runtime_context().get_job_id()
