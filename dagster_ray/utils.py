import os
from typing import Dict, List, Optional


def resolve_env_vars_list(env_vars: Optional[List[str]]) -> Dict[str, str]:
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
