from __future__ import annotations

import os
from typing import cast

from dagster import AssetExecutionContext, InitResourceContext, OpExecutionContext

from dagster_ray._base.constants import DEFAULT_DEPLOYMENT_NAME


def get_dagster_tags(context: InitResourceContext | OpExecutionContext | AssetExecutionContext) -> dict[str, str]:
    """
    Returns a dictionary with common Dagster tags.
    """
    assert context.dagster_run is not None

    labels = {
        "dagster.io/run_id": cast(str, context.run_id),
        "dagster.io/deployment": DEFAULT_DEPLOYMENT_NAME,
        # TODO: add more labels
    }

    if context.dagster_run.tags.get("user"):
        labels["dagster.io/user"] = context.dagster_run.tags["user"]

    if os.getenv("DAGSTER_CLOUD_GIT_BRANCH"):
        labels["dagster.io/git-branch"] = os.environ["DAGSTER_CLOUD_GIT_BRANCH"]

    if os.getenv("DAGSTER_CLOUD_GIT_SHA"):
        labels["dagster.io/git-sha"] = os.environ["DAGSTER_CLOUD_GIT_SHA"]

    return labels
