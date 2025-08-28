from __future__ import annotations

import os

from dagster import AssetExecutionContext, InitResourceContext, OpExecutionContext

from dagster_ray._base.constants import DEFAULT_DEPLOYMENT_NAME


def get_dagster_tags(
    context: InitResourceContext | OpExecutionContext | AssetExecutionContext, extra_tags: dict[str, str] | None = None
) -> dict[str, str]:
    """
    Returns a dictionary with common Dagster tags.
    """
    assert context.dagster_run is not None

    labels: dict[str, str] = {
        "dagster.io/deployment": DEFAULT_DEPLOYMENT_NAME,  # TODO: this should come from extra_tags,
        **(extra_tags or {}),
    }

    if isinstance(context, InitResourceContext):
        labels.update(
            **context.dagster_run.dagster_execution_info,
        )

        for resource_key, resource_def in context.all_resource_defs.items():
            if resource_def is context.resource_def:
                # inject the resource key used for this KubeRay resource
                # this enables e.g reusing the same `RayCluster` across Dagster steps that require it without recreating the cluster
                labels["dagster.io/resource_key"] = resource_key
    else:
        labels.update(
            **context.run.dagster_execution_info,
        )

    if context.dagster_run.tags.get("user"):
        labels["dagster.io/user"] = context.dagster_run.tags["user"]

    if os.getenv("DAGSTER_CLOUD_GIT_BRANCH"):
        labels["dagster.io/git-branch"] = os.environ["DAGSTER_CLOUD_GIT_BRANCH"]

    if os.getenv("DAGSTER_CLOUD_GIT_SHA"):
        labels["dagster.io/git-sha"] = os.environ["DAGSTER_CLOUD_GIT_SHA"]

    return labels
