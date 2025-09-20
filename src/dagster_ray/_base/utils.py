from __future__ import annotations

import dagster as dg
from dagster._core.executor.step_delegating import (
    StepHandlerContext,
)
from dagster._core.launcher.base import LaunchRunContext

from dagster_ray._base.constants import DEFAULT_DEPLOYMENT_NAME
from dagster_ray.types import AnyDagsterContext


def get_dagster_tags(
    context: dg.InitResourceContext | AnyDagsterContext | StepHandlerContext | LaunchRunContext,
    extra_tags: dict[str, str] | None = None,
) -> dict[str, str]:
    """
    Returns a dictionary with common Dagster tags.
    """
    assert context.dagster_run is not None

    labels: dict[str, str] = {
        "dagster/deployment": DEFAULT_DEPLOYMENT_NAME,  # TODO: this should come from extra_tags,
        **(extra_tags or {}),
    }

    if isinstance(context, dg.InitResourceContext):
        labels.update(
            **context.dagster_run.dagster_execution_info,
        )

        for resource_key, resource_def in context.all_resource_defs.items():
            if resource_def is context.resource_def:
                # inject the resource key used for this KubeRay resource
                # this enables e.g reusing the same `RayCluster` across Dagster steps that require it without recreating the cluster
                labels["dagster/resource-key"] = resource_key

    elif isinstance(context, StepHandlerContext):
        labels.update(
            **context.dagster_run.dagster_execution_info,
        )
    elif isinstance(context, LaunchRunContext):
        labels.update(
            **context.dagster_run.dagster_execution_info,
        )
    elif isinstance(context, dg.OpExecutionContext):
        labels.update(
            **context.run.dagster_execution_info,
        )
    elif isinstance(context, dg.AssetExecutionContext):
        labels.update(
            **context.run.dagster_execution_info,
        )
    else:
        raise ValueError(f"Unexpected context type: {type(context)}")

    return labels
