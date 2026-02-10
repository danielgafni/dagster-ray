from __future__ import annotations

import re
from abc import ABC, abstractmethod

from pydantic import Field

from dagster_ray._base.constants import DEFAULT_DEPLOYMENT_NAME
from dagster_ray._base.resources import RayResource
from dagster_ray.kuberay.utils import get_k8s_object_name
from dagster_ray.types import AnyDagsterContext


class BaseKubeRayResource(RayResource, ABC):
    image: str | None = Field(
        default=None,
        description="Image to inject into the `RayCluster` spec. Defaults to `dagster/image` run tag. Images already provided in the `RayCluster` spec won't be overridden.",
    )
    deployment_name: str = Field(
        default=DEFAULT_DEPLOYMENT_NAME,
        description="Dagster deployment name. Is used as a prefix for the Kubernetes resource name. Dagster Cloud variables are used to determine the default value.",
    )
    failure_tolerance_timeout: float = Field(
        default=0.0,
        description="The period in seconds to wait for the cluster to transition out of `failed` state if it reaches it. This state can be transient under certain conditions. With the default value of 0, the first `failed` state appearance will raise an exception immediately.",
    )
    poll_interval: float = Field(default=1.0, description="Poll interval for various API requests")

    @property
    @abstractmethod
    def namespace(self) -> str:
        raise NotImplementedError

    def _get_step_name(self, context: AnyDagsterContext) -> str:
        assert isinstance(context.run_id, str)
        assert context.dagster_run is not None

        # try to make the name as short as possible
        cluster_name_prefix = f"dg-{self.deployment_name.replace('-', '')[:8]}-{context.run_id[:8]}"

        dagster_user_email = context.dagster_run.tags.get("user")
        if dagster_user_email is not None:
            cluster_name_prefix += f"-{dagster_user_email.replace('.', '').replace('-', '').split('@')[0][:6]}"

        name_key = get_k8s_object_name(
            context.run_id,
            self.resource_uid,
        )

        step_name = f"{cluster_name_prefix}-{name_key}".lower()
        step_name = re.sub(r"[^-0-9a-z]", "-", step_name)

        return step_name
