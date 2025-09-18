from __future__ import annotations

import re
from abc import abstractmethod
from uuid import uuid4

import dagster as dg
from pydantic import Field, PrivateAttr

from dagster_ray._base.constants import DEFAULT_DEPLOYMENT_NAME
from dagster_ray.kuberay.utils import get_k8s_object_name
from dagster_ray.types import AnyDagsterContext


class BaseKubeRayResourceConfig(dg.Config):
    image: str | None = Field(
        default=None,
        description="Image to inject into the `RayCluster` spec. Defaults to `dagster/image` run tag. Images already provided in the `RayCluster` spec won't be overridden.",
    )
    deployment_name: str = Field(
        default=DEFAULT_DEPLOYMENT_NAME,
        description="Dagster deployment name. Is used as a prefix for the Kubernetes resource name. Dagster Cloud variables are used to determine the default value.",
    )
    poll_interval: float = Field(default=1.0, description="Poll interval for various API requests")

    _host: str = PrivateAttr()

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

        step_key = str(uuid4())

        name_key = get_k8s_object_name(
            context.run_id,
            step_key,
        )

        step_name = f"{cluster_name_prefix}-{name_key}".lower()
        step_name = re.sub(r"[^-0-9a-z]", "-", step_name)

        return step_name
