from __future__ import annotations

from typing import Optional

from dagster import Config
from pydantic import Field


class ExecutionOptionsConfig(Config):
    cpu: Optional[int] = None
    gpu: Optional[int] = None
    object_store_memory: Optional[int] = None


class RayDataExecutionOptions(Config):
    execution_options: ExecutionOptionsConfig = Field(default_factory=ExecutionOptionsConfig)

    cpu_limit: int = 5000
    gpu_limit: int = 0
    verbose_progress: bool = True
    use_polars: bool = True

    def apply(self):
        import ray
        from ray.data import ExecutionResources

        ctx = ray.data.DatasetContext.get_current()

        ctx.execution_options.resource_limits = ExecutionResources.for_limits(
            cpu=self.execution_options.cpu,
            gpu=self.execution_options.gpu,
            object_store_memory=self.execution_options.object_store_memory,
        )

        ctx.verbose_progress = self.verbose_progress
        ctx.use_polars = self.use_polars

    def apply_remote(self):
        import ray

        @ray.remote
        def apply():
            self.apply()

        ray.get(apply.remote())
