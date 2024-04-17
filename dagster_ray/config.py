from __future__ import annotations

from dagster import Config


class RayDataExecutionOptions(Config):
    # TODO: rework this

    cpu_limit: int = 5000
    gpu_limit: int = 0
    verbose_progress: bool = True
    use_polars: bool = True

    def apply(self):
        import ray

        ctx = ray.data.DatasetContext.get_current()

        ctx.execution_options.resource_limits.cpu = self.cpu_limit
        ctx.execution_options.resource_limits.gpu = self.gpu_limit

        ray.data.DataContext.get_current().verbose_progress = self.verbose_progress
        ray.data.DataContext.get_current().use_polars = self.use_polars
