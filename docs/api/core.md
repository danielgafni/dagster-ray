# Core API Reference

Core `dagster-ray` APIs for using external Ray clusters. Learn how to use it [here](../tutorial/external.md).

---

## Misc

::: dagster_ray.resources.LocalRay
    options:
      members:
        - "host"
        - "ray_address"

---

## Run Launcher

::: dagster_ray.run_launcher.RayRunLauncher
    options:
      members:
        - "__init__"

---

### Executor

::: dagster_ray.executor.ray_executor
    options:
      members: true

---

## Pipes

Run external Ray scripts as Ray jobs while streaming back logs and metadata into Dagster.

::: dagster_ray.pipes.PipesRayJobClient
    options:
      members:
        - "__init__"
        - "run"

::: dagster_ray.pipes.PipesRayJobMessageReader
    options:
      members:
        - "__init__"

---

## IO Manager

::: dagster_ray.io_manager.RayIOManager
    options:
      members:
        - "__init__"

---

## Types

::: dagster_ray.Lifecycle
    options:
      members:
        - "create"
        - "wait"
        - "connect"
        - "cleanup"

::: dagster_ray.types.AnyDagsterContext

::: dagster_ray.resources.BaseRayResource
    options:
      members: true

::: dagster_ray.config.RayDataExecutionOptions
    options:
      members: true

::: dagster_ray.config.ExecutionOptionsConfig
    options:
      members: true
---
