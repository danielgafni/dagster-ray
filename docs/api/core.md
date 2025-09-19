# Core API Reference

Core `dagster-ray` APIs for using external Ray clusters. Learn how to use it [here](../tutorial/external.md).

---

## Ray Resources

`RayResource` can be used to connect to external Ray clusters when provided as a Dagster resource, or as a type annotation (all other Ray resources in `dagster-ray` inherit from `RayResource`)

::: dagster_ray.RayResource
    options:
      members: true

The `LocalRay` can be used to connect to a local Ray cluster.

::: dagster_ray.core.resources.LocalRay
    options:
      members:
        - "host"
        - "ray_address"

---

## Run Launcher

::: dagster_ray.core.run_launcher.RayRunLauncher
    options:
      members: true

---

### Executor

::: dagster_ray.core.executor.ray_executor
    options:
      members: true

---

## Pipes

Run external Ray scripts as Ray jobs while streaming back logs and metadata into Dagster with Dagster Pipes.

::: dagster_ray.core.pipes.PipesRayJobClient
    options:
      members:
        - "__init__"
        - "run"

::: dagster_ray.core.pipes.PipesRayJobMessageReader
    options:
      members:
        - "__init__"

---

## IO Manager

Send data between Dagster steps while they are running inside a Ray cluster.

::: dagster_ray.core.io_manager.RayIOManager
    options:
      members:
        - "__init__"

---
