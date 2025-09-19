# API Reference

Complete API documentation for `dagster-ray` components, organized by functionality. Learn how to use `dagster-ray` [here](tutorial/index.md).

# Key Components

## [Core](api/core.md)
- [RayResource](api/core.md#dagster_ray.RayResource)
- [LocalRay](api/core.md#dagster_ray.core.resources.LocalRay)
- [RayRunLauncher](api/core.md#dagster_ray.core.run_launcher.RayRunLauncher)
- [ray_executor](api/core.md#dagster_ray.core.executor.ray_executor)
- [PipesRayJobClient](api/core.md#dagster_ray.core.pipes.PipesRayJobClient)
- [PipesRayJobMessageReader](api/core.md#dagster_ray.core.pipes.PipesRayJobMessageReader)
- [RayIOManager](api/core.md#dagster_ray.core.io_manager.RayIOManager)

## [KubeRay](api/kuberay.md)
- [KubeRayInteractiveJob](api/kuberay.md#dagster_ray.kuberay.KubeRayInteractiveJob)
- [PipesKubeRayJobClient](api/kuberay.md#dagster_ray.kuberay.PipesKubeRayJobClient)

## [Configs](api/configs.md)
- [Lifecycle](api/configs.md#dagster_ray.configs.Lifecycle)
- [RayDataExecutionOptions](api/configs.md#dagster_ray.configs.RayDataExecutionOptions)
- [ExecutionOptionsConfig](api/configs.md#dagster_ray.configs.ExecutionOptionsConfig)

## [Types](api/types.md)
- [AnyDagsterContext](api/types.md#dagster_ray.types.AnyDagsterContext)
- [OpOrAssetExecutionContext](api/types.md#dagster_ray.types.OpOrAssetExecutionContext)
