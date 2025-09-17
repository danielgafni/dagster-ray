# Dagster + External Ray Clusters Tutorial

This tutorial covers how to use dagster-ray with external Ray clusters - clusters that are managed outside of Dagster. This approach is ideal when you have existing Ray infrastructure or want to separate cluster management from your data pipelines.

## When to Use Each Approach

### Use [`LocalRay`](../api/core.md#dagster_ray.resources.LocalRay) when:
- Developing and testing locally

### Use [`RayRunLauncher`](../api/core.md#dagster_ray.run_launcher.RayRunLauncher) when:
- You want to run all Dagster pipelines on Ray
- You want very fast Dagster run submission

### Use [`ray_executor`](../api/core.md#dagster_ray.executor.ray_executor) when:
- You want selective Ray execution for specific assets
- You want very fast Dagster step submission

### Use [`PipesRayJobClient`](../api/core.md#dagster_ray.pipes.PipesRayJobClient) when:
- You want to decouple Ray workloads from orchestration code
- You have existing Ray scripts you want to integrate
- You want full separation between Dagster and Ray environments


## Prerequisites

Before getting started, you'll need:

- A Ray cluster (can be local Ray for development, or remote Ray cluster for production)
- dagster-ray installed:
  ```bash
  pip install dagster-ray
  ```
- For remote clusters: Ray cluster address and appropriate network access

## LocalRay - Development and Testing

[`LocalRay`](../api/core.md#dagster_ray.resources.LocalRay) is perfect for local development and testing. It provides the same interface as other Ray resources but runs Ray locally on your machine.

!!! example
      ```python
      from dagster import asset, Definitions
      from dagster_ray import LocalRay
      import ray


      @asset
      def batch_processing_results(ray_cluster: LocalRay) -> dict:
          """Process multiple batches in parallel using local Ray."""
          refs = [process_batch.remote(i, size) for i, size in enumerate(batch_sizes)]

          # Collect results
          results = ray.get(refs)

          return aggregate(results)


      definitions = Definitions(
          assets=[batch_processing_results], resources={"ray_cluster": LocalRay()}
      )
      ```

You can customize the local Ray configuration:

!!! example
    ```python
    from dagster_ray import LocalRay

    local_ray = LocalRay(
        # Ray initialization options
        ray_init_options={
            "num_cpus": 8,
            "num_gpus": 1,
            "object_store_memory": 1000000000,  # 1GB
            "runtime_env": {"pip": ["numpy", "polars", "scikit-learn"]},
        },
    )
    ```

## RayRunLauncher

[`RayRunLauncher`](../api/core.md#dagster_ray.run_launcher.RayRunLauncher) executes entire Dagster runs as Ray jobs. This is useful for Dagster deployments that need to be fully executed on Ray.

!!! tip
    Make sure the Ray cluster has access to Dagster's metadata database!

### Usage

Configure the run launcher in your `dagster.yaml`:

```yaml
run_launcher:
  module: dagster_ray
  class: RayRunLauncher
  config:
    address:
      env: RAY_ADDRESS
    timeout: 1800
    metadata:
      foo: bar
      runtime_env:
      env_vars:
        FOO: bar
      pip:
        - polars
```

With `RayRunLauncher` enabled, your regular Dagster assets will automatically run on Ray:

```python
from dagster import asset, Definitions


@asset
def regular_asset():
    """This asset will be submitted as a Ray job."""
    ...
```

All the steps will be executed in a single Ray job, unless a custom executor is used.

It's possible to provide additional runtime configuration via the `dagster-ray/config` run tag.

## ray_executor

[`ray_executor`](../api/core.md#dagster_ray.executor.ray_executor) runs Dagster steps (ops or assets) as Ray jobs (in parallel).

!!! tip
    Make sure the Ray cluster has access to Dagster's metadata database!

### Usage

The executor can be enabled at `Definitions` level:

```python
from dagster import Definitions, EnvVar
from dagster_ray import ray_executor


definitions = Definitions(
    executor=ray_executor.configured(
        {"address": EnvVar("RAY_ADDRESS"), "runtime_env": {"pip": ["polars"]}}
    )
)
```

It's possible to configure individual assets via the `dagster-ray/config` op tag:

```py
@asset(
    op_tags={
        "dagster-ray/config": {
            "num_cpus": 2,
        }
    }
)
def my_asset(): ...
```


## PipesRayJobClient - External Script Execution

[`PipesRayJobClient`](../api/core.md#dagster_ray.pipes.PipesRayJobClient) lets you submit external Python scripts to Ray clusters as Ray jobs. This is perfect for decoupling your Ray workloads from Dagster orchestration code and Python environment.

### External Ray Script

First, create a script that will run on the Ray cluster:

```python title="ml_training.py"
# ml_training.py - External Ray script
import ray
from dagster_pipes import open_dagster_pipes


@ray.remote
def train_ml_model(partition_id: int):
    """Dummy ML training function."""
    import time

    time.sleep(1)  # Simulate work
    return {"partition_id": partition_id, "accuracy": 0.95}


def main():
    with open_dagster_pipes() as context:
        context.log.info("Starting distributed ML training")

        # Get configuration from Dagster
        num_partitions = context.get_extra("num_partitions", 4)

        # Submit training jobs
        futures = [train_ml_model.remote(i) for i in range(num_partitions)]
        results = ray.get(futures)

        context.log.info(f"Training complete on {len(results)} partitions")

        # Report results
        context.report_asset_materialization(
            metadata={"num_partitions": len(results), "results": results},
            data_version="alpha",
        )


if __name__ == "__main__":
    main()
```

### Dagster Asset Using Pipes

Now, let's define a Dagster asset that will be calling the above external script via Dagster Pipes.

```python
from dagster import asset, AssetExecutionContext, Config
from dagster_ray import PipesRayJobClient
from ray.job_submission import JobSubmissionClient


class MLTrainingConfig(Config):
    num_partitions: int = 4


@asset
def distributed_ml_training(
    context: AssetExecutionContext,
    ray_client: PipesRayJobClient,
    config: MLTrainingConfig,
) -> dict:
    """Run distributed ML training using Ray Pipes."""

    return ray_client.run(
        context=context,
        submit_job_params={
            "entrypoint": "python ml_training.py",
            "runtime_env": {
                "pip": ["dagster-pipes", "torch"],
            },
        },
        extras={
            "num_partitions": config.num_partitions,
        },
    )


definitions = Definitions(
    assets=[distributed_ml_training],
    resources={
        "ray_client": PipesRayJobClient(
            client=JobSubmissionClient(),
            timeout=1800,
        )
    },
)
```

When materializing the asset, the `PipesRayJobClient` will submit the script as a Ray job, monitor its status, and stream back logs and Dagster metadata.

## Conclusion

That's it! You now have a comprehensive understanding of how to use dagster-ray with external Ray clusters, from local development with `LocalRay` to production deployments with `PipesRayJobClient`.
