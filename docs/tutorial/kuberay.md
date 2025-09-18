# Dagster + KubeRay

This tutorial shows how to use dagster-ray with KubeRay to automatically manage Ray clusters on Kubernetes. KubeRay integration allows you to create and manage Ray clusters directly from your Dagster pipelines without manual cluster management.

## Prerequisites

Before getting started, you'll need:

- A Kubernetes cluster with KubeRay Operator installed
- A `kubectl` configured to access your cluster or a kubeconfig file (resources can be configured to use it)
- dagster-ray installed with KubeRay support:
  ```bash
  pip install 'dagster-ray[kuberay]'
  ```

## KubeRayInteractiveJob

[`KubeRayInteractiveJob`](../api/kuberay.md#dagster_ray.kuberay.KubeRayInteractiveJob) is the recommended way to run Ray workloads with automatic cluster management. It creates a `RayJob`, connects to it in client mode and sets the `jobId` field. Cleanup is handled by the KubeRay controller or by the resource lifecycle logic.


!!! warning

    KubeRay Operator 1.3.0 is required for this feature.

### Basic Example

Here's a simple example that creates a Ray cluster and runs distributed computation:

```python
import dagster as dg
from dagster_ray.kuberay import KubeRayInteractiveJob, RayResource
import ray


@ray.remote
def compute_pi_slice(start: int, num_samples: int) -> int:
    """Compute a slice of pi using Monte Carlo method."""
    import random

    count = 0
    for _ in range(num_samples):
        x, y = random.random(), random.random()
        if x * x + y * y <= 1:
            count += 1
    return count


@dg.asset
def estimate_pi(ray_cluster: RayResource) -> float:
    """Estimate pi using distributed Ray computation."""
    num_samples = 10_000_000
    num_workers = 10
    samples_per_worker = num_samples // num_workers

    # Submit work to Ray cluster
    futures = [
        compute_pi_slice.remote(i * samples_per_worker, samples_per_worker)
        for i in range(num_workers)
    ]

    # Collect results
    total_inside = sum(ray.get(futures))
    pi_estimate = 4 * total_inside / num_samples

    return pi_estimate


definitions = dg.Definitions(
    assets=[estimate_pi], resources={"ray_cluster": KubeRayInteractiveJob()}
)
```

!!! note
    `RayResource` is the common interface for all `dagster-ray` Ray resource which can be used as backend-agnostic type annotation

By default, the image will be inherited from the `dagster/image` Run tag. Alternatively, you can specify it using the `image` parameter.

`RayJob`'s `.metadata.name` will be generated automatically if not provided.

### Advanced Configuration

You can customize the Ray cluster configuration:

```python
from dagster_ray.kuberay import KubeRayInteractiveJob
from dagster_ray.kuberay.configs import RayJobConfig, RayJobSpec, RayClusterSpec

ray_cluster = KubeRayInteractiveJob(
    ray_job=RayJobConfig(
        metadata={
            "namespace": "my-custom-namespace",
            "labels": {"team": "my-team"},
            "annotations": {"example": "annotation"},
        },
        spec=RayJobSpec(
            ttl_seconds_after_finished=3600,
            deletion_strategy={
                "onSuccess": {"policy": "DeleteSelf"},
                "onFailure": {"policy": "DeleteSelf"},
            },
            ray_cluster_spec=RayClusterSpec(
                worker_group_specs=[
                    {
                        "groupName": "workers",
                        "replicas": 0,
                        "minReplicas": 0,
                        "maxReplicas": 10,
                        "rayStartParams": {},
                        "template": {
                            "metadata": {"labels": {}, "annotations": {}},
                            "spec": {
                                "imagePullSecrets": [],
                                "containers": [
                                    {
                                        "volumeMounts": [],
                                        "name": "worker",
                                        "imagePullPolicy": "Always",
                                    }
                                ],
                                "volumes": [],
                                "affinity": {},
                                "tolerations": [],
                                "nodeSelector": {},
                            },
                        },
                    }
                ]
            ),
        ),
    ),
    lifecycle=Lifecycle(cleanup="always"),
    timeout=600.0,
)
```

### KubeRayCluster Alternative

While [`KubeRayInteractiveJob`](../api/kuberay.md#dagster_ray.kuberay.KubeRayInteractiveJob) is recommended for most use cases, you can also use [`KubeRayCluster`](../api/kuberay.md#dagster_ray.kuberay.KubeRayCluster) for more persistent clusters:

```python
from dagster_ray.kuberay import KubeRayCluster
from dagster_ray.kuberay.configs import RayClusterConfig

# KubeRayCluster creates a persistent RayCluster CR
ray_cluster = KubeRayCluster(
    ray_cluster=RayClusterConfig(
        # Cluster configuration
        metadata={"name": "my-persistent-cluster"},
    )
)
```

!!! note
    `KubeRayCluster` is generally a weaker alternative to `KubeRayInteractiveJob` because:

    - It creates persistent clusters that may not get cleaned up properly, for example if something happens to the Dagster pod
    - It lacks `RayJob`'s features such as timeouts and existing cluster selection

## PipesKubeRayJobClient

[`PipesKubeRayJobClient`](../api/kuberay.md#dagster_ray.kuberay.PipesKubeRayJobClient) allows you to submit external Python scripts as Ray jobs with automatic cluster management. This is ideal when you want to decouple your Ray workload from your Dagster orchestration code or Python environment.

### Basic Pipes Example

First, create a Ray script that will run on the cluster:

```python title="ray_workload.py"
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

Now create a Dagster asset that uses `PipesKubeRayJobClient`:

```python
import dagster as dg
from dagster_ray.kuberay import PipesKubeRayJobClient


class MLTrainingConfig(dg.Config):
    num_partitions: int = 4


@dg.asset
def distributed_computation(
    context: dg.AssetExecutionContext,
    ray_pipes_client: PipesKubeRayJobClient,
) -> None:
    """Run distributed computation using Pipes + KubeRay."""

    # Submit the external Ray script
    return ray_pipes_client.run(
        context=context,
        ray_job={
            "entrypoint": "python ray_workload.py",
            "runtime_env": {
                "pip": ["dagster-pipes", "torch"],  # (1)!
            },
            "entrypoint_num_cpus": 1.0,
            "entrypoint_memory": 2 * 1024 * 1024 * 1024,  # 2GB
        },
        extras={
            "num_partitions": config.num_partitions,
        },
    )


definitions = dg.Definitions(
    assets=[distributed_computation],
    resources={"ray_pipes_client": PipesKubeRayJobClient()},
)
```

1. :bulb: `dagster-pipes` have to be installed in the remote environment!

When materializing the asset, the `PipesKubeRayJobClient` will submit the script as a `RayJob` custom resource, monitor its status, and stream back logs and Dagster metadata.
