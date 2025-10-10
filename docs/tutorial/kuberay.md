# Dagster + KubeRay

This tutorial explains how to use `dagster-ray` with KubeRay to automatically create and manage Ray clusters for Dagster steps.

## Prerequisites

Before getting started, you'll need:

- A Kubernetes cluster with KubeRay Operator installed
- A `kubectl` configured to access your cluster or a kubeconfig file (resources can be configured to use it)
- `dagster-ray` installed with KubeRay support:
  ```bash
  pip install 'dagster-ray[kuberay]'
  ```

## KubeRayInteractiveJob

[`KubeRayInteractiveJob`](../api/kuberay.md#dagster_ray.kuberay.KubeRayInteractiveJob) is the recommended way to run Ray workloads with automatic cluster management. It creates a `RayJob`, connects to it in client mode and sets the `jobId` field. Cleanup is handled by the KubeRay controller or by the resource lifecycle logic.


!!! warning

    KubeRay Operator 1.3.0 is required for this feature.

### Basic Example

Here's a simple example that creates a Ray cluster and runs a distributed computation:

```python
import dagster as dg
from dagster_ray.kuberay import KubeRayInteractiveJob, RayResource
import ray


@ray.remote
def sum_of_squares_in_slice(start: int, end: int) -> int:
    return sum(i**2 for i in range(start, end))


@dg.asset
def sum_of_squares(ray_cluster: RayResource) -> int:
    # Split work across workers
    num_workers = 4
    chunk_size = 1000 // num_workers

    futures = [
        sum_of_squares_in_slice.remote(i * chunk_size + 1, (i + 1) * chunk_size + 1)
        for i in range(num_workers)
    ]

    # Sum results from all workers
    return sum(ray.get(futures))


definitions = dg.Definitions(
    assets=[compute_sum_of_squares], resources={"ray_cluster": KubeRayInteractiveJob()}
)
```

!!! note
    `RayResource` is the common interface for all `dagster-ray` Ray resource which can be used as backend-agnostic type annotation

By default, the image will be inherited from the `dagster/image` Run tag. Alternatively, you can specify it using the `image` parameter.

`RayJob`'s `.metadata.name` will be generated automatically if not provided.

### Advanced Configuration

You can customize the Ray cluster configuration:

```python
from dagster_ray.kuberay import (
    InteractiveRayJobConfig,
    InteractiveRayJobSpec,
    KubeRayInteractiveJob,
)
from dagster_ray.kuberay.configs import RayClusterSpec

ray_cluster = KubeRayInteractiveJob(
    ray_job=InteractiveRayJobConfig(
        metadata={
            "namespace": "my-custom-namespace",
            "labels": {"team": "my-team"},
            "annotations": {"example": "annotation"},
        },
        spec=InteractiveRayJobSpec(
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

## KubeRayCluster

While [`KubeRayInteractiveJob`](../api/kuberay.md#dagster_ray.kuberay.KubeRayInteractiveJob) is recommended for production environments, [`KubeRayCluster`](../api/kuberay.md#dagster_ray.kuberay.KubeRayCluster) might be a better alternative for dev environments.

Unlike `KubeRayInteractiveJob`, which can outsource garbage collection to the KubeRay controller, `KubeRayCluster` is entirely responsible for cluster management. This is bad for production environments (may result in dangling `RayCluster` instances if the Dagster step pod fails unexpectedly), but good for dev environments, because it allows `dagster-ray` to implement **cluster sharing**.

### Cluster Sharing

With cluster sharing, `dagster-ray` can reuse existing `RayCluster` instances left from previous Dagster steps, making `KubeRayCluster` startup immediate.

Therefore, `KubeRayCluster` is a good choice for dev environments as it can speed up iteration cycles and reduce infrastructure costs at the cost of lower job isolation/stability.

Cluster sharing has to be enabled explicitly.

```python
from dagster_ray.kuberay import KubeRayCluster
from dagster_ray.kuberay.configs import RayClusterConfig, ClusterSharing

ray_cluster = KubeRayCluster(
    ray_cluster=RayClusterConfig(
        cluster_sharing=ClusterSharing(enabled=True, ttl_seconds=3600)
    )
)
```

When enabled, `dagster-ray` will use configured user-provided and dagster-generated labels to select appropriate clusters from the available ones. By default `dagster-ray` will match on the following labels:

- `dagster/cluster-sharing`
- `dagster/code-location`
- `dagster/git-sha`
- `dagster/resource-key`

Each time a cluster is chosen for a step, `dagster-ray` will apply an annotation to the selected cluster to indicate that it's being used by the current step. This annotation effectively extends the cluster sharing TTL by the configured `ttl_seconds` amount. Note that the countdown for the TTL starts from the time the annotation is applied, not from the time when the Ray job starts.

Configuration options for cluster sharing can be found [here](../api/kuberay.md#dagster_ray.kuberay.KubeRayCluster.cluster_sharing).

### `RayCluster` Garbage Collection

A `RayCluster` created by `dagster-ray` may become dangling for two reasons:
- the Dagster step process exits unexpectedly (e.g. OOM), missing the change to run cleanup
- if [Cluster Sharing](#cluster-sharing) is used **and** the cluster did not expire at the time of the Dagster step completion

Since `RayCluster` doesn't support native garbage collection yet (see [TTL](https://github.com/ray-project/kuberay/issues/4033) and [idle termination](https://github.com/ray-project/kuberay/issues/2998) feature requests), `dagster-ray` provides a custom garbage collection Dagster sensor.

```py
import dagster as dg
from dagster_ray.kuberay import cleanup_expired_rayclusters

defs = dg.Definitions(
    sensors=[cleanup_expired_rayclusters],
)
```

It's not recommended for production environments as it will interrupt active long-running jobs and is not safe by any means. It's intended to be used with short-running development environments where job interruption is acceptable.

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

        accuracy = sum(result["accuracy"] for result in results) / len(results)

        # Report results
        context.report_asset_materialization(
            metadata={"num_partitions": len(results), "accuracy": accuracy},
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
    config: MLTrainingConfig,
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

1. :bulb: `dagster-pipes` has to be installed in the remote environment!

When materializing the asset, `PipesKubeRayJobClient` will submit the script as a `RayJob` custom resource, monitor its status, and stream back logs and Dagster metadata.
