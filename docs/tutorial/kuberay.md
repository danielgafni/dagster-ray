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
from dagster_ray.kuberay import KubeRayInteractiveJob
from dagster_ray.kuberay.configs import RayClusterSpec
from dagster_ray.kuberay.resources.rayjob import (
    InteractiveRayJobConfig,
    InteractiveRayJobSpec,
)

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

### Deadlines

`RayJob` has two server-side deadlines. KubeRay enforces them itself, so they still apply when the Dagster step pod dies.

- `active_deadline_seconds` — how long the job may run in total, from `.status.startTime`. Defaults to 24 hours.
- `pre_running_deadline_seconds` — how long the job may take to reach the `Running` state. Unset by default.

    !!! warning

        `pre_running_deadline_seconds` requires KubeRay 1.6.0

`pre_running_deadline_seconds` reaps jobs stuck in `Initializing` or `Waiting` — for example when the `RayCluster` can never be scheduled because the requested resources don't exist in the cluster. Without it, such a job occupies the namespace until `active_deadline_seconds` expires.

```python
from dagster_ray.kuberay.resources.rayjob import InteractiveRayJobSpec

spec = InteractiveRayJobSpec(
    pre_running_deadline_seconds=600,
    active_deadline_seconds=3600,
)
```


Pick a value above the worst-case cluster startup time. A job waiting on autoscaling or scarce GPU capacity can legitimately take a long time to reach `Running`, and the deadline can't tell that apart from being stuck.

### Deletion Strategy

By default `deletion_strategy` is unset and cleanup is governed by `shutdown_after_job_finishes` (`True`), which deletes the `RayCluster` once the job succeeds or fails. This works on every supported KubeRay version.

`deletion_strategy` gives finer control — deleting only workers, keeping a failed cluster around for debugging, or deleting the `RayJob` itself — but it is version- and feature-gate-dependent.

!!! warning

    `deletion_strategy` requires the `RayJobDeletionPolicy` feature gate. If it is disabled, the KubeRay controller does not reject the request — it creates the `RayJob` and then moves it to `jobDeploymentStatus: ValidationFailed`. `dagster-ray` surfaces this as a step failure rather than waiting indefinitely.

=== "KubeRay 1.6.x"

    The feature gate is beta and **enabled by default**, so no operator configuration is needed.

    Prefer `deletionRules` — the legacy `onSuccess`/`onFailure` fields are deprecated and scheduled for removal:

    ```python
    from dagster_ray.kuberay.resources.rayjob import InteractiveRayJobSpec

    spec = InteractiveRayJobSpec(
        # deletionRules is mutually exclusive with both of these
        shutdown_after_job_finishes=False,
        ttl_seconds_after_finished=None,
        deletion_strategy={
            "deletionRules": [
                # keep a failed cluster's head around briefly for debugging
                {
                    "policy": "DeleteWorkers",
                    "condition": {"jobStatus": "FAILED", "ttlSeconds": 100},
                },
                {
                    "policy": "DeleteCluster",
                    "condition": {"jobStatus": "FAILED", "ttlSeconds": 600},
                },
                {
                    "policy": "DeleteCluster",
                    "condition": {"jobStatus": "SUCCEEDED", "ttlSeconds": 0},
                },
            ]
        },
    )
    ```

    Both companion settings are required: `deletionRules` conflicts with `shutdown_after_job_finishes=True`, and `ttl_seconds_after_finished` must be unset whenever `shutdown_after_job_finishes` is `False`.

    Within a single condition, TTLs must be non-decreasing in the order `DeleteWorkers` → `DeleteCluster` → `DeleteSelf`, and each (condition, policy) pair may appear only once. `DeleteWorkers` is not supported when in-tree autoscaling is enabled.

=== "KubeRay 1.5.x"

    The feature gate is alpha and **disabled by default**. Enable it on the operator first, or any `deletion_strategy` you set will fail validation:

    ```bash
    helm upgrade --install kuberay-operator kuberay/kuberay-operator \
      --set featureGates[0].name=RayJobDeletionPolicy \
      --set featureGates[0].enabled=true
    ```

    `deletionRules` is available here too and takes the same form as the 1.6.x example above. If you would rather not enable the gate, leave `deletion_strategy` unset and rely on `shutdown_after_job_finishes`.

=== "KubeRay ≤ 1.4.x"

    Not supported. The field was named `deletionPolicy` and took a different shape before 1.5.0, so anything set via `deletion_strategy` is dropped by the CRD without an error.

    Leave it unset and use `shutdown_after_job_finishes` with `ttl_seconds_after_finished`.

## KubeRayCluster

While [`KubeRayInteractiveJob`](../api/kuberay.md#dagster_ray.kuberay.KubeRayInteractiveJob) is recommended for production environments, [`KubeRayCluster`](../api/kuberay.md#dagster_ray.kuberay.KubeRayCluster) might be a better alternative for dev environments.

Unlike `KubeRayInteractiveJob`, which can outsource garbage collection to the KubeRay controller, `KubeRayCluster` is entirely responsible for cluster management. This is bad for production environments (may result in dangling `RayCluster` instances if the Dagster step pod fails unexpectedly), but good for dev environments, because it allows `dagster-ray` to implement **cluster sharing**.

### Cluster Sharing

With cluster sharing, `dagster-ray` can reuse existing `RayCluster` instances left from previous Dagster steps, making `KubeRayCluster` startup immediate.

Therefore, `KubeRayCluster` is a good choice for dev environments as it can speed up iteration cycles and reduce infrastructure costs at the cost of lower job isolation/stability.

Cluster sharing has to be enabled explicitly.

!!! warning "Required Kubernetes Permissions"

    Cluster sharing uses the Kubernetes [Lease](https://kubernetes.io/docs/concepts/architecture/leases/) API (`coordination.k8s.io`) for leader election to prevent multiple parallel steps from creating separate clusters simultaneously. The Dagster `ServiceAccount` must have the following RBAC permissions:

    ```yaml
    apiVersion: rbac.authorization.k8s.io/v1
    kind: Role
    metadata:
      name: dagster-ray-cluster-sharing
    rules:
      - apiGroups: ["coordination.k8s.io"]
        resources: ["leases"]
        verbs: ["create", "get", "delete"]
    ```

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

Each time a cluster is chosen for a step, `dagster-ray` will apply and continuously update a heartbeat lock annotation to the selected cluster to indicate that it's currently in use by the step.
The lock is considered expired once `ttl_seconds` has passed since its last heartbeat and may be deleted by [garbage collection](#raycluster-garbage-collection).

Configuration options for cluster sharing can be found [here](../api/kuberay.md#dagster_ray.kuberay.KubeRayCluster.cluster_sharing).

### `RayCluster` Garbage Collection

A `RayCluster` created by `dagster-ray` may become dangling for two reasons:
- the Dagster step process exits unexpectedly (e.g. OOM), missing the change to run cleanup
- if [Cluster Sharing](#cluster-sharing) is used **and** the cluster did not expire at the time of the Dagster step completion


Since `RayCluster` doesn't support native garbage collection yet (see [TTL](https://github.com/ray-project/kuberay/issues/4033) and [idle termination](https://github.com/ray-project/kuberay/issues/2998) feature requests), `dagster-ray` provides a custom garbage collection Dagster sensor.

```py
import dagster as dg
from dagster_ray.kuberay import cleanup_expired_kuberay_clusters

defs = dg.Definitions(
    sensors=[cleanup_expired_kuberay_clusters],
)
```

The sensor deletes shared clusters once all of their locks have expired — a lock expires when the time since its last heartbeat exceeds the TTL.

!!! tip

    You can configure [run timeouts](https://docs.dagster.io/deployment/execution/run-monitoring#general-run-timeouts) to prevent Dagster steps from hanging indefinitely.

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
        submit_job_params={
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

## Custom Host Resolution

By default, KubeRay resources resolve the Ray head address as an in-cluster FQDN (`<service>.<namespace>.svc.cluster.local`). Override the [`resolve_hostname`][dagster_ray.kuberay.resources.base.BaseKubeRayResource.resolve_hostname] method to customize this, for example when connecting across clusters or through a custom DNS:

```python
from dagster_ray.kuberay import KubeRayInteractiveJob


class MyKubeRayInteractiveJob(KubeRayInteractiveJob):
    def resolve_hostname(self, service_name: str, namespace: str) -> str:
        return f"{service_name}.{namespace}.company.com"
```

## Extra Spec Fields

`RayJobSpec` and `RayClusterSpec` aim to declare every field their `CRD`s support as a typed field.

However, they are also [permissive Dagster configs](https://docs.dagster.io/guides/operate/configuration/advanced-config-types#permissive-schemas), so extra keys will be passed through (useful for setting keys that are not supported by `dagster-ray` yet).

Field names are accepted in either `snake_case` or `camelCase`: for example, setting either `some_new_crd_field` and `someNewCrdField` will both produce `someNewCrdField` in the manifest.
