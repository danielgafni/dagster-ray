# `dagster-ray`

[![image](https://img.shields.io/pypi/v/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![image](https://img.shields.io/pypi/l/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![image](https://img.shields.io/pypi/pyversions/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![CI](https://github.com/danielgafni/dagster-ray/actions/workflows/CI.yml/badge.svg)](https://github.com/danielgafni/dagster-ray/actions/workflows/CI.yml)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![basedpyright - checked](https://img.shields.io/badge/basedpyright-checked-42b983)](https://docs.basedpyright.com)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

---

[Ray](https://github.com/ray-project/ray) integration for [Dagster](https://github.com/dagster-io/dagster).

`dagster-ray` allows orchestrating distributed Ray compute from Dagster pipelines. It includes:

- `RayRunLauncher` - a `RunLauncher` which submits Dagster runs as isolated Ray jobs (in cluster mode) to a Ray cluster.

- `ray_executor` - an `Executor` which submits individual Dagster steps as isolated Ray jobs (in cluster mode) to a Ray cluster.

- `RayIOManager` - an `IOManager` which stores intermediate values in Ray's object store. Ideal in conjunction with `RayRunLauncher` and `ray_executor`.

- `PipesRayjobClient`, a [Dagster Pipes](https://docs.dagster.io/concepts/dagster-pipes) client for launching and monitoring Ray jobs on remote clusters. Typically used with external Pythons scripts. Allows receiving rich logs, events and metadata from the job. Doesn't handle cluster management, can be used with any Ray cluster.

- `PipesKubeRayJobClient`, a [Dagster Pipes](https://docs.dagster.io/concepts/dagster-pipes) client for launching and monitoring [KubeRay](https://github.com/ray-project/kuberay)'s `RayJob` CR on Kubernetes. Typically used with external Pythons scripts. Allows receiving rich logs, events and metadata from the job.

- `RayResource`, a resource representing a Ray cluster. Interactions with Ray are performed in **client mode** (requires stable persistent connection), so it's most suitable for relatively short-lived jobs. It has implementations for `KubeRay` and local (mostly for testing) backends. `dagster_ray.RayResource` defines the common interface shared by all backends and can be used for backend-agnostic type annotations.

- Miscellaneous utilities like `@op`, `@job` and `@schedule` for managing `KubeRay` clusters

`dagster-ray` is tested across multiple versions of Python, Ray, Dagster, and KubeRay Operator. It integrates with [Dagster+](https://dagster.io/plus) where possible.

Documentation can be found below.

> [!NOTE]
> This project is in early development. APIs are unstable and can change at any time. Contributions are very welcome! See the [Development](#development) section below.

# Feature Matrix

There are different options available for running Dagster code on Ray. The following table summarizes the features of each option:

| Feature | `RayRunLauncher` | `ray_executor` | `PipesRayJobClient` | `PipesKubeRayJobClient` | `KubeRayCluster` |
| --- | --- | --- | --- | --- | --- |
| Creates Ray cluster | ❌ | ❌ | ❌ | ✅ | ✅ |
| Submits jobs in cluster mode | ✅ | ✅ | ✅ | ✅ | ❌ |
| For long-running jobs | ✅ | ✅ | ✅ | ✅ | ❌ |
| Enabled per-asset  | ❌ | ❌ | ✅ | ✅ | ✅ |
| Configurable per-asset | ❌ | ✅ | ✅ | ✅ | ✅ |
| Doesn't need an external script | ✅ | ✅ | ❌ | ❌ | ✅ |
| Ray cluster doesn't need access to Dagster's metadata DB and logs storage | ❌ | ❌ | ✅ | ✅ | ✅ |

# Examples

See the [examples](examples) directory.

# Installation

```shell
pip install dagster-ray
```

To install with extra dependencies for a particular backend (like `kuberay`), run:

```shell
pip install 'dagster-ray[kuberay]'
```

# Features

## RunLauncher

> [!WARNING]
> The `RayRunLauncher` is a work in progress

```shell
pip install dagster-ray[run_launcher]
```

The `RayRunLauncher` can be configured via `dagster.yaml`:

```yaml
run_launcher:
  module: dagster_ray
  class: RayRunLauncher
  config:
    ray:
      num_cpus: 1
      num_gpus: 0
```

Individual Runs can **override** Ray configuration:


```python
from dagster import job


@job(
    tags={
        "dagster-ray/config": {
            "num_cpus": 16,
            "num_gpus": 1,
        }
    }
)
def my_job():
    return my_op()
```

## Executor

> [!WARNING]
> The `ray_executor` is a work in progress

```shell
pip install dagster-ray[executor]
```

The `ray_executor` can be used to execute Dagster steps on an existing remote Ray cluster.
The executor submits steps as Ray jobs. They are started directly in the Ray cluster. Example:


```python
from dagster import job, op
from dagster_ray import ray_executor


@op(
    tags={
        "dagster-ray/config": {
            "num_cpus": 8,
            "num_gpus": 2,
            "runtime_env": {"pip": {"packages": ["torch"]}},
        }
    }
)
def my_op():
    import torch

    # your expensive computation here

    result = ...

    return result


@job(executor_def=ray_executor.configured({"ray": {"num_cpus": 1}}))
def my_job():
    return my_op()
```

Fields in the `dagster-ray/config` tag **override** corresponding fields in the Executor config.


## IOManager

`RayIOManager` allows storing and retrieving intermediate values in Ray's object store. Most useful in conjunction with `RayRunLauncher` and `ray_executor`.

It works by storing Dagster step keys in a global Ray actor. This actor contains a mapping between step keys and Ray `ObjectRef`s. It can be used with any pickable Python objects.


```python
from dagster import asset, Definitions
from dagster_ray import RayIOManager


@asset(io_manager_key="ray_io_manager")
def upstream() -> int:
    return 42


@asset
def downstream(upstream: int):
    return 0


definitions = Definitions(
    assets=[upstream, downstream], resources={"ray_io_manager": RayIOManager()}
)
```

It supports partitioned assets.


```python
from dagster import (
    asset,
    Definitions,
    StaticPartitionsDefinition,
    AssetExecutionContext,
)
from dagster_ray import RayIOManager


partitions_def = StaticPartitionsDefinition(["a", "b", "c"])


@asset(io_manager_key="ray_io_manager", partitions_def=partitions_def)
def upstream(context: AssetExecutionContext):
    return context.partition_key


@asset(partitions_def=partitions_def)
def downstream(context: AssetExecutionContext, upstream: str) -> None:
    assert context.partition_key == upstream
```


It supports partition mappings. When loading **multiple** upstream partitions, they should be annotated with a `Dict[str, ...]`, `dict[str, ...]`, or `Mapping[str, ...]` type hint.


```python
from dagster import (
    asset,
    Definitions,
    StaticPartitionsDefinition,
    AssetExecutionContext,
)
from dagster_ray import RayIOManager


partitions_def = StaticPartitionsDefinition(["A", "B", "C"])


@asset(io_manager_key="ray_io_manager", partitions_def=partitions_def)
def upstream(context: AssetExecutionContext):
    return context.partition_key.lower()


@asset
def downstream_unpartitioned(upstream: Dict[str, str]) -> None:
    assert upstream == {"A": "a", "B": "b", "C": "c"}
```

## Pipes

### `PipesRayJobClient`

A general-purpose Ray job client that can be used to submit Ray jobs and receive logs and Dagster events from them. It doesn't manage the cluser lifecycle and can be used with any Ray cluster.

Examples:

In Dagster code, import `PipesRayJobClient` and invoke it inside an `@op` or an `@asset`:

```python
from dagster import AssetExecutionContext, Definitions, asset

from dagster_ray import PipesRayJobClient
from ray.job_submission import JobSubmissionClient


@asset
def my_asset(context: AssetExecutionContext, pipes_ray_job_client: PipesRayJobClient):
    pipes_ray_job_client.run(
        context=context,
        submit_job_params={
            "entrypoint": "python /app/my_script.py",
        },
        extra={"param": "value"},
    )


definitions = Definitions(
    resources={"pipes_ray_job_client": PipesRayJobClient(client=JobSubmissionClient())},
    assets=[my_asset],
)
```

In the Ray job, import `dagster_pipes` (must be provided as a dependency) and emit regular Dagster events such as logs or asset materializations:

```python
from dagster_pipes import open_dagster_pipes


with open_dagster_pipes() as context:
    assert context.get_extra("param") == "value"
    context.log.info("Hello from Ray Pipes!")
    context.report_asset_materialization(
        metadata={"some_metric": {"raw_value": 57, "type": "int"}},
        data_version="alpha",
    )
```

A convenient way to provide `dagster-pipes` to the Ray job is [runtime_env](https://docs.ray.io/en/latest/ray-core/handling-dependencies.html):

```python
submit_job_params = {
    "entrypoint": "python /app/my_script.py",
    "runtime_env": {"pip": ["dagster-pipes"]},
}
```

Events emitted by the Ray job will be captured by `PipesRayJobClient` and will become available in the Dagster event log. Standard output and standard error streams will be forwarded to the standard output of the Dagster process.

## Resources

### `LocalRay`

A dummy resource which is useful for testing and development.
It doesn't do anything, but provides the same interface as the other `*Ray` resources.

Examples:


Using the `LocalRay` resource

```python
from dagster import asset, Definitions
from dagster_ray import LocalRay, RayResource
import ray


@asset
def my_asset(
    ray_cluster: RayResource,  # RayResource is only used as a type annotation
):  # this type annotation only defines the interface
    return ray.get(ray.put(42))


definitions = Definitions(resources={"ray_cluster": LocalRay()}, assets=[my_asset])
```

Conditionally using the `LocalRay` resource in development and `KubeRayCluster` in production:

```python
from dagster import asset, Definitions
from dagster_ray import LocalRay, RayResource
from dagster_ray.kuberay import KubeRayCluster
import ray


@asset
def my_asset(
    ray_cluster: RayResource,  # RayResource is only used as a type annotation
):  # this type annotation only defines the interface
    return ray.get(ray.put(42))


IN_K8s = ...


definitions = Definitions(
    resources={"ray_cluster": KubeRayCluster() if IN_K8s else LocalRay()},
    assets=[my_asset],
)
```

# KubeRay

```shell
pip install dagster-ray[kuberay]
```

This backend requires a Kubernetes cluster with `KubeRay Operator` installed.

Integrates with [Dagster+](https://dagster.io/plus) by injecting environment variables such as `DAGSTER_CLOUD_DEPLOYMENT_NAME` and tags such as `dagster/user` into default configuration values and Kubernetes labels.

To run `ray` code in client mode (from the Dagster Python process directly), use the `KubeRayClient` resource (see the [KubeRayCluster](#KubeRayCluster) section).
To run `ray` code in job mode, use the `PipesKubeRayJobClient` with Dagster Pipes (see the [Pipes](#pipes) section).

The public objects can be imported from `dagster_ray.kuberay` module.

## Pipes

### `PipesKubeRayJobClient`

`dagster-ray` provides the `PipesKubeRayJobClient` which can be used to execute remote Ray jobs on Kubernetes and receive Dagster events and logs from them.
[RayJob](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/rayjob-quick-start.html) will manage the lifecycle of the underlying `RayCluster`, which will be cleaned up after the specified entrypoint exits. Doesn't require a persistent connection to the Ray cluster.

Examples:

In Dagster code, import `PipesKubeRayJobClient` and invoke it inside an `@op` or an `@asset`:

```python
from dagster import AssetExecutionContext, Definitions, asset

from dagster_ray.kuberay import PipesKubeRayJobClient


@asset
def my_asset(
    context: AssetExecutionContext, pipes_kube_rayjob_client: PipesKubeRayJobClient
):
    pipes_kube_rayjob_client.run(
        context=context,
        ray_job={
            # RayJob manifest goes here
            # full reference: https://ray-project.github.io/kuberay/reference/api/#rayjob
            "metadata": {
                # .metadata.name is not required and will be generated if not provided
                "namespace": "ray"
            },
            "spec": {
                "entrypoint": "python /app/my_script.py",
                # *.container.image is not required and will be set to the current `dagster/image` tag if not provided
                "rayClusterSpec": {
                    "headGroupSpec": {...},
                    "workerGroupSpecs": [...],
                },
            },
        },
        extra={"param": "value"},
    )


definitions = Definitions(
    resources={"pipes_kube_rayjob_client": PipesKubeRayJobClient()}, assets=[my_asset]
)
```

In the Ray job, import `dagster_pipes` (must be provided as a dependency) and emit regular Dagster events such as logs or asset materializations:

```python
from dagster_pipes import open_dagster_pipes


with open_dagster_pipes() as context:
    assert context.get_extra("param") == "value"
    context.log.info("Hello from Ray Pipes!")
    context.report_asset_materialization(
        metadata={"some_metric": {"raw_value": 57, "type": "int"}},
        data_version="alpha",
    )
```

A convenient way to provide `dagster-pipes` to the Ray job is with `runtimeEnvYaml` field:

```python
import yaml

ray_job = {"spec": {"runtimeEnvYaml": yaml.safe_dump({"pip": ["dagster-pipes"]})}}
```

Events emitted by the Ray job will be captured by `PipesKubeRayJobClient` and will become available in the Dagster event log. Standard output and standard error streams will be forwarded to the standard output of the Dagster process.

**Running locally**

When running locally, the `port_forward` option has to be set to `True` in the `PipesKubeRayJobClient` resource in order to interact with the Ray job. For convenience, it can be set automatically with:

```python
from dagster_ray.kuberay.configs import in_k8s

pipes_kube_rayjob_client = PipesKubeRayJobClient(..., port_forward=not in_k8s)
```

## Resources

### `KubeRayCluster`

`KubeRayCluster` can be used for running Ray computations on Kubernetes in client (interactive) mode. Requires stable persistent connection through the duration of the Dagster step.

When added as resource dependency to an `@op/@asset`, the `KubeRayCluster`:
 - Starts a dedicated `RayCluster` for it
 - Connects  to the cluster in client mode with `ray.init()` (unless `skip_init` is set to `True`)
 - Tears down the cluster after the step is executed (unless `skip_cleanup` is set to `True`)

`RayCluster` comes with minimal default configuration, matching `KubeRay` defaults.

Examples:

Basic usage (will create a single-node, non-scaling `RayCluster`):

```python
from dagster import asset, Definitions
from dagster_ray import RayResource
from dagster_ray.kuberay import KubeRayCluster
import ray


@asset
def my_asset(
    ray_cluster: RayResource,  # RayResource is a backend-agnostic type annotation
):
    return ray.get(ray.put(42))  # interact with the Ray cluster!


definitions = Definitions(
    resources={"ray_cluster": KubeRayCluster()}, assets=[my_asset]
)
```

Larger cluster with auto-scaling enabled:

```python
from dagster_ray.kuberay import KubeRayCluster, RayClusterConfig

ray_cluster = KubeRayCluster(
    ray_cluster=RayClusterConfig(
        enable_in_tree_autoscaling=True,
        worker_group_specs=[
            {
                "groupName": "workers",
                "replicas": 2,
                "minReplicas": 1,
                "maxReplicas": 10,
                # ...
            }
        ],
    )
)
```
### `KubeRayClient`

This resource can be used to interact with the Kubernetes API Server.

Examples:

Listing currently running `RayClusters`:

```python
from dagster import op, Definitions
from dagster_ray.kuberay import KubeRayClient


@op
def list_ray_clusters(
    kube_ray_client: KubeRayClient,
):
    return kube_ray_client.client.list(namespace="kuberay")
```

## Jobs

### `delete_kuberay_clusters`

This `job` can be used to delete `RayClusters` from a given list of names.

### `cleanup_old_ray_clusters`

This `job` can be used to delete old `RayClusters` which no longer correspond to any active Dagster Runs.
They may be left behind if the automatic cluster cleanup was disabled or failed.

## Schedules

Cleanup schedules can be trivially created using the `cleanup_old_ray_clusters` or `delete_kuberay_clusters` jobs.

### `cleanup_old_ray_clusters`
`dagster-ray` provides an example daily cleanup schedule.

# Development

```shell
uv sync --all-extras
uv run pre-commit install
```

## Testing

```shell
uv run pytest
```

### KubeRay

Required tools:

- `docker`
- `kubectl`
- `helm`
- `minikube`

Running `pytest` will **automatically**:
 - build an image with the local `dagster-ray` code
 - start a `minikube` Kubernetes cluster
 - load the built `dagster-ray` and loaded `kuberay-operator` images into the cluster
 - install `KubeRay Operator` into the cluster with `helm`
 - run the tests

Thus, no manual setup is required, just the presence of the tools listed above. This makes testing a breeze!

> [!NOTE]
> Specifying a comma-separated list of `KubeRay Operator` versions in the `PYTEST_KUBERAY_VERSIONS` environment variable will spawn a new test for each version.

> [!NOTE]
> it may take a while to download `minikube` and `kuberay-operator` images and build the local `dagster-ray` image during the first tests invocation
