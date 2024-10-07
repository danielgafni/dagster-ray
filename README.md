# `dagster-ray`

[![image](https://img.shields.io/pypi/v/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![image](https://img.shields.io/pypi/l/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![image](https://img.shields.io/pypi/pyversions/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![CI](https://github.com/danielgafni/dagster-ray/actions/workflows/CI.yml/badge.svg)](https://github.com/danielgafni/dagster-ray/actions/workflows/CI.yml)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Checked with pyright](https://microsoft.github.io/pyright/img/pyright_badge.svg)](https://microsoft.github.io/pyright/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

---

[Ray](https://github.com/ray-project/ray) integration for [Dagster](https://github.com/dagster-io/dagster).

`dagster-ray` allows creating Ray clusters and running distributed computations from Dagster code. Features include:

- `ray_execturo` - an Executor which runs Dagster steps a jobs submitted to a Ray cluster.

- `PipesRayJobClient`, a [Dagster Pipes](https://docs.dagster.io/concepts/dagster-pipes) client for launching and monitoring `RayJob` resources in Kubernetes via [KubeRay](https://github.com/ray-project/kuberay). Most suitable for submitting long-running jobs (via external Python scripts) with no direct Ray access from Dagster code. Allows receiving rich logs, events and metadata from the job. Implemented for the `KubeRay` backend.

- `RayResource`, a resource representing a Ray cluster. Interactions are performed in client mode (requires stable persistent connection), so it's most suitable for relatively short jobs. Provide direct Ray access from the Dagster Python process. It has implementations for `KubeRay` and local (mostly for testing) backends. `dagster_ray.RayResource` defines the common interface shared by all backends and can be used for backend-agnostic type annotations.

- Miscellaneous utilities like `@op`, `@job` and `@schedule` for managing `KubeRay` clusters

`dagster-ray` is tested across multiple versions of Python, Ray, Dagster, and KubeRay Operator. It integrates with [Dagster+](https://dagster.io/plus) where possible.

Documentation can be found below.

> [!NOTE]
> This project is in early development. APIs are unstable and can change at any time. Contributions are very welcome! See the [Development](#development) section below.

# Installation

```shell
pip install dagster-ray
```

To install with extra dependencies for a particular backend (like `kuberay`), run:

```shell
pip install 'dagster-ray[kuberay]'
```

# Executor

> [!WARNING]
> The `ray_executor` is a work in progress

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

    return torch.tensor([42])


@job(executor_def=ray_executor)
def my_job():
    return my_op()
```

Fields in the `dagster-ray/config` tag **replace** corresponding fields in the Executor config.

# Backends

## KubeRay

This backend requires a Kubernetes cluster with `KubeRay Operator` installed.

Integrates with [Dagster+](https://dagster.io/plus) by injecting environment variables such as `DAGSTER_CLOUD_DEPLOYMENT_NAME` and tags such as `dagster/user` into default configuration values and Kubernetes labels.

To run `ray` code in client mode (from the Dagster Python process directly), use the `KubeRayClient` resource (see the [KubeRayCluster](#KubeRayCluster) section).
To run `ray` code in job mode, use the `PipesRayJobClient` with Dagster Pipes (see the [Pipes](#pipes) section).

The public objects can be imported from `dagster_ray.kuberay` module.

### Pipes

`dagster-ray` provides the `PipesRayJobClient` which can be used to execute remote Ray jobs on Kubernetes and receive Dagster events and logs from them.
[RayJob](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/rayjob-quick-start.html) will manage the lifecycle of the underlying `RayCluster`, which will be cleaned up after the specified entrypoint exits. Doesn't require a persistent connection to the Ray cluster.

Examples:

In Dagster code, import `PipesRayJobClient` and invoke it inside an `@op` or an `@asset`:

```python
from dagster import AssetExecutionContext, Definitions, asset

from dagster_ray.kuberay import PipesRayJobClient


@asset
def my_asset(context: AssetExecutionContext, pipes_rayjob_client: PipesRayJobClient):
    pipes_rayjob_client.run(
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
    resources={"pipes_rayjob_client": PipesRayJobClient()}, assets=[my_asset]
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

Events emitted by the Ray job will be captured by `PipesRayJobClient` and will become available in the Dagster event log. Standard output and standard error streams will be forwarded to the standard output of the Dagster process.

**Running locally**

When running locally, the `port_forward` option has to be set to `True` in the `PipesRayJobClient` resource in order to interact with the Ray job. For convenience, it can be set automatically with:

```python
from dagster_ray.kuberay.configs import in_k8s

pipes_rayjob_client = PipesRayJobClient(..., port_forward=not in_k8s)
```

### Resources

#### `KubeRayCluster`

`KubeRayCluster` can be used for running Ray computations on Kubernetes in client (interactive) mode. Requies stable persistent connection through the duration of the Dagster step.

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
    ray_cluster: RayResource,  # RayResource is a backeand-agnostic type annotation
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
#### `KubeRayClient`

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

### Jobs

#### `delete_kuberay_clusters`

This `job` can be used to delete `RayClusters` from a given list of names.

#### `cleanup_old_ray_clusters`

This `job` can be used to delete old `RayClusters` which no longer correspond to any active Dagster Runs.
They may be left behind if the automatic cluster cleanup was disabled or failed.

### Schedules

Cleanup schedules can be trivially created using the `cleanup_old_ray_clusters` or `delete_kuberay_clusters` jobs.

#### `cleanup_old_ray_clusters`
`dagster-ray` provides an example daily cleanup schedule.


## Local

These resources can be used for development and testing purposes.
They provide the same interface as the other `*Ray` resources, but don't require any external infrastructure.

The public objects can be imported from `dagster_ray.local` module.

### Resources

#### `LocalRay`

A dummy resource which is useful for testing and development.
It doesn't do anything, but provides the same interface as the other `*Ray` resources.

Examples:


Using the `LocalRay` resource

```python
from dagster import asset, Definitions
from dagster_ray import RayResource
from dagster_ray.local import LocalRay
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
from dagster_ray import RayResource
from dagster_ray.local import LocalRay
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
