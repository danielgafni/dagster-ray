# `dagster-ray`


[![image](https://img.shields.io/pypi/v/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![image](https://img.shields.io/pypi/l/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![image](https://img.shields.io/pypi/pyversions/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![CI](https://github.com/danielgafni/dagster-ray/actions/workflows/ci.yml/badge.svg)](https://github.com/danielgafni/dagster-ray/actions/workflows/CI.yml)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Checked with pyright](https://microsoft.github.io/pyright/img/pyright_badge.svg)](https://microsoft.github.io/pyright/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

[Ray](https://github.com/ray-project/ray) integration library for [Dagster](https://github.com/dagster-io/dagster).

`dagster-ray` allows running Ray computations in Dagster pipelines. It provides various Dagster abstractions, the most important being `Resource`, and helper `@op`s and `@schedule`s, for multiple backends.

The following backends are implemented:
- local
- `KubeRay` (kubernetes)

`dagster-ray` is tested across multiple version combinations of components such as `ray`, `dagster`, `KubeRay Operator`, and `Python`.

`dagster-ray` integrates with [Dagster+](https://dagster.io/plus) out of the box.

Documentation can be found below.

> [!NOTE]
> This project is in early development. Contributions are very welcome! See the [Development](#development) section below.

# Backends

`dagster-ray` provides a `RayResource` class, which does not implement any specific backend.
It defines the common interface for all `Ray` resources.
It can be used for type annotations in your `@op` and `@asset` definitions.

Examples:

```python
from dagster import asset
from dagster_ray import RayResource
import ray


@asset
def my_asset(
    ray_cluster: RayResource,  # RayResource is only used as a type annotation
):
    return ray.get(ray.put(42))
```

The other resources below are the actual backends that implement the `RayResource` interface.

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

## KubeRay

This backend requires a Kubernetes cluster with the `KubeRay Operator` installed.

Integrates with [Dagster+](https://dagster.io/plus) by injecting environment variables such as `DAGSTER_CLOUD_DEPLOYMENT_NAME` and tags such as `dagster/user` into default configuration values and `RayCluster` labels.

The public objects can be imported from `dagster_ray.kuberay` module.

### Resources

#### `KubeRayCluster`

`KubeRayCluster` can be used for running Ray computations on Kubernetes.

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
    ray_cluster: RayResource,  # RayResource is only used as a type annotation
):  # this type annotation only defines the interface
    return ray.get(ray.put(42))


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
#### `KubeRayAPI`

This resource can be used to interact with the Kubernetes API Server.

Examples:

Listing currently running `RayClusters`:

```python
from dagster import op, Definitions
from dagster_ray.kuberay import KubeRayAPI


@op
def list_ray_clusters(
    kube_ray_api: KubeRayAPI,
):
    return kube_ray_api.kuberay.list_ray_clusters(k8s_namespace="kuberay")
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

## Executor
WIP

# Development

```shell
poetry install --all-extras
poetry shell
pre-commit install
```

## Testing

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
 - install the `KubeRay Operator` in the cluster with `helm`
 - run the tests

Thus, no manual setup is required, just the presence of the tools listed above. This makes testing a breeze!

> [!NOTE]
> Specifying a comma-separated list of `KubeRay Operator` versions in the `KUBE_RAY_OPERATOR_VERSIONS` environment variable will spawn a new test for each version.

> [!NOTE]
> it may take a while to download `minikube` and `kuberay-operator` images and build the local `dagster-ray` image during the first tests invocation
