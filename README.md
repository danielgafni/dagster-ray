# `dagster-ray`


[![image](https://img.shields.io/pypi/v/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![image](https://img.shields.io/pypi/l/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![image](https://img.shields.io/pypi/pyversions/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![CI](https://github.com/danielgafni/dagster-ray/actions/workflows/ci.yml/badge.svg)](https://github.com/danielgafni/dagster-ray/actions/workflows/ci.yml)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Checked with pyright](https://microsoft.github.io/pyright/img/pyright_badge.svg)](https://microsoft.github.io/pyright/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

[Ray](https://github.com/ray-project/ray) integration library for [Dagster](https://github.com/dagster-io/dagster).

`dagster-ray` allows you to run Ray computations in your Dagster pipelines. The following backends are implemented:
- `KubeRay` (kubernetes)

`dagster-ray` is tested across multiple version combinations of components such as `ray`, `dagster`, `KubeRay Operator`, and `Python`.

# Features

## Resources

### `LocalRay`

A dummy resource which is useful for testing and development.
It doesn't do anything, but provides the same interface as the other `*Ray` resources.

### `KubeRayCluster`

`KubeRayCluster` can be used for running Ray computations on Kubernetes. Requires `KubeRay Operator` to be installed the Kubernetes cluster.

When added as resource dependency to an `@op/@asset`, the `KubeRayCluster`:
 - Starts a dedicated `RayCluster` for it
 - connects `ray.init()` to the cluster (if `ray` is installed)
 - tears down the cluster after the step is executed

## Schedules

`dagster-ray` provides a schedule for automatic cleanup of old `RayClusters` in the cluster.
They may be left behind if the automatic cleanup was disabled or failed.

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
 - build an image with the local `dagster-ray` code, using the current Python's interpreter version
 - start a `minikube` Kubernetes cluster
 - load the built `dagster-ray` and loaded `kuberay-operator` images into the cluster
 - install the `KubeRay Operator` in the cluster with `helm`
 - run the tests

Thus, no manual setup is required, just the presence of the tools listed above.

> [!NOTE]
> Specifying a comma-separated list of `KubeRay Operator` versions in the `KUBE_RAY_OPERATOR_VERSIONS` environment variable will spawn a new test for each version.

> [!NOTE]
> it may take a while to download `minikube` and `kuberay-operator` images and build the local `dagster-ray` image during the first tests invocation
