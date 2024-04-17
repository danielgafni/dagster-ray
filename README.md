# `dagster-ray`


[![image](https://img.shields.io/pypi/v/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![image](https://img.shields.io/pypi/l/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![image](https://img.shields.io/pypi/pyversions/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![CI](https://github.com/danielgafni/dagster-ray/actions/workflows/ci.yml/badge.svg)](https://github.com/danielgafni/dagster-ray/actions/workflows/ci.yml)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Checked with pyright](https://microsoft.github.io/pyright/img/pyright_badge.svg)](https://microsoft.github.io/pyright/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

[Ray](https://github.com/ray-project/ray) integration library for [Dagster](https://github.com/dagster-io/dagster).

> [!WARNING]
> WIP, perhaps not usable.

# Features

## Resources

### `KubeRayCluster`

`KubeRayCluster` can be used for running Ray computations on Kubernetes. `KubeRay Operator` must be installed in the cluster for it to work.

When added as resource dependency to an `@op/@asset`, the `KubeRayCluster`:
 - Starts a dedicated `RayCluster` for it
 - connects `ray.init()` to the cluster (if `ray` is installed)
 - tears down the cluster after the step is executed

## Schedules

`dagster-ray` provides a schedule for automatic cleanup of old `RayClusters` in the cluster.
They may be left behind if the automatic cleanup was disabled or failed.

## IOManagers
WIP

## Executor
WIP

# Development

```shell
poetry install --all-extras
poetry shell
pre-commit install
```