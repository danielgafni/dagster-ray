# Welcome to dagster-ray

**Ray integration for Dagster** - Orchestrate distributed Ray compute from Dagster pipelines with seamless integration between Dagster's orchestration capabilities and Ray's distributed computing power.

[![PyPI version](https://img.shields.io/pypi/v/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![License](https://img.shields.io/pypi/l/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![Python versions](https://img.shields.io/pypi/pyversions/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![CI](https://github.com/danielgafni/dagster-ray/actions/workflows/CI.yml/badge.svg)](https://github.com/danielgafni/dagster-ray/actions/workflows/CI.yml)

!!! info
    This project is ready for production use, but some APIs may change between minor releases as we continue to improve the integration.

## 🚀 Key Features

- **🎯 Run Launchers & Executors**: Submit Dagster runs or individual steps as Ray jobs
- **🔧 Ray Resources**: Connect to Ray cluster in client mode and manage their lifecycle
- **📡 Dagster Pipes Integration**: Execute external Ray scripts with rich logging and metadata
- **☸️ KubeRay Support**: Utilize `RayJob` and `RayCluster` custom resources in client or job mode ([tutorial](tutorial/kuberay.md))
- **🏭 Production Ready**: Tested against a matrix of core dependencies, integrated with Dagster+

## ⚡ Quick Start

### Installation

=== "Basic"
    ```bash
    pip install dagster-ray
    ```

=== "With KubeRay"
    ```bash
    pip install 'dagster-ray[kuberay]'
    ```

!!! example

    ```python
    from dagster import asset, Definitions
    from dagster_ray import LocalRay, RayResource, KubeRayInteractiveJob
    import ray


    @ray.remote
    def compute_square(x: int) -> int:
        return x**2


    @asset
    def my_distributed_computation(ray_cluster: RayResource) -> int:  # (2)!
        futures = [compute_square.remote(i) for i in range(10)]  # (1)!
        return sum(ray.get(futures))


    ray_cluster = LocalRay() if not IN_KUBERNETES else KubeRayInteractiveJob()

    definitions = Definitions(
        assets=[my_distributed_computation],
        resources={"ray_cluster": ray_cluster},
    )
    ```

    1. :zap: I am already running in Ray!
    2. :bulb: `RayResource` is a type annotation that provides a common interface for Ray resources


## 🛠️ Choosing Your Integration

`dagster-ray` offers multiple ways to integrate Ray with your Dagster pipelines. The right choice depends on your deployment setup and use case:

### 🤔 Key Questions to Consider

- **Do you want to manage Ray clusters automatically?** If yes, use KubeRay components
- **Do you prefer to submit external scripts or run code directly?** External scripts offer better separation of concerns and environments, but direct code is more convenient
- **Do you need per-asset configuration?** Some components allow fine-grained control per asset

### 📊 Feature Comparison

<div class="comparison-table" markdown>

| Feature | `RayRunLauncher` | `ray_executor` | `PipesRayJobClient` | `PipesKubeRayJobClient` | `KubeRayCluster` | `KubeRayInteractiveJob` |
|---------|:------------:|:--------:|:------------:|:-------------:|:-------:|:--------------:|
| **Manages cluster** | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ |
| **Submits jobs in cluster mode** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| **Per-asset enabled** | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| **Per-asset configurable** | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **No external script needed** | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ |
| **No Dagster DB access needed** | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |

</div>

### 🎯 Which One Should You Use?

=== "🏢 External Ray Cluster"
    **You have a Ray cluster already running**

    - Use `RayRunLauncher` for deployment-wide Ray execution
    - Use `ray_executor` for code location-scoped execution
    - Use `PipesRayJobClient` to submit external Python scripts

    !!! tip
        See [external cluster tutorial](tutorial/external.md)

=== "☸️ Kubernetes + Automatic Management"
    **You want `dagster-ray` to handle cluster lifecycle**

    - Use `KubeRayInteractiveJob` to create a `RayJob` and connect in client mode
    - Use `PipesKubeRayJobClient` to submit external scripts as `RayJob`

    !!! tip
        See [KubeRay tutorial](tutorial/kuberay.md)
## 📚 What's Next?

<div class="grid cards" markdown>

- :material-school: **[Tutorial](tutorial/index.md)**

    ---

    Step-by-step guide with practical examples to get you started with `dagster-ray`

- :material-api: **[API Reference](api.md)**

    ---

    Complete documentation of all classes, methods, and configuration options

- :material-code-braces: **[Examples](examples.md)**

    ---

    Basic examples of using `dagster-ray` with Dagster assets and ops

</div>
