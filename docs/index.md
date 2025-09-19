# Welcome to dagster-ray

**Ray integration for Dagster** - Orchestrate distributed Ray compute from Dagster pipelines with seamless integration between Dagster's orchestration capabilities and Ray's distributed computing power.

[![PyPI version](https://img.shields.io/pypi/v/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![License](https://img.shields.io/pypi/l/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![Python versions](https://img.shields.io/pypi/pyversions/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![CI](https://github.com/danielgafni/dagster-ray/actions/workflows/CI.yml/badge.svg)](https://github.com/danielgafni/dagster-ray/actions/workflows/CI.yml)

> [!NOTE]
> This project is ready for production use, but some APIs may change between minor releases.

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

### Basic Usage

!!! example
    Define a Dagster asset that uses Ray in client mode
    ```python
    import dagster as dg
    from dagster_ray import RayResource
    import ray


    @ray.remote
    def compute_square(x: int) -> int:
        return x**2


    @dg.asset
    def my_distributed_computation(ray_cluster: RayResource) -> int:  # (2)!
        futures = [compute_square.remote(i) for i in range(10)]  # (1)!
        return sum(ray.get(futures))
    ```

    1. :zap: I am already running in Ray!
    2. :bulb: `RayResource` is a type annotation that provides a common interface for Ray resources

    Now use `LocalRay` for local development and swap it with a thick cluster in Kubernetes!

    ```python
    from dagster_ray.kuberay import in_k8s, KubeRayInteractiveJob

    ray_cluster = LocalRay() if not in_k8s else KubeRayInteractiveJob()

    definitions = dg.Definitions(
        assets=[my_distributed_computation],
        resources={"ray_cluster": ray_cluster},
    )
    ```

Learn more by reading the [tutorials](/dagster-ray/tutorial).

## 🛠️ Choosing Your Integration

`dagster-ray` offers multiple ways to integrate Ray with your Dagster pipelines. The right choice depends on your deployment setup and use case:

### 🤔 Key Questions to Consider

- **Do you want to manage Ray clusters automatically?** If yes, use KubeRay components
- **Do you prefer to submit external scripts or run code directly?** [External scripts](https://docs.ray.io/en/latest/cluster/running-applications/job-submission/index.html#ray-jobs-api) offer better separation of concerns and environments, but [interactive code](https://docs.ray.io/en/latest/cluster/running-applications/job-submission/index.html#running-jobs-interactively) is more convenient
- **Do you need per-asset configuration?** Some components allow fine-grained control per asset

### 📊 Feature Comparison

<div class="comparison-table" markdown>

| Feature | `RayRunLauncher` | `ray_executor` | `PipesRayJobClient` | `PipesKubeRayJobClient` | `KubeRayCluster` | `KubeRayInteractiveJob` |
|---------|:------------:|:--------:|:------------:|:-------------:|:-------:|:--------------:|
| **Manages the cluster** | ❌ | ❌ | ❌ | ✅ | ✅ | ✅ |
| **Uses Ray Jobs API** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| **Enabled per-asset** | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |
| **Configurable per-asset** | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **No external script needed** | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ |
| **No Dagster DB access needed** | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ |

</div>

### 🎯 Which One Should You Use?

=== "🏢 External Ray Cluster"
    **You have a Ray cluster already running**

    - Use [`RayRunLauncher`](api/core.md#dagster_ray.run_launcher.RayRunLauncher) to run the entire Dagster deployment on Ray
    - Use [`ray_executor`](api/core.md#dagster_ray.executor.ray_executor) to run specific jobs on Ray
    - Use [`PipesRayJobClient`](api/core.md#dagster_ray.pipes.PipesRayJobClient) to submit external Python scripts as Ray jobs

    !!! tip
        See [external cluster tutorial](tutorial/external.md)

=== "☸️ Dagster-owned Ray Cluster (KubeRay)"
    **You want `dagster-ray` to handle cluster lifecycle**

    `dagster-ray` supports running Ray on Kubernetes with [KubeRay](https://docs.ray.io/en/latest/cluster/kubernetes/index.html).

    - Use [`KubeRayInteractiveJob`](api/kuberay.md#dagster_ray.kuberay.KubeRayInteractiveJob) to create a `RayJob` and connect in client mode
    - Use [`PipesKubeRayJobClient`](api/kuberay.md#dagster_ray.kuberay.PipesKubeRayJobClient) to submit external scripts as `RayJob`

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

</div>
