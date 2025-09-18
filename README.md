# `dagster-ray`

[![PyPI version](https://img.shields.io/pypi/v/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![License](https://img.shields.io/pypi/l/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![Python versions](https://img.shields.io/pypi/pyversions/dagster-ray.svg)](https://pypi.python.org/pypi/dagster-ray)
[![CI](https://github.com/danielgafni/dagster-ray/actions/workflows/CI.yml/badge.svg)](https://github.com/danielgafni/dagster-ray/actions/workflows/CI.yml)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![basedpyright - checked](https://img.shields.io/badge/basedpyright-checked-42b983)](https://docs.basedpyright.com)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

---

**Ray integration for Dagster.**

`dagster-ray` enables you to orchestrate distributed Ray compute from Dagster pipelines, providing seamless integration between Dagster's orchestration capabilities and Ray's distributed computing power.

> ![NOTE]
> This project is ready for production use, but some APIs may change between minor releases.

Learn more in the [docs](https://danielgafni.github.io/dagster-ray)

## 🚀 Key Features

- **Run Launchers & Executors**: Submit Dagster runs or individual ops as Ray jobs
- **Ray Resources**: Manage Ray clusters with Kubernetes (KubeRay) or local backends, connect to them in client mode
- **Dagster Pipes**: Execute external Ray scripts with rich logging and metadata
- **Production Ready**: Tested against a matrix of core dependencies and platform versions, integrated with Dagster+

## 📦 Quick Start

### Installation

```shell
pip install dagster-ray
```

### Example

```python
import dagster as dg
from dagster_ray import LocalRay, RayResource, KubeRayInteractiveJob
import ray


@ray.remote
def compute_square(x: int) -> int:
    return x**2


@dg.asset
def my_distributed_computation(ray_cluster: RayResource) -> int:
    futures = [compute_square.remote(i) for i in range(10)]
    return sum(ray.get(futures))


ray_cluster = LocalRay() if not IN_KUBERNETES else KubeRayInteractiveJob()


definitions = dg.Definitions(
    assets=[my_distributed_computation],
    resources={"ray_cluster": ray_cluster},
)
```

## 📚 Docs

**📖 [Full Documentation](https://danielgafni.github.io/dagster-ray)**

- **[Tutorial](https://danielgafni.github.io/dagster-ray/tutorial/)**: Step-by-step guide with examples
- **[API Reference](https://danielgafni.github.io/dagster-ray/api/)**: Complete API documentation

## 🛠️ Integration Options

| Component | Use Case | Cluster Management | Ray Mode |
|-----------|----------|-------------------|------|
| `RayRunLauncher` | Deployment-wide Ray runtime | External | Job Mode |
| `ray_executor` | Ray runtime scoped to a Code Location | External | Job Mode |
| `PipesRayJobClient` | Submit external scripts as Ray jobs | External | Job Mode |
| `PipesKubeRayJobClient` | Submit an external script as a `RayJob`, forward logs and Dagster metadata | Automatic | Job Mode |
| `KubeRayInteractiveJob` | Create a `RayJob`, connect in Client mode without an external script  | Automatic | Client Mode |

## 🤝 Contributing

Contributions are very welcome! To get started:

```bash
git clone https://github.com/danielgafni/dagster-ray.git
cd dagster-ray
uv sync --all-extras
uv run pre-commit install
```

### 🧪 Testing

```bash
uv run pytest
```

Running KubeRay tests requires the following tools to be present:
- `docker`, `kubectl`, `helm`, `minikube`

### Documentation

To build and serve the documentation locally:

```bash
# Serve documentation locally
uv run --group docs mkdocs serve

# Build documentation
uv run--group docs mkdocs build
```

The documentation is automatically deployed to GitHub Pages.
