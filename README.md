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

`dagster-ray` enables working with distributed Ray compute from Dagster pipelines, combining Dagster's excellent orchestration capabilities and Ray's distributed computing power together.

> [!NOTE]
> This project is ready for production use, but some APIs may change between minor releases.

Learn more in the [docs](https://danielgafni.github.io/dagster-ray)

## 🚀 Key Features

- **🎯 Run Launchers & Executors**: Submit Dagster runs or individual steps by submitting Ray jobs
- **🔧 Ray Resources**: Automatically create and destroy ephemeral Ray clusters and connect to them in client mode
- **📡 Dagster Pipes Integration**: Submit external scripts as Ray jobs, stream back logs and rich Dagster metadata
- **☸️ KubeRay Support**: Utilize `RayJob` and `RayCluster` custom resources in client or job submission mode ([tutorial](tutorial/kuberay.md))
- **🏭 Production Ready**: Tested against a matrix of core dependencies, integrated with Dagster+

## Installation

```shell
pip install dagster-ray
```

## 📚 Docs

**📖 [Full Documentation](https://danielgafni.github.io/dagster-ray)**

- **[Tutorial](https://danielgafni.github.io/dagster-ray/tutorial/)**: Step-by-step guide with examples
- **[API Reference](https://danielgafni.github.io/dagster-ray/api/)**: Complete API reference

## 🤝 Contributing

Contributions are very welcome! To get started:

```bash
uv sync --all-extras --all-groups
uv run pre-commit install
```

### 🧪 Testing

```bash
uv run pytest
```

Running KubeRay tests requires the following tools to be present:
- `docker`
- `kubectl`
- `helm`
- `minikube`

❄️ Nix users will find them provided in the dev shell:

```
nix develop
```

### Documentation

To build and serve the documentation locally:

```bash
# Serve documentation locally
uv run --group docs mkdocs serve
```
