# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

dagster-ray is a Dagster integration library for Ray distributed computing. It provides Run Launchers, Executors, IO Managers, Resources, and Pipes clients that connect Dagster orchestration with Ray clusters (local and KubeRay on Kubernetes).

## Commands

```bash
# Setup
uv sync --all-extras --all-groups
uv run prek install              # install pre-commit hooks

# Tests
uv run pytest                    # all tests (excludes kuberay_auth by default)
uv run pytest tests/test_pipes.py               # single test file
uv run pytest tests/test_pipes.py::test_name -k "pattern"  # single test or pattern
uv run pytest -m kuberay_auth    # auth-specific KubeRay tests (needs minikube)

# Linting & type checking
uv run ruff check src tests      # lint
uv run ruff format src tests     # format
uv run basedpyright              # type check

# Docs
uv run --group docs mkdocs serve
```

KubeRay tests require: docker, kubectl, helm, minikube. Nix users: `nix develop`.

## Architecture

Source is in `src/dagster_ray/` with three layers:

- **`_base/`** — Abstract `RayResource` base class with lifecycle management (create/wait/connect/cleanup policies), cluster sharing lock, and utilities. All Ray resource implementations inherit from this.
- **`core/`** — Local/generic Ray integration: `RayExecutor` (submits Dagster steps as Ray jobs), `RayRunLauncher` (submits entire runs as Ray jobs), `RayIOManager` (Ray object store), `LocalRay` resource, and Pipes integration (`PipesRayJobClient`).
- **`kuberay/`** — Kubernetes-native Ray via KubeRay operator: `KubeRayCluster` and `KubeRayInteractiveJob` resources, `PipesKubeRayJobClient` for Pipes, Kubernetes API clients (`client/`), config dataclasses (`configs.py`), leader election, and cleanup sensors.

Configuration classes in `configs.py` (root) define `RayExecutionConfig` and `RayJobSubmissionClientConfig` with per-job overrides via dagster-ray/config tags.

## Documentation

Write concise, direct prose. State facts, not feelings. No filler words, no hedging, no "leverage", "streamline", "empower", or other corporate/AI buzzwords. Short sentences. Say what something does, not how excited you are about it. If a sentence adds no information, delete it.

Make sure to update `docs/` when making changes.

## Conventions

- **Conventional Commits** for PR titles (PRs are squash-merged): `feat(kuberay): add X`, `fix: resolve Y`
- **Snapshot tests** use syrupy (see `tests/kuberay/__snapshots__/`)

## Code Intelligence

Prefer LSP over Grep/Read for code navigation — it's faster, precise, and avoids reading entire files:
- `workspaceSymbol` to find where something is defined
- `findReferences` to see all usages across the codebase
- `goToDefinition` / `goToImplementation` to jump to source
- `hover` for type info without reading the file

Use Grep only when LSP isn't available or for text/pattern searches (comments, strings, config).

After writing or editing code, check LSP diagnostics and fix errors before proceeding.
