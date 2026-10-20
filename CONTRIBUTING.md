# Contributing

## Development

```bash
uv sync --all-extras --all-groups
uv run prek install
```

## Commit Messages

We use [Conventional Commits](https://www.conventionalcommits.org/). PRs are squash-merged, so the **PR title becomes the commit message** in `main`. This means the PR title must follow conventional commit format — individual commit messages on the branch don't matter. A CI check enforces this.

```
<type>(<optional scope>): <description>
```

| Type | Purpose |
|------|---------|
| `feat` | New feature |
| `fix` | Bug fix |
| `docs` | Documentation only |
| `refactor` | Code change that neither fixes a bug nor adds a feature |
| `perf` | Performance improvement |
| `test` | Adding or updating tests |
| `chore` | Maintenance (deps, CI, releases) |
| `ci` | CI/CD changes |
| `build` | Build system changes |
| `style` | Code style changes (formatting, etc.) |

Append `!` after the type/scope for breaking changes.

Examples:

```
feat: add BigQuery support
docs: update Pipes usage guide
fix!: rename RayResource to WhatTheHell
```

## Testing

Tests run as [Dagger](https://dagger.io) checks, the same ones CI runs (see [.dagger/README.md](.dagger/README.md)). The dev shell provides Dagger:

```bash
devenv shell
dagger check                       # everything CI runs
dagger check 'test-core-*'         # tests outside tests/kuberay
dagger check 'test-kuberay*'       # KubeRay tests, each against its own k3s cluster
dagger check -l                    # list checks
```

`uv run pytest` runs the tests that need neither Kubernetes nor a container image. It skips the KubeRay tests that need a cluster.

The shell defaults to Python 3.11. To test against another supported version, activate the
matching profile — each profile keeps its own venv under `.devenv/profiles/`, so switching
between them does not rebuild anything:

```bash
devenv shell -P python-3.13
```

## Documentation

To build and serve the documentation locally:

```bash
uv run --group docs mkdocs serve
```
