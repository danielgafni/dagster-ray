# Contributing

## Development

```bash
uv sync --all-extras --all-groups
uv run pre-commit install
```

## Commit Messages

We use [Conventional Commits](https://www.conventionalcommits.org/). PRs are squash-merged, so the **PR title becomes the commit message** in `master`. This means the PR title must follow conventional commit format — individual commit messages on the branch don't matter. A CI check enforces this.

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

```bash
uv run pytest
```

Running KubeRay tests requires the following tools to be present:
- `docker`
- `kubectl`
- `helm`
- `minikube`

Nix users will find them provided in the dev shell:

```bash
nix develop
```

## Documentation

To build and serve the documentation locally:

```bash
uv run --group docs mkdocs serve
```
