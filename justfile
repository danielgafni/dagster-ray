sync:
    uv sync --all-extras --all-groups

docs-build:
    uv run --group docs zensical build --clean --strict

docs-serve:
    uv run --group docs zensical serve

docs-publish:
    uv run --group docs --all-extras python scripts/deploy_docs.py --push --update-aliases $(git describe --tags --abbrev=0 | sed 's/^v//')

ruff:
    uv run --no-sync ruff check --fix --exit-zero
    uv run --no-sync ruff format

# Preview unreleased changelog entries
changelog-preview:
    git cliff --unreleased --strip all

# Generate full CHANGELOG.md
changelog:
    git cliff -o docs/changelog.md

# Create a release (version auto-detected from commits, or manually specified)
release bump="" message="":
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -n "{{bump}}" ]; then
        uv version --bump {{bump}}
    else
        uv version "$(git cliff --bumped-version | sed 's/^v//')"
    fi
    version="v$(uv version --short)"
    echo "__version__ = \"$(uv version --short)\"" > src/dagster_ray/_version.py
    if [ -n "{{message}}" ]; then
        git cliff --tag "$version" --with-tag-message "{{message}}" -o docs/changelog.md
    else
        git cliff --tag "$version" -o docs/changelog.md
    fi
