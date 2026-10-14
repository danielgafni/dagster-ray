sync:
    uv sync --all-extras --all-groups

docs-build:
    uv run --all-extras --all-groups zensical build --clean --strict

docs-serve:
    uv run --all-extras --all-groups zensical serve

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

# Outside CI it runs in a temporary copy of the worktree, so local files and the dev venv stay untouched.
# Run a CI test matrix job: pin Ray and Dagster for the given Python, then run the full test suite.
test-matrix py ray dagster kuberay:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -z "${CI:-}" ]; then
        workdir=$(mktemp -d)
        trap 'rm -rf "$workdir"' EXIT
        git ls-files -z -co --exclude-standard \
            | while IFS= read -r -d '' f; do [ -e "$f" ] && printf '%s\0' "$f"; done \
            | tar --null -T - -cf - | tar -xf - -C "$workdir"
        cd "$workdir"
        unset UV_PROJECT_ENVIRONMENT VIRTUAL_ENV
    fi
    set -x
    export UV_PYTHON={{py}}
    marker="python_full_version == '{{py}}.*'"
    # scoped to this Python so the lock still resolves for the rest of requires-python
    uv add --no-sync "ray[all]=={{ray}} ; $marker" "dagster=={{dagster}} ; $marker"
    export UV_LOCKED=1
    uv sync
    uv run python -c "import dagster_ray"
    uv sync --all-extras --dev
    # nothing may re-sync the environment from here on (e.g. Ray workers launched with `uv run`)
    export UV_NO_SYNC=1
    export PYTEST_KUBERAY_VERSIONS={{kuberay}}
    uv run pytest -v .
    uv run pytest -v -m kuberay_auth .
