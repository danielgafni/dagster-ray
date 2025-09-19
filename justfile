sync:
    uv sync --all-extras --all-groups

publish-dev:
    echo '__version__ = "0.0.0"  # managed by hatch' > src/dagster_ray/_version.py
    uv run --with dunamai --with hatch hatch version $(uv run dunamai from any --style pep440 --ignore-untracked --no-metadata)
    uv run hatch clean
    uv build
    uv publish
    uv run --with hatch hatch clean
    echo '__version__ = "0.0.0"  # managed by hatch' > src/dagster_ray/_version.py

docs-build:
    uv run --group docs mkdocs build --clean --strict

docs-serve:
    uv run --group docs mkdocs serve

docs-publish:
    uv run --group docs --all-extras mike deploy --push --update-aliases $(uv run dunamai from any --style pep440)

ruff:
    uv run --no-sync ruff check --fix --exit-zero
    uv run --no-sync ruff format
