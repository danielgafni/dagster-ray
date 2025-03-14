publish-dev:
    echo '__version__ = "0.0.0"  # managed by hatch' > dagster_ray/_version.py
    uv run --with dunamai --with hatch hatch version $(uv run dunamai from any --style pep440 --ignore-untracked --no-metadata)
    uv run hatch clean
    uv build
    uv publish
    uv run --with hatch hatch clean
    echo '__version__ = "0.0.0"  # managed by hatch' > dagster_ray/_version.py


ruff:
    uv run --no-sync ruff check --fix --exit-zero
    uv run --no-sync ruff format
