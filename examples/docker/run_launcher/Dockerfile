# Dagster libraries to run both dagster-webserver and the dagster-daemon. Does not
# need to have access to any pipeline code.

FROM python:3.10-slim

ENV UV_SYSTEM_PYTHON=1
ENV UV_PROJECT_ENVIRONMENT=/usr/local/

# install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Set $DAGSTER_HOME and copy dagster instance and workspace YAML there
ENV DAGSTER_HOME=/dagster_home

WORKDIR $DAGSTER_HOME

COPY examples/docker/run_launcher/pyproject.toml examples/docker/run_launcher/uv.lock ./


RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-editable --frozen

COPY pyproject.toml README.md /dagster-ray/
COPY dagster_ray /dagster-ray/dagster_ray

RUN uv pip install /dagster-ray

COPY examples/docker/run_launcher/example ./example

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --inexact

COPY examples/docker/run_launcher/dagster.yaml examples/docker/run_launcher/workspace.yaml ./
