# Dagster libraries to run both dagster-webserver and the dagster-daemon. Does not
# need to have access to any pipeline code.

FROM python:3.10-slim

ENV UV_SYSTEM_PYTHON=1

# install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install \
    dagster \
    dagster-graphql \
    dagster-webserver \
    dagster-postgres \
    dagster-docker \
    dagster-k8s \
    ray[all]

WORKDIR /src

COPY pyproject.toml README.md ./
COPY dagster_ray ./dagster_ray

RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install -e .

# Set $DAGSTER_HOME and copy dagster instance and workspace YAML there
ENV DAGSTER_HOME=/dagster_home

WORKDIR $DAGSTER_HOME

COPY examples/docker/run_launcher/ ./
