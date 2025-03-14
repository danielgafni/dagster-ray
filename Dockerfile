#syntax=docker/dockerfile:1.14

# options: prod,dev
ARG BUILD_DEPENDENCIES=prod
ARG PYTHON_VERSION=3.11.7

FROM python:${PYTHON_VERSION}-slim AS base

ENV DEBIAN_FRONTEND=noninteractive
RUN --mount=type=cache,target=/var/cache/apt \
    apt-get update && apt-get install -y git jq curl gcc python3-dev libpq-dev wget

COPY --from=bitnami/kubectl:1.32.3 /opt/bitnami/kubectl/bin/kubectl /usr/local/bin/

# install uv (https://github.com/astral-sh/uv)
# docs for using uv with Docker: https://docs.astral.sh/uv/guides/integration/docker/
COPY --from=ghcr.io/astral-sh/uv:0.6.6 /uv /bin/uv

ENV UV_PROJECT_ENVIRONMENT=/usr/local/
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME

FROM base AS base-prod

WORKDIR /src

COPY pyproject.toml uv.lock  ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --all-extras --no-dev --no-install-project --inexact

FROM base-prod AS base-dev

# # Node.js is needed for pyright in CI (no longer needed since basedpyright is now used)
# ARG NODE_VERSION=23.0.0
# ARG NODE_PACKAGE=node-v$NODE_VERSION-linux-x64
# ARG NODE_HOME=/opt/$NODE_PACKAGE
# ENV NODE_PATH $NODE_HOME/lib/node_modules
# ENV PATH $NODE_HOME/bin:$PATH
# RUN --mount=type=cache,target=/cache/downloads \
#     curl --retry 3 https://nodejs.org/dist/v$NODE_VERSION/$NODE_PACKAGE.tar.gz -o /cache/downloads/$NODE_PACKAGE.tar.gz \
#     && tar -xzC /opt/ -f /cache/downloads/$NODE_PACKAGE.tar.gz


RUN mkdir dagster_ray && touch dagster_ray/__init__.py && touch README.md
COPY dagster_ray/_version.py dagster_ray/_version.py

# Install specific Dagster and Ray versions (for integration tests)
ARG RAY_VERSION=2.35.0
ARG DAGSTER_VERSION=1.8.12
RUN --mount=type=cache,target=/root/.cache/uv \
    uv add --no-sync "ray[all]==$RAY_VERSION" "dagster==$DAGSTER_VERSION"

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --all-extras --no-install-project

# -------------------------------------------------------------
FROM base-${BUILD_DEPENDENCIES} AS final

# Copy all the rest of the code
COPY . .

# finally install all our code
RUN uv sync --frozen --all-extras --inexact
