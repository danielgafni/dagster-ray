#syntax=docker/dockerfile:1.4

# options: prod,dev
ARG BUILD_DEPENDENCIES=prod
ARG PYTHON_VERSION=3.11.7

FROM python:${PYTHON_VERSION}-slim AS base

ENV DEBIAN_FRONTEND=noninteractive
RUN --mount=type=cache,target=/var/cache/apt \
    apt-get update && apt-get install -y git jq curl gcc python3-dev libpq-dev wget

# install poetry
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_HOME="/opt/poetry" \
    POETRY_NO_INTERACTION=1 \
    PYSETUP_PATH="/opt/pysetup" \
    VENV_PATH="/opt/pysetup/.venv"

ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH" \
    POETRY_VERSION=1.8.2 \
    POETRY_DYNAMIC_VERSIONING_PLUGIN_VERSION=1.2.0 \
    POETRY_DYNAMIC_VERSIONING_COMMANDS=version,build,publish

RUN mkdir "$HOME/opt/" \
    && curl -sSL https://install.python-poetry.org > /tmp/get-poetry.py \
    && python3 /tmp/get-poetry.py \
    && poetry config virtualenvs.create false \
    && mkdir -p /cache/poetry \
    && poetry config cache-dir /cache/poetry \
    && python -m pip install --upgrade pip wheel setuptools \
    && poetry self add "poetry-dynamic-versioning[plugin]=$POETRY_DYNAMIC_VERSIONING_PLUGIN_VERSION"

ARG INSTALLER_PARALLEL=true
RUN poetry config installer.parallel $INSTALLER_PARALLEL

ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME

FROM base AS base-prod

WORKDIR /src

COPY pyproject.toml poetry.lock  ./

RUN --mount=type=cache,target=/cache/poetry \
    poetry install --no-root --only main --all-extras

FROM base-prod AS base-dev

# Node.js is needed for pyright in CI
ARG NODE_VERSION=20.7.0
ARG NODE_PACKAGE=node-v$NODE_VERSION-linux-x64
ARG NODE_HOME=/opt/$NODE_PACKAGE
ENV NODE_PATH $NODE_HOME/lib/node_modules
ENV PATH $NODE_HOME/bin:$PATH
RUN --mount=type=cache,target=/cache/downloads \
    curl https://nodejs.org/dist/v$NODE_VERSION/$NODE_PACKAGE.tar.gz -o /cache/downloads/$NODE_PACKAGE.tar.gz \
    && tar -xzC /opt/ -f /cache/downloads/$NODE_PACKAGE.tar.gz

RUN --mount=type=cache,target=/cache/poetry \
    poetry install --no-root --only dev --all-extras

# -------------------------------------------------------------
FROM base-${BUILD_DEPENDENCIES} AS final

# Copy all the rest of the code
COPY . .

# finally install all our code
RUN poetry install --all-extras

ARG POETRY_DYNAMIC_VERSIONING_BYPASS=unset
ENV POETRY_DYNAMIC_VERSIONING_BYPASS_TMP=$POETRY_DYNAMIC_VERSIONING_BYPASS
RUN if [ $POETRY_DYNAMIC_VERSIONING_BYPASS_TMP != unset ]; \
    then export POETRY_DYNAMIC_VERSIONING_BYPASS=$POETRY_DYNAMIC_VERSIONING_BYPASS_TMP && poetry dynamic-versioning; \
    fi
