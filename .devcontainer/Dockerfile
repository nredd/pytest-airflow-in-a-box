ARG PYTHON_VERSION=3.12
ARG UV_VERSION=0.12.2

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv
FROM mcr.microsoft.com/devcontainers/python:1-${PYTHON_VERSION}-bookworm

COPY --from=uv /uv /uvx /usr/local/bin/

RUN install -d -m 1777 /tmp/pytest-airflow-in-a-box

USER vscode
