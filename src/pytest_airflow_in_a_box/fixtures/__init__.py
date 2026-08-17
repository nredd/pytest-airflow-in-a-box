"""Public pytest fixtures registered by the plugin.

References:
    https://docs.pytest.org/en/stable/how-to/fixtures.html
"""

from __future__ import annotations

from pytest_airflow_in_a_box.fixtures.api import api_base_url, api_client, api_server_url
from pytest_airflow_in_a_box.fixtures.dag import dag_maker
from pytest_airflow_in_a_box.fixtures.dagbag import full_dag_bag
from pytest_airflow_in_a_box.fixtures.logging import cap_structlog
from pytest_airflow_in_a_box.fixtures.render import render_task
from pytest_airflow_in_a_box.fixtures.seed import (
    airflow_connections,
    airflow_parse_secrets,
    airflow_variables,
)
from pytest_airflow_in_a_box.fixtures.session import session
from pytest_airflow_in_a_box.fixtures.taskrun import run_task

DATABASE_FIXTURE_NAMES = frozenset(
    {
        "airflow_connections",
        "airflow_parse_secrets",
        "airflow_variables",
        "api_client",
        "api_server_url",
        "dag_maker",
        "full_dag_bag",
        "session",
    }
)

__all__ = (
    "airflow_connections",
    "airflow_parse_secrets",
    "airflow_variables",
    "api_base_url",
    "api_client",
    "api_server_url",
    "cap_structlog",
    "dag_maker",
    "full_dag_bag",
    "render_task",
    "run_task",
    "session",
)
