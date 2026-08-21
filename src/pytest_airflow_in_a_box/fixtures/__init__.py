"""Public pytest fixtures registered by the plugin.

References:
    https://docs.pytest.org/en/stable/how-to/fixtures.html
"""

from __future__ import annotations

from pytest_airflow_in_a_box.fixtures.api import api_base_url, api_client, api_server_url
from pytest_airflow_in_a_box.fixtures.components import airflow_components
from pytest_airflow_in_a_box.fixtures.configure import airflow_configure
from pytest_airflow_in_a_box.fixtures.context import task_context
from pytest_airflow_in_a_box.fixtures.dag import dag_maker, run_dag
from pytest_airflow_in_a_box.fixtures.dagbag import full_dag_bag
from pytest_airflow_in_a_box.fixtures.logging import cap_structlog
from pytest_airflow_in_a_box.fixtures.paths import (
    airflow_dags_folder_path,
    airflow_home_path,
)
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
        "run_dag",
        "session",
    }
)

__all__ = (
    "airflow_components",
    "airflow_configure",
    "airflow_connections",
    "airflow_dags_folder_path",
    "airflow_home_path",
    "airflow_parse_secrets",
    "airflow_variables",
    "api_base_url",
    "api_client",
    "api_server_url",
    "cap_structlog",
    "dag_maker",
    "full_dag_bag",
    "render_task",
    "run_dag",
    "run_task",
    "session",
    "task_context",
)
