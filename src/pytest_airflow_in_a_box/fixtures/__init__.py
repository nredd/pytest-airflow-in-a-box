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
from pytest_airflow_in_a_box.fixtures.dagbag import dag_bag
from pytest_airflow_in_a_box.fixtures.logging import cap_structlog
from pytest_airflow_in_a_box.fixtures.paths import (
    airflow_dags_folder,
    airflow_home,
)
from pytest_airflow_in_a_box.fixtures.render import render_task
from pytest_airflow_in_a_box.fixtures.seed import (
    airflow_connections,
    airflow_parse_secrets,
    airflow_variables,
)
from pytest_airflow_in_a_box.fixtures.session import session
from pytest_airflow_in_a_box.fixtures.taskrun import run_task
from pytest_airflow_in_a_box.fixtures.upstream import (
    create_dummy_dag,
    create_task_instance,
    testing_dag_bundle,
)

DATABASE_FIXTURE_NAMES = frozenset(
    {
        "airflow_connections",
        "airflow_parse_secrets",
        "airflow_variables",
        "api_client",
        "api_server_url",
        "create_dummy_dag",
        "create_task_instance",
        "dag_maker",
        "dag_bag",
        "run_dag",
        "session",
        "testing_dag_bundle",
    }
)

__all__ = (
    "airflow_components",
    "airflow_configure",
    "airflow_connections",
    "airflow_dags_folder",
    "airflow_home",
    "airflow_parse_secrets",
    "airflow_variables",
    "api_base_url",
    "api_client",
    "api_server_url",
    "cap_structlog",
    "create_dummy_dag",
    "create_task_instance",
    "dag_bag",
    "dag_maker",
    "render_task",
    "run_dag",
    "run_task",
    "session",
    "task_context",
    "testing_dag_bundle",
)
