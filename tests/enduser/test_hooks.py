"""Exercise custom hooks, connections, and an installed SQL provider."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, cast
from unittest import mock

import pytest
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import DAG, BaseHook, BaseOperator
from airflow.utils.state import TaskInstanceState

from pytest_airflow_in_a_box.types import AirflowConnections, DagMaker, RunTask

pytestmark = pytest.mark.compat


class ClientHook(BaseHook):
    """Build a synthetic client configuration from one connection."""

    conn_name_attr = "conn_id"
    default_conn_name = "compat_service"
    conn_type = "http"
    hook_name = "Compatibility service"

    def __init__(self, conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.conn_id = conn_id

    def get_conn(self) -> dict[str, Any]:
        connection = self.get_connection(self.conn_id)
        return {
            "host": connection.host,
            "schema": connection.schema,
            "login": connection.login,
            "region": connection.extra_dejson["region"],
        }


class HookOperator(BaseOperator):
    """Return the custom hook's client configuration."""

    def __init__(self, *, conn_id: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id

    def execute(self, context: Any) -> dict[str, Any]:
        del context
        return ClientHook(self.conn_id).get_conn()


def _expected_connection() -> dict[str, str]:
    return {
        "host": "127.0.0.1",
        "schema": "synthetic",
        "login": "tester",
        "region": "local",
    }


@pytest.mark.db_test
def test_custom_hook_reads_a_persisted_connection(
    airflow_connections: AirflowConnections, dag_maker: DagMaker
) -> None:
    """Resolve host, schema, login, and typed extra through metadata."""

    with dag_maker(dag_id="compat_hook_db"):
        HookOperator(task_id="connect", conn_id="compat_hook_db_conn")
    airflow_connections(
        {
            "compat_hook_db_conn": {
                "conn_type": "http",
                "host": "127.0.0.1",
                "schema": "synthetic",
                "login": "tester",
                "extra": '{"region": "local"}',
            }
        }
    )

    ti = dag_maker.run_ti("connect")

    assert ti.xcom_pull(task_ids="connect", session=dag_maker.session) == _expected_connection()


def test_custom_hook_reads_a_seeded_connection(run_task: RunTask) -> None:
    """Resolve the same connection shape through fake supervision."""

    with DAG(dag_id="compat_hook_free", schedule=None) as dag:
        HookOperator(task_id="connect", conn_id="compat_hook_free_conn")

    result = run_task(
        dag.get_task("connect"),
        connections={
            "compat_hook_free_conn": {
                "conn_type": "http",
                "host": "127.0.0.1",
                "schema": "synthetic",
                "login": "tester",
                "extra": '{"region": "local"}',
            }
        },
    )

    assert result.xcoms["return_value"] == _expected_connection()


def test_custom_hook_mocked_with_unittest_mock(run_task: RunTask) -> None:
    """Patch a custom hook's connection resolution instead of seeding one."""

    with DAG(dag_id="compat_hook_mocked", schedule=None) as dag:
        HookOperator(task_id="connect", conn_id="unused")

    with mock.patch.object(ClientHook, "get_conn", return_value={"region": "mocked"}):
        result = run_task(dag.get_task("connect"))

    assert result.xcoms["return_value"] == {"region": "mocked"}


def _fetch_scalar(cursor: Any) -> int:
    return cursor.fetchone()[0]


@pytest.mark.db_test
def test_sqlite_provider_operator_runs_end_to_end(
    airflow_connections: AirflowConnections, dag_maker: DagMaker, tmp_path: Path
) -> None:
    """Execute a real installed-provider operator against synthetic SQLite."""

    database = tmp_path / "provider.db"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE answers (value INTEGER NOT NULL)")
        connection.execute("INSERT INTO answers VALUES (42)")
    with dag_maker(dag_id="compat_sql_provider"):
        SQLExecuteQueryOperator(
            task_id="query",
            conn_id="compat_sqlite_conn",
            sql="SELECT value FROM answers",
            handler=cast(Any, _fetch_scalar),
        )
    airflow_connections({"compat_sqlite_conn": {"conn_type": "sqlite", "host": str(database)}})

    ti = dag_maker.run_ti("query")

    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids="query", session=dag_maker.session) == 42
