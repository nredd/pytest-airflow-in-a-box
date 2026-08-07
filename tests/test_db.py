"""Test registry-driven metadata database cleanup."""

from __future__ import annotations

import os
from typing import Any

import pytest
from airflow.models.connection import Connection
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.models.log import Log
from airflow.models.taskinstance import TaskInstance
from airflow.models.variable import Variable
from airflow.models.xcom import XComModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import task
from airflow.utils.session import create_session
from sqlalchemy import func, select

from pytest_airflow_in_a_box._compat import db as compat_db
from pytest_airflow_in_a_box.db import DatabaseCleanupError, TableGroup, clear_db
from pytest_airflow_in_a_box.types import DagMaker

pytestmark = pytest.mark.db_test

SERIAL_ONLY = pytest.mark.skipif(
    bool(os.environ.get("PYTEST_XDIST_WORKER")),
    reason="whole-database resets are documented as serial-only",
)


def _count(model: Any) -> int:
    """Count committed rows of one mapped model.

    Parameters:
        model: Any containing a SQLAlchemy mapped class.

    Returns:
        int containing the committed row count.
    """

    with create_session() as session:
        counted = session.scalar(select(func.count()).select_from(model))
    assert isinstance(counted, int)
    return counted


def _insert_rows() -> None:
    """Commit one representative row into several standalone tables."""

    with create_session() as session:
        session.add(Variable(key="clear_db_variable", val="value"))
        session.add(Log(event="clear_db_event"))
        session.add(Connection(conn_id="clear_db_connection", conn_type="sqlite"))


def test_implied_groups_expands_transitively() -> None:
    """Expand implied groups transitively and keep registry delete order."""

    assert compat_db.implied_groups(("runs",)) == (
        "xcom",
        "task_instances",
        "deadlines",
        "runs",
    )
    assert compat_db.implied_groups(("triggers",)) == ("xcom", "task_instances", "triggers")
    assert compat_db.implied_groups(("bundles",)) == (
        "xcom",
        "task_instances",
        "deadlines",
        "runs",
        "serialized_dags",
        "dags",
        "bundles",
    )
    assert compat_db.implied_groups(()) == ()


def test_implied_groups_rejects_unknown_names() -> None:
    """Reject group names absent from the registry."""

    with pytest.raises(ValueError, match="Unknown table groups: `nope`"):
        compat_db.implied_groups(("runs", "nope"))


def test_clear_tables_rejects_unknown_names() -> None:
    """Validate group names before touching the database."""

    with pytest.raises(ValueError, match="Unknown table groups: `nope`"):
        compat_db.clear_tables(("nope",))


def test_clear_tables_wraps_resolution_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wrap registry resolution failures in `DatabaseCleanupError`."""

    monkeypatch.setattr(
        compat_db,
        "_TABLE_REGISTRY",
        (("variables", (("pytest_airflow_in_a_box._missing", "Nope"),)),),
    )

    with pytest.raises(DatabaseCleanupError, match="`variables`") as caught:
        compat_db.clear_tables(("variables",))

    assert isinstance(caught.value.__cause__, ImportError)


def test_clear_db_rejects_non_table_group_entries() -> None:
    """Reject plain strings so typos cannot silently clear nothing."""

    plain_strings: Any = {"variables"}

    with pytest.raises(TypeError, match="must be TableGroup members: 'variables'"):
        clear_db(tables=plain_strings)


def test_clear_db_rejects_empty_selection() -> None:
    """Reject an empty selection instead of silently doing nothing."""

    with pytest.raises(ValueError, match="must not be empty"):
        clear_db(tables=set())


def test_clear_db_scoped_selection_leaves_other_groups() -> None:
    """Clear only the requested group and keep unrelated rows."""

    _insert_rows()

    clear_db(tables={TableGroup.VARIABLES})

    try:
        assert _count(Variable) == 0
        assert _count(Log) >= 1
        with create_session() as session:
            connection_count = session.scalar(
                select(func.count())
                .select_from(Connection)
                .where(Connection.conn_id == "clear_db_connection")
            )
        assert connection_count == 1
    finally:
        clear_db(tables={TableGroup.LOGS, TableGroup.CONNECTIONS})


def test_clear_db_triggers_clears_referencing_task_instances() -> None:
    """Clear trigger rows plus the task instances that reference them."""

    # Deferred so trigger construction happens after bootstrap.
    from airflow.models.trigger import Trigger

    with create_session() as session:
        session.add(Trigger(classpath="tests.fake.Trigger", kwargs={}))
    assert _count(Trigger) >= 1

    clear_db(tables={TableGroup.TRIGGERS})

    assert _count(Trigger) == 0
    assert _count(TaskInstance) == 0


@SERIAL_ONLY
def test_clear_db_runs_expansion_leaves_no_orphans(dag_maker: DagMaker) -> None:
    """Clear task instances and XCom rows when only runs are requested."""

    with dag_maker(dag_id="clear_db_runs"):

        @task
        def emit() -> str:
            """Push one XCom value for orphan detection."""

            return "value"

        emit()
    dag_maker.run_ti("emit")
    assert _count(XComModel) >= 1

    clear_db(tables={TableGroup.RUNS})

    assert _count(DagRun) == 0
    assert _count(TaskInstance) == 0
    assert _count(XComModel) == 0
    assert _count(DagModel) >= 1


@SERIAL_ONLY
def test_clear_db_resets_every_group_and_restores_defaults(dag_maker: DagMaker) -> None:
    """Reset every registered group and recreate default connections."""

    with dag_maker(dag_id="clear_db_everything"):
        EmptyOperator(task_id="noop")
    dag_maker.create_ti("noop")
    _insert_rows()

    clear_db()

    assert _count(DagModel) == 0
    assert _count(DagRun) == 0
    assert _count(TaskInstance) == 0
    assert _count(Variable) == 0
    assert _count(Log) == 0
    with create_session() as session:
        default_connection = session.scalar(
            select(func.count()).select_from(Connection).where(Connection.conn_id == "airflow_db")
        )
    assert default_connection == 1
