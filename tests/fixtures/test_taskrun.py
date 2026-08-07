"""Test persisted DagRun and task-instance execution through ``DagMaker``.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html
    https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html
"""

from __future__ import annotations

from typing import Any

import pytest
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import XComModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import task
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState
from sqlalchemy import func, select

from pytest_airflow_in_a_box.fixtures.dag import RUN_ID_MAX_LENGTH
from pytest_airflow_in_a_box.taskinstance import ordered_task_instances, run_task_instance
from pytest_airflow_in_a_box.types import DagMaker

try:
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:
    from airflow.exceptions import AirflowSkipException

pytestmark = pytest.mark.db_test


def test_taskflow_task_runs_to_success_and_pushes_xcom(dag_maker: DagMaker) -> None:
    """Execute authored TaskFlow code and persist its return value."""

    with dag_maker(dag_id="taskflow_success"):

        @task
        def answer() -> int:
            """Return a deterministic XCom value."""

            return 42

        answer()

    ti = dag_maker.run_ti("answer")

    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids="answer", session=dag_maker.session) == 42


def test_failure_propagates_and_refreshes_original_ti(dag_maker: DagMaker) -> None:
    """Retain the task exception while exposing persisted failure state."""

    with dag_maker(dag_id="taskflow_failure"):

        @task
        def fail() -> None:
            """Raise the representative task failure."""

            raise ValueError("task body failed")

        fail()

    ti = dag_maker.create_ti("fail")
    with pytest.raises(ValueError, match="task body failed"):
        run_task_instance(
            ti,
            dag_maker.dag.get_task("fail"),
            session=dag_maker.session,
        )

    assert ti.state == TaskInstanceState.FAILED


def test_skip_exception_refreshes_skipped_state(dag_maker: DagMaker) -> None:
    """Translate ``AirflowSkipException`` into persisted skipped state."""

    with dag_maker(dag_id="taskflow_skip"):

        @task
        def skip() -> None:
            """Skip the representative task."""

            raise AirflowSkipException("not applicable")

        skip()

    ti = dag_maker.run_ti("skip")

    assert ti.state == TaskInstanceState.SKIPPED


def test_mark_success_does_not_execute_task_body(dag_maker: DagMaker) -> None:
    """Mark success while leaving observable task code untouched."""

    executions: list[str] = []
    with dag_maker(dag_id="taskflow_mark_success"):

        @task
        def record_execution() -> None:
            """Record task body execution in process memory."""

            executions.append("executed")

        record_execution()

    ti = dag_maker.run_ti("record_execution", mark_success=True)

    assert executions == []
    assert ti.state == TaskInstanceState.SUCCESS


def test_ordered_task_instances_uses_topological_order(dag_maker: DagMaker) -> None:
    """Order reverse-alphabetical tasks by graph dependencies."""

    with dag_maker(dag_id="topological_order") as dag:
        z_upstream = EmptyOperator(task_id="z_upstream")
        a_downstream = EmptyOperator(task_id="a_downstream")
        z_upstream >> a_downstream

    dag_run = dag_maker.create_dagrun()

    assert [
        ti.task_id for ti in ordered_task_instances(dag_run, dag, session=dag_maker.session)
    ] == ["z_upstream", "a_downstream"]


def test_default_run_ids_are_independent_and_bounded(dag_maker: DagMaker) -> None:
    """Create distinct deterministic bounded defaults with UTC dates and current linkage."""

    with dag_maker(dag_id="default_run_ids"):
        EmptyOperator(task_id="empty")

    first: Any = dag_maker.create_dagrun()
    second: Any = dag_maker.create_dagrun()

    assert first.run_id != second.run_id
    assert len(first.run_id) <= RUN_ID_MAX_LENGTH
    assert len(second.run_id) <= RUN_ID_MAX_LENGTH
    assert first.logical_date.tzinfo is not None
    assert first.run_after.tzinfo is not None
    assert first.start_date is not None
    assert first.start_date.tzinfo is not None
    assert first.created_dag_version_id is not None


def test_create_ti_selection_and_errors(dag_maker: DagMaker) -> None:
    """Select exact task instances and report invalid task, map, and ownership inputs."""

    with dag_maker(dag_id="create_ti_selection"):
        EmptyOperator(task_id="available")

    dag_run = dag_maker.create_dagrun()
    ti = dag_maker.create_ti("available", dag_run)

    assert ti.task_id == "available"
    assert ti.task is dag_maker.dag.get_task("available")
    with pytest.raises(ValueError, match="available task IDs"):
        dag_maker.create_ti("missing", dag_run)
    with pytest.raises(ValueError, match="available instances"):
        dag_maker.create_ti("available", dag_run, map_index=3)
    with pytest.raises(ValueError, match="cannot be supplied"):
        dag_maker.create_ti("available", dag_run, dag_run_kwargs={})

    external: Any = DagRun(dag_id=dag_maker.dag.dag_id, run_id="external")
    external.id = -1
    with pytest.raises(ValueError, match="not owned"):
        dag_maker.create_ti("available", external)


def test_dagrun_ti_and_xcom_cleanup_after_finalization(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Delete executed run metadata and XComs before persisted Dag metadata."""

    pytester.makepyfile(
        """
        import pytest
        from airflow.models.dagrun import DagRun
        from airflow.models.taskinstance import TaskInstance
        from airflow.models.xcom import XComModel
        from airflow.sdk import task
        from airflow.utils.session import create_session
        from sqlalchemy import func, select

        pytestmark = pytest.mark.db_test
        DAG_ID = "pytester_taskrun_cleanup"


        def test_create(dag_maker):
            with dag_maker(dag_id=DAG_ID):
                @task
                def result():
                    return "owned"

                result()
            assert dag_maker.run_ti("result").xcom_pull(
                task_ids="result", session=dag_maker.session
            ) == "owned"


        def test_cleaned():
            with create_session() as session:
                assert session.scalar(
                    select(func.count()).select_from(DagRun).where(DagRun.dag_id == DAG_ID)
                ) == 0
                assert session.scalar(
                    select(func.count()).select_from(TaskInstance).where(
                        TaskInstance.dag_id == DAG_ID
                    )
                ) == 0
                assert session.scalar(
                    select(func.count()).select_from(XComModel).where(XComModel.dag_id == DAG_ID)
                ) == 0
        """
    )

    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    result = pytester.runpytest_subprocess("-p", "pytest_airflow_in_a_box.plugin", "-q")

    result.assert_outcomes(passed=2)


def test_owned_metadata_is_visible_before_finalization(dag_maker: DagMaker) -> None:
    """Prove the fixture retains exact run, task-instance, and XCom rows while active."""

    with dag_maker(dag_id="owned_metadata_visible"):

        @task
        def result() -> str:
            """Return one persisted XCom value."""

            return "visible"

        result()
    dag_maker.run_ti("result")

    with create_session() as session:
        assert (
            session.scalar(
                select(func.count())
                .select_from(DagRun)
                .where(DagRun.dag_id == "owned_metadata_visible")
            )
            == 1
        )
        assert (
            session.scalar(
                select(func.count())
                .select_from(TaskInstance)
                .where(TaskInstance.dag_id == "owned_metadata_visible")
            )
            == 1
        )
        assert (
            session.scalar(
                select(func.count())
                .select_from(XComModel)
                .where(XComModel.dag_id == "owned_metadata_visible")
            )
            == 1
        )
