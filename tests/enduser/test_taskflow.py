"""Exercise TaskFlow, mapping, branching, and task groups."""

from __future__ import annotations

from typing import Any

import pytest
from airflow.sdk import DAG, task
from airflow.utils.state import DagRunState, TaskInstanceState

from pytest_airflow_in_a_box.types import DagMaker, RunTask

pytestmark = pytest.mark.compat


@pytest.mark.db_test
def test_multiple_outputs_persist_per_key_xcoms(dag_maker: DagMaker) -> None:
    """Persist each mapping key and the complete TaskFlow return value."""

    with dag_maker(dag_id="compat_multiple_outputs"):

        @task(multiple_outputs=True)
        def split() -> dict[str, int]:
            return {"left": 20, "right": 22}

        split()

    ti = dag_maker.run_ti("split")

    assert ti.xcom_pull(task_ids="split", key="left", session=dag_maker.session) == 20
    assert ti.xcom_pull(task_ids="split", key="right", session=dag_maker.session) == 22


def test_multiple_outputs_work_without_metadata(run_task: RunTask) -> None:
    """Emit per-key XComs through the DB-free Task SDK runner."""

    with DAG(dag_id="compat_multiple_outputs_free", schedule=None) as dag:

        @task(multiple_outputs=True)
        def split() -> dict[str, int]:
            return {"left": 20, "right": 22}

        split()

    result = run_task(dag.get_task("split"))

    assert result.xcoms["left"] == 20
    assert result.xcoms["right"] == 22


@pytest.mark.db_test
def test_branch_task_skips_the_unselected_path(dag_maker: DagMaker) -> None:
    """Apply TaskFlow branch skip state through persisted execution."""

    with dag_maker(dag_id="compat_branch"):

        @task.branch
        def choose() -> str:
            return "chosen"

        @task
        def chosen() -> None:
            pass

        @task
        def rejected() -> None:
            pass

        choose() >> [chosen(), rejected()]

    dag_run = dag_maker.create_dagrun()
    dag_maker.run_ti("choose", dag_run)

    assert dag_maker.create_ti("chosen", dag_run).state is None
    assert dag_maker.create_ti("rejected", dag_run).state == TaskInstanceState.SKIPPED


@pytest.mark.db_test
def test_literal_dynamic_mapping_selects_map_index(dag_maker: DagMaker) -> None:
    """Expand a literal and execute one requested mapped instance."""

    with dag_maker(dag_id="compat_literal_mapping"):

        @task
        def double(value: int) -> int:
            return value * 2

        double.expand(value=[10, 20, 30])

    dag_run = dag_maker.create_dagrun()
    ti = dag_maker.run_ti("double", dag_run, map_index=1)

    assert ti.xcom_pull(task_ids="double", map_indexes=1, session=dag_maker.session) == 40


@pytest.mark.db_test
def test_xcom_dynamic_mapping_selects_map_index(dag_maker: DagMaker) -> None:
    """Expand over an upstream XCom after its producer succeeds."""

    with dag_maker(dag_id="compat_xcom_mapping"):

        @task
        def produce() -> list[int]:
            return [7, 8, 9]

        @task
        def double(value: int) -> int:
            return value * 2

        double.expand(value=produce())

    dag_run = dag_maker.create_dagrun()
    dag_maker.run_ti("produce", dag_run)
    ti = dag_maker.run_ti("double", dag_run, map_index=2)

    assert ti.xcom_pull(task_ids="double", map_indexes=2, session=dag_maker.session) == 18


@pytest.mark.db_test
def test_nested_task_group_ids_survive_persistence(dag_maker: DagMaker) -> None:
    """Address nested group tasks by their fully qualified identifiers."""

    from airflow.sdk import TaskGroup

    with dag_maker(dag_id="compat_task_groups"), TaskGroup("outer"), TaskGroup("inner"):

        @task
        def grouped() -> int:
            return 42

        grouped()

    ti = dag_maker.run_ti("outer.inner.grouped")

    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids="outer.inner.grouped", session=dag_maker.session) == 42


@pytest.mark.db_test
def test_complete_taskflow_dag_reaches_success(dag_maker: DagMaker) -> None:
    """Execute all task instances through `dag_maker.run` and finish the DagRun."""

    with dag_maker(dag_id="compat_complete_dag"):

        @task
        def produce() -> int:
            return 21

        @task
        def consume(value: int) -> int:
            return value * 2

        produced: Any = produce()
        consume(produced)

    result = dag_maker.run()

    assert result.success
    assert result.dag_run.state == DagRunState.SUCCESS
    assert result.xcoms == {"produce": 21, "consume": 42}
