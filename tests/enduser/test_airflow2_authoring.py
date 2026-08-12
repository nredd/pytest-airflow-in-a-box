"""End-user contract for the certified Airflow 2.x authoring surface.

Mirrors the 3.x consumer contract's shape on the 2.x family: classic operators,
TaskFlow via `airflow.decorators`, XCom flow, and pinned param validation, all driven
through the same `dag_maker`/`run_ti` fixture surface a migrating suite uses on both
sides. Every Airflow import stays inside a test body and resolves dynamically ON
PURPOSE: the module targets 2.x-only import paths that a 3.x environment (including
the type checker's) cannot resolve statically, and the `requires_airflow2` marker
skips each test before its body imports anything.

References:
    https://airflow.apache.org/docs/apache-airflow/2.11.2/tutorial/taskflow.html
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

import pytest

pytestmark = [pytest.mark.compat, pytest.mark.requires_airflow2, pytest.mark.db_test]


def _symbol(module: str, name: str) -> Any:
    """Resolve one 2.x-only symbol dynamically.

    Parameters:
        module: str containing the 2.x module path.
        name: str containing the symbol name.

    Returns:
        Any containing the resolved symbol.
    """

    return getattr(import_module(module), name)


def test_classic_operator_runs_to_success(dag_maker) -> None:
    """Run one classic `PythonOperator` through `dag_maker.run_ti`.

    Parameters:
        dag_maker: pytest_airflow_in_a_box.types.DagMaker building the Dag under test.
    """

    python_operator = _symbol("airflow.operators.python", "PythonOperator")
    from airflow.utils.state import TaskInstanceState

    with dag_maker("airflow2_classic"):
        python_operator(task_id="answer", python_callable=lambda: 42)

    dag_run = dag_maker.create_dagrun()
    ti = dag_maker.run_ti("answer", dag_run)

    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids="answer") == 42


def test_taskflow_xcom_flows_between_tasks(dag_maker) -> None:
    """Pass an XCom between two `airflow.decorators` TaskFlow tasks.

    Parameters:
        dag_maker: pytest_airflow_in_a_box.types.DagMaker building the Dag under test.
    """

    task = _symbol("airflow.decorators", "task")
    from airflow.utils.state import TaskInstanceState

    with dag_maker("airflow2_taskflow"):

        @task
        def produce() -> int:
            return 7

        @task
        def consume(value: int) -> int:
            return value * 6

        consume(produce())

    dag_run = dag_maker.create_dagrun()
    dag_maker.run_ti("produce", dag_run)
    ti = dag_maker.run_ti("consume", dag_run)

    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids="consume") == 42


def test_dynamic_task_mapping_expands_and_runs(dag_maker) -> None:
    """Expand a runtime-length mapped task and run one expanded instance.

    Runtime-length mapping only materializes expanded instances through
    `expand_mapped_task_instances`, so this holds the 2.x class-based mapped-task
    detection that an attribute probe cannot cover (2.x `MappedOperator` has no
    `is_mapped`); a literal-length `expand` would mask it via `verify_integrity`.

    Parameters:
        dag_maker: pytest_airflow_in_a_box.types.DagMaker building the Dag under test.
    """

    task = _symbol("airflow.decorators", "task")
    from airflow.utils.state import TaskInstanceState

    with dag_maker("airflow2_mapping"):

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

    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids="double", map_indexes=2) == 18


def test_execution_date_context_is_populated(dag_maker) -> None:
    """Expose the 2.x logical/execution date through the rendered task context.

    Parameters:
        dag_maker: pytest_airflow_in_a_box.types.DagMaker building the Dag under test.
    """

    python_operator = _symbol("airflow.operators.python", "PythonOperator")
    from airflow.utils.state import TaskInstanceState

    seen: dict[str, object] = {}

    def record(**context: object) -> None:
        seen["execution_date"] = context.get("execution_date")
        seen["ds"] = context.get("ds")

    with dag_maker("airflow2_context"):
        python_operator(task_id="record", python_callable=record)

    dag_run = dag_maker.create_dagrun()
    ti = dag_maker.run_ti("record", dag_run)

    assert ti.state == TaskInstanceState.SUCCESS
    assert seen["execution_date"] is not None
    assert isinstance(seen["ds"], str)
    assert len(seen["ds"]) == 10


def test_run_after_is_rejected_loudly(dag_maker) -> None:
    """Reject the 3.x-only `run_after` kwarg instead of silently changing semantics.

    Parameters:
        dag_maker: pytest_airflow_in_a_box.types.DagMaker building the Dag under test.
    """

    python_operator = _symbol("airflow.operators.python", "PythonOperator")
    from datetime import datetime, timezone

    with dag_maker("airflow2_run_after"):
        python_operator(task_id="noop", python_callable=lambda: None)

    with pytest.raises(ValueError, match=r"no 2\.x equivalent"):
        dag_maker.create_dagrun(run_after=datetime(2021, 5, 5, tzinfo=timezone.utc))


def test_declared_params_reject_a_bad_pinned_case(dag_maker) -> None:
    """Validate pinned params against the 2.x `airflow.models.param` schema.

    Parameters:
        dag_maker: pytest_airflow_in_a_box.types.DagMaker building the Dag under test.
    """

    param = _symbol("airflow.models.param", "Param")
    python_operator = _symbol("airflow.operators.python", "PythonOperator")

    from pytest_airflow_in_a_box._compat.params import ParamsCaseError, validate_dag_params

    with dag_maker(
        "airflow2_params",
        params={"retries": param(1, type="integer", minimum=0)},
    ) as dag:
        python_operator(task_id="noop", python_callable=lambda: None)

    validate_dag_params(dag, {"retries": 3})
    with pytest.raises(ParamsCaseError, match="rejected the pinned params"):
        validate_dag_params(dag, {"retries": "not-a-number"})
    with pytest.raises(ParamsCaseError, match="declares no params"):
        validate_dag_params(dag, {"undeclared": True})
