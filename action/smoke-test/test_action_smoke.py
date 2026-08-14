"""Dogfood fixture for `action/action.yml`, exercised by the `action-smoke` CI job.

Adapted from the README quickstart, but resolves the TaskFlow decorator dynamically
between `airflow.sdk` (3.x) and `airflow.decorators` (2.x) since the `action-smoke` job
runs this file against both the `airflow3` and `airflow2` extras.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any


def _resolve_task() -> Any:
    """Resolve the TaskFlow decorator for whichever Airflow family is installed.

    Returns:
        Any containing `airflow.sdk.task` (3.x) or `airflow.decorators.task` (2.x).
    """

    try:
        return import_module("airflow.sdk").task
    except ModuleNotFoundError:
        return import_module("airflow.decorators").task


task = _resolve_task()


def test_dag(dag_maker) -> None:
    """Run a two-task Dag end to end through the action-provisioned interpreter.

    Parameters:
        dag_maker: pytest_airflow_in_a_box.types.DagMaker building the Dag under test.
    """

    with dag_maker():

        @task
        def produce() -> int:
            return 21

        @task
        def consume(value: int) -> int:
            return value * 2

        consume(produce())

    result = dag_maker.run()

    assert result.success
    assert result.xcoms == {"produce": 21, "consume": 42}
    assert result.order == ["produce", "consume"]
