"""Provide DB-free, xdist-safe in-process task execution.

References:
    https://airflow.apache.org/docs/task-sdk/stable/
    https://docs.pytest.org/en/stable/how-to/fixtures.html
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from pytest_airflow_in_a_box._compat.in_process import (
    DEFAULT_RUN_ID,
    FakeSupervisorComms,
    run_task_in_process,
)

if TYPE_CHECKING:
    from datetime import datetime

    from pytest_airflow_in_a_box.types import RunTask, TaskRunResult


def _run_task(
    task: Any,
    *,
    dag_id: str | None = None,
    run_id: str = DEFAULT_RUN_ID,
    logical_date: datetime | None = None,
    params: dict[str, Any] | None = None,
    xcoms: dict[str, Any] | None = None,
    variables: dict[str, str] | None = None,
    connections: dict[str, dict[str, Any]] | None = None,
    map_index: int = -1,
    try_number: int = 1,
    run_callbacks: bool = False,
) -> TaskRunResult:
    """Execute one operator in process with seeded fake supervisor state.

    Parameters:
        task: Any containing the Airflow operator or bound TaskFlow task.
        dag_id: str | None overriding the Dag identifier, or ``None`` to read
            it from the task's bound Dag.
        run_id: str identifying the synthetic manual run.
        logical_date: datetime | None pinning the run's logical date.
        params: dict[str, Any] | None overriding declared Dag params.
        xcoms: dict[str, Any] | None seeding XCom values by key.
        variables: dict[str, str] | None seeding Variable values by key.
        connections: dict[str, dict[str, Any]] | None seeding connection
            fields by connection id.
        map_index: int selecting the mapped task index.
        try_number: int selecting the synthetic task attempt number.
        run_callbacks: bool dispatching task callbacks and listeners after
            execution.

    Returns:
        TaskRunResult containing terminal state, error, and XCom values.
    """

    comms = FakeSupervisorComms(xcoms=xcoms, variables=variables, connections=connections)
    return run_task_in_process(
        task,
        dag_id=dag_id,
        run_id=run_id,
        logical_date=logical_date,
        params=params,
        comms=comms,
        map_index=map_index,
        try_number=try_number,
        run_callbacks=run_callbacks,
    )


@pytest.fixture
def run_task() -> RunTask:
    """Return a DB-free in-process Task SDK runner.

    Returns:
        RunTask executing one operator per call with isolated seeded state.
    """

    return _run_task


__all__ = ("run_task",)
