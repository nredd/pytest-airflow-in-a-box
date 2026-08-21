"""Provide DB-free, xdist-safe in-process task execution.

References:
    https://airflow.apache.org/docs/task-sdk/stable/
    https://docs.pytest.org/en/stable/how-to/fixtures.html
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from pytest_airflow_in_a_box._compat.capabilities import require_v3
from pytest_airflow_in_a_box._compat.in_process import (
    DEFAULT_RUN_ID,
    FakeSupervisorComms,
    bound_dag_or_none,
    run_task_in_process,
)
from pytest_airflow_in_a_box.fixtures.dag import _default_dag_id

if TYPE_CHECKING:
    from datetime import datetime

    from pytest_airflow_in_a_box.types import RunTask, TaskRunResult


def _run_task(
    task: Any,
    *,
    dag_id: str | None = None,
    fileloc: str | None = None,
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
        dag_id: str | None overriding the Dag identifier and naming the
            synthetic Dag auto-bound in place to an unbound task, or ``None``
            to read it from the task's bound Dag.
        fileloc: str | None naming the consumer test module, stamped on the
            synthetic Dag so ``template_ext`` files resolve next to the test,
            or ``None`` to keep Airflow's own default.
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
        fileloc=fileloc,
        run_id=run_id,
        logical_date=logical_date,
        params=params,
        comms=comms,
        map_index=map_index,
        try_number=try_number,
        run_callbacks=run_callbacks,
    )


@pytest.fixture
def run_task(request: pytest.FixtureRequest) -> RunTask:
    """Return a DB-free in-process Task SDK runner.

    A task bound to no Dag is bound in place to a synthetic Dag whose
    identifier is either the call's explicit ``dag_id`` or a deterministic,
    bounded, xdist-safe identifier derived from the test's nodeid, the xdist
    worker, and a per-fixture invocation counter -- the same derivation
    `dag_maker` uses for its default Dag ids, salted so the two never collide.

    Parameters:
        request: pytest.FixtureRequest identifying the requesting test.

    Returns:
        RunTask executing one operator per call with isolated seeded state.
    """

    require_v3(
        "run_task",
        "it drives the Task SDK in-process runner, which 2.x predates. Use "
        "`dag_maker.run_ti` or `run_task_instance` for DB-backed execution instead.",
    )

    nodeid = request.node.nodeid
    fileloc = str(Path(str(request.node.path)).resolve())
    worker = os.environ.get("PYTEST_XDIST_WORKER", "master")
    invocations = 0

    def _fixture_run_task(
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
        """Execute one operator in process, deriving a Dag id when none exists.

        Parameters:
            task: Any containing the Airflow operator or bound TaskFlow task.
            dag_id: str | None overriding the Dag identifier and naming the
                synthetic Dag auto-bound in place to an unbound task, or
                ``None`` to read it from the task's bound Dag -- deriving a
                deterministic per-test identifier when there is none.
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

        nonlocal invocations
        resolved_dag_id = dag_id
        if resolved_dag_id is None and bound_dag_or_none(task) is None:
            invocations += 1
            resolved_dag_id = _default_dag_id(f"{nodeid}::run_task", worker, invocations)
        return _run_task(
            task,
            dag_id=resolved_dag_id,
            fileloc=fileloc,
            run_id=run_id,
            logical_date=logical_date,
            params=params,
            xcoms=xcoms,
            variables=variables,
            connections=connections,
            map_index=map_index,
            try_number=try_number,
            run_callbacks=run_callbacks,
        )

    return _fixture_run_task


__all__ = ("run_task",)
