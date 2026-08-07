"""Run one task through the Task SDK in process, without a metadata database.

The Task SDK normally executes under a supervisor process connected through
``SUPERVISOR_COMMS``. Substituting an in-memory, protocol-conformant fake for
the supervisor lets ``task_runner.run`` execute real operator code with XCom,
Variable, and Connection traffic answered from seeded dictionaries. The
``parse``/bundle path is never used, so ``bundle_instance`` stays unset;
``finalize`` (callback dispatch) is deliberately out of v1 scope.

References:
    https://airflow.apache.org/docs/task-sdk/stable/
    https://github.com/apache/airflow/blob/main/task-sdk/src/airflow/sdk/execution_time/task_runner.py
    https://github.com/apache/airflow/blob/main/task-sdk/src/airflow/sdk/execution_time/comms.py
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pytest_airflow_in_a_box._compat.capabilities import resolve_capabilities

LOGGER = logging.getLogger(__name__)

DEFAULT_RUN_ID = "in-process-test"


class FakeSupervisorComms:
    """In-memory supervisor endpoint answering Task SDK messages.

    Parameters:
        xcoms: dict[str, Any] | None seeding XCom values by key.
        variables: dict[str, str] | None seeding Variable values by key.
        connections: dict[str, dict[str, Any]] | None seeding connection
            fields by connection id.
    """

    def __init__(
        self,
        *,
        xcoms: dict[str, Any] | None = None,
        variables: dict[str, str] | None = None,
        connections: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.xcoms: dict[str, Any] = dict(xcoms) if xcoms else {}
        self.variables: dict[str, str] = dict(variables) if variables else {}
        self.connections: dict[str, dict[str, Any]] = (
            {conn_id: dict(fields) for conn_id, fields in connections.items()}
            if connections
            else {}
        )
        self.sent: list[Any] = []

    def send(self, msg: Any) -> Any:
        """Record one message and answer it from in-memory state.

        Unseeded Variable and Connection requests are answered with the same
        not-found ``ErrorResponse`` a real supervisor produces, so tasks fail
        exactly as they would against a live deployment; the seeding hint
        travels in the response ``detail``.

        Parameters:
            msg: Any containing one Task SDK supervisor message.

        Returns:
            Any containing the response message, or ``None`` for one-way
            messages and unrecognized message types.
        """

        # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
        from airflow.sdk.execution_time.comms import (
            ConnectionResult,
            ErrorResponse,
            ErrorType,
            VariableResult,
            XComCountResponse,
            XComResult,
            XComSequenceSliceResult,
        )

        self.sent.append(msg)
        name = type(msg).__name__
        if name == "SetXCom":
            self.xcoms[msg.key] = msg.value
            return None
        if name == "GetXCom":
            return XComResult(key=msg.key, value=self.xcoms.get(msg.key))
        if name == "GetXComSequenceSlice":
            present = msg.key in self.xcoms
            return XComSequenceSliceResult(root=[self.xcoms[msg.key]] if present else [])
        if name == "DeleteXCom":
            self.xcoms.pop(msg.key, None)
            return None
        if name == "GetXComCount":
            return XComCountResponse(len=int(msg.key in self.xcoms))
        if name == "GetVariable":
            if msg.key not in self.variables:
                return ErrorResponse(
                    error=ErrorType.VARIABLE_NOT_FOUND,
                    detail={
                        "hint": f"Seed it via `run_task(..., variables={{{msg.key!r}: ...}})`"
                    },
                )
            return VariableResult(key=msg.key, value=self.variables[msg.key])
        if name == "GetConnection":
            if msg.conn_id not in self.connections:
                return ErrorResponse(
                    error=ErrorType.CONNECTION_NOT_FOUND,
                    detail={
                        "hint": (
                            f"Seed it via `run_task(..., connections="
                            f"{{{msg.conn_id!r}: {{'conn_type': ...}}}})`"
                        )
                    },
                )
            fields = self.connections[msg.conn_id]
            return ConnectionResult.model_validate(
                {"conn_type": "generic", **fields, "conn_id": msg.conn_id}
            )
        return None


@dataclass(frozen=True)
class InProcessRunResult:
    """Outcome of one DB-free in-process task execution.

    Parameters:
        state: Any containing the terminal ``TaskInstanceState``.
        msg: Any | None containing the final supervisor message, when produced.
        error: BaseException | None raised by the task, when it failed.
        xcoms: dict[str, Any] containing XCom values after execution.
        sent: tuple[Any, ...] containing every supervisor message in order.
    """

    state: Any
    msg: Any | None
    error: BaseException | None
    xcoms: dict[str, Any]
    sent: tuple[Any, ...]


def _task_runner_module() -> Any:
    """Import the Task SDK runner module without static attribute constraints.

    The module-global ``SUPERVISOR_COMMS`` must be replaced and restored, which
    a statically typed module reference would reject.

    Returns:
        Any containing the ``airflow.sdk.execution_time.task_runner`` module.
    """

    # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
    from airflow.sdk.execution_time import task_runner

    return task_runner


def run_task_in_process(
    task: Any,
    *,
    dag_id: str | None = None,
    run_id: str = DEFAULT_RUN_ID,
    logical_date: datetime | None = None,
    params: dict[str, Any] | None = None,
    comms: FakeSupervisorComms | None = None,
    map_index: int = -1,
) -> InProcessRunResult:
    """Execute one operator through ``task_runner.run`` with fake supervision.

    ``params`` are delivered as the synthetic DagRun's ``conf`` and validated
    against the Dag's declared params exactly like a triggered run.

    Parameters:
        task: Any containing the Airflow operator or bound TaskFlow task.
        dag_id: str | None overriding the Dag identifier, or ``None`` to read
            it from the task's bound Dag.
        run_id: str identifying the synthetic manual run.
        logical_date: datetime | None pinning the run's logical date.
        params: dict[str, Any] | None overriding declared Dag params.
        comms: FakeSupervisorComms | None carrying seeded supervisor state.
        map_index: int selecting the mapped task index.

    Returns:
        InProcessRunResult containing terminal state, error, and XCom values.

    Raises:
        TypeError: The task does not expose a string ``task_id``.
        ValueError: No Dag identifier is available or ``run_id`` is empty.
        AirflowCompatibilityError: The installed Airflow interface is unsupported.
    """

    task_id = getattr(task, "task_id", None)
    if not isinstance(task_id, str) or not task_id:
        raise TypeError("`task` must be an Airflow operator exposing a string `task_id`")
    if not run_id:
        raise ValueError("`run_id` must be a non-empty run identifier")
    try:
        bound_dag = task.dag
    except (AttributeError, RuntimeError):
        # The SDK operator's `dag` property raises when the task is unbound.
        bound_dag = None
    resolved_dag_id = dag_id if dag_id else getattr(bound_dag, "dag_id", None)
    if not isinstance(resolved_dag_id, str) or not resolved_dag_id:
        raise ValueError("`dag_id` is required when the task is not bound to a Dag")

    resolve_capabilities()

    # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
    import structlog
    from airflow.sdk.api.datamodels._generated import DagRun as DagRunDataModel
    from airflow.sdk.api.datamodels._generated import TaskInstance as TaskInstanceDataModel
    from airflow.sdk.api.datamodels._generated import TIRunContext
    from uuid6 import uuid7

    task_runner = _task_runner_module()
    now = datetime.now(timezone.utc)
    run_date = logical_date if logical_date is not None else now
    task_instance = TaskInstanceDataModel(
        id=uuid7(),
        task_id=task_id,
        dag_id=resolved_dag_id,
        run_id=run_id,
        try_number=1,
        dag_version_id=uuid7(),
        map_index=map_index,
    )
    context_from_server = TIRunContext(
        dag_run=DagRunDataModel(
            dag_id=resolved_dag_id,
            run_id=run_id,
            logical_date=run_date,
            run_after=run_date,
            start_date=now,
            run_type="manual",
            state="running",
            conf=dict(params) if params else None,
            consumed_asset_events=[],
        ),
        max_tries=0,
    )
    runtime_ti = task_runner.RuntimeTaskInstance.model_construct(
        **task_instance.model_dump(exclude_unset=True),
        task=task,
        _ti_context_from_server=context_from_server,
        max_tries=0,
        start_date=now,
    )

    active_comms = comms if comms is not None else FakeSupervisorComms()
    absent = object()
    previous_comms = getattr(task_runner, "SUPERVISOR_COMMS", absent)
    task_runner.SUPERVISOR_COMMS = active_comms
    try:
        context = runtime_ti.get_template_context()
        log = structlog.get_logger("pytest_airflow_in_a_box.run_task")
        state, msg, error = task_runner.run(runtime_ti, context, log)
    finally:
        if previous_comms is absent:
            del task_runner.SUPERVISOR_COMMS
        else:
            task_runner.SUPERVISOR_COMMS = previous_comms

    return InProcessRunResult(
        state=state,
        msg=msg,
        error=error,
        xcoms=dict(active_comms.xcoms),
        sent=tuple(active_comms.sent),
    )


__all__ = (
    "DEFAULT_RUN_ID",
    "FakeSupervisorComms",
    "InProcessRunResult",
    "run_task_in_process",
)
