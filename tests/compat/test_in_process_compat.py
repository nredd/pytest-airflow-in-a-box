"""Test DB-free in-process task execution against the installed release.

References:
    https://airflow.apache.org/docs/task-sdk/stable/
"""

from __future__ import annotations

from typing import Any

import pytest
from airflow.sdk import DAG, BaseOperator
from airflow.sdk.execution_time.comms import (
    DeleteXCom,
    ErrorResponse,
    ErrorType,
    GetConnection,
    GetVariable,
    GetXCom,
    GetXComCount,
    GetXComSequenceSlice,
    SetXCom,
    TaskState,
)
from airflow.utils.state import TaskInstanceState

from pytest_airflow_in_a_box._compat.in_process import (
    FakeSupervisorComms,
    _task_runner_module,
    run_task_in_process,
)

try:
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:
    from airflow.exceptions import AirflowSkipException


class ReturnOperator(BaseOperator):
    """Return a templated expression from ``execute``."""

    template_fields = ("expression",)

    def __init__(self, *, expression: Any, **kwargs: Any) -> None:
        """Store the templated expression.

        Parameters:
            expression: Any containing a possibly templated return value.
            kwargs: Any forwarded to ``BaseOperator``.
        """

        super().__init__(**kwargs)
        self.expression = expression

    def execute(self, context: Any) -> Any:
        """Return the rendered expression.

        Parameters:
            context: Any containing the task execution context.

        Returns:
            Any containing the rendered expression.
        """

        del context
        return self.expression


class RaiseOperator(BaseOperator):
    """Raise a representative task failure."""

    def execute(self, context: Any) -> None:
        """Raise the representative failure.

        Parameters:
            context: Any containing the task execution context.

        Raises:
            ValueError: Always, as the representative failure.
        """

        del context
        raise ValueError("representative task failure")


class SkipOperator(BaseOperator):
    """Skip itself through the SDK skip exception."""

    def execute(self, context: Any) -> None:
        """Raise the SDK skip exception.

        Parameters:
            context: Any containing the task execution context.

        Raises:
            AirflowSkipException: Always, to request the skipped state.
        """

        del context
        raise AirflowSkipException("skipped on purpose")


class XComRoundTripOperator(BaseOperator):
    """Push one manual XCom and pull it back through the fake supervisor."""

    def execute(self, context: Any) -> Any:
        """Round-trip one XCom value.

        Parameters:
            context: Any containing the task execution context.

        Returns:
            Any containing the pulled XCom value.
        """

        ti = context["ti"]
        ti.xcom_push(key="manual", value={"a": 1})
        return ti.xcom_pull(task_ids=ti.task_id, key="manual")


def _build(operator_type: type[BaseOperator], dag_id: str, **kwargs: Any) -> Any:
    """Create one operator bound to a fresh SDK Dag.

    Parameters:
        operator_type: type[BaseOperator] to instantiate.
        dag_id: str identifying the fresh Dag.
        kwargs: Any forwarded to the operator constructor.

    Returns:
        Any containing the operator bound to the new Dag.
    """

    with DAG(dag_id=dag_id, schedule=None) as dag:
        operator_type(task_id="probe", **kwargs)
    return dag.get_task("probe")


def test_successful_execution_captures_return_xcom() -> None:
    """Run authored operator code to SUCCESS and record its return value."""

    operator = _build(ReturnOperator, "in_process_success", expression="plain")

    result = run_task_in_process(operator)

    assert result.state == TaskInstanceState.SUCCESS
    assert result.error is None
    assert result.xcoms["return_value"] == "plain"
    assert type(result.msg).__name__ == "SucceedTask"


def test_failure_preserves_the_task_exception() -> None:
    """Return FAILED with the original exception object."""

    operator = _build(RaiseOperator, "in_process_failure")

    result = run_task_in_process(operator)

    assert result.state == TaskInstanceState.FAILED
    assert isinstance(result.error, ValueError)
    assert str(result.error) == "representative task failure"


def test_skip_exception_produces_skipped_state() -> None:
    """Translate the SDK skip exception into the SKIPPED state."""

    operator = _build(SkipOperator, "in_process_skip")

    result = run_task_in_process(operator)

    assert result.state == TaskInstanceState.SKIPPED
    assert result.error is None


def test_params_render_into_templated_fields() -> None:
    """Render declared Dag params overridden by the call's ``params``."""

    declared_params: Any = {"left": "declared"}
    with DAG(dag_id="in_process_params", schedule=None, params=declared_params) as dag:
        ReturnOperator(task_id="probe", expression="{{ params.left }}")
    operator: Any = dag.get_task("probe")

    result = run_task_in_process(operator, params={"left": "overridden"})

    assert result.state == TaskInstanceState.SUCCESS
    assert result.xcoms["return_value"] == "overridden"


def test_xcom_push_pull_round_trip() -> None:
    """Answer pushes and pulls from the same in-memory store."""

    operator = _build(XComRoundTripOperator, "in_process_xcom")

    result = run_task_in_process(operator)

    assert result.state == TaskInstanceState.SUCCESS
    assert result.xcoms["manual"] == {"a": 1}
    assert result.xcoms["return_value"] == {"a": 1}


def test_supervisor_comms_deleted_when_previously_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Remove the installed fake when no supervisor was registered before."""

    task_runner = _task_runner_module()
    monkeypatch.delattr(task_runner, "SUPERVISOR_COMMS", raising=False)
    operator = _build(ReturnOperator, "in_process_absent", expression="x")

    run_task_in_process(operator)

    assert not hasattr(task_runner, "SUPERVISOR_COMMS")


def test_supervisor_comms_restored_when_previously_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reinstall the prior supervisor object after execution."""

    task_runner = _task_runner_module()
    sentinel = object()
    monkeypatch.setattr(task_runner, "SUPERVISOR_COMMS", sentinel, raising=False)
    operator = _build(ReturnOperator, "in_process_present", expression="x")

    run_task_in_process(operator)

    assert task_runner.SUPERVISOR_COMMS is sentinel


def test_rejects_task_without_task_id() -> None:
    """Reject objects that are not operators before importing the runner."""

    with pytest.raises(TypeError, match="must be an Airflow operator"):
        run_task_in_process(object())


def test_requires_dag_id_for_unbound_task() -> None:
    """Require an explicit Dag identifier for a task without a bound Dag."""

    operator = ReturnOperator(task_id="floating", expression="x")

    with pytest.raises(ValueError, match="`dag_id` is required"):
        run_task_in_process(operator)


def test_rejects_empty_run_id() -> None:
    """Reject an empty run identifier."""

    operator = _build(ReturnOperator, "in_process_run_id", expression="x")

    with pytest.raises(ValueError, match="`run_id` must be a non-empty run identifier"):
        run_task_in_process(operator, run_id="")


def test_fake_comms_answers_xcom_traffic() -> None:
    """Dispatch every XCom message type against the in-memory store."""

    comms = FakeSupervisorComms(xcoms={"seeded": 7})
    base: dict[str, Any] = {"dag_id": "d", "run_id": "r", "task_id": "t"}

    assert comms.send(SetXCom(**base, key="new", value=1)) is None
    assert comms.xcoms["new"] == 1
    assert comms.send(GetXCom(**base, key="seeded")).value == 7
    assert comms.send(GetXCom(**base, key="absent")).value is None
    slice_args: dict[str, Any] = {**base, "start": None, "stop": None, "step": None}
    assert comms.send(GetXComSequenceSlice(**slice_args, key="seeded")).root == [7]
    assert comms.send(GetXComSequenceSlice(**slice_args, key="absent")).root == []
    assert comms.send(GetXComCount(**base, key="seeded")).len == 1
    assert comms.send(GetXComCount(**base, key="absent")).len == 0
    assert comms.send(DeleteXCom(**base, key="seeded")) is None
    assert "seeded" not in comms.xcoms
    assert comms.send(DeleteXCom(**base, key="absent")) is None
    assert [type(msg).__name__ for msg in comms.sent] == [
        "SetXCom",
        "GetXCom",
        "GetXCom",
        "GetXComSequenceSlice",
        "GetXComSequenceSlice",
        "GetXComCount",
        "GetXComCount",
        "DeleteXCom",
        "DeleteXCom",
    ]


def test_fake_comms_answers_variables_and_connections() -> None:
    """Serve seeded Variables and Connections and guide unseeded requests."""

    comms = FakeSupervisorComms(
        variables={"answer": "42"},
        connections={"db": {"conn_type": "postgres", "host": "example.com"}},
    )

    assert comms.send(GetVariable(key="answer")).value == "42"
    connection = comms.send(GetConnection(conn_id="db"))
    assert connection.conn_type == "postgres"
    assert connection.host == "example.com"

    missing_variable = comms.send(GetVariable(key="missing"))
    assert isinstance(missing_variable, ErrorResponse)
    assert missing_variable.error is ErrorType.VARIABLE_NOT_FOUND
    assert missing_variable.detail is not None
    assert "variables={'missing': ...}" in missing_variable.detail["hint"]

    missing_connection = comms.send(GetConnection(conn_id="missing"))
    assert isinstance(missing_connection, ErrorResponse)
    assert missing_connection.error is ErrorType.CONNECTION_NOT_FOUND
    assert missing_connection.detail is not None
    assert "connections=" in missing_connection.detail["hint"]


def test_fake_comms_records_unrecognized_messages() -> None:
    """Record one-way messages it does not answer."""

    # Deferred import cost is irrelevant here; the SDK enum differs from
    # `airflow.utils.state` and `TaskState` validates against the SDK one.
    from airflow.sdk.api.datamodels._generated import TaskInstanceState as SdkTaskInstanceState

    comms = FakeSupervisorComms()

    assert comms.send(TaskState(state=SdkTaskInstanceState.FAILED)) is None
    assert [type(msg).__name__ for msg in comms.sent] == ["TaskState"]
