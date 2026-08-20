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
    ValidateInletsAndOutlets,
)
from airflow.utils.state import TaskInstanceState

from pytest_airflow_in_a_box._compat.in_process import (
    FakeSupervisorComms,
    _dag_run_payload_extras,
    _fill_declared_nones,
    _task_runner_module,
    render_task_in_process,
    run_task_in_process,
    task_context_in_process,
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


class ExecutionTrackingOperator(BaseOperator):
    """Record whether ``execute`` was ever called."""

    template_fields = ("expression",)

    def __init__(self, *, expression: Any, **kwargs: Any) -> None:
        """Store the templated expression and reset the execution flag.

        Parameters:
            expression: Any containing a possibly templated value.
            kwargs: Any forwarded to ``BaseOperator``.
        """

        super().__init__(**kwargs)
        self.expression = expression
        self.executed = False

    def execute(self, context: Any) -> Any:
        """Record that execution happened and return the expression.

        Parameters:
            context: Any containing the task execution context.

        Returns:
            Any containing the stored expression.
        """

        del context
        self.executed = True
        return self.expression


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


def test_rejects_invalid_try_number() -> None:
    """Require one-based synthetic attempt numbers."""

    operator = _build(ReturnOperator, "in_process_try_number", expression="x")

    with pytest.raises(ValueError, match="`try_number` must be at least 1"):
        run_task_in_process(operator, try_number=0)


def test_render_task_resolves_templated_fields_without_mutating_the_original() -> None:
    """Resolve template fields on a fresh copy, leaving the caller's operator untouched."""

    operator = _build(ReturnOperator, "in_process_render", expression="{{ 21 * 2 }}")

    rendered_operator = render_task_in_process(operator)

    assert rendered_operator is not operator
    assert rendered_operator.expression == "42"
    assert operator.expression == "{{ 21 * 2 }}"


def test_render_task_does_not_contaminate_a_shared_operator_across_calls() -> None:
    """Render the same shared operator twice, each call independent of the other."""

    operator = _build(ReturnOperator, "in_process_render_reuse", expression="{{ custom }}")

    first = render_task_in_process(operator, context_overrides={"custom": "first-value"})
    second = render_task_in_process(operator, context_overrides={"custom": "second-value"})

    assert first.expression == "first-value"
    assert second.expression == "second-value"
    assert operator.expression == "{{ custom }}"


def test_render_task_does_not_execute_the_operator() -> None:
    """Render fields without ever calling ``execute``."""

    operator = _build(ExecutionTrackingOperator, "in_process_render_no_run", expression="x")

    rendered_operator = render_task_in_process(operator)

    assert rendered_operator.executed is False
    assert operator.executed is False


def test_render_task_merges_context_overrides() -> None:
    """Merge caller-supplied context keys into the synthesized context before rendering."""

    operator = _build(ReturnOperator, "in_process_render_overrides", expression="{{ custom }}")

    rendered_operator = render_task_in_process(
        operator, context_overrides={"custom": "override-value"}
    )

    assert rendered_operator.expression == "override-value"


def test_render_task_params_render_like_run_task() -> None:
    """Render declared Dag params overridden by the call's ``params``, without running."""

    declared_params: Any = {"left": "declared"}
    with DAG(dag_id="in_process_render_params", schedule=None, params=declared_params) as dag:
        ReturnOperator(task_id="probe", expression="{{ params.left }}")
    operator: Any = dag.get_task("probe")

    rendered_operator = render_task_in_process(operator, params={"left": "overridden"})

    assert rendered_operator.expression == "overridden"


def test_render_task_variables_and_connections_resolve_through_templates() -> None:
    """Serve seeded Variables and Connections while rendering, same as ``run_task``."""

    operator = _build(
        ReturnOperator,
        "in_process_render_seeded",
        expression="{{ var.value.answer }}-{{ conn.db.host }}",
    )
    comms = FakeSupervisorComms(
        variables={"answer": "42"},
        connections={"db": {"conn_type": "postgres", "host": "example.com"}},
    )

    rendered_operator = render_task_in_process(operator, comms=comms)

    assert rendered_operator.expression == "42-example.com"


def test_render_task_resolves_a_mapped_operator_to_its_concrete_instance() -> None:
    """Render one mapped index onto the concrete unmapped instance Airflow produces."""

    with DAG(dag_id="in_process_render_mapped", schedule=None) as dag:
        ReturnOperator.partial(task_id="probe").expand(expression=["{{ 1 + 1 }}", "{{ 2 + 2 }}"])
    mapped_operator = dag.get_task("probe")

    rendered_operator = render_task_in_process(mapped_operator, map_index=1)

    assert rendered_operator is not mapped_operator
    assert rendered_operator.expression == "4"


def test_render_task_supervisor_comms_deleted_when_previously_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Remove the installed fake when no supervisor was registered before."""

    task_runner = _task_runner_module()
    monkeypatch.delattr(task_runner, "SUPERVISOR_COMMS", raising=False)
    operator = _build(ReturnOperator, "in_process_render_absent", expression="x")

    render_task_in_process(operator)

    assert not hasattr(task_runner, "SUPERVISOR_COMMS")


def test_render_task_supervisor_comms_restored_when_previously_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reinstall the prior supervisor object after rendering."""

    task_runner = _task_runner_module()
    sentinel = object()
    monkeypatch.setattr(task_runner, "SUPERVISOR_COMMS", sentinel, raising=False)
    operator = _build(ReturnOperator, "in_process_render_present", expression="x")

    render_task_in_process(operator)

    assert task_runner.SUPERVISOR_COMMS is sentinel


def test_render_task_rejects_task_without_task_id() -> None:
    """Reject objects that are not operators, same guard as ``run_task_in_process``."""

    with pytest.raises(TypeError, match="must be an Airflow operator"):
        render_task_in_process(object())


def test_task_context_prerenders_and_leaves_the_original_untouched() -> None:
    """Pre-render onto the execution copy by default, never the caller's operator."""

    operator = _build(ReturnOperator, "in_process_context", expression="{{ 21 * 2 }}")

    with task_context_in_process(operator) as handle:
        assert handle.task is not operator
        assert handle.task.expression == "42"

    assert operator.expression == "{{ 21 * 2 }}"


def test_task_context_render_false_leaves_fields_raw() -> None:
    """Skip pre-rendering so ``execute`` can render through ``context["ti"]`` itself."""

    operator = _build(ReturnOperator, "in_process_context_raw", expression="{{ 21 * 2 }}")

    with task_context_in_process(operator, render=False) as handle:
        assert handle.task.expression == "{{ 21 * 2 }}"
        handle.ti.render_templates(handle.context)
        assert handle.task.expression == "42"


def test_task_context_merges_context_overrides() -> None:
    """Merge caller-supplied context keys before rendering."""

    operator = _build(ReturnOperator, "in_process_context_overrides", expression="{{ custom }}")

    with task_context_in_process(operator, context_overrides={"custom": "override"}) as handle:
        assert handle.task.expression == "override"


def test_task_context_tracks_a_mapped_operator_through_its_task_property() -> None:
    """Track Airflow's swap to the concrete unmapped instance through ``handle.task``."""

    with DAG(dag_id="in_process_context_mapped", schedule=None) as dag:
        ReturnOperator.partial(task_id="probe").expand(expression=["{{ 1 + 1 }}", "{{ 2 + 2 }}"])
    mapped_operator = dag.get_task("probe")

    with task_context_in_process(mapped_operator, map_index=1) as handle:
        assert handle.task is not mapped_operator
        assert handle.task.expression == "4"
        assert handle.task is handle.ti.task


def test_task_context_supervisor_comms_deleted_when_previously_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Remove the installed fake when no supervisor was registered before."""

    task_runner = _task_runner_module()
    monkeypatch.delattr(task_runner, "SUPERVISOR_COMMS", raising=False)
    operator = _build(ReturnOperator, "in_process_context_absent", expression="x")

    with task_context_in_process(operator) as handle:
        assert task_runner.SUPERVISOR_COMMS is not None
        del handle

    assert not hasattr(task_runner, "SUPERVISOR_COMMS")


def test_task_context_supervisor_comms_restored_after_an_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reinstall the prior supervisor even when the caller's block raises."""

    task_runner = _task_runner_module()
    sentinel = object()
    monkeypatch.setattr(task_runner, "SUPERVISOR_COMMS", sentinel, raising=False)
    operator = _build(RaiseOperator, "in_process_context_raise")

    with (
        pytest.raises(ValueError, match="representative task failure"),
        task_context_in_process(operator) as handle,
    ):
        handle.task.execute(handle.context)

    assert task_runner.SUPERVISOR_COMMS is sentinel


def test_task_context_rejects_task_without_task_id() -> None:
    """Reject objects that are not operators, same guard as ``run_task_in_process``."""

    with (
        pytest.raises(TypeError, match="must be an Airflow operator"),
        task_context_in_process(object()),
    ):
        pytest.fail("the guard must raise before the block runs")


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


def test_fake_comms_accepts_active_asset_inlets_and_outlets() -> None:
    """Answer Task SDK asset validation with no inactive assets."""

    from uuid6 import uuid7

    result = FakeSupervisorComms().send(ValidateInletsAndOutlets(ti_id=uuid7()))

    assert result.inactive_assets == []


def test_dag_run_payload_extras_track_the_declared_model_fields() -> None:
    """Send explicit None for exactly the newer generated fields the model declares.

    3.3.1 promoted `end_date` and `partition_key` to required-without-default, while
    releases before 3.3.0 lack `partition_key` entirely and the generated models forbid
    extra fields -- both shapes are pinned here with fakes so every branch is reachable
    on any single installed release.
    """

    newer: Any = type(
        "NewerDagRun",
        (),
        {"model_fields": {"end_date": object(), "partition_key": object(), "run_id": object()}},
    )
    older: Any = type("OlderDagRun", (), {"model_fields": {"run_id": object()}})

    assert _dag_run_payload_extras(newer) == {"end_date": None, "partition_key": None}
    assert _dag_run_payload_extras(older) == {}


def test_fill_declared_nones_completes_only_required_fields_by_alias() -> None:
    """Complete required fields with None, keyed by alias, without touching the payload.

    3.3.1 regenerated the comms models with None-able fields required-without-default
    (and `schema_` aliased to `schema`); earlier releases keep defaults, so the
    completion must no-op there. Both shapes are pinned with fakes so every branch is
    reachable on any single installed release.
    """

    class _Field:
        def __init__(self, required: bool, alias: str | None = None) -> None:
            self.alias = alias
            self._required = required

        def is_required(self) -> bool:
            return self._required

    newer: Any = type(
        "NewerResult",
        (),
        {
            "model_fields": {
                "conn_id": _Field(True),
                "schema_": _Field(True, alias="schema"),
                "port": _Field(True),
                "note": _Field(False),
            }
        },
    )
    older: Any = type("OlderResult", (), {"model_fields": {"conn_id": _Field(False)}})

    completed = _fill_declared_nones(newer, {"conn_id": "db", "port": 5432})
    assert completed == {"conn_id": "db", "schema": None, "port": 5432}
    assert _fill_declared_nones(older, {"conn_id": "db"}) == {"conn_id": "db"}
