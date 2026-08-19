"""Test static conformance checks for custom timetables, listeners, and executors."""

from __future__ import annotations

from typing import Any, ClassVar

import pytest
from airflow.executors.base_executor import BaseExecutor
from airflow.listeners import hookimpl
from airflow.timetables.base import DataInterval, Timetable

from pytest_airflow_in_a_box.components import (
    ComponentContractError,
    ComponentKind,
    ComponentProblem,
    ComponentReport,
    check_component,
)


class _CleanTimetable(Timetable):
    """Conformant custom timetable: every check should pass."""

    def infer_manual_data_interval(self, *, run_after: Any) -> DataInterval:
        return DataInterval.exact(run_after)

    def next_dagrun_info(self, *, last_automated_data_interval: Any, restriction: Any) -> None:
        del last_automated_data_interval, restriction
        return

    def serialize(self) -> dict[str, Any]:
        return {"marker": "clean"}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> _CleanTimetable:
        del data
        return cls()


def _make_local_timetable() -> type:
    """Build a timetable class whose `__qualname__` contains `<locals>`."""

    class _LocalTimetable(Timetable):
        """Defined inside a function on purpose, for `timetable-local-qualname`."""

        def serialize(self) -> dict[str, Any]:
            return {"unpaired": True}

    return _LocalTimetable


class _NotJsonTimetable(Timetable):
    """Serializes to a value `json.dumps` rejects."""

    def infer_manual_data_interval(self, *, run_after: Any) -> DataInterval:
        return DataInterval.exact(run_after)

    def next_dagrun_info(self, *, last_automated_data_interval: Any, restriction: Any) -> None:
        del last_automated_data_interval, restriction
        return

    def serialize(self) -> dict[str, Any]:
        return {"bad": object()}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> _NotJsonTimetable:
        del data
        return cls()


class _CleanListener:
    """Conformant custom listener: every check should pass."""

    calls: ClassVar[list[str]] = []

    @hookimpl
    def on_task_instance_success(self, previous_state: Any, task_instance: Any) -> None:
        del previous_state, task_instance


class _BrokenListener:
    """Listener exercising every listener check at once."""

    @hookimpl
    def on_totally_made_up_event(self, x: Any) -> None:
        del x

    @hookimpl
    def on_task_instance_success(
        self, previous_state: Any, task_instance: Any, bogus: Any
    ) -> None:
        del previous_state, task_instance, bogus

    @hookimpl
    def on_dag_run_success(self, dag_run: Any, msg: Any) -> None:
        del dag_run, msg


class _CleanExecutor(BaseExecutor):
    """Conformant custom executor: every check should pass."""

    def sync(self) -> None:
        pass

    def _process_workloads(self, workload_items: Any) -> None:
        del workload_items


class _BrokenExecutor(BaseExecutor):
    """Executor exercising every executor check at once."""

    is_single_threaded = True
    # Wrong on every certified contract, not just one: on 3.1 `sentry_integration` is
    # simply the wrong name (3.1 wants `supports_sentry`); on 3.2+ it is the right name
    # holding the wrong type (an int, not `str`).
    sentry_integration = 123


def test_check_component_reports_no_problems_for_a_clean_timetable() -> None:
    """Accept a fully conformant module-level timetable."""

    report = check_component(_CleanTimetable)

    assert report.ok is True
    assert report.problems == ()
    assert report.summary() == "`_CleanTimetable`: no problems found."


def test_check_component_flags_local_qualname_and_missing_methods() -> None:
    """Flag a locally defined timetable missing required overrides and its pair."""

    local_timetable = _make_local_timetable()

    report = check_component(local_timetable)

    codes = {problem.code for problem in report.problems}
    assert codes == {
        "timetable-local-qualname",
        "timetable-missing-protocol-method",
        "timetable-serialize-pair-incomplete",
    }
    assert report.ok is False


def test_check_component_flags_non_json_serialize_only_on_an_instance() -> None:
    """Call `serialize()` only when given an instance, never a bare class."""

    class_report = check_component(_NotJsonTimetable)
    instance_report = check_component(_NotJsonTimetable())

    assert "timetable-serialize-not-json" not in {p.code for p in class_report.problems}
    assert [p.code for p in instance_report.problems] == ["timetable-serialize-not-json"]


def test_check_component_reports_no_problems_for_a_clean_listener() -> None:
    """Accept a listener whose one hookimpl matches a real hookspec exactly."""

    report = check_component(_CleanListener)

    assert report.ok is True


def test_check_component_flags_every_listener_problem() -> None:
    """Flag an unmatched hookspec, an unknown argument, and (on 3.2+) a core-only hook.

    `on_dag_run_success` has no hookspec in the SDK listener manager only where that
    manager exists at all (3.2+); on 3.1.x, which has one manager registering every
    hookspec, it is not a real problem, so `listener-core-manager-only` is conditioned
    on `AirflowCapabilities.sdk_listener_manager_available` here.
    """

    from pytest_airflow_in_a_box._compat import resolve_capabilities

    report = check_component(_BrokenListener)

    codes = {problem.code for problem in report.problems}
    expected = {"listener-no-matching-hookspec", "listener-unknown-argument"}
    if resolve_capabilities().sdk_listener_manager_available:
        expected.add("listener-core-manager-only")
    assert codes == expected


def test_check_component_reports_no_problems_for_a_clean_executor() -> None:
    """Accept an executor overriding both required methods with no stale attributes."""

    report = check_component(_CleanExecutor)

    assert report.ok is True


def test_check_component_flags_every_executor_problem() -> None:
    """Flag missing overrides, a stale attribute, and a wrong-contract sentry flag."""

    report = check_component(_BrokenExecutor)

    codes = {problem.code for problem in report.problems}
    assert codes == {
        "executor-missing-override",
        "executor-stale-attribute",
        "executor-flag-wrong-type",
    }
    assert report.ok is False


def test_check_component_classifies_by_kind_automatically() -> None:
    """Auto-detect a component's kind when none is given explicitly."""

    assert check_component(_CleanTimetable).ok is True
    assert check_component(_CleanListener).ok is True
    assert check_component(_CleanExecutor).ok is True


def test_check_component_forced_kind_overrides_classification() -> None:
    """Run a kind's checks even when the component would not classify as it."""

    class _NotAnExecutor:
        """Plain class that does not subclass `BaseExecutor` at all."""

    report = check_component(_NotAnExecutor(), kind=ComponentKind.EXECUTOR)

    assert report.ok is False
    assert {problem.code for problem in report.problems} == {"executor-missing-override"}


def test_check_component_returns_a_clean_report_for_an_unrecognized_component() -> None:
    """Return an empty, clean report rather than raising for an unmatched component."""

    report = check_component(object())

    assert report.ok is True
    assert report.problems == ()


def test_raise_for_problems_raises_component_contract_error() -> None:
    """Raise `ComponentContractError` carrying the rendered summary."""

    report = check_component(_BrokenExecutor)

    with pytest.raises(ComponentContractError) as caught:
        report.raise_for_problems()

    assert str(caught.value) == report.summary()
    assert "executor-missing-override" in str(caught.value)


def test_raise_for_problems_is_a_no_op_when_ok() -> None:
    """Do nothing when the report found no problems."""

    check_component(_CleanExecutor).raise_for_problems()


def test_component_report_summary_lists_every_problem() -> None:
    """Render one line per problem, each carrying its code and message."""

    report = ComponentReport(
        component_name="Fake",
        problems=(ComponentProblem(code="fake-code", message="fake message", hint="fake hint"),),
    )

    summary = report.summary()

    assert "`Fake`: 1 problem(s) found:" in summary
    assert "[fake-code] fake message" in summary


def test_component_kind_values_are_stable_strings() -> None:
    """Keep `ComponentKind` values as the plain strings the private registry keys by."""

    assert ComponentKind.TIMETABLE.value == "timetable"
    assert ComponentKind.LISTENER.value == "listener"
    assert ComponentKind.EXECUTOR.value == "executor"
