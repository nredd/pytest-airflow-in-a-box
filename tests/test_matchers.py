"""Test bulk-outcome matchers without Airflow."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box.matchers import (
    ANY,
    TaskOutcome,
    deferred,
    failed,
    not_run,
    skipped,
    succeeded,
    upstream_failed,
)


class _State:
    """Mimic an Airflow state enum exposing ``value``."""

    def __init__(self, value: str) -> None:
        """Store the display value.

        Parameters:
            value: str containing the state value.
        """

        self.value = value


def _snapshot(
    state: Any = "success",
    xcom: Any = None,
    error: BaseException | None = None,
) -> SimpleNamespace:
    """Build one task-result-shaped object for matcher comparison.

    Parameters:
        state: Any containing the settled state.
        xcom: Any containing the pulled ``return_value`` XCom.
        error: BaseException | None captured from the task body.

    Returns:
        types.SimpleNamespace exposing ``state``, ``xcom``, and ``error``.
    """

    return SimpleNamespace(state=state, xcom=xcom, error=error)


def test_succeeded_matches_state_and_optional_xcom() -> None:
    """Match on state alone by default and on the xcom when supplied."""

    assert succeeded() == _snapshot(xcom=42)
    assert succeeded(42) == _snapshot(xcom=42)
    assert succeeded(42) != _snapshot(xcom=41)
    assert succeeded() != _snapshot(state="failed")


def test_succeeded_unwraps_enum_valued_states() -> None:
    """Compare enum-shaped states through their ``value`` attribute."""

    assert succeeded() == _snapshot(state=_State("success"))
    assert succeeded() != _snapshot(state=_State("failed"))


def test_failed_narrows_the_captured_exception_type() -> None:
    """Match any captured error by default and an ``isinstance`` check when narrowed."""

    assert failed() == _snapshot(state="failed", error=ValueError("nope"))
    assert failed() == _snapshot(state="failed")
    assert failed(ValueError) == _snapshot(state="failed", error=ValueError("nope"))
    assert failed(KeyError) != _snapshot(state="failed", error=ValueError("nope"))


def test_state_only_factories_match_their_states() -> None:
    """Match skipped, deferred, upstream_failed, and never-ran states exactly."""

    assert skipped() == _snapshot(state="skipped")
    assert deferred() == _snapshot(state="deferred")
    assert upstream_failed() == _snapshot(state="upstream_failed")
    assert not_run() == _snapshot(state=None)
    assert not_run() != _snapshot(state="success")
    assert skipped() != _snapshot(state="deferred")


def test_comparison_against_unshaped_objects_is_not_implemented() -> None:
    """Defer to the other operand when it lacks the task-result attributes."""

    assert succeeded().__eq__(object()) is NotImplemented
    assert succeeded() != object()


def test_constructor_validates_state_and_error_type() -> None:
    """Reject non-string states, empty states, and non-exception error types."""

    bad_state: Any = 1
    bad_error_type: Any = int
    with pytest.raises(TypeError, match="`state` must be a string"):
        TaskOutcome(bad_state)
    with pytest.raises(ValueError, match="`state` must be non-empty"):
        TaskOutcome("")
    with pytest.raises(TypeError, match="`error_type` must be an exception type"):
        TaskOutcome("failed", error_type=bad_error_type)


def test_reprs_render_the_factory_call() -> None:
    """Rebuild each expected outcome as its originating factory call."""

    assert repr(succeeded()) == "succeeded()"
    assert repr(succeeded(42)) == "succeeded(42)"
    assert repr(failed(ValueError)) == "failed(ValueError)"
    assert repr(skipped()) == "skipped()"
    assert repr(not_run()) == "not_run()"
    assert repr(TaskOutcome("removed")) == "TaskOutcome('removed')"
    assert repr(TaskOutcome("removed", xcom=1)) == "TaskOutcome('removed', 1)"
    assert repr(ANY) == "ANY"
