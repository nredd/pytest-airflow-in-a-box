"""Probe-double coverage for the 3.x-only fixture gates and family markers."""

from __future__ import annotations

from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box import markers
from pytest_airflow_in_a_box._compat import capabilities as capability_module
from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily
from pytest_airflow_in_a_box.fixtures import api as api_module
from pytest_airflow_in_a_box.fixtures import context as context_module
from pytest_airflow_in_a_box.fixtures import logging as logging_module
from pytest_airflow_in_a_box.fixtures import render as render_module
from pytest_airflow_in_a_box.fixtures import taskrun as taskrun_module


def test_gate_message_names_surface_detail_and_issue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Build the actionable message only on the 2.x family."""

    monkeypatch.setattr(capability_module, "installed_family", lambda: AirflowFamily.V2)
    message = capability_module.v2_gate_message("run_task", "use `run_ti` instead.")
    assert message is not None
    assert "`run_task`" in message
    assert "use `run_ti` instead." in message
    assert "issues/25" in message

    monkeypatch.setattr(capability_module, "installed_family", lambda: AirflowFamily.V3)
    assert capability_module.v2_gate_message("run_task", "detail.") is None


def _drive_fixture(raw: Any, arguments: tuple[Any, ...]) -> None:
    """Run a raw fixture function far enough to reach its family gate.

    Parameters:
        raw: Any callable unwrapped from a pytest fixture.
        arguments: tuple[Any, ...] of positional stand-ins for fixture parameters.
    """

    result = raw(*arguments)
    if isinstance(result, Iterator):
        next(result)


@pytest.mark.parametrize(
    ("module", "fixture_name"),
    [
        (taskrun_module, "run_task"),
        (render_module, "render_task"),
        (context_module, "task_context"),
        (logging_module, "cap_structlog"),
        (api_module, "api_server_url"),
    ],
)
def test_fixtures_fail_loud_when_gated(
    monkeypatch: pytest.MonkeyPatch,
    module: object,
    fixture_name: str,
) -> None:
    """Fail each gated fixture with the family message instead of proceeding.

    The raw fixture functions run directly via `__wrapped__` because a failure
    raised through the fixture machinery is cached for the fixture's scope, and
    the session-scoped `api_server_url` would stay poisoned for every later
    in-process test that requests it.
    """

    def fake_gate(surface: str, detail: str) -> str:
        del detail
        return f"gated `{surface}`"

    monkeypatch.setattr(module, "v2_gate_message", fake_gate)
    raw = getattr(module, fixture_name).__wrapped__
    needs_request = {"api_server_url", "run_task", "render_task"}
    arguments = (SimpleNamespace(),) if fixture_name in needs_request else ()

    with pytest.raises(pytest.fail.Exception, match=f"gated `{fixture_name}`"):
        _drive_fixture(raw, arguments)


def _item_with_marker(name: str | None) -> Any:
    """Build a minimal marked node for family-gate unit tests.

    Parameters:
        name: str | None containing the single marker name to expose.

    Returns:
        types.SimpleNamespace exposing `get_closest_marker`.
    """

    return SimpleNamespace(
        get_closest_marker=lambda requested: (
            SimpleNamespace(name=requested) if requested == name else None
        )
    )


def test_family_gate_skips_on_the_other_family(monkeypatch: pytest.MonkeyPatch) -> None:
    """Skip a `requires_airflow2` test on the 3.x family with a named reason."""

    monkeypatch.setattr(markers, "installed_family", lambda: AirflowFamily.V3)

    with pytest.raises(pytest.skip.Exception, match="requires_airflow2") as caught:
        markers.apply_family_gate(_item_with_marker("requires_airflow2"))

    assert "requires the Airflow 2.x family" in str(caught.value)
    assert "installed: Airflow 3.x" in str(caught.value)


def test_family_gate_skips_without_any_airflow(monkeypatch: pytest.MonkeyPatch) -> None:
    """Skip family-marked tests in an Airflow-free session."""

    monkeypatch.setattr(markers, "installed_family", lambda: None)

    with pytest.raises(pytest.skip.Exception, match="no Airflow"):
        markers.apply_family_gate(_item_with_marker("requires_airflow3"))


def test_family_gate_passes_matching_and_unmarked_items(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run matching-family and unmarked items without touching the probe."""

    monkeypatch.setattr(markers, "installed_family", lambda: AirflowFamily.V2)
    markers.apply_family_gate(_item_with_marker("requires_airflow2"))
    markers.apply_family_gate(_item_with_marker(None))
