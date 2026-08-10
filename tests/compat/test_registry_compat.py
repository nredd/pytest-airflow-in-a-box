"""Test the process-local authoring Dag registry."""

from __future__ import annotations

from typing import Any

import pytest

from pytest_airflow_in_a_box._compat import registry


@pytest.fixture(autouse=True)
def _empty_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolate every test from process-level registry state."""

    monkeypatch.setattr(registry, "_AUTHORING_DAGS", {})


def test_register_and_lookup_round_trip() -> None:
    """Return the exact registered authoring Dag."""

    dag: Any = object()

    registry.register_authoring_dag("round_trip", dag)

    assert registry.lookup_authoring_dag("round_trip") is dag


def test_lookup_absent_dag_returns_none() -> None:
    """Return ``None`` for an identifier that was never registered."""

    assert registry.lookup_authoring_dag("absent") is None


def test_register_overwrites_existing_entry() -> None:
    """Replace an existing entry for the same identifier."""

    first: Any = object()
    second: Any = object()

    registry.register_authoring_dag("overwrite", first)
    registry.register_authoring_dag("overwrite", second)

    assert registry.lookup_authoring_dag("overwrite") is second


def test_unregister_removes_entry_and_is_idempotent() -> None:
    """Remove a registered entry and tolerate repeated removal."""

    dag: Any = object()

    registry.register_authoring_dag("removed", dag)
    registry.unregister_authoring_dag("removed")
    registry.unregister_authoring_dag("removed")

    assert registry.lookup_authoring_dag("removed") is None
