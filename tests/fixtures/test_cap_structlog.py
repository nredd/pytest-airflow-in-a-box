"""Test the structlog capture fixture.

References:
    https://www.structlog.org/en/stable/configuration.html
"""

from __future__ import annotations

import pytest
import structlog

from pytest_airflow_in_a_box.fixtures.logging import _capture_structlog
from pytest_airflow_in_a_box.logging import StructlogCapture
from pytest_airflow_in_a_box.types import DagMaker

pytestmark = pytest.mark.db_test


def test_cap_structlog_records_events(cap_structlog: StructlogCapture) -> None:
    """Capture events with their bound values and log level."""

    structlog.get_logger("consumer").info("fixture_event", key=1)

    assert "fixture_event" in cap_structlog
    assert {"key": 1, "log_level": "info"} in cap_structlog
    assert "fixture_event" in cap_structlog.text


def test_capture_restores_exact_configuration() -> None:
    """Install the capture before the renderer and restore the prior chain."""

    before = structlog.get_config()["processors"]
    generator = _capture_structlog()
    capture = next(generator)

    installed = structlog.get_config()["processors"]
    assert installed[-2] is capture
    assert installed[:-2] == before[:-1]

    with pytest.raises(StopIteration):
        next(generator)
    assert structlog.get_config()["processors"] == before


def test_capture_handles_empty_processor_chain() -> None:
    """Install the capture alone when no processors are configured."""

    saved = structlog.get_config()
    try:
        structlog.configure(processors=[])
        generator = _capture_structlog()
        capture = next(generator)

        assert structlog.get_config()["processors"] == [capture]

        with pytest.raises(StopIteration):
            next(generator)
        assert structlog.get_config()["processors"] == []
    finally:
        structlog.configure(**saved)


def test_cap_structlog_sees_task_logging(
    cap_structlog: StructlogCapture,
    dag_maker: DagMaker,
) -> None:
    """Capture structlog events emitted from inside an executed task."""

    # Deferred so the Dag context builds the TaskFlow task at run time.
    from airflow.sdk import task

    with dag_maker(dag_id="cap_structlog_task"):

        @task
        def speak() -> None:
            """Emit one structlog event from task code."""

            structlog.get_logger("task_logger").warning("task_event", answer=42)

        speak()

    dag_maker.run_ti("speak")

    assert "task_event" in cap_structlog
    assert {"answer": 42} in cap_structlog
