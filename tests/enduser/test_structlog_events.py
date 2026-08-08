"""Exercise consumer-style Airflow structlog capture."""

from __future__ import annotations

import pytest
import structlog
from airflow.sdk import task

from pytest_airflow_in_a_box.logging import StructlogCapture
from pytest_airflow_in_a_box.types import DagMaker

pytestmark = [pytest.mark.compat, pytest.mark.db_test]


def test_structlog_capture_sees_task_events(
    cap_structlog: StructlogCapture,
    dag_maker: DagMaker,
) -> None:
    """Capture a structured event emitted from executed task code."""

    with dag_maker(dag_id="compat_structlog"):

        @task
        def speak() -> None:
            structlog.get_logger("compat_task").warning("compat_event", answer=42)

        speak()

    dag_maker.run_ti("speak")

    assert "compat_event" in cap_structlog
    assert {"answer": 42} in cap_structlog
