"""Provider-shaped custom listener.

`airflow.listeners.hookimpl` is unchanged across families and stays a static import, the
same precedent `tests/enduser/test_callbacks.py::RecordingListener` follows: a plain class
with no base at all, its methods marked individually with `@hookimpl`.
"""

from __future__ import annotations

from typing import Any, ClassVar

from airflow.listeners import hookimpl


class ExampleListener:
    """Record task-instance success notifications for the compatibility corpus."""

    calls: ClassVar[list[str]] = []

    @hookimpl
    def on_task_instance_success(self, previous_state: Any, task_instance: Any) -> None:
        del previous_state
        self.calls.append(getattr(task_instance, "task_id", "<unknown>"))
