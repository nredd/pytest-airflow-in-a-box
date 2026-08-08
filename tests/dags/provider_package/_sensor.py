"""Provider-shaped custom sensor."""

from __future__ import annotations

from typing import Any

from airflow.sdk import BaseSensorOperator


class ExampleSensor(BaseSensorOperator):
    """Succeed immediately for corpus parsing."""

    def poke(self, context: Any) -> bool:
        del context
        return True
