"""Provider-shaped custom sensor."""

from __future__ import annotations

from importlib import import_module
from typing import Any


def _resolve(*candidates: str) -> Any:
    """Import the first available module; the corpus parses on both Airflow families.

    Parameters:
        candidates: str module paths ordered newest family first.

    Returns:
        Any containing the first importable module.
    """

    for name in candidates[:-1]:
        try:
            return import_module(name)
        except ImportError:
            continue
    return import_module(candidates[-1])


# Airflow 2.x carries the base class in `airflow.sensors.base`.
BaseSensorOperator = _resolve("airflow.sdk", "airflow.sensors.base").BaseSensorOperator


class ExampleSensor(BaseSensorOperator):
    """Succeed immediately for corpus parsing."""

    def poke(self, context: Any) -> bool:
        del context
        return True
