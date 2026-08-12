"""Provider-shaped custom sensor."""

from __future__ import annotations

from importlib import import_module
from typing import Any

# `provider.py` puts `tests/dags` on `sys.path` before importing this package, the same
# path `_family` itself is reached through.
_resolve = import_module("_family")._resolve

# Airflow 2.x carries the base class in `airflow.sensors.base`.
BaseSensorOperator = _resolve("airflow.sdk", "airflow.sensors.base").BaseSensorOperator


class ExampleSensor(BaseSensorOperator):
    """Succeed immediately for corpus parsing."""

    def poke(self, context: Any) -> bool:
        del context
        return True
