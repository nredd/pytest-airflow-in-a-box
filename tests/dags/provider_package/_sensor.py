"""Provider-shaped custom sensor."""

from __future__ import annotations

from importlib import import_module
from typing import Any

# Both entry points into this package prime `sys.path` with `tests/dags` first --
# `provider.py` for DagBag parsing, and `tests/enduser/test_provider_package.py`'s
# direct `syspath_prepend` + `import_module("provider_package")` -- which is also how
# `_family` itself is reached.
_resolve = import_module("_family")._resolve

# Airflow 2.x carries the base class in `airflow.sensors.base`.
BaseSensorOperator = _resolve("airflow.sdk", "airflow.sensors.base").BaseSensorOperator


class ExampleSensor(BaseSensorOperator):
    """Succeed immediately for corpus parsing."""

    def poke(self, context: Any) -> bool:
        del context
        return True
