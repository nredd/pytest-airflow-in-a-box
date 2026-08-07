"""Validated Apache Airflow compatibility capabilities.

This package is import-safe before Airflow bootstrap. Airflow modules are loaded only when
``resolve_capabilities`` is called.

References:
    https://docs.python.org/3/library/importlib.metadata.html
    https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html
"""

from __future__ import annotations

from pytest_airflow_in_a_box._compat.capabilities import (
    AirflowCapabilities,
    AirflowCompatibilityError,
    DagBagLocation,
    TaskInstanceRunner,
    resolve_capabilities,
)

__all__ = (
    "AirflowCapabilities",
    "AirflowCompatibilityError",
    "DagBagLocation",
    "TaskInstanceRunner",
    "resolve_capabilities",
)
