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
from pytest_airflow_in_a_box._compat.dagbag import DagBagConstructionError, build_dag_bag
from pytest_airflow_in_a_box._compat.database import (
    DatabaseInitializationError,
    initialize_database,
)

__all__ = (
    "AirflowCapabilities",
    "AirflowCompatibilityError",
    "DagBagConstructionError",
    "DagBagLocation",
    "DatabaseInitializationError",
    "TaskInstanceRunner",
    "build_dag_bag",
    "initialize_database",
    "resolve_capabilities",
)
