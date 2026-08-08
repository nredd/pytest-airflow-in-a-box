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
    ensure_database,
    initialize_database,
)
from pytest_airflow_in_a_box._compat.db import (
    DatabaseCleanupError,
    clear_tables,
    implied_groups,
)
from pytest_airflow_in_a_box._compat.in_process import (
    FakeSupervisorComms,
    InProcessRunResult,
    run_task_in_process,
)
from pytest_airflow_in_a_box._compat.params import ParamsCaseError, validate_dag_params

__all__ = (
    "AirflowCapabilities",
    "AirflowCompatibilityError",
    "DagBagConstructionError",
    "DagBagLocation",
    "DatabaseCleanupError",
    "DatabaseInitializationError",
    "FakeSupervisorComms",
    "InProcessRunResult",
    "ParamsCaseError",
    "TaskInstanceRunner",
    "build_dag_bag",
    "clear_tables",
    "ensure_database",
    "implied_groups",
    "initialize_database",
    "resolve_capabilities",
    "run_task_in_process",
    "validate_dag_params",
)
