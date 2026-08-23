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
from pytest_airflow_in_a_box._compat.dagbag import (
    DagBagConstructionError,
    DagBagShardStat,
    build_dag_bag,
    build_partial_dag_bag,
    list_dag_file_paths,
)
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
    InProcessTaskContext,
    render_task_in_process,
    run_task_in_process,
    task_context_in_process,
)
from pytest_airflow_in_a_box._compat.params import ParamsCaseError, validate_dag_params
from pytest_airflow_in_a_box._compat.parse_time import ParseTimeComms, parse_time_supervision
from pytest_airflow_in_a_box._compat.seed import (
    SeedCleanupError,
    SeedPersistenceError,
    SeedRecord,
    cleanup_seeds,
    open_seed_session,
    seed_connections,
    seed_variables,
    validate_connections,
    validate_variables,
)

__all__ = (
    "AirflowCapabilities",
    "AirflowCompatibilityError",
    "DagBagConstructionError",
    "DagBagLocation",
    "DagBagShardStat",
    "DatabaseCleanupError",
    "DatabaseInitializationError",
    "FakeSupervisorComms",
    "InProcessRunResult",
    "InProcessTaskContext",
    "ParamsCaseError",
    "ParseTimeComms",
    "SeedCleanupError",
    "SeedPersistenceError",
    "SeedRecord",
    "TaskInstanceRunner",
    "build_dag_bag",
    "build_partial_dag_bag",
    "cleanup_seeds",
    "clear_tables",
    "ensure_database",
    "implied_groups",
    "initialize_database",
    "list_dag_file_paths",
    "open_seed_session",
    "parse_time_supervision",
    "render_task_in_process",
    "resolve_capabilities",
    "run_task_in_process",
    "seed_connections",
    "seed_variables",
    "task_context_in_process",
    "validate_connections",
    "validate_dag_params",
    "validate_variables",
)
