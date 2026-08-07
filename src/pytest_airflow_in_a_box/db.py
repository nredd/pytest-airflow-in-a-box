"""Public registry-driven metadata database cleanup.

``clear_db`` is a whole-database reset for serial setup and teardown contexts.
It is not safe while parallel xdist workers are mutating the shared database;
per-Dag cleanup during parallel runs is owned by the ``dag_maker`` fixture.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/database-erd-ref.html
"""

from __future__ import annotations

from collections.abc import Collection
from enum import Enum

from pytest_airflow_in_a_box._compat.db import (
    REGISTRY_GROUPS,
    DatabaseCleanupError,
    clear_tables,
    implied_groups,
)


class TableGroup(str, Enum):
    """Clearable groups of Airflow metadata tables."""

    ASSETS = "assets"
    BUNDLES = "bundles"
    CONNECTIONS = "connections"
    DAGS = "dags"
    DEADLINES = "deadlines"
    LOGS = "logs"
    RUNS = "runs"
    SERIALIZED_DAGS = "serialized_dags"
    TASK_INSTANCES = "task_instances"
    TRIGGERS = "triggers"
    VARIABLES = "variables"
    XCOM = "xcom"


def clear_db(*, tables: Collection[TableGroup] | None = None) -> None:
    """Clear the requested table groups plus every transitively implied group.

    Requesting a group also clears the groups whose rows reference it (for
    example ``RUNS`` clears deadlines, task instances, and XCom rows, and
    ``TRIGGERS`` clears the task instances that reference triggers), because
    the tuned test database does not enforce foreign keys. Clearing
    ``CONNECTIONS`` recreates Airflow's default connections. Clearing ``DAGS``
    without ``ASSETS`` deliberately leaves asset definitions and their Dag
    reference rows in place.

    Parameters:
        tables: Collection[TableGroup] | None selecting groups to clear, or
            ``None`` to clear every registered group.

    Raises:
        TypeError: An entry is not a ``TableGroup``.
        ValueError: The selection is empty.
        DatabaseCleanupError: A registry target cannot be resolved or deleted.
    """

    if tables is None:
        clear_tables(REGISTRY_GROUPS)
        return
    for entry in tables:
        if not isinstance(entry, TableGroup):
            raise TypeError(f"`tables` entries must be TableGroup members: '{entry}'")
    if not tables:
        raise ValueError("`tables` must not be empty; pass None to clear every group")
    clear_tables(implied_groups(tuple(entry.value for entry in tables)))


__all__ = ("DatabaseCleanupError", "TableGroup", "clear_db")
