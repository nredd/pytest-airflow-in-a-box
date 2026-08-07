"""Clear Apache Airflow metadata tables across certified releases.

The registry lists every clearable table group in one global foreign-key-safe
delete order. SQLite foreign keys are not enforced by the tuned test database,
so referencing groups are expanded through ``implied_groups`` instead of
relying on cascades. The ``dagrun_asset_event`` association table appears in
both the ``runs`` and ``assets`` groups deliberately: its rows must go when
either side goes, and repeated deletes are idempotent.

Out of v1 scope: triggers, deadlines, and Airflow-internal bookkeeping tables;
clearing ``dags`` without ``assets`` leaves asset definitions and their Dag
reference rows in place.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/database-erd-ref.html
    https://docs.sqlalchemy.org/en/20/core/dml.html#sqlalchemy.delete
"""

from __future__ import annotations

import importlib
from collections.abc import Collection
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

ModelSpec = tuple[str, str]

_TABLE_REGISTRY: tuple[tuple[str, tuple[ModelSpec, ...]], ...] = (
    ("xcom", (("airflow.models.xcom", "XComModel"),)),
    (
        "task_instances",
        (
            ("airflow.models.renderedtifields", "RenderedTaskInstanceFields"),
            ("airflow.models.taskreschedule", "TaskReschedule"),
            ("airflow.models.taskmap", "TaskMap"),
            ("airflow.models.taskinstancehistory", "TaskInstanceHistory"),
            ("airflow.models.taskinstance", "TaskInstance"),
        ),
    ),
    (
        "runs",
        (
            ("airflow.models.asset", "association_table"),
            ("airflow.models.dagrun", "DagRun"),
        ),
    ),
    (
        "serialized_dags",
        (
            ("airflow.models.serialized_dag", "SerializedDagModel"),
            ("airflow.models.dagcode", "DagCode"),
            ("airflow.models.dag_version", "DagVersion"),
        ),
    ),
    (
        "assets",
        (
            ("airflow.models.asset", "asset_alias_asset_event_association_table"),
            ("airflow.models.asset", "association_table"),
            ("airflow.models.asset", "AssetDagRunQueue"),
            ("airflow.models.asset", "AssetEvent"),
            ("airflow.models.asset", "TaskOutletAssetReference"),
            ("airflow.models.asset", "DagScheduleAssetReference"),
            ("airflow.models.asset", "AssetActive"),
            ("airflow.models.asset", "alias_association_table"),
            ("airflow.models.asset", "AssetAliasModel"),
            ("airflow.models.asset", "AssetModel"),
        ),
    ),
    (
        "dags",
        (
            ("airflow.models.dag", "DagTag"),
            ("airflow.models.dag", "DagOwnerAttributes"),
            ("airflow.models.dagwarning", "DagWarning"),
            ("airflow.models.dag", "DagModel"),
        ),
    ),
    ("bundles", (("airflow.models.dagbundle", "DagBundleModel"),)),
    ("logs", (("airflow.models.log", "Log"),)),
    ("variables", (("airflow.models.variable", "Variable"),)),
    ("connections", (("airflow.models.connection", "Connection"),)),
)

# Requesting a key group also clears the value groups, whose rows reference the
# key group's rows and would otherwise be orphaned without foreign-key cascades.
_IMPLIED: dict[str, tuple[str, ...]] = {
    "bundles": ("dags",),
    "dags": ("serialized_dags", "runs"),
    "serialized_dags": ("runs",),
    "runs": ("task_instances",),
    "task_instances": ("xcom",),
}

REGISTRY_GROUPS: tuple[str, ...] = tuple(group for group, _specs in _TABLE_REGISTRY)


class DatabaseCleanupError(RuntimeError):
    """Report failure to clear the isolated Airflow metadata database."""


def implied_groups(groups: Collection[str]) -> tuple[str, ...]:
    """Expand requested groups with every transitively implied group.

    Parameters:
        groups: Collection[str] containing requested registry group names.

    Returns:
        tuple[str, ...] containing the expanded groups in registry delete order.

    Raises:
        ValueError: A requested group is not a registered group name.
    """

    invalid = sorted(set(groups) - set(REGISTRY_GROUPS))
    if invalid:
        names = ", ".join(f"`{name}`" for name in invalid)
        raise ValueError(f"Unknown table groups: {names}")
    selected: set[str] = set()
    frontier = list(groups)
    while frontier:
        group = frontier.pop()
        if group in selected:
            continue
        selected.add(group)
        frontier.extend(_IMPLIED.get(group, ()))
    return tuple(group for group in REGISTRY_GROUPS if group in selected)


def _resolve_spec(spec: ModelSpec) -> Any:
    """Resolve one registry spec to a mapped class or table.

    Parameters:
        spec: ModelSpec containing the module path and attribute name.

    Returns:
        Any containing the resolved SQLAlchemy delete target.

    Raises:
        ImportError: The module cannot be imported on this release.
        AttributeError: The attribute is absent from the module.
    """

    module_name, attribute = spec
    module = importlib.import_module(module_name)
    return getattr(module, attribute)


def clear_tables(groups: Collection[str]) -> None:
    """Delete every row of the requested groups in registry delete order.

    Deletions run in one session and commit once; default Airflow connections
    are recreated whenever the ``connections`` group is cleared.

    Parameters:
        groups: Collection[str] containing already expanded registry groups.

    Raises:
        ValueError: A requested group is not a registered group name.
        DatabaseCleanupError: A registry target cannot be resolved or deleted.
    """

    invalid = sorted(set(groups) - set(REGISTRY_GROUPS))
    if invalid:
        names = ", ".join(f"`{name}`" for name in invalid)
        raise ValueError(f"Unknown table groups: {names}")

    # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
    from airflow.utils.session import create_session
    from sqlalchemy import delete

    requested = set(groups)
    try:
        with create_session() as session:
            for group, specs in _TABLE_REGISTRY:
                if group not in requested:
                    continue
                for spec in specs:
                    session.execute(delete(_resolve_spec(spec)))
            if "connections" in requested:
                _restore_default_connections(session)
    except Exception as error:
        names = ", ".join(f"`{name}`" for name in sorted(requested))
        raise DatabaseCleanupError(
            f"Could not clear Airflow metadata table groups {names}: {error}"
        ) from error


def _restore_default_connections(session: Session) -> None:
    """Recreate Airflow's default connections after clearing the table.

    Parameters:
        session: sqlalchemy.orm.Session containing the active metadata session.
    """

    # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
    from airflow.utils.db import create_default_connections

    create_default_connections(session=session)


__all__ = (
    "REGISTRY_GROUPS",
    "DatabaseCleanupError",
    "clear_tables",
    "implied_groups",
)
