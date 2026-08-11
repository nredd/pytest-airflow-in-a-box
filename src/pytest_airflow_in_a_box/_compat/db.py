"""Clear Apache Airflow metadata tables across certified releases.

The registry lists every clearable table group in one global foreign-key-safe
delete order. SQLite foreign keys are not enforced by the tuned test database,
so referencing groups are expanded through ``implied_groups`` instead of
relying on cascades. Association tables appear in every group that owns one of
their sides deliberately: their rows must go when either side goes, and
repeated deletes are idempotent. Specs in ``_OPTIONAL_SPECS`` exist only on
some certified releases (asset partition tables arrived in 3.2, the
``asset_trigger`` association left after 3.1) and are skipped where absent;
the compat suite pins expected presence per release.

Out of scope: ``deadline_alert`` definitions (user configuration, like asset
definitions) and Airflow-internal bookkeeping tables; clearing ``dags``
without ``assets`` leaves asset definitions and their Dag reference rows in
place.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/database-erd-ref.html
    https://docs.sqlalchemy.org/en/20/core/dml.html#sqlalchemy.delete
"""

from __future__ import annotations

import importlib
from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily, resolve_capabilities

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

ModelSpec = tuple[str, str]
Registry = tuple[tuple[str, tuple[ModelSpec, ...]], ...]

_TABLE_REGISTRY: Registry = (
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
    ("deadlines", (("airflow.models.deadline", "Deadline"),)),
    (
        "runs",
        (
            ("airflow.models.asset", "association_table"),
            ("airflow.models.asset", "AssetPartitionDagRun"),
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
            ("airflow.models.asset", "PartitionedAssetKeyLog"),
            ("airflow.models.asset", "AssetPartitionDagRun"),
            ("airflow.models.asset", "asset_alias_asset_event_association_table"),
            ("airflow.models.asset", "association_table"),
            ("airflow.models.asset", "asset_trigger_association_table"),
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
        "triggers",
        (
            ("airflow.models.asset", "asset_trigger_association_table"),
            ("airflow.models.trigger", "Trigger"),
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
    "runs": ("deadlines", "task_instances"),
    "task_instances": ("xcom",),
    "triggers": ("task_instances",),
}

# Present only on some certified releases; skipped where unresolvable and
# pinned per release by the compat suite.
_OPTIONAL_SPECS: frozenset[ModelSpec] = frozenset(
    {
        ("airflow.models.asset", "AssetPartitionDagRun"),
        ("airflow.models.asset", "PartitionedAssetKeyLog"),
        ("airflow.models.asset", "asset_trigger_association_table"),
    }
)

# The 2.x registry mirrors the 3.x group sequence exactly so shared suites and the
# `TableGroup` enum stay family-independent: `deadlines` and `bundles` are vacuously
# satisfied (the tables arrived in 3.x), `assets` maps to the renamed `dataset*`
# tables, and the XCom delete target is the `BaseXCom` ORM model because 2.x `XCom`
# is a backend-resolved alias. Spike-verified on 2.9.3/2.10.5/2.11.2 (2026-08-11);
# `dataset_alias_dataset_event_assocation_table` spells "assocation" as upstream does.
_V2_TABLE_REGISTRY: Registry = (
    ("xcom", (("airflow.models.xcom", "BaseXCom"),)),
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
    ("deadlines", ()),
    (
        "runs",
        (
            ("airflow.models.dataset", "association_table"),
            ("airflow.models.dagrun", "DagRun"),
        ),
    ),
    (
        "serialized_dags",
        (
            ("airflow.models.serialized_dag", "SerializedDagModel"),
            ("airflow.models.dagcode", "DagCode"),
        ),
    ),
    (
        "assets",
        (
            ("airflow.models.dataset", "dataset_alias_dataset_event_assocation_table"),
            ("airflow.models.dataset", "association_table"),
            ("airflow.models.dataset", "DatasetDagRunQueue"),
            ("airflow.models.dataset", "DatasetEvent"),
            ("airflow.models.dataset", "TaskOutletDatasetReference"),
            ("airflow.models.dataset", "DagScheduleDatasetAliasReference"),
            ("airflow.models.dataset", "DagScheduleDatasetReference"),
            ("airflow.models.dataset", "alias_association_table"),
            ("airflow.models.dataset", "DatasetAliasModel"),
            ("airflow.models.dataset", "DatasetModel"),
        ),
    ),
    ("triggers", (("airflow.models.trigger", "Trigger"),)),
    (
        "dags",
        (
            ("airflow.models.dag", "DagTag"),
            ("airflow.models.dag", "DagOwnerAttributes"),
            ("airflow.models.dagwarning", "DagWarning"),
            ("airflow.models.dag", "DagModel"),
        ),
    ),
    ("bundles", ()),
    ("logs", (("airflow.models.log", "Log"),)),
    ("variables", (("airflow.models.variable", "Variable"),)),
    ("connections", (("airflow.models.connection", "Connection"),)),
)

# Dataset aliases and task-instance history arrived in 2.10; absent on 2.9.3.
_OPTIONAL_SPECS_V2: frozenset[ModelSpec] = frozenset(
    {
        ("airflow.models.taskinstancehistory", "TaskInstanceHistory"),
        ("airflow.models.dataset", "dataset_alias_dataset_event_assocation_table"),
        ("airflow.models.dataset", "DagScheduleDatasetAliasReference"),
        ("airflow.models.dataset", "alias_association_table"),
        ("airflow.models.dataset", "DatasetAliasModel"),
    }
)

REGISTRY_GROUPS: tuple[str, ...] = tuple(group for group, _specs in _TABLE_REGISTRY)
assert tuple(group for group, _specs in _V2_TABLE_REGISTRY) == REGISTRY_GROUPS


def _active_registry() -> tuple[Registry, frozenset[ModelSpec]]:
    """Select the table registry and optional-spec set for the installed family.

    Returns:
        tuple[Registry, frozenset[ModelSpec]] containing the family's registry and the
        specs allowed to be absent on some of its certified releases.
    """

    if resolve_capabilities().family is AirflowFamily.V2:
        return _V2_TABLE_REGISTRY, _OPTIONAL_SPECS_V2
    return _TABLE_REGISTRY, _OPTIONAL_SPECS


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
    registry, optional_specs = _active_registry()
    try:
        with create_session() as session:
            for group, specs in registry:
                if group not in requested:
                    continue
                for spec in specs:
                    if spec in optional_specs:
                        try:
                            target = _resolve_spec(spec)
                        except (ImportError, AttributeError):
                            # Absent on this release; presence per certified
                            # release is pinned by the compat suite.
                            continue
                    else:
                        target = _resolve_spec(spec)
                    session.execute(delete(target))
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
