"""Test cleanup-registry invariants against the installed Airflow release."""

from __future__ import annotations

import importlib

import pytest
import sqlalchemy

from pytest_airflow_in_a_box._compat import db as compat_db
from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily, installed_family
from pytest_airflow_in_a_box.db import TableGroup


def test_registry_groups_match_public_enum() -> None:
    """Register every public group exactly once."""

    assert list(compat_db.REGISTRY_GROUPS) == sorted(
        compat_db.REGISTRY_GROUPS, key=list(compat_db.REGISTRY_GROUPS).index
    )
    assert len(set(compat_db.REGISTRY_GROUPS)) == len(compat_db.REGISTRY_GROUPS)
    assert set(compat_db.REGISTRY_GROUPS) == {group.value for group in TableGroup}


def test_implied_groups_reference_registered_groups() -> None:
    """Point every implication at registered groups only."""

    registered = set(compat_db.REGISTRY_GROUPS)
    for group, implied in compat_db._IMPLIED.items():
        assert group in registered
        assert set(implied) <= registered


def test_implied_groups_delete_before_their_source() -> None:
    """Order every implied group before the group that implies it."""

    positions = {group: index for index, group in enumerate(compat_db.REGISTRY_GROUPS)}
    for group, implied in compat_db._IMPLIED.items():
        for name in implied:
            assert positions[name] < positions[group], f"`{name}` must delete before `{group}`"


# Collection-time family selection keeps the parametrization aligned with the installed
# distribution without importing Airflow: the fake-metadata capability tests never run
# these compat items, so the import-free probe is accurate here.
_COLLECTION_FAMILY = installed_family()
_ACTIVE_REGISTRY, _ACTIVE_OPTIONAL_SPECS = (
    (compat_db._V2_TABLE_REGISTRY, compat_db._OPTIONAL_SPECS_V2)
    if _COLLECTION_FAMILY is AirflowFamily.V2
    else (compat_db._TABLE_REGISTRY, compat_db._OPTIONAL_SPECS)
)


@pytest.mark.parametrize(
    "spec",
    [
        pytest.param(spec, id=f"{spec[0]}.{spec[1]}")
        for _group, specs in _ACTIVE_REGISTRY
        for spec in specs
        if spec not in _ACTIVE_OPTIONAL_SPECS
    ],
)
def test_registry_specs_resolve_on_installed_release(spec: tuple[str, str]) -> None:
    """Resolve every required registry spec to a mapped class or core table."""

    module_name, attribute = spec
    resolved = getattr(importlib.import_module(module_name), attribute)

    is_mapped_class = isinstance(resolved, type) and hasattr(resolved, "__table__")
    is_core_table = isinstance(resolved, sqlalchemy.Table)
    assert is_mapped_class or is_core_table


def test_optional_specs_match_certified_release_shape() -> None:
    """Pin each optional spec's presence to the installed release line."""

    from pytest_airflow_in_a_box._compat import resolve_capabilities

    capabilities = resolve_capabilities()
    minor = capabilities.release[:2]
    if capabilities.family is AirflowFamily.V2:
        expected_presence = {
            ("airflow.models.taskinstancehistory", "TaskInstanceHistory"): minor >= (2, 10),
            (
                "airflow.models.dataset",
                "dataset_alias_dataset_event_assocation_table",
            ): minor >= (2, 10),
            ("airflow.models.dataset", "DagScheduleDatasetAliasReference"): minor >= (2, 10),
            ("airflow.models.dataset", "alias_association_table"): minor >= (2, 10),
            ("airflow.models.dataset", "DatasetAliasModel"): minor >= (2, 10),
        }
        assert set(expected_presence) == set(compat_db._OPTIONAL_SPECS_V2)
    else:
        expected_presence = {
            ("airflow.models.asset", "AssetPartitionDagRun"): minor >= (3, 2),
            ("airflow.models.asset", "PartitionedAssetKeyLog"): minor >= (3, 2),
            ("airflow.models.asset", "asset_trigger_association_table"): minor < (3, 2),
        }
        assert set(expected_presence) == set(compat_db._OPTIONAL_SPECS)

    for (module_name, attribute), expected in expected_presence.items():
        module_present = True
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            module_present = False
        present = module_present and hasattr(module, attribute)
        assert present is expected, f"`{module_name}.{attribute}` presence drifted"


def test_registry_selection_follows_the_family(monkeypatch: pytest.MonkeyPatch) -> None:
    """Select the family-matching registry and optional-spec set."""

    from types import SimpleNamespace

    from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily

    monkeypatch.setattr(
        compat_db, "resolve_capabilities", lambda: SimpleNamespace(family=AirflowFamily.V2)
    )
    assert compat_db._active_registry() == (
        compat_db._V2_TABLE_REGISTRY,
        compat_db._OPTIONAL_SPECS_V2,
    )

    monkeypatch.setattr(
        compat_db, "resolve_capabilities", lambda: SimpleNamespace(family=AirflowFamily.V3)
    )
    assert compat_db._active_registry() == (
        compat_db._TABLE_REGISTRY,
        compat_db._OPTIONAL_SPECS,
    )


def test_v2_registry_mirrors_the_group_sequence() -> None:
    """Keep the 2.x registry group names identical to the 3.x sequence."""

    v3_groups = tuple(group for group, _specs in compat_db._TABLE_REGISTRY)
    v2_groups = tuple(group for group, _specs in compat_db._V2_TABLE_REGISTRY)

    assert v2_groups == v3_groups
    assert {group.value for group in TableGroup} == set(v2_groups)
