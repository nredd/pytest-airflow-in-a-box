"""Test cleanup-registry invariants against the installed Airflow release."""

from __future__ import annotations

import importlib

import pytest
import sqlalchemy

from pytest_airflow_in_a_box._compat import db as compat_db
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


@pytest.mark.parametrize(
    "spec",
    [
        pytest.param(spec, id=f"{spec[0]}.{spec[1]}")
        for _group, specs in compat_db._TABLE_REGISTRY
        for spec in specs
    ],
)
def test_registry_specs_resolve_on_installed_release(spec: tuple[str, str]) -> None:
    """Resolve every registry spec to a mapped class or core table."""

    module_name, attribute = spec
    resolved = getattr(importlib.import_module(module_name), attribute)

    is_mapped_class = isinstance(resolved, type) and hasattr(resolved, "__table__")
    is_core_table = isinstance(resolved, sqlalchemy.Table)
    assert is_mapped_class or is_core_table
