"""Exercise the capability bridge between legacy xdist and structured pytest IDs.

References:
    https://github.com/pytest-dev/pytest-xdist/blob/v3.8.0/src/xdist/remote.py
"""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import cast

import pytest

from pytest_airflow_in_a_box._pytest_compat import reconcile_xdist_node_ids


@dataclass(frozen=True)
class _StructuredId:
    """Stand in for pytest's structured ID without importing new private APIs."""

    value: str

    @classmethod
    def parse(cls, value: str) -> _StructuredId:
        """Parse a string ID, preserving the entire xdist suffix.

        Parameters:
            value: str containing the legacy ID.

        Returns:
            _StructuredId containing the same value.
        """

        return cls(value)


def test_reconcile_legacy_group_suffix() -> None:
    """Apply a legacy xdist suffix to a structured ID without losing group names."""

    item = SimpleNamespace(
        id=_StructuredId("test.py::test_ok"),
        nodeid="test.py::test_ok",
        _id=_StructuredId("test.py::test_ok"),
        _nodeid="test.py::test_ok@first_second",
    )

    reconcile_xdist_node_ids([cast(pytest.Item, item)])

    assert item._id == _StructuredId("test.py::test_ok@first_second")


@pytest.mark.parametrize(
    "attrs",
    [
        {"nodeid": "test.py::test_ok", "_nodeid": "test.py::test_ok@group"},
        {"nodeid": "test.py::test_ok", "id": _StructuredId("test.py::test_ok")},
        {
            "nodeid": "test.py::test_ok@group",
            "id": _StructuredId("test.py::test_ok@group"),
            "_nodeid": "test.py::test_ok@group",
        },
    ],
)
def test_leave_compatible_node_ids_untouched(attrs: dict[str, object]) -> None:
    """Leave old pytest, ordinary items, and already-reconciled items untouched.

    Parameters:
        attrs: dict[str, object] representing the available node-ID capabilities.
    """

    item = SimpleNamespace(**attrs)

    reconcile_xdist_node_ids([cast(pytest.Item, item)])

    assert vars(item) == attrs
