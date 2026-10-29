"""Bridge xdist's legacy node-id writes to pytest's structured node IDs.

pytest-xdist 3.8 writes ``Item._nodeid`` to implement loadgroup scheduling. Pytest
9.2 reads ``Item._id`` instead, silently ignoring those writes. Reconcile only
actual legacy writes after collection hooks finish, before xdist sends the IDs.
Remove this bridge when the minimum supported xdist handles structured IDs itself.

References:
    https://github.com/pytest-dev/pytest-xdist/blob/v3.8.0/src/xdist/remote.py
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_collection_modifyitems
"""

from __future__ import annotations

from collections.abc import Iterable

import pytest


def reconcile_xdist_node_ids(items: Iterable[pytest.Item]) -> None:
    """Apply legacy xdist node-id writes when structured IDs are available.

    Parameters:
        items: Iterable[pytest.Item] after all collection-modification hooks.

    Returns:
        None. Updates only IDs that a legacy plugin actually changed.
    """

    for item in items:
        structured_id = getattr(item, "id", None)
        legacy_id = getattr(item, "_nodeid", None)
        if structured_id is None or legacy_id is None or legacy_id == item.nodeid:
            continue
        item._id = type(structured_id).parse(legacy_id)
