"""Unit tests for `plugin`-module helpers that need no pytester session.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.UsageError
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pytest_airflow_in_a_box import plugin
from pytest_airflow_in_a_box._compat import AirflowCompatibilityError


def test_database_incompatibility_renders_as_usage_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Render `AirflowCompatibilityError` as a single actionable usage error.

    Parameters:
        monkeypatch: pytest.MonkeyPatch replacing the database initialization seam.
        tmp_path: Path providing a throwaway bootstrap root.
    """

    failure = AirflowCompatibilityError("Apache Airflow 2.x is installed")

    def broken_database(root: Path) -> None:
        """Raise the representative installation failure."""

        del root
        raise failure

    monkeypatch.setattr(plugin, "ensure_database", broken_database)

    with pytest.raises(pytest.UsageError, match=r"Airflow 2\.x is installed") as caught:
        plugin._ensure_database_or_usage_error(tmp_path)

    assert caught.value.__cause__ is failure
