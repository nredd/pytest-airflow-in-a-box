"""Test the migration orchestrator's shared types and exception hierarchy."""

from __future__ import annotations

import pytest

from pytest_airflow_in_a_box.migration.types import (
    ArtifactError,
    CategoryCounts,
    DiffResult,
    MigrationToolingError,
    PluginSpecResolutionError,
    ProvisioningError,
    RecordRunError,
    UvNotFoundError,
)


def _counts(**overrides: int) -> CategoryCounts:
    """Build a `CategoryCounts` with every field zeroed except the given overrides.

    Parameters:
        overrides: int values keyed by field name to override.

    Returns:
        CategoryCounts containing the requested counts.
    """

    base = {
        "regression": 0,
        "fixed": 0,
        "broken_on_both": 0,
        "still_passing": 0,
        "gated": 0,
        "new": 0,
        "missing": 0,
    }
    base.update(overrides)
    return CategoryCounts(**base)


def test_diff_result_has_regressions_true_when_regression_count_is_nonzero() -> None:
    """Report regressions present when the regression count is nonzero."""
    diff = DiffResult(
        counts=_counts(regression=1),
        regression_nodeids=("tests/test_a.py::test_a",),
        fixed_nodeids=(),
        new_nodeids=(),
        missing_nodeids=(),
        warnings=(),
    )

    assert diff.has_regressions is True


def test_diff_result_has_regressions_false_when_regression_count_is_zero() -> None:
    """Report no regressions when the regression count is zero."""
    diff = DiffResult(
        counts=_counts(),
        regression_nodeids=(),
        fixed_nodeids=(),
        new_nodeids=(),
        missing_nodeids=(),
        warnings=(),
    )

    assert diff.has_regressions is False


@pytest.mark.parametrize(
    "error_type",
    [
        UvNotFoundError,
        ProvisioningError,
        PluginSpecResolutionError,
        RecordRunError,
        ArtifactError,
    ],
)
def test_tooling_errors_subclass_migration_tooling_error(
    error_type: type[MigrationToolingError],
) -> None:
    """Keep every tooling-or-environment error mapped to the exit-code-2 family."""
    error = error_type("boom")

    assert isinstance(error, MigrationToolingError)
    assert isinstance(error, RuntimeError)
    assert str(error) == "boom"
