"""Shared types for the `airflow-migration-diff` orchestrator.

`DiffResult` and `ComputeCategoryDiff` are the ONE thin interface this package uses to
reach the seven-bucket categorization owned by issue #42 (`--airflow-record` /
`--airflow-baseline`). This package never loads an artifact or re-derives a category
itself; `provision.py` and `run.py` depend only on the callable shape below.
`run.default_compute_category_diff` is the real implementation, loading both artifacts
through `pytest_airflow_in_a_box.artifact.load_artifact` and categorizing them through
`pytest_airflow_in_a_box.baseline.compute_categories` -- the seam stays injectable so
tests can still drive `cli.execute`/`run.compute_diff` against a fake. See
`CHANGELOG.md` and issue #44.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/42
    https://github.com/nredd/pytest-airflow-in-a-box/issues/44
    https://github.com/nredd/pytest-airflow-in-a-box/blob/main/docs/guide/migration-diff.md
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path


class MigrationToolingError(RuntimeError):
    """A tooling-or-environment failure in the migration orchestrator (exit code 2).

    Distinct from a migration regression (exit code 1): this always means the
    orchestrator itself could not complete its job, not that the diff it produced
    contains bad news.
    """


class UvNotFoundError(MigrationToolingError):
    """No `uv` executable was found on `$PATH` and none was passed via `--uv-path`."""


class ProvisioningError(MigrationToolingError):
    """A `uv venv` create, constraints fetch, or package install step failed."""


class PluginSpecResolutionError(MigrationToolingError):
    """The (possibly-defaulted) `--plugin-spec` could not be resolved by `uv`.

    Raised only for the install step that names the plugin spec, and only when the
    spec was defaulted rather than passed explicitly -- an explicit user-supplied spec
    that fails to resolve is an ordinary `ProvisioningError` instead.
    """


class RecordRunError(MigrationToolingError):
    """A `pytest --airflow-record` invocation crashed before writing its artifact."""


class ArtifactError(MigrationToolingError):
    """An artifact was absent, malformed, schema-mismatched, or incomplete."""


@dataclass(frozen=True)
class CategoryCounts:
    """The seven outcome-transition buckets `--airflow-baseline` defines (issue #42).

    Parameters:
        regression: int counting baseline-pass -> live-fail/error transitions.
        fixed: int counting baseline-fail/error -> live-pass transitions.
        broken_on_both: int counting baseline-fail/error -> live-fail/error transitions.
        still_passing: int counting baseline-pass -> live-pass transitions.
        gated: int counting `requires_airflow2`/`requires_airflow3` skips on either side.
        new: int counting nodeids absent from the baseline.
        missing: int counting baseline nodeids not collected live.
    """

    regression: int
    fixed: int
    broken_on_both: int
    still_passing: int
    gated: int
    new: int
    missing: int


@dataclass(frozen=True)
class DiffResult:
    """A fully categorized 2.x -> 3.x migration diff.

    Parameters:
        counts: CategoryCounts containing the seven bucket totals.
        regression_nodeids: tuple[str, ...] naming every regressed nodeid.
        fixed_nodeids: tuple[str, ...] naming every fixed nodeid.
        new_nodeids: tuple[str, ...] naming every nodeid absent from the baseline.
        missing_nodeids: tuple[str, ...] naming every baseline nodeid not collected live.
        warnings: tuple[str, ...] containing non-fatal advisories (for example, a
            same-family baseline-vs-live comparison).
    """

    counts: CategoryCounts
    regression_nodeids: tuple[str, ...]
    fixed_nodeids: tuple[str, ...]
    new_nodeids: tuple[str, ...]
    missing_nodeids: tuple[str, ...]
    warnings: tuple[str, ...]

    @property
    def has_regressions(self) -> bool:
        """Report whether the diff contains at least one regression.

        Returns:
            bool indicating whether `counts.regression` is nonzero.
        """

        return self.counts.regression > 0


# The one seam bridging this package to issue #42's artifact contract: given a baseline
# artifact path, a live artifact path, and the two `--allow-incomplete-*` overrides,
# load both artifacts and return their categorized diff. Implementations raise
# `ArtifactError` for an absent, malformed, schema-mismatched, or (without its override)
# incomplete artifact.
ComputeCategoryDiff = Callable[[Path, Path, bool, bool], DiffResult]


__all__ = (
    "ArtifactError",
    "CategoryCounts",
    "ComputeCategoryDiff",
    "DiffResult",
    "MigrationToolingError",
    "PluginSpecResolutionError",
    "ProvisioningError",
    "RecordRunError",
    "UvNotFoundError",
)
