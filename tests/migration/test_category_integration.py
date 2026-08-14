"""Table-driven tests for the seam boundary this package owns pre-#42.

This package never computes a category itself (see `migration/types.py`): the seven
buckets, the schema-version/family/`complete` checks, and the `--allow-incomplete-*`
overrides all belong to issue #42's `compute_categories`. What this package DOES own is
faithfully driving that seam from `execute()` through to `render_diff()` -- passing the
right paths and override flags in, and turning whatever comes back (a `DiffResult` or a
raised `ArtifactError`) into the right exit code and report text.

Each fake `compute_category_diff` below stands in for a slice of #42's real contract,
shaped from issue #42's own body (the seven categories, the schema/family/complete
rules, the two override flags). When this package is rebased onto #42, these fakes are
replaced by the real `artifact.py` loader plus `compute_categories`, and these same
scenarios should keep passing unchanged -- the whole point of table-driving them here.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily
from pytest_airflow_in_a_box.migration import cli
from pytest_airflow_in_a_box.migration.provision import ProvisionedEnvironment
from pytest_airflow_in_a_box.migration.types import ArtifactError, CategoryCounts, DiffResult

_ZERO_COUNTS = {
    "regression": 0,
    "fixed": 0,
    "broken_on_both": 0,
    "still_passing": 0,
    "gated": 0,
    "new": 0,
    "missing": 0,
}


def _counts(**overrides: int) -> CategoryCounts:
    """Build a `CategoryCounts` with every field zeroed except the given overrides.

    Parameters:
        overrides: int values keyed by field name to override.

    Returns:
        CategoryCounts containing the requested counts.
    """

    return CategoryCounts(**{**_ZERO_COUNTS, **overrides})


def _env(family: AirflowFamily) -> ProvisionedEnvironment:
    """Build a canned `ProvisionedEnvironment` for one Airflow family.

    Parameters:
        family: AirflowFamily naming the provisioned Airflow major.

    Returns:
        ProvisionedEnvironment containing the constructed environment.
    """

    return ProvisionedEnvironment(
        family=family,
        airflow_version="2.11.2" if family is AirflowFamily.V2 else "3.3.1",
        python_version="3.12",
        venv_dir=Path(f"/work/venv-{family.name.lower()}"),
        python_path=Path(f"/work/venv-{family.name.lower()}/bin/python"),
    )


def _config(work_dir: Path) -> cli.ResolvedConfig:
    """Build a minimal `ResolvedConfig` rooted at a pytest-managed scratch directory.

    Parameters:
        work_dir: pathlib.Path used as `--work-dir`, cleaned up by pytest's `tmp_path`.

    Returns:
        ResolvedConfig containing a minimal, fully-populated configuration.
    """

    args = cli.build_parser().parse_args(["--uv-path", "/opt/uv", "--work-dir", str(work_dir)])
    return cli.resolve_config(args, [])


def _execute(diff: DiffResult, work_dir: Path, **config_overrides: object) -> tuple[int, str]:
    """Run `execute()` with a fake seam that always returns `diff`.

    Parameters:
        diff: DiffResult the fake seam returns regardless of its arguments.
        work_dir: pathlib.Path used as `--work-dir`, cleaned up by pytest's `tmp_path`.
        config_overrides: object values overriding `_config()`'s fields.

    Returns:
        tuple[int, str] containing `execute()`'s exit code and rendered report.
    """

    cfg = dataclasses.replace(_config(work_dir), **config_overrides)
    return cli.execute(
        cfg,
        provision_environment=lambda *, family, **_kwargs: _env(family),
        run_record=lambda **_kwargs: None,
        compute_category_diff=lambda *_args: diff,
    )


@pytest.mark.parametrize(
    ("scenario", "diff", "expected_exit", "expected_snippets"),
    [
        (
            "regression",
            DiffResult(
                counts=_counts(regression=1, still_passing=4),
                regression_nodeids=("tests/test_a.py::test_regressed",),
                fixed_nodeids=(),
                new_nodeids=(),
                missing_nodeids=(),
                warnings=(),
            ),
            cli.EXIT_REGRESSIONS,
            ("REGRESSIONS FOUND", "tests/test_a.py::test_regressed"),
        ),
        (
            "fixed",
            DiffResult(
                counts=_counts(fixed=1, still_passing=4),
                regression_nodeids=(),
                fixed_nodeids=("tests/test_b.py::test_fixed",),
                new_nodeids=(),
                missing_nodeids=(),
                warnings=(),
            ),
            cli.EXIT_OK,
            ("no regressions", "tests/test_b.py::test_fixed"),
        ),
        (
            "broken_on_both",
            DiffResult(
                counts=_counts(broken_on_both=2, still_passing=4),
                regression_nodeids=(),
                fixed_nodeids=(),
                new_nodeids=(),
                missing_nodeids=(),
                warnings=(),
            ),
            cli.EXIT_OK,
            ("Broken on both: 2",),
        ),
        (
            "still_passing",
            DiffResult(
                counts=_counts(still_passing=10),
                regression_nodeids=(),
                fixed_nodeids=(),
                new_nodeids=(),
                missing_nodeids=(),
                warnings=(),
            ),
            cli.EXIT_OK,
            ("Still passing: 10",),
        ),
        (
            "gated_either_side",
            DiffResult(
                counts=_counts(gated=3, still_passing=4),
                regression_nodeids=(),
                fixed_nodeids=(),
                new_nodeids=(),
                missing_nodeids=(),
                warnings=(),
            ),
            cli.EXIT_OK,
            ("Gated: 3",),
        ),
        (
            "new",
            DiffResult(
                counts=_counts(new=1, still_passing=4),
                regression_nodeids=(),
                fixed_nodeids=(),
                new_nodeids=("tests/test_c.py::test_new",),
                missing_nodeids=(),
                warnings=(),
            ),
            cli.EXIT_OK,
            ("tests/test_c.py::test_new",),
        ),
        (
            "missing",
            DiffResult(
                counts=_counts(missing=1, still_passing=4),
                regression_nodeids=(),
                fixed_nodeids=(),
                new_nodeids=(),
                missing_nodeids=("tests/test_d.py::test_missing",),
                warnings=(),
            ),
            cli.EXIT_OK,
            ("tests/test_d.py::test_missing",),
        ),
        (
            "neutral_projections_never_appear_as_a_category",
            # xfailed/xpassed/non-gated-skipped fold to neutral on either end (issue #42):
            # they must never surface as their own bucket, and never as a regression/fix.
            # A neutral-only transition is represented here as an untouched still_passing
            # count, with no regression and no fixed nodeids.
            DiffResult(
                counts=_counts(still_passing=4, broken_on_both=1),
                regression_nodeids=(),
                fixed_nodeids=(),
                new_nodeids=(),
                missing_nodeids=(),
                warnings=(),
            ),
            cli.EXIT_OK,
            ("Regression: 0", "Fixed: 0"),
        ),
        (
            "cross_family_warning",
            # Same-family baseline/live is a warning, not an error (issue #42): still
            # exit 0 absent an actual regression, but the warning must render.
            DiffResult(
                counts=_counts(still_passing=4),
                regression_nodeids=(),
                fixed_nodeids=(),
                new_nodeids=(),
                missing_nodeids=(),
                warnings=("baseline and live artifacts are the same Airflow family",),
            ),
            cli.EXIT_OK,
            ("## Warnings", "same Airflow family"),
        ),
    ],
)
def test_category_scenarios_drive_exit_code_and_report(
    scenario: str,
    diff: DiffResult,
    expected_exit: int,
    expected_snippets: tuple[str, ...],
    tmp_path: Path,
) -> None:
    """Drive every named category scenario from the seam through to the rendered report."""
    del scenario
    exit_code, report = _execute(diff, tmp_path)

    assert exit_code == expected_exit
    for snippet in expected_snippets:
        assert snippet in report


def test_category_counts_has_exactly_the_seven_documented_fields() -> None:
    """Keep `CategoryCounts` limited to issue #42's seven buckets -- no separate neutral."""
    field_names = {field.name for field in dataclasses.fields(CategoryCounts)}

    assert field_names == {
        "regression",
        "fixed",
        "broken_on_both",
        "still_passing",
        "gated",
        "new",
        "missing",
    }


def test_schema_mismatch_propagates_as_artifact_error(tmp_path: Path) -> None:
    """Propagate a schema-version mismatch as ArtifactError (an error, not a warning)."""

    def mismatched_schema_seam(*_args: object) -> DiffResult:
        raise ArtifactError("schema_version mismatch: baseline=1 live=2")

    with pytest.raises(ArtifactError, match="schema_version mismatch"):
        cli.execute(
            _config(tmp_path),
            provision_environment=lambda *, family, **_kwargs: _env(family),
            run_record=lambda **_kwargs: None,
            compute_category_diff=mismatched_schema_seam,
        )


def test_incomplete_baseline_without_override_fails_closed(tmp_path: Path) -> None:
    """Fail an incomplete (`complete: false`) baseline without `--allow-incomplete-baseline`."""

    def seam(
        _baseline_path: Path, _live_path: Path, allow_incomplete_baseline: bool, _allow_live: bool
    ) -> DiffResult:
        if not allow_incomplete_baseline:
            raise ArtifactError("baseline artifact recorded complete: false")
        return DiffResult(
            counts=_counts(still_passing=1),
            regression_nodeids=(),
            fixed_nodeids=(),
            new_nodeids=(),
            missing_nodeids=(),
            warnings=(),
        )

    cfg = _config(tmp_path)
    assert cfg.allow_incomplete_baseline is False

    with pytest.raises(ArtifactError, match="complete: false"):
        cli.execute(
            cfg,
            provision_environment=lambda *, family, **_kwargs: _env(family),
            run_record=lambda **_kwargs: None,
            compute_category_diff=seam,
        )


def test_allow_incomplete_baseline_override_reaches_the_seam(tmp_path: Path) -> None:
    """Forward `--allow-incomplete-baseline` through to the seam, unblocking the run."""

    def seam(
        _baseline_path: Path, _live_path: Path, allow_incomplete_baseline: bool, _allow_live: bool
    ) -> DiffResult:
        if not allow_incomplete_baseline:
            raise ArtifactError("baseline artifact recorded complete: false")
        return DiffResult(
            counts=_counts(still_passing=1),
            regression_nodeids=(),
            fixed_nodeids=(),
            new_nodeids=(),
            missing_nodeids=(),
            warnings=(),
        )

    cfg = dataclasses.replace(_config(tmp_path), allow_incomplete_baseline=True)

    exit_code, _report = cli.execute(
        cfg,
        provision_environment=lambda *, family, **_kwargs: _env(family),
        run_record=lambda **_kwargs: None,
        compute_category_diff=seam,
    )

    assert exit_code == cli.EXIT_OK


def test_incomplete_live_without_override_fails_closed(tmp_path: Path) -> None:
    """Fail an incomplete (`complete: false`) live artifact without `--allow-incomplete-live`."""

    def seam(
        _baseline_path: Path, _live_path: Path, _allow_baseline: bool, allow_incomplete_live: bool
    ) -> DiffResult:
        if not allow_incomplete_live:
            raise ArtifactError("live artifact recorded complete: false")
        return DiffResult(
            counts=_counts(still_passing=1),
            regression_nodeids=(),
            fixed_nodeids=(),
            new_nodeids=(),
            missing_nodeids=(),
            warnings=(),
        )

    with pytest.raises(ArtifactError, match="complete: false"):
        cli.execute(
            _config(tmp_path),
            provision_environment=lambda *, family, **_kwargs: _env(family),
            run_record=lambda **_kwargs: None,
            compute_category_diff=seam,
        )
