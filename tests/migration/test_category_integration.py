"""Table-driven tests exercising the seam boundary through the REAL #42 contract.

Post-#42-rebase: `run_record` is faked (no real `uv`/`pytest` subprocess), but it
writes genuine `Artifact` JSON via `pytest_airflow_in_a_box.artifact.write_artifact`,
and `compute_category_diff` is left at its real default
(`migration.run.default_compute_category_diff`) rather than a canned `DiffResult`. Every
scenario below therefore runs through the actual `load_artifact` +
`pytest_airflow_in_a_box.baseline.compute_categories` this package depends on, exactly
as `docs/guide/migration-diff.md` documents it -- this package supplies real recorded
artifacts and asserts on the real categorized result, never a stand-in.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily
from pytest_airflow_in_a_box.artifact import Artifact, Outcome, OutcomeEntry, write_artifact
from pytest_airflow_in_a_box.migration import cli
from pytest_airflow_in_a_box.migration.provision import ProvisionedEnvironment
from pytest_airflow_in_a_box.migration.types import ArtifactError, CategoryCounts


def _entry(outcome: Outcome, *, gated: bool = False) -> OutcomeEntry:
    """Build one minimal, schema-valid `OutcomeEntry`.

    Parameters:
        outcome: Outcome containing the recorded outcome.
        gated: bool recording whether a family marker would gate this item.

    Returns:
        OutcomeEntry containing the constructed entry. `phase` is set to `teardown`
        when `outcome` is `error` and left `None` otherwise, matching `artifact.py`'s
        `phase` set-if-and-only-if-`error` invariant.
    """

    phase = "teardown" if outcome is Outcome.ERROR else None
    return OutcomeEntry(outcome=outcome.value, phase=phase, gated=gated, duration=0.01)


def _artifact(
    *, family: AirflowFamily, complete: bool = True, outcomes: dict[str, OutcomeEntry]
) -> Artifact:
    """Build one minimal, schema-valid `Artifact`.

    Parameters:
        family: AirflowFamily naming the recorded Airflow family.
        complete: bool recording whether the recording session finished cleanly.
        outcomes: dict[str, OutcomeEntry] mapping nodeid to its recorded outcome.

    Returns:
        Artifact containing the constructed artifact.
    """

    return Artifact(
        schema_version=1,
        plugin_version="0.4.0",
        airflow_version="2.11.2" if family is AirflowFamily.V2 else "3.3.1",
        airflow_family=family.value,
        python_version="3.12.0",
        pytest_version="8.4.2",
        created_at="2026-08-14T00:00:00+00:00",
        complete=complete,
        outcomes=outcomes,
    )


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


def _execute_through_real_seam(
    baseline_artifact: Artifact,
    live_artifact: Artifact,
    work_dir: Path,
    **config_overrides: object,
) -> tuple[int, str]:
    """Run `execute()` with faked provisioning/recording but the REAL category seam.

    `run_record`'s fake writes `baseline_artifact`/`live_artifact` to whatever path
    `execute()` asks it to record to (distinguishing the two passes by `baseline_path`,
    exactly as `run.run_record` does: `None` on the 2.x pass, set on the 3.x pass) --
    `compute_category_diff` is left at its real default, so the whole chain from
    "recorded artifact on disk" through `compute_categories` to the rendered report
    runs for real.

    Parameters:
        baseline_artifact: Artifact written to the 2.x recording pass's `record_path`.
        live_artifact: Artifact written to the 3.x recording pass's `record_path`.
        work_dir: pathlib.Path used as `--work-dir`, cleaned up by pytest's `tmp_path`.
        config_overrides: object values overriding `_config()`'s fields.

    Returns:
        tuple[int, str] containing `execute()`'s exit code and rendered report.
    """

    def fake_run_record(
        *,
        env: ProvisionedEnvironment,
        project_dir: Path,
        record_path: Path,
        baseline_path: Path | None,
        pytest_args: tuple[str, ...],
    ) -> None:
        del env, project_dir, pytest_args
        write_artifact(record_path, baseline_artifact if baseline_path is None else live_artifact)

    cfg = dataclasses.replace(_config(work_dir), **config_overrides)
    return cli.execute(
        cfg,
        provision_environment=lambda *, family, **_kwargs: _env(family),
        run_record=fake_run_record,
    )


@pytest.mark.parametrize(
    ("scenario", "baseline_outcome", "live_outcome", "expected_exit", "expected_snippets"),
    [
        ("regression", Outcome.PASSED, Outcome.FAILED, cli.EXIT_REGRESSIONS, ("- regression: 1",)),
        ("fixed", Outcome.FAILED, Outcome.PASSED, cli.EXIT_OK, ("- fixed: 1",)),
        (
            "broken_on_both",
            Outcome.FAILED,
            Outcome.ERROR,
            cli.EXIT_OK,
            ("- broken-on-both: 1",),
        ),
        ("still_passing", Outcome.PASSED, Outcome.PASSED, cli.EXIT_OK, ("- still-passing: 1",)),
        (
            "neutral_baseline_never_becomes_a_fix",
            # A neutral (non-gated skip) baseline outcome must never count as `fixed`,
            # even though the live side passes -- it folds to `still-passing` per
            # docs/guide/migration-diff.md's projection table.
            Outcome.SKIPPED,
            Outcome.PASSED,
            cli.EXIT_OK,
            ("- still-passing: 1", "- fixed: 0", "- regression: 0"),
        ),
        (
            "neutral_live_never_becomes_a_regression",
            Outcome.PASSED,
            Outcome.SKIPPED,
            cli.EXIT_OK,
            ("- still-passing: 1", "- regression: 0"),
        ),
    ],
)
def test_category_scenarios_drive_exit_code_and_report_through_the_real_seam(
    scenario: str,
    baseline_outcome: Outcome,
    live_outcome: Outcome,
    expected_exit: int,
    expected_snippets: tuple[str, ...],
    tmp_path: Path,
) -> None:
    """Drive each pass/fail/neutral transition through the real `compute_categories`."""
    nodeid = f"test_a.py::test_{scenario}"
    baseline = _artifact(family=AirflowFamily.V2, outcomes={nodeid: _entry(baseline_outcome)})
    live = _artifact(family=AirflowFamily.V3, outcomes={nodeid: _entry(live_outcome)})

    exit_code, report = _execute_through_real_seam(baseline, live, tmp_path)

    assert exit_code == expected_exit
    for snippet in expected_snippets:
        assert snippet in report
    if expected_exit == cli.EXIT_REGRESSIONS:
        assert f"`{nodeid}`" in report


def test_gated_either_side_is_never_a_regression_or_fix(tmp_path: Path) -> None:
    """Bucket a family-marker-gated nodeid as `gated`, never regression/fixed/broken."""
    nodeid = "test_a.py::test_v2_only"
    baseline = _artifact(family=AirflowFamily.V2, outcomes={nodeid: _entry(Outcome.PASSED)})
    live = _artifact(
        family=AirflowFamily.V3, outcomes={nodeid: _entry(Outcome.SKIPPED, gated=True)}
    )

    exit_code, report = _execute_through_real_seam(baseline, live, tmp_path)

    assert exit_code == cli.EXIT_OK
    assert "- gated: 1" in report
    assert "- regression: 0" in report


def test_new_and_missing_nodeids_render(tmp_path: Path) -> None:
    """Bucket a live-only nodeid as `new` and a baseline-only nodeid as `missing`."""
    baseline = _artifact(
        family=AirflowFamily.V2,
        outcomes={"test_a.py::test_missing": _entry(Outcome.PASSED)},
    )
    live = _artifact(
        family=AirflowFamily.V3,
        outcomes={"test_a.py::test_new": _entry(Outcome.PASSED)},
    )

    exit_code, report = _execute_through_real_seam(baseline, live, tmp_path)

    assert exit_code == cli.EXIT_OK
    assert "- new: 1" in report
    assert "- missing: 1" in report
    assert "`test_a.py::test_new`" in report
    assert "`test_a.py::test_missing`" in report


def test_cross_family_warning_renders_without_failing(tmp_path: Path) -> None:
    """Warn, but still exit 0 absent a real regression, on a same-family comparison."""
    baseline = _artifact(
        family=AirflowFamily.V3, outcomes={"test_a.py::t": _entry(Outcome.PASSED)}
    )
    live = _artifact(family=AirflowFamily.V3, outcomes={"test_a.py::t": _entry(Outcome.PASSED)})

    exit_code, report = _execute_through_real_seam(baseline, live, tmp_path)

    assert exit_code == cli.EXIT_OK
    assert "## Warnings" in report
    assert "same Airflow family" in report


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


def test_schema_mismatch_propagates_as_an_error(tmp_path: Path) -> None:
    """Reject an artifact whose `schema_version` this plugin does not support."""
    baseline_artifact: Artifact = {
        **_artifact(family=AirflowFamily.V2, outcomes={}),
        "schema_version": 2,
    }
    live = _artifact(family=AirflowFamily.V3, outcomes={})

    with pytest.raises(ArtifactError, match="schema_version") as excinfo:
        _execute_through_real_seam(baseline_artifact, live, tmp_path)
    assert "schema_version" in str(excinfo.value)


def test_incomplete_baseline_without_override_fails_closed(tmp_path: Path) -> None:
    """Fail an incomplete (`complete: false`) baseline without `--allow-incomplete-baseline`."""
    baseline = _artifact(family=AirflowFamily.V2, complete=False, outcomes={})
    live = _artifact(family=AirflowFamily.V3, outcomes={})

    with pytest.raises(ArtifactError, match="complete: false"):
        _execute_through_real_seam(baseline, live, tmp_path)


def test_allow_incomplete_baseline_override_reaches_the_real_seam(tmp_path: Path) -> None:
    """Forward `--allow-incomplete-baseline` through to the real seam, unblocking the run."""
    baseline = _artifact(family=AirflowFamily.V2, complete=False, outcomes={})
    live = _artifact(family=AirflowFamily.V3, outcomes={})

    exit_code, _report = _execute_through_real_seam(
        baseline, live, tmp_path, allow_incomplete_baseline=True
    )

    assert exit_code == cli.EXIT_OK


def test_incomplete_live_without_override_fails_closed(tmp_path: Path) -> None:
    """Fail an incomplete (`complete: false`) live artifact without `--allow-incomplete-live`."""
    baseline = _artifact(family=AirflowFamily.V2, outcomes={})
    live = _artifact(family=AirflowFamily.V3, complete=False, outcomes={})

    with pytest.raises(ArtifactError, match="complete: false"):
        _execute_through_real_seam(baseline, live, tmp_path)


def test_allow_incomplete_live_override_reaches_the_real_seam(tmp_path: Path) -> None:
    """Forward `--allow-incomplete-live` through to the real seam, unblocking the run."""
    baseline = _artifact(family=AirflowFamily.V2, outcomes={})
    live = _artifact(family=AirflowFamily.V3, complete=False, outcomes={})

    exit_code, _report = _execute_through_real_seam(
        baseline, live, tmp_path, allow_incomplete_live=True
    )

    assert exit_code == cli.EXIT_OK
