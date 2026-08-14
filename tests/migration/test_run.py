"""Test the `pytest --airflow-record` execution and category-diff seam dispatch."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily
from pytest_airflow_in_a_box.artifact import Artifact, Outcome, OutcomeEntry, write_artifact
from pytest_airflow_in_a_box.migration.provision import ProvisionedEnvironment
from pytest_airflow_in_a_box.migration.run import (
    compute_diff,
    default_compute_category_diff,
    run_record,
)
from pytest_airflow_in_a_box.migration.types import (
    ArtifactError,
    CategoryCounts,
    DiffResult,
    RecordRunError,
)


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
    *,
    family: AirflowFamily,
    complete: bool = True,
    outcomes: dict[str, OutcomeEntry],
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


def _env(family: AirflowFamily, python_path: Path) -> ProvisionedEnvironment:
    """Build one `ProvisionedEnvironment` for a record-run test.

    Parameters:
        family: AirflowFamily naming the provisioned Airflow major.
        python_path: pathlib.Path standing in for the venv's `python` executable.

    Returns:
        ProvisionedEnvironment containing the constructed environment.
    """

    return ProvisionedEnvironment(
        family=family,
        airflow_version="3.3.1" if family is AirflowFamily.V3 else "2.11.2",
        python_version="3.12",
        venv_dir=python_path.parent.parent,
        python_path=python_path,
    )


def test_run_record_writes_artifact_and_omits_baseline_flag_when_none(tmp_path: Path) -> None:
    """Run the plugin's own args plus forwarded pytest args, without `--airflow-baseline`."""
    record_path = tmp_path / "baseline.json"
    calls: list[list[str]] = []

    def fake_runner(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        assert kwargs["cwd"] == str(tmp_path)
        assert kwargs["check"] is False
        record_path.write_text("{}", encoding="utf-8")
        return subprocess.CompletedProcess(args, 0, "", "")

    run_record(
        env=_env(AirflowFamily.V2, tmp_path / "venv-v2" / "bin" / "python"),
        project_dir=tmp_path,
        record_path=record_path,
        baseline_path=None,
        pytest_args=["-k", "smoke"],
        runner=fake_runner,
    )

    (args,) = calls
    assert args[0] == str(tmp_path / "venv-v2" / "bin" / "python")
    assert f"--airflow-record={record_path}" in args
    assert not any(part.startswith("--airflow-baseline=") for part in args)
    assert args[-2:] == ["-k", "smoke"]


def test_run_record_includes_baseline_flag_when_given(tmp_path: Path) -> None:
    """Include `--airflow-baseline` on the live (3.x) recording pass."""
    record_path = tmp_path / "live.json"
    baseline_path = tmp_path / "baseline.json"
    calls: list[list[str]] = []

    def fake_runner(args: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        record_path.write_text("{}", encoding="utf-8")
        return subprocess.CompletedProcess(args, 1, "", "")

    run_record(
        env=_env(AirflowFamily.V3, tmp_path / "venv-v3" / "bin" / "python"),
        project_dir=tmp_path,
        record_path=record_path,
        baseline_path=baseline_path,
        pytest_args=[],
        runner=fake_runner,
    )

    (args,) = calls
    assert f"--airflow-baseline={baseline_path}" in args


def test_run_record_raises_when_runner_cannot_exec(tmp_path: Path) -> None:
    """Wrap an OSError from an unexecutable Python interpreter as RecordRunError."""

    def broken_runner(_args: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        raise OSError("no such file or directory")

    with pytest.raises(RecordRunError, match="Could not run"):
        run_record(
            env=_env(AirflowFamily.V2, tmp_path / "venv-v2" / "bin" / "python"),
            project_dir=tmp_path,
            record_path=tmp_path / "baseline.json",
            baseline_path=None,
            pytest_args=[],
            runner=broken_runner,
        )


def test_run_record_raises_when_artifact_absent_after_crash(tmp_path: Path) -> None:
    """Raise RecordRunError when the invocation exits without writing the artifact."""

    def crashing_runner(args: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args, 3, "", "INTERNALERROR")

    with pytest.raises(RecordRunError, match="did not write an artifact"):
        run_record(
            env=_env(AirflowFamily.V2, tmp_path / "venv-v2" / "bin" / "python"),
            project_dir=tmp_path,
            record_path=tmp_path / "baseline.json",
            baseline_path=None,
            pytest_args=[],
            runner=crashing_runner,
        )


def test_run_record_tolerates_ordinary_test_failure_exit_code(tmp_path: Path) -> None:
    """Do not raise when the invocation fails tests but still writes its artifact.

    Asserts `check=False` too: the whole point of this test is that a nonzero pytest
    exit code is not fatal, a property real `subprocess.run(..., check=True)` would
    outright break by raising `CalledProcessError` before this function ever inspects
    the return code. A fake that ignored `check` could not catch that regression.
    """
    record_path = tmp_path / "baseline.json"

    def failing_but_recording_runner(
        args: list[str], **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        assert kwargs["check"] is False
        record_path.write_text("{}", encoding="utf-8")
        return subprocess.CompletedProcess(args, 1, "", "")

    run_record(
        env=_env(AirflowFamily.V2, tmp_path / "venv-v2" / "bin" / "python"),
        project_dir=tmp_path,
        record_path=record_path,
        baseline_path=None,
        pytest_args=[],
        runner=failing_but_recording_runner,
    )

    assert record_path.is_file()


def test_default_compute_category_diff_categorizes_via_the_real_contract(tmp_path: Path) -> None:
    """Categorize real artifacts through the real `pytest_airflow_in_a_box.baseline` contract.

    One nodeid per row of `docs/guide/migration-diff.md`'s seven-category table, so this
    is a genuine integration test of the wiring, not a re-derivation of the algorithm.
    """
    baseline_path = tmp_path / "baseline.json"
    live_path = tmp_path / "live.json"
    write_artifact(
        baseline_path,
        _artifact(
            family=AirflowFamily.V2,
            outcomes={
                "test_a.py::still_passing": _entry(Outcome.PASSED),
                "test_a.py::regressed": _entry(Outcome.PASSED),
                "test_a.py::fixed": _entry(Outcome.FAILED),
                "test_a.py::broken_on_both": _entry(Outcome.FAILED),
                "test_a.py::gated_on_baseline": _entry(Outcome.SKIPPED, gated=True),
                "test_a.py::missing_live": _entry(Outcome.PASSED),
            },
        ),
    )
    write_artifact(
        live_path,
        _artifact(
            family=AirflowFamily.V3,
            outcomes={
                "test_a.py::still_passing": _entry(Outcome.PASSED),
                "test_a.py::regressed": _entry(Outcome.FAILED),
                "test_a.py::fixed": _entry(Outcome.PASSED),
                "test_a.py::broken_on_both": _entry(Outcome.ERROR),
                "test_a.py::gated_on_baseline": _entry(Outcome.PASSED),
                "test_a.py::new_live": _entry(Outcome.PASSED),
            },
        ),
    )

    diff = default_compute_category_diff(baseline_path, live_path, False, False)

    assert diff.counts == CategoryCounts(
        regression=1, fixed=1, broken_on_both=1, still_passing=1, gated=1, new=1, missing=1
    )
    assert diff.regression_nodeids == ("test_a.py::regressed",)
    assert diff.fixed_nodeids == ("test_a.py::fixed",)
    assert diff.new_nodeids == ("test_a.py::new_live",)
    assert diff.missing_nodeids == ("test_a.py::missing_live",)
    assert diff.warnings == ()


def test_default_compute_category_diff_wraps_a_missing_artifact(tmp_path: Path) -> None:
    """Translate `load_artifact`'s `pytest.UsageError` into this package's `ArtifactError`."""

    with pytest.raises(ArtifactError, match="Could not read artifact"):
        default_compute_category_diff(
            tmp_path / "does-not-exist-baseline.json",
            tmp_path / "does-not-exist-live.json",
            False,
            False,
        )


def test_default_compute_category_diff_rejects_incomplete_baseline_without_override(
    tmp_path: Path,
) -> None:
    """Fail closed on an incomplete (`complete: false`) baseline artifact by default."""
    baseline_path = tmp_path / "baseline.json"
    live_path = tmp_path / "live.json"
    write_artifact(
        baseline_path,
        _artifact(family=AirflowFamily.V2, complete=False, outcomes={}),
    )
    write_artifact(live_path, _artifact(family=AirflowFamily.V3, outcomes={}))

    with pytest.raises(ArtifactError, match="--allow-incomplete-baseline"):
        default_compute_category_diff(baseline_path, live_path, False, False)


def test_default_compute_category_diff_accepts_incomplete_baseline_with_override(
    tmp_path: Path,
) -> None:
    """Accept an incomplete baseline artifact when the override is set."""
    baseline_path = tmp_path / "baseline.json"
    live_path = tmp_path / "live.json"
    write_artifact(
        baseline_path,
        _artifact(family=AirflowFamily.V2, complete=False, outcomes={}),
    )
    write_artifact(live_path, _artifact(family=AirflowFamily.V3, outcomes={}))

    diff = default_compute_category_diff(baseline_path, live_path, True, False)

    assert diff.counts == CategoryCounts(0, 0, 0, 0, 0, 0, 0)


def test_default_compute_category_diff_rejects_incomplete_live_without_override(
    tmp_path: Path,
) -> None:
    """Fail closed on an incomplete (`complete: false`) live artifact by default."""
    baseline_path = tmp_path / "baseline.json"
    live_path = tmp_path / "live.json"
    write_artifact(baseline_path, _artifact(family=AirflowFamily.V2, outcomes={}))
    write_artifact(
        live_path,
        _artifact(family=AirflowFamily.V3, complete=False, outcomes={}),
    )

    with pytest.raises(ArtifactError, match="--allow-incomplete-live"):
        default_compute_category_diff(baseline_path, live_path, False, False)


def test_default_compute_category_diff_warns_on_same_family_comparison(tmp_path: Path) -> None:
    """Warn, but not fail, when baseline and live share the same recorded Airflow family."""
    baseline_path = tmp_path / "baseline.json"
    live_path = tmp_path / "live.json"
    write_artifact(baseline_path, _artifact(family=AirflowFamily.V3, outcomes={}))
    write_artifact(live_path, _artifact(family=AirflowFamily.V3, outcomes={}))

    diff = default_compute_category_diff(baseline_path, live_path, False, False)

    assert len(diff.warnings) == 1
    assert "same Airflow family" in diff.warnings[0]


def test_compute_diff_uses_the_real_seam_by_default(tmp_path: Path) -> None:
    """Use `default_compute_category_diff` -- the real wiring -- when none is injected."""
    baseline_path = tmp_path / "baseline.json"
    live_path = tmp_path / "live.json"
    write_artifact(
        baseline_path,
        _artifact(family=AirflowFamily.V2, outcomes={"test_a.py::t": _entry(Outcome.PASSED)}),
    )
    write_artifact(
        live_path,
        _artifact(family=AirflowFamily.V3, outcomes={"test_a.py::t": _entry(Outcome.FAILED)}),
    )

    diff = compute_diff(
        baseline_path=baseline_path,
        live_path=live_path,
        allow_incomplete_baseline=False,
        allow_incomplete_live=False,
    )

    assert diff.counts.regression == 1
    assert diff.regression_nodeids == ("test_a.py::t",)


def test_compute_diff_forwards_arguments_and_returns_seam_result(tmp_path: Path) -> None:
    """Forward every argument to the injected seam and return its result verbatim."""
    expected = DiffResult(
        counts=CategoryCounts(
            regression=1,
            fixed=0,
            broken_on_both=0,
            still_passing=1,
            gated=0,
            new=0,
            missing=0,
        ),
        regression_nodeids=("tests/test_a.py::test_a",),
        fixed_nodeids=(),
        new_nodeids=(),
        missing_nodeids=(),
        warnings=(),
    )
    calls: list[tuple[Path, Path, bool, bool]] = []

    def fake_seam(
        baseline_path: Path, live_path: Path, allow_baseline: bool, allow_live: bool
    ) -> DiffResult:
        calls.append((baseline_path, live_path, allow_baseline, allow_live))
        return expected

    baseline_path = tmp_path / "baseline.json"
    live_path = tmp_path / "live.json"

    result = compute_diff(
        baseline_path=baseline_path,
        live_path=live_path,
        allow_incomplete_baseline=True,
        allow_incomplete_live=False,
        compute_category_diff=fake_seam,
    )

    assert result is expected
    assert calls == [(baseline_path, live_path, True, False)]


def test_compute_diff_propagates_seam_exception(tmp_path: Path) -> None:
    """Propagate an ArtifactError the seam raises (for example, a schema mismatch)."""

    def failing_seam(*_args: object) -> DiffResult:
        raise ArtifactError("schema_version mismatch")

    with pytest.raises(ArtifactError, match="schema_version mismatch"):
        compute_diff(
            baseline_path=tmp_path / "baseline.json",
            live_path=tmp_path / "live.json",
            allow_incomplete_baseline=False,
            allow_incomplete_live=False,
            compute_category_diff=failing_seam,
        )
