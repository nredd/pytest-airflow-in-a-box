"""Test the `pytest --airflow-record` execution and category-diff seam dispatch."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily
from pytest_airflow_in_a_box.migration.provision import ProvisionedEnvironment
from pytest_airflow_in_a_box.migration.run import (
    compute_diff,
    run_record,
    unavailable_compute_category_diff,
)
from pytest_airflow_in_a_box.migration.types import (
    ArtifactError,
    CategoryCounts,
    DiffResult,
    RecordRunError,
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
    """Do not raise when the invocation fails tests but still writes its artifact."""
    record_path = tmp_path / "baseline.json"

    def failing_but_recording_runner(
        args: list[str], **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
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


def test_unavailable_compute_category_diff_names_issue_42(tmp_path: Path) -> None:
    """Report that no category-computation implementation is wired in yet."""
    baseline_path = tmp_path / "baseline.json"
    live_path = tmp_path / "live.json"

    with pytest.raises(ArtifactError, match="issue #42's artifact contract") as excinfo:
        unavailable_compute_category_diff(baseline_path, live_path, False, False)

    assert str(baseline_path) in str(excinfo.value)
    assert str(live_path) in str(excinfo.value)


def test_compute_diff_uses_unavailable_seam_by_default(tmp_path: Path) -> None:
    """Default to the unavailable seam, raising ArtifactError, when none is injected."""

    with pytest.raises(ArtifactError):
        compute_diff(
            baseline_path=tmp_path / "baseline.json",
            live_path=tmp_path / "live.json",
            allow_incomplete_baseline=False,
            allow_incomplete_live=False,
        )


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
