"""Execute the two-pass `--airflow-record` / `--airflow-baseline` run.

Runs `pytest --airflow-record=...` on the provisioned 2.x environment to produce a
baseline artifact, then `pytest --airflow-record=... --airflow-baseline=...` on the
provisioned 3.x environment to produce a live artifact and (for the user watching the
stream) pytest's own on-screen summary. Ordinary pytest test-failure exit codes are not
fatal here -- see issue #42's own record contract -- only a missing artifact afterward is.

`default_compute_category_diff` is the real `ComputeCategoryDiff` seam (see
`migration/types.py`): it loads both artifacts through
`pytest_airflow_in_a_box.artifact.load_artifact` and categorizes them through
`pytest_airflow_in_a_box.baseline.compute_categories`, issue #42's own pure
seven-bucket comparison. This module never re-derives a category itself.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/42
    https://docs.python.org/3/library/subprocess.html#subprocess.run
"""

from __future__ import annotations

import logging
import subprocess
from collections.abc import Sequence
from pathlib import Path

import pytest

from pytest_airflow_in_a_box.artifact import Artifact, load_artifact
from pytest_airflow_in_a_box.baseline import compute_categories
from pytest_airflow_in_a_box.migration.provision import (
    ProvisionedEnvironment,
    SubprocessRunner,
    redact_command_line,
)
from pytest_airflow_in_a_box.migration.types import (
    ArtifactError,
    CategoryCounts,
    ComputeCategoryDiff,
    DiffResult,
    RecordRunError,
)

LOGGER = logging.getLogger(__name__)


def run_record(
    *,
    env: ProvisionedEnvironment,
    project_dir: Path,
    record_path: Path,
    baseline_path: Path | None,
    pytest_args: Sequence[str],
    runner: SubprocessRunner = subprocess.run,
) -> None:
    """Run one `pytest --airflow-record` invocation, streaming its output live.

    Parameters:
        env: ProvisionedEnvironment whose `python_path` runs pytest.
        project_dir: pathlib.Path used as the pytest invocation's working directory.
        record_path: pathlib.Path the invocation must write an artifact to.
        baseline_path: pathlib.Path | None passed as `--airflow-baseline`, or None to
            omit it (the 2.x recording pass never compares against a prior baseline).
        pytest_args: Sequence[str] forwarded verbatim after the plugin's own options.
        runner: SubprocessRunner executing the pytest invocation.

    Raises:
        RecordRunError: The invocation exited without writing `record_path`.
    """

    args = [
        str(env.python_path),
        "-m",
        "pytest",
        f"--airflow-record={record_path}",
        *([f"--airflow-baseline={baseline_path}"] if baseline_path is not None else []),
        *pytest_args,
    ]
    LOGGER.info(f"Recording {env.family.name} outcomes: {redact_command_line(args)}")
    try:
        result = runner(args, cwd=str(project_dir), check=False)
    except OSError as error:
        raise RecordRunError(
            f"Could not run the {env.family.name} record pytest invocation: {error}"
        ) from error
    if not record_path.is_file():
        raise RecordRunError(
            f"The {env.family.name} record pytest invocation (exit {result.returncode}) did "
            f"not write an artifact to '{record_path}'. Ordinary test failures are not fatal "
            f"here -- this means the run crashed before `pytest_sessionfinish` could write it."
        )


def _load(path: Path) -> Artifact:
    """Load one migration artifact, translating pytest's error type to this package's own.

    Parameters:
        path: pathlib.Path containing the artifact JSON file.

    Returns:
        Artifact containing the validated artifact.

    Raises:
        ArtifactError: The artifact is absent, malformed, or an unsupported schema
            version (`load_artifact` itself raises `pytest.UsageError`, a type this
            package's callers -- a bare console script, not a pytest session -- have no
            reason to know about).
    """

    try:
        return load_artifact(path)
    except pytest.UsageError as error:
        raise ArtifactError(str(error)) from error


def _check_complete(artifact: Artifact, path: Path, allow_incomplete: bool, role: str) -> None:
    """Reject an incomplete artifact unless the matching override is set.

    Parameters:
        artifact: Artifact loaded from `path`.
        path: pathlib.Path containing the artifact file, named in the error message.
        allow_incomplete: bool overriding the rejection.
        role: str naming the artifact's role (`baseline` or `live`), named in the error
            message and the suggested flag.

    Raises:
        ArtifactError: `artifact["complete"]` is `False` and `allow_incomplete` is
            `False`.
    """

    if artifact["complete"] or allow_incomplete:
        return
    raise ArtifactError(
        f"The {role} artifact `{path}` was recorded from an incomplete session "
        f"(`complete: false`); pass `--allow-incomplete-{role}` to use it anyway."
    )


def default_compute_category_diff(
    baseline_path: Path,
    live_path: Path,
    allow_incomplete_baseline: bool,
    allow_incomplete_live: bool,
) -> DiffResult:
    """Load both recorded artifacts and categorize them via issue #42's own contract.

    Parameters:
        baseline_path: pathlib.Path containing the recorded 2.x baseline artifact.
        live_path: pathlib.Path containing the recorded 3.x live artifact.
        allow_incomplete_baseline: bool overriding the incomplete-baseline error.
        allow_incomplete_live: bool overriding the incomplete-live error.

    Returns:
        DiffResult containing the categorized migration diff.

    Raises:
        ArtifactError: An artifact was absent, malformed, an unsupported schema
            version, or (without the matching override) incomplete.
    """

    baseline = _load(baseline_path)
    live = _load(live_path)
    _check_complete(baseline, baseline_path, allow_incomplete_baseline, "baseline")
    _check_complete(live, live_path, allow_incomplete_live, "live")

    warnings: list[str] = []
    if baseline["airflow_family"] == live["airflow_family"]:
        warnings.append(
            f"Baseline and live artifacts were both recorded on the same Airflow "
            f"family (`{baseline['airflow_family']}`) -- a same-family comparison is "
            f"legitimately useful (e.g. 3.1 -> 3.3), but is not the cross-family "
            f"2.x -> 3.x migration this orchestrator targets by default."
        )

    buckets = compute_categories(baseline, live)
    counts = CategoryCounts(
        regression=len(buckets["regression"]),
        fixed=len(buckets["fixed"]),
        broken_on_both=len(buckets["broken_on_both"]),
        still_passing=len(buckets["still_passing"]),
        gated=len(buckets["gated"]),
        new=len(buckets["new"]),
        missing=len(buckets["missing"]),
    )
    return DiffResult(
        counts=counts,
        regression_nodeids=buckets["regression"],
        fixed_nodeids=buckets["fixed"],
        new_nodeids=buckets["new"],
        missing_nodeids=buckets["missing"],
        warnings=tuple(warnings),
    )


def compute_diff(
    *,
    baseline_path: Path,
    live_path: Path,
    allow_incomplete_baseline: bool,
    allow_incomplete_live: bool,
    compute_category_diff: ComputeCategoryDiff = default_compute_category_diff,
) -> DiffResult:
    """Categorize the recorded baseline and live artifacts through the injected seam.

    Parameters:
        baseline_path: pathlib.Path containing the recorded 2.x baseline artifact.
        live_path: pathlib.Path containing the recorded 3.x live artifact.
        allow_incomplete_baseline: bool overriding the incomplete-baseline error.
        allow_incomplete_live: bool overriding the incomplete-live error.
        compute_category_diff: ComputeCategoryDiff loading and categorizing both
            artifacts; defaults to `default_compute_category_diff`, the real
            implementation.

    Returns:
        DiffResult containing the categorized migration diff.

    Raises:
        ArtifactError: An artifact was absent, malformed, an unsupported schema
            version, or (without the matching override) incomplete.
    """

    return compute_category_diff(
        baseline_path,
        live_path,
        allow_incomplete_baseline,
        allow_incomplete_live,
    )


__all__ = ("compute_diff", "default_compute_category_diff", "run_record")
