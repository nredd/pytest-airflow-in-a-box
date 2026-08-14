"""Execute the two-pass `--airflow-record` / `--airflow-baseline` run.

Runs `pytest --airflow-record=...` on the provisioned 2.x environment to produce a
baseline artifact, then `pytest --airflow-record=... --airflow-baseline=...` on the
provisioned 3.x environment to produce a live artifact and (for the user watching the
stream) pytest's own on-screen summary. Ordinary pytest test-failure exit codes are not
fatal here -- see issue #42's own record contract -- only a missing artifact afterward is.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/42
    https://docs.python.org/3/library/subprocess.html#subprocess.run
"""

from __future__ import annotations

import logging
import subprocess
from collections.abc import Sequence
from pathlib import Path

from pytest_airflow_in_a_box.migration.provision import ProvisionedEnvironment, SubprocessRunner
from pytest_airflow_in_a_box.migration.types import (
    ArtifactError,
    ComputeCategoryDiff,
    DiffResult,
    RecordRunError,
)

LOGGER = logging.getLogger(__name__)


def unavailable_compute_category_diff(
    baseline_path: Path,
    live_path: Path,
    allow_incomplete_baseline: bool,
    allow_incomplete_live: bool,
) -> DiffResult:
    """Report that no category-computation implementation is wired in yet.

    This package ships ahead of issue #42's artifact contract by design (see
    `migration/types.py`); this is the default `ComputeCategoryDiff` until a rebase onto
    #42 replaces it with a real loader plus `compute_categories(baseline, live)` call.

    Parameters:
        baseline_path: pathlib.Path containing the recorded 2.x baseline artifact.
        live_path: pathlib.Path containing the recorded 3.x live artifact.
        allow_incomplete_baseline: bool ignored by this placeholder.
        allow_incomplete_live: bool ignored by this placeholder.

    Raises:
        ArtifactError: Always -- no category-computation implementation is wired in.
    """

    del allow_incomplete_baseline, allow_incomplete_live
    raise ArtifactError(
        "No category-computation implementation is wired in: this build of "
        "`airflow-migration-diff` predates issue #42's artifact contract. Recorded "
        f"artifacts are available at '{baseline_path}' and '{live_path}' for manual "
        "inspection; rerun once this package has been rebased onto #42."
    )


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
    LOGGER.info(f"Recording {env.family.name} outcomes: {' '.join(args)}")
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


def compute_diff(
    *,
    baseline_path: Path,
    live_path: Path,
    allow_incomplete_baseline: bool,
    allow_incomplete_live: bool,
    compute_category_diff: ComputeCategoryDiff = unavailable_compute_category_diff,
) -> DiffResult:
    """Categorize the recorded baseline and live artifacts through the injected seam.

    Parameters:
        baseline_path: pathlib.Path containing the recorded 2.x baseline artifact.
        live_path: pathlib.Path containing the recorded 3.x live artifact.
        allow_incomplete_baseline: bool overriding the incomplete-baseline error.
        allow_incomplete_live: bool overriding the incomplete-live error.
        compute_category_diff: ComputeCategoryDiff loading and categorizing both
            artifacts; defaults to a placeholder that always raises `ArtifactError`.

    Returns:
        DiffResult containing the categorized migration diff.

    Raises:
        ArtifactError: An artifact was absent, malformed, schema-mismatched, or (without
            the matching override) incomplete.
    """

    return compute_category_diff(
        baseline_path,
        live_path,
        allow_incomplete_baseline,
        allow_incomplete_live,
    )


__all__ = ("compute_diff", "run_record", "unavailable_compute_category_diff")
