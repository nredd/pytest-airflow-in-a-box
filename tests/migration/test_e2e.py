"""Real end-to-end coverage for `airflow-migration-diff`, against real `uv`/network.

Opt-in only (`migration_e2e` marker, `make test-migration-e2e`): every other test in
`tests/migration/` proves the orchestrator's own logic through injected seams, with no
real `uv`, network, or Airflow install. This module is the one place that provisions
real environments, which is slow (two fresh Airflow installs) and network-dependent, so
it is excluded from `make test` and `make all` (see `Makefile`, `tests/conftest.py`).

PRE-#42 SCOPE: issue #42's `--airflow-record`/`--airflow-baseline` flags do not exist in
this codebase yet (see `migration/types.py`), so a real run cannot record an artifact or
detect a regression today. This test instead proves the part that IS real right now:
`uv` provisioning genuinely builds both environments end to end. It expects the run to
fail cleanly at the recording step (`RecordRunError`, exit code 2) because pytest
rejects the still-nonexistent `--airflow-record` flag as an unrecognized argument.

POST-#42 TODO: once this package is rebased onto #42 (see `migration/run.py`'s
`unavailable_compute_category_diff` and issue #44's sequencing note), replace this
xfail-shaped assertion with the real empirical case: plant one deliberate regression in
a tiny throwaway suite, assert both artifacts recorded `complete: true`, assert the
regression was categorized, and assert exit code 1. Capture real output for the PR body.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/42
    https://github.com/nredd/pytest-airflow-in-a-box/issues/44
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from pytest_airflow_in_a_box.migration import cli

pytestmark = pytest.mark.migration_e2e


def test_real_provisioning_then_clean_record_failure_pre_42(tmp_path: Path) -> None:
    """Provision real 2.x and 3.x environments with `uv`, then fail cleanly at recording.

    Parameters:
        tmp_path: pathlib.Path used as both `--project-dir` (empty, so project
            installation is skipped) and `--work-dir`.

    Raises:
        pytest.skip.Exception: No `uv` executable is available on `$PATH`.
    """

    if shutil.which("uv") is None:
        pytest.skip("uv is not installed on $PATH")

    exit_code = cli.main(
        [
            "--project-dir",
            str(tmp_path),
            "--work-dir",
            str(tmp_path / "work"),
            "--keep-work-dir",
        ]
    )

    # Pre-#42: pytest rejects the still-nonexistent `--airflow-record` flag as an
    # unrecognized argument, so no artifact is ever written and the orchestrator's own
    # "artifact absent after the run" check maps that to a clean exit code 2 -- proving
    # real provisioning succeeded (both venvs were really built) without a real diff.
    assert exit_code == cli.EXIT_TOOLING_ERROR
    work_dir = tmp_path / "work"
    run_dirs = list(work_dir.iterdir())
    assert len(run_dirs) == 1
    (run_dir,) = run_dirs
    assert (run_dir / "venv-v2" / "bin" / "python").is_file()
    assert (run_dir / "venv-v3" / "bin" / "python").is_file()
