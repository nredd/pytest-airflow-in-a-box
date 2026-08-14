"""Real end-to-end coverage for `airflow-migration-diff`, against real `uv`/network.

Opt-in only, on two independent gates: the `migration_e2e` marker (excluded from
`make test`/`make all` by `-m "not migration_e2e"`, see `Makefile`/`tests/conftest.py`)
AND an explicit `PYTEST_AIRFLOW_IN_A_BOX_MIGRATION_E2E=1` environment variable this
module checks itself. The marker alone is not enough: a bare `pytest` or `pytest tests/`
invocation with no `-m` filter would otherwise still collect and run this module,
silently kicking off two real Airflow installs over the network on any contributor
machine that happens to have `uv` on `$PATH` -- which, per this repo's own CLAUDE.md,
is the assumed baseline ("Everything goes through `uv`"), so the `uv`-presence check
alone was not a safe default. The environment-variable check below fails closed even
then; only `make test-migration-e2e` (which exports the variable) runs this for real.

Every other test in `tests/migration/` proves the orchestrator's own logic through
injected seams, with no real `uv`, network, or Airflow install. This module is the one
place that provisions real environments, which is slow (two fresh Airflow installs).

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

import os
import shutil
from pathlib import Path

import pytest

from pytest_airflow_in_a_box.migration import cli

pytestmark = pytest.mark.migration_e2e

# Explicit opt-in gate this test checks itself, independent of the `-m` marker filter a
# bare `pytest` invocation might not apply. `make test-migration-e2e` sets this.
E2E_OPT_IN_ENVIRONMENT_VARIABLE = "PYTEST_AIRFLOW_IN_A_BOX_MIGRATION_E2E"


def test_real_provisioning_then_clean_record_failure_pre_42(tmp_path: Path) -> None:
    """Provision real 2.x and 3.x environments with `uv`, then fail cleanly at recording.

    Parameters:
        tmp_path: pathlib.Path used as both `--project-dir` (empty, so project
            installation is skipped) and `--work-dir`.

    Raises:
        pytest.skip.Exception: The `PYTEST_AIRFLOW_IN_A_BOX_MIGRATION_E2E` opt-in
            environment variable is unset, or no `uv` executable is available on
            `$PATH`.
    """

    if os.environ.get(E2E_OPT_IN_ENVIRONMENT_VARIABLE) != "1":
        pytest.skip(
            f"set {E2E_OPT_IN_ENVIRONMENT_VARIABLE}=1 to run the real-uv/network "
            f"migration e2e test (use `make test-migration-e2e`)"
        )
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
