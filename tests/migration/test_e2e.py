"""Real end-to-end coverage for `airflow-migration-diff`, against real `uv`/network/Airflow.

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

`--plugin-spec` is pinned to this checkout's own worktree path: this branch's version
(still `0.4.0` in `pyproject.toml`) is already released, but the *code* at this commit
-- the migration-diff artifact contract (#42) and this orchestrator (#44) -- is not on
PyPI under any released version, so the default `--plugin-spec` (which resolves to
`pytest-airflow-in-a-box==<installed version>` from an index) would silently install a
prior release lacking `--airflow-record` entirely. Installing from the local worktree
instead exercises the actual code under test.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/42
    https://github.com/nredd/pytest-airflow-in-a-box/issues/44
    https://github.com/nredd/pytest-airflow-in-a-box/blob/main/docs/guide/migration-diff.md
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import pytest

from pytest_airflow_in_a_box.migration import cli

pytestmark = pytest.mark.migration_e2e

# Explicit opt-in gate this test checks itself, independent of the `-m` marker filter a
# bare `pytest` invocation might not apply. `make test-migration-e2e` sets this.
E2E_OPT_IN_ENVIRONMENT_VARIABLE = "PYTEST_AIRFLOW_IN_A_BOX_MIGRATION_E2E"

_PLANTED_REGRESSION_TEST = '''\
from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily, installed_family


def test_survives_the_migration():
    """Pass on the Airflow 2.x recording pass, fail on the 3.x live pass -- planted."""
    assert installed_family() is AirflowFamily.V2
'''
_PLANTED_REGRESSION_NODEID = "test_planted_regression.py::test_survives_the_migration"


def _worktree_root() -> Path:
    """Resolve this checkout's repository root, for a local `--plugin-spec`.

    Returns:
        pathlib.Path containing the repository root (three parents up from this file:
        `tests/migration/test_e2e.py` -> `tests/migration` -> `tests` -> root).
    """

    return Path(__file__).resolve().parents[2]


def test_real_migration_diff_categorizes_a_planted_regression(tmp_path: Path) -> None:
    """Provision real 2.x/3.x environments, run a real recording pass in each, and diff them.

    Plants exactly one regression: a test asserting `installed_family() is
    AirflowFamily.V2`, true (passing) on the 2.x recording pass and false (failing) on
    the 3.x live pass. Verifies both artifacts recorded `complete: true`, the planted
    nodeid is categorized as `regression`, and the process exit code is 1.

    Parameters:
        tmp_path: pathlib.Path used as both `--project-dir` (the planted test file) and
            the base of `--work-dir`.

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

    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / "test_planted_regression.py").write_text(
        _PLANTED_REGRESSION_TEST, encoding="utf-8"
    )
    work_dir = tmp_path / "work"

    exit_code = cli.main(
        [
            "--project-dir",
            str(project_dir),
            "--work-dir",
            str(work_dir),
            "--plugin-spec",
            str(_worktree_root()),
            "--keep-work-dir",
        ]
    )

    (run_dir,) = list(work_dir.iterdir())
    baseline = json.loads((run_dir / "baseline.json").read_text(encoding="utf-8"))
    live = json.loads((run_dir / "live.json").read_text(encoding="utf-8"))

    assert baseline["complete"] is True, baseline
    assert live["complete"] is True, live
    assert baseline["outcomes"][_PLANTED_REGRESSION_NODEID]["outcome"] == "passed"
    assert live["outcomes"][_PLANTED_REGRESSION_NODEID]["outcome"] == "failed"
    assert exit_code == cli.EXIT_REGRESSIONS
