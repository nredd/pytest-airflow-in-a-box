"""Expose the run's isolated `AIRFLOW_HOME` and Dag directory as plain paths.

Both directories are resolved by the plugin already, but reaching them from a consumer's own
fixture meant importing ``bootstrap.get_bootstrap_state`` -- an internal -- and deferring that
import into the fixture body to avoid tripping bootstrap too early. These two fixtures are the
supported way to ask.

Both names carry a ``_path`` suffix because ``airflow_home`` and ``airflow_dags_folder`` are
already taken by the ini options these fixtures resolve, and ``airflow_home`` is additionally a
module `plugin.py` imports into the namespace pytest scans for fixtures.

Neither fixture touches the metadata database, so neither belongs in
``fixtures.DATABASE_FIXTURE_NAMES``: asking where a directory is must not import Airflow or
migrate a database.

References:
    https://docs.pytest.org/en/stable/how-to/fixtures.html
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pytest_airflow_in_a_box.bootstrap import get_bootstrap_state
from pytest_airflow_in_a_box.fixtures.dagbag import _dag_folder

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(scope="session")
def airflow_home_path(pytestconfig: pytest.Config) -> Path:
    """Return this run's isolated `AIRFLOW_HOME`.

    The directory holding `airflow.cfg`, `logs/`, the SimpleAuthManager password file, and --
    on the default backend -- the SQLite metadata database. It is created before any consumer
    conftest is imported and, under the default retention policy, removed when the session ends
    cleanly, so treat anything written into it as disposable.

    Under xdist every worker inherits the controller's directory rather than creating its own,
    so the path is identical across workers in one run.

    Parameters:
        pytestconfig: pytest.Config carrying the session's bootstrap state.

    Returns:
        pathlib.Path containing this run's isolated `AIRFLOW_HOME`.

    Raises:
        pytest.UsageError: Airflow bootstrap state is unavailable.
    """

    return get_bootstrap_state(pytestconfig).root


@pytest.fixture(scope="session")
def airflow_dags_folder_path(pytestconfig: pytest.Config) -> Path:
    """Return the Dag directory `full_dag_bag` parses.

    Resolved exactly as `full_dag_bag` resolves it: `--dag-folder` first, then the
    `airflow_dags_folder` ini option (relative values resolving against `config.rootpath`), and
    otherwise the empty `dags/` directory inside this run's isolated `AIRFLOW_HOME`.

    That last fallback is a disposable scratch directory, not a corpus. A consumer reading this
    fixture to locate its own Dag files should configure one of the two options first; the
    fallback means "no Dag folder was configured", and `--airflow-doctor` says so explicitly.

    This is a different option from `--collect-dag-folder` / `airflow_collect_dags_folder`,
    which drives Dag-file collection rather than the Dag bag.

    Parameters:
        pytestconfig: pytest.Config containing plugin options, ini values, and rootpath.

    Returns:
        pathlib.Path containing the Dag directory `full_dag_bag` parses.

    Raises:
        pytest.UsageError: A parsed Dag folder option has an invalid type, or Airflow
            bootstrap state is unavailable.
    """

    return _dag_folder(pytestconfig)


__all__ = ("airflow_dags_folder_path", "airflow_home_path")
