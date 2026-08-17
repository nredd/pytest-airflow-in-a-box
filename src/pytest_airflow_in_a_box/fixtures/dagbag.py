"""Provide a session-scoped Dag bag for a configured Dag directory.

References:
    https://docs.pytest.org/en/stable/how-to/fixtures.html
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Final

import pytest

from pytest_airflow_in_a_box._compat import build_dag_bag, ensure_database
from pytest_airflow_in_a_box.bootstrap import get_bootstrap_state

if TYPE_CHECKING:
    from pytest_airflow_in_a_box._compat.dagbag import DagBag

LIVE_DAG_BAG_KEY = pytest.StashKey["DagBag"]()
FULL_DAG_BAG_FIXTURE_NAME: Final[str] = "full_dag_bag"
FULL_DAG_BAG_XDIST_GROUP: Final[str] = "pytest-airflow-in-a-box::full-dag-bag"


def _dag_folder(config: pytest.Config) -> Path:
    """Resolve the CLI, ini, or bootstrap Dag directory.

    A relative CLI value stays relative to the invocation directory. A relative ini
    value resolves against `config.rootpath`, matching normal pytest configuration-file
    semantics.

    Parameters:
        config: pytest.Config containing plugin options, ini values, and rootpath.

    Returns:
        pathlib.Path containing the selected Dag directory.

    Raises:
        pytest.UsageError: A parsed Dag folder option has an invalid type.
    """

    command_line: object = config.getoption("dag_folder")
    if command_line is not None:
        if not isinstance(command_line, str):
            raise pytest.UsageError("Option `--dag-folder` must be a path string")
        return Path(command_line)

    ini_value: object = config.getini("airflow_dags_folder")
    if not isinstance(ini_value, str):
        raise pytest.UsageError("Ini option `airflow_dags_folder` must be a path string")
    if ini_value:
        path = Path(ini_value)
        return path if path.is_absolute() else config.rootpath / path
    return get_bootstrap_state(config).dags_folder


def _cached_dag_bag(session: pytest.Session, config: pytest.Config) -> DagBag:
    """Parse and process-cache a live DagBag for one worker session.

    The bundled smoke catalog's corpus builder (`smoke.py`) reuses this DagBag instead of
    parsing the Dag folder a second time, if `full_dag_bag` already ran in this process.
    When the catalog is enabled and in scope for this run, applies its configured per-file
    parse timeout before parsing, so the timeout is honored regardless of which of the two
    parses first.

    Parameters:
        session: pytest.Session used to cache the parsed DagBag.
        config: pytest.Config containing plugin options and bootstrap state.

    Returns:
        airflow.models.dagbag.DagBag containing parsed Dags and import errors.

    Raises:
        pytest.UsageError: A smoke-enablement, parse-timeout, or Dag-folder option has an
            invalid type or value.
    """

    if LIVE_DAG_BAG_KEY not in session.stash:
        # Local import breaks a cycle: smoke.py imports from this module at load time.
        from pytest_airflow_in_a_box.smoke import _parse_timeout, _smoke_enabled, _smoke_in_scope

        if _smoke_enabled(config) and _smoke_in_scope(config):
            os.environ["AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT"] = str(_parse_timeout(config))
        session.stash[LIVE_DAG_BAG_KEY] = build_dag_bag(_dag_folder(config))
    return session.stash[LIVE_DAG_BAG_KEY]


@pytest.fixture(scope="session")
def full_dag_bag(request: pytest.FixtureRequest, pytestconfig: pytest.Config) -> DagBag:
    """Parse all Dags from the configured Dag directory once per worker process.

    Shared with the bundled smoke catalog when `--airflow-smoke` is enabled and in scope for
    this run: if `full_dag_bag` parses first in this process, the catalog's corpus builder
    reuses the same live DagBag instead of parsing again (the catalog is always collected
    last, so this is the common case). Treat the returned object as read-only when that
    applies -- mutating it is visible to the smoke catalog's checks too.

    Under `--dist loadgroup`, `plugin.py` forces this fixture's consumers onto the same
    xdist worker as the smoke catalog (`FULL_DAG_BAG_XDIST_GROUP`) whenever both are
    present in the run, so the reuse above actually has a chance to trigger instead of
    each landing on a different worker and parsing independently.

    Parameters:
        request: pytest.FixtureRequest used to reach the session-scoped DagBag cache.
        pytestconfig: pytest.Config containing plugin options and bootstrap state.

    Returns:
        airflow.models.dagbag.DagBag containing parsed Dags and import errors.
    """

    ensure_database(get_bootstrap_state(pytestconfig).root)
    return _cached_dag_bag(request.session, pytestconfig)


__all__ = ("full_dag_bag",)
