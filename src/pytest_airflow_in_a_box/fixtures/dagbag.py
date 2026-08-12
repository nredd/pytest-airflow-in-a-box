"""Provide a session-scoped Dag bag for a configured Dag directory.

References:
    https://docs.pytest.org/en/stable/how-to/fixtures.html
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pytest_airflow_in_a_box._compat import build_dag_bag, ensure_database
from pytest_airflow_in_a_box.bootstrap import get_bootstrap_state

if TYPE_CHECKING:
    from pytest_airflow_in_a_box._compat.dagbag import DagBag


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


LIVE_DAG_BAG_KEY = pytest.StashKey["DagBag"]()


def _cached_dag_bag(session: pytest.Session, config: pytest.Config) -> DagBag:
    """Parse and process-cache a live DagBag for one worker session.

    Shared between the `full_dag_bag` fixture and the bundled smoke catalog's corpus
    builder (`smoke.py`) so whichever one runs first in a worker process pays for the
    parse and the other reuses its result, instead of each parsing independently.

    Parameters:
        session: pytest.Session used to cache the parsed DagBag.
        config: pytest.Config containing plugin options and bootstrap state.

    Returns:
        airflow.models.dagbag.DagBag containing parsed Dags and import errors.
    """

    if LIVE_DAG_BAG_KEY not in session.stash:
        ensure_database(get_bootstrap_state(config).root)
        session.stash[LIVE_DAG_BAG_KEY] = build_dag_bag(_dag_folder(config))
    return session.stash[LIVE_DAG_BAG_KEY]


@pytest.fixture(scope="session")
def full_dag_bag(request: pytest.FixtureRequest, pytestconfig: pytest.Config) -> DagBag:
    """Parse all Dags from the configured Dag directory once per worker process.

    Parameters:
        request: pytest.FixtureRequest used to reach the session-scoped DagBag cache.
        pytestconfig: pytest.Config containing plugin options and bootstrap state.

    Returns:
        airflow.models.dagbag.DagBag containing parsed Dags and import errors.
    """

    return _cached_dag_bag(request.session, pytestconfig)


__all__ = ("full_dag_bag",)
