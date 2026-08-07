"""Provide a session-scoped Dag bag for a configured Dag directory.

References:
    https://docs.pytest.org/en/stable/how-to/fixtures.html
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pytest_airflow_in_a_box._compat import build_dag_bag
from pytest_airflow_in_a_box.bootstrap import get_bootstrap_state

if TYPE_CHECKING:
    from pytest_airflow_in_a_box._compat.dagbag import DagBag


def _dag_folder(config: pytest.Config) -> Path:
    """Resolve the CLI, ini, or bootstrap Dag directory.

    Parameters:
        config: pytest.Config containing plugin options and bootstrap state.

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
        return Path(ini_value)
    return get_bootstrap_state(config).dags_folder


@pytest.fixture(scope="session")
def full_dag_bag(pytestconfig: pytest.Config) -> DagBag:
    """Parse all Dags from the configured Dag directory once per test session.

    Parameters:
        pytestconfig: pytest.Config containing plugin options and bootstrap state.

    Returns:
        airflow.models.dagbag.DagBag containing parsed Dags and import errors.
    """

    return build_dag_bag(_dag_folder(pytestconfig))


__all__ = ("full_dag_bag",)
