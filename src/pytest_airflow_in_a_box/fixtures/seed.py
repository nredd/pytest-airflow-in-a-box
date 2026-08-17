"""Seed Variable and Connection metadata rows for database-backed tests.

These fixtures are the metastore counterpart to ``run_task``'s ``variables=``
and ``connections=`` keywords: the field shapes match exactly, but rows are
committed so every reader in the process -- hooks, operators, and Airflow's
metastore secrets backend -- resolves them the way a live deployment would.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html
    https://docs.pytest.org/en/stable/how-to/fixtures.html
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING, Any

import pytest

from pytest_airflow_in_a_box._compat import ensure_database
from pytest_airflow_in_a_box._compat.parse_time import parse_time_supervision
from pytest_airflow_in_a_box._compat.seed import (
    SeedRecord,
    cleanup_seeds,
    open_seed_session,
    seed_connections,
    seed_variables,
    validate_connections,
    validate_variables,
)
from pytest_airflow_in_a_box.bootstrap import get_bootstrap_state
from pytest_airflow_in_a_box.parse_secrets import parse_time_comms

if TYPE_CHECKING:
    from pytest_airflow_in_a_box.types import AirflowConnections, AirflowVariables


class _VariableSeeder:
    """Commit fixture-owned Variable rows, accumulating across calls.

    Parameters:
        record: SeedRecord owning every row this seeder commits.
    """

    def __init__(self, record: SeedRecord) -> None:
        self._record = record

    def __call__(self, variables: Mapping[str, str]) -> None:
        """Validate one whole batch, then commit it.

        Parameters:
            variables: Mapping[str, str] containing Variable values by key.
        """

        seed_variables(self._record, validate_variables(variables))


class _ConnectionSeeder:
    """Commit fixture-owned Connection rows, accumulating across calls.

    Parameters:
        record: SeedRecord owning every row this seeder commits.
    """

    def __init__(self, record: SeedRecord) -> None:
        self._record = record

    def __call__(self, connections: Mapping[str, Mapping[str, Any]]) -> None:
        """Validate one whole batch, then commit it.

        Parameters:
            connections: Mapping[str, Mapping[str, Any]] containing connection
                fields by connection id.
        """

        seed_connections(self._record, validate_connections(connections))


def _seed_record(pytestconfig: pytest.Config, kind: str) -> SeedRecord:
    """Initialize the database and open one owning session.

    A dedicated session is required: the ``session`` fixture rolls back, and
    Airflow's metastore backend reads through sessions of its own, so seeded
    rows must be committed to be visible at all.

    Parameters:
        pytestconfig: pytest.Config carrying the session's bootstrap state.
        kind: str naming the seeded entity for failure diagnostics.

    Returns:
        SeedRecord owning the rows one fixture commits for one test.
    """

    ensure_database(get_bootstrap_state(pytestconfig).root)
    return SeedRecord(session=open_seed_session(kind), kind=kind)


@pytest.fixture
def airflow_variables(pytestconfig: pytest.Config) -> Iterator[AirflowVariables]:
    """Seed Airflow Variables for one test and delete them on teardown.

    Parameters:
        pytestconfig: pytest.Config carrying the session's bootstrap state.

    Yields:
        AirflowVariables committing one batch of Variables per call.
    """

    record = _seed_record(pytestconfig, "Variables")
    try:
        yield _VariableSeeder(record)
    finally:
        cleanup_seeds(record)


@pytest.fixture
def airflow_connections(pytestconfig: pytest.Config) -> Iterator[AirflowConnections]:
    """Seed Airflow Connections for one test and delete them on teardown.

    Parameters:
        pytestconfig: pytest.Config carrying the session's bootstrap state.

    Yields:
        AirflowConnections committing one batch of Connections per call.
    """

    record = _seed_record(pytestconfig, "Connections")
    try:
        yield _ConnectionSeeder(record)
    finally:
        cleanup_seeds(record)


@pytest.fixture
def airflow_parse_secrets(pytestconfig: pytest.Config) -> Iterator[None]:
    """Resolve top-level Variable and Connection lookups for the whole test.

    Every Dag parse site installs this shim itself, so this fixture is for the lookups
    that run outside one: inside a `with dag_maker(...)` block, or in the test body.
    Seed first and request the fixture second -- a batch committed while the fixture is
    active is visible immediately, because the shim reads the metadata database per
    lookup rather than snapshotting it up front.

    A no-op on Airflow 2.x, which resolves both from the metastore with no shim, and
    under `--airflow-parse-secrets=off`.

    Parameters:
        pytestconfig: pytest.Config carrying plugin options and bootstrap state.

    Yields:
        None, with parse-time resolution installed for the duration of the test.
    """

    with parse_time_supervision(parse_time_comms(pytestconfig)):
        yield


__all__ = ("airflow_connections", "airflow_parse_secrets", "airflow_variables")
