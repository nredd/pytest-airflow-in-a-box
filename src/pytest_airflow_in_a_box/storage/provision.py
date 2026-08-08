"""Select and drive the metadata-database provisioner for one test run.

A provisioner starts a metadata backend and hands Airflow a SQLAlchemy URL; the
SQLite tier is the trivial provisioner so both backends ride one seam. The
owner path in :mod:`pytest_airflow_in_a_box.bootstrap` selects a provisioner,
starts it controller-side, and shares its URL through the environment handoff.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#sql-alchemy-conn
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from pytest_airflow_in_a_box.airflow_cfg import sqlite_url
from pytest_airflow_in_a_box.storage.locate import StrEnum
from pytest_airflow_in_a_box.storage.postgres import (
    AvailabilityProbe,
    ContainerFactory,
    PostgresProvisioner,
    require_postgres_available,
)


class DbBackend(StrEnum):
    """Supported isolated Airflow metadata database backends."""

    SQLITE = "sqlite"
    POSTGRES = "postgres"


class Provisioner(Protocol):
    """Start a metadata backend and release it during teardown."""

    def start(self, *, database_path: Path, database_name: str) -> str:
        """Provision the backend and return its SQLAlchemy URL.

        Parameters:
            database_path: pathlib.Path selecting the SQLite file location.
            database_name: str naming a per-run server-side database.

        Returns:
            str containing the provisioned SQLAlchemy URL.
        """

    def stop(self) -> None:
        """Release provisioned resources; idempotent."""


@dataclass(frozen=True)
class SqliteProvisioner:
    """Trivial provisioner: the SQLite file rides the existing storage seam."""

    def start(self, *, database_path: Path, database_name: str) -> str:
        """Return the absolute SQLite URL for the run database.

        Parameters:
            database_path: pathlib.Path selecting the SQLite file location.
            database_name: str ignored by the SQLite backend.

        Returns:
            str containing the absolute SQLite SQLAlchemy URL.
        """

        del database_name
        return sqlite_url(database_path)

    def stop(self) -> None:
        """Release nothing; the run-directory cleanup removes the file."""


def select_provisioner(
    backend: DbBackend,
    *,
    container_factory: ContainerFactory | None = None,
    availability: AvailabilityProbe | None = None,
) -> Provisioner:
    """Return the provisioner for a backend, failing loudly when unavailable.

    Parameters:
        backend: DbBackend selecting the metadata database backend.
        container_factory: ContainerFactory | None building a Postgres container.
        availability: AvailabilityProbe | None asserting Postgres prerequisites.

    Returns:
        Provisioner ready to start the selected backend.

    Raises:
        pytest.UsageError: Postgres was selected but its prerequisites are absent.
    """

    if backend is DbBackend.SQLITE:
        return SqliteProvisioner()
    (availability or require_postgres_available)()
    return PostgresProvisioner(container_factory=container_factory)


__all__ = (
    "DbBackend",
    "Provisioner",
    "SqliteProvisioner",
    "select_provisioner",
)
