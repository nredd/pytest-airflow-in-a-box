"""Test metadata-database provisioner selection and the SQLite provisioner."""

from __future__ import annotations

from pathlib import Path

import pytest

from pytest_airflow_in_a_box.airflow_cfg import sqlite_url
from pytest_airflow_in_a_box.storage.postgres import PostgresProvisioner
from pytest_airflow_in_a_box.storage.provision import (
    DbBackend,
    SqliteProvisioner,
    select_provisioner,
)


def test_db_backend_parses_supported_values() -> None:
    """Parse both supported backend strings into enum members."""

    assert DbBackend("sqlite") is DbBackend.SQLITE
    assert DbBackend("postgres") is DbBackend.POSTGRES
    assert str(DbBackend.POSTGRES) == "postgres"


def test_db_backend_rejects_unknown_value() -> None:
    """Reject an unsupported backend string."""

    with pytest.raises(ValueError, match="not a valid DbBackend"):
        DbBackend("mysql")


def test_sqlite_provisioner_returns_absolute_url(tmp_path: Path) -> None:
    """Return the absolute SQLite URL and ignore the per-run database name."""

    provisioner = SqliteProvisioner()
    database_path = tmp_path / "airflow.db"

    url = provisioner.start(database_path=database_path, database_name="ignored")

    assert url == sqlite_url(database_path)


def test_sqlite_provisioner_stop_is_a_no_op() -> None:
    """Release nothing; the run-directory cleanup removes the SQLite file."""

    provisioner = SqliteProvisioner()

    assert provisioner.stop() is None


def test_select_provisioner_returns_sqlite_for_sqlite() -> None:
    """Return the trivial SQLite provisioner without probing prerequisites."""

    provisioner = select_provisioner(DbBackend.SQLITE)

    assert isinstance(provisioner, SqliteProvisioner)


def test_select_provisioner_propagates_postgres_unavailable() -> None:
    """Surface the availability probe failure before building a provisioner."""

    def unavailable() -> None:
        raise pytest.UsageError("no postgres for you")

    with pytest.raises(pytest.UsageError, match="no postgres for you"):
        select_provisioner(DbBackend.POSTGRES, availability=unavailable)


def test_select_provisioner_builds_postgres_when_available() -> None:
    """Build a Postgres provisioner once the availability probe passes."""

    calls: list[str] = []

    provisioner = select_provisioner(
        DbBackend.POSTGRES,
        availability=lambda: calls.append("checked"),
        container_factory=lambda _image, _dbname: pytest.fail("factory must stay unused"),
    )

    assert isinstance(provisioner, PostgresProvisioner)
    assert calls == ["checked"]
