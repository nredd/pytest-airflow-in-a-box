"""Test SQLite profile scaling and SQLAlchemy engine integration."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import create_engine as sqlalchemy_create_engine
from sqlalchemy import event
from sqlalchemy.engine import Engine

from pytest_airflow_in_a_box.storage import sqlite
from pytest_airflow_in_a_box.storage.sqlite import (
    PragmaProfile,
    calculate_profile,
    create_metadata_engine,
    install_legacy_sqlite_listener,
)

MIB = 1024 * 1024


@pytest.fixture(autouse=True)
def _isolate_legacy_sqlite_listener() -> Iterator[None]:
    """Prevent the process-global SQLAlchemy listener from leaking between tests."""

    sqlite._reset_legacy_sqlite_listener_for_testing()
    yield
    sqlite._reset_legacy_sqlite_listener_for_testing()


def test_calculate_profile_scales_low_resource_host(monkeypatch: pytest.MonkeyPatch) -> None:
    """Divide a constrained host budget across expected concurrent connections."""

    monkeypatch.setattr(sqlite, "_physical_memory_bytes", lambda: 288 * MIB)
    monkeypatch.setattr(sqlite, "_cpu_count", lambda: 8)

    profile = calculate_profile()

    assert profile == PragmaProfile(
        journal_mode="WAL",
        synchronous="OFF",
        temp_store="MEMORY",
        mmap_size=8 * MIB,
        cache_size=-4096,
        busy_timeout=30_000,
        page_size=8192,
    )


def test_calculate_profile_caps_high_resource_host(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cap a large host at the measured default memory settings."""

    monkeypatch.setattr(sqlite, "_physical_memory_bytes", lambda: 64 * 1024 * MIB)
    monkeypatch.setattr(sqlite, "_cpu_count", lambda: 4)

    profile = calculate_profile()

    assert profile.mmap_size == 256 * MIB
    assert profile.cache_size == -131_072


def test_detection_uses_conservative_fallbacks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Use one CPU and bounded memory when stdlib detection is unavailable."""

    monkeypatch.setattr(sqlite.os, "cpu_count", lambda: None)

    def unavailable_sysconf(name: str) -> int:
        """Simulate a platform without usable POSIX memory detection."""

        raise ValueError(name)

    monkeypatch.setattr(sqlite.os, "sysconf", unavailable_sysconf)

    assert sqlite._cpu_count() == 1
    assert sqlite._physical_memory_bytes() == 512 * MIB
    profile = calculate_profile()
    assert 0 < profile.mmap_size < 256 * MIB
    assert -131_072 < profile.cache_size < 0


def test_detection_rejects_invalid_stdlib_values(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reject non-positive and non-integer CPU and RAM observations."""

    monkeypatch.setattr(sqlite.os, "cpu_count", lambda: 0)
    monkeypatch.setattr(sqlite.os, "sysconf", lambda name: False if name == "SC_PAGE_SIZE" else 1)

    assert sqlite._cpu_count() == 1
    assert sqlite._physical_memory_bytes() == 512 * MIB


def test_sqlite_engine_applies_pragmas_to_new_connections(tmp_path: Path) -> None:
    """Apply the complete profile to a real file database after pool disposal."""

    profile = calculate_profile()
    engine_args: dict[str, object] = {"pool_pre_ping": True}
    connect_args: dict[str, object] = {"check_same_thread": False}
    original_engine_args = dict(engine_args)
    original_connect_args = dict(connect_args)
    engine = create_metadata_engine(
        f"sqlite:///{tmp_path / 'metadata.db'}",
        engine_args=engine_args,
        connect_args=connect_args,
    )

    def pragma_values() -> tuple[object, ...]:
        """Read every tuned value through a checked-out SQLAlchemy connection."""

        with engine.connect() as connection:
            return tuple(
                connection.exec_driver_sql(f"PRAGMA {name}").scalar_one()
                for name in (
                    "journal_mode",
                    "synchronous",
                    "temp_store",
                    "mmap_size",
                    "cache_size",
                    "busy_timeout",
                    "page_size",
                    "locking_mode",
                )
            )

    expected = (
        "wal",
        0,
        2,
        profile.mmap_size,
        profile.cache_size,
        profile.busy_timeout,
        profile.page_size,
        "normal",
    )
    assert pragma_values() == expected
    engine.dispose()
    assert pragma_values() == expected
    assert engine_args == original_engine_args
    assert connect_args == original_connect_args
    engine.dispose()


def test_non_sqlite_engine_delegates_without_listener(monkeypatch: pytest.MonkeyPatch) -> None:
    """Delegate a non-SQLite URL with copied arguments and Airflow's future flag."""

    delegated_engine = sqlalchemy_create_engine("sqlite://")
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_create_engine(url: str, **kwargs: object) -> Engine:
        """Record SQLAlchemy delegation without contacting an external server."""

        calls.append((url, kwargs))
        return delegated_engine

    monkeypatch.setattr(sqlite, "create_engine", fake_create_engine)
    engine_args: dict[str, object] = {"pool_pre_ping": True}
    connect_args: dict[str, object] = {"application_name": "tests"}

    result = create_metadata_engine(
        "postgresql://user:password@database/airflow",
        engine_args=engine_args,
        connect_args=connect_args,
    )

    assert result is delegated_engine
    assert calls == [
        (
            "postgresql://user:password@database/airflow",
            {
                "connect_args": {"application_name": "tests"},
                "pool_pre_ping": True,
                "future": True,
            },
        )
    ]
    assert engine_args == {"pool_pre_ping": True}
    assert connect_args == {"application_name": "tests"}
    delegated_engine.dispose()


def test_legacy_listener_installation_is_idempotent_and_ignores_other_dbapi_connections(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Install one Engine listener for Airflow 3.1 and ignore non-SQLite connections."""

    database_path = tmp_path / "metadata.db"
    requested_distributions: list[str] = []

    def installed_version(distribution: str) -> str:
        """Record the package metadata lookup and select the legacy Airflow release."""

        requested_distributions.append(distribution)
        return "3.1.8"

    monkeypatch.setattr(sqlite.metadata, "version", installed_version)
    monkeypatch.setenv(sqlite.SQL_ALCHEMY_CONN_ENVIRONMENT_VARIABLE, f"sqlite:///{database_path}")

    install_legacy_sqlite_listener()
    listener = sqlite._LEGACY_SQLITE_LISTENER
    install_legacy_sqlite_listener()

    assert requested_distributions == ["apache-airflow-core", "apache-airflow-core"]
    assert listener is not None
    assert sqlite._LEGACY_SQLITE_LISTENER is listener
    assert event.contains(Engine, "connect", listener)
    listener(object(), object())


def test_legacy_listener_scopes_real_sqlite_engines_to_configured_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tune the configured SQLite file without changing a separate SQLite engine."""

    target_path = tmp_path / "target.db"
    other_path = tmp_path / "other.db"
    monkeypatch.setattr(sqlite.metadata, "version", lambda _distribution: "3.1.8")
    monkeypatch.setenv(sqlite.SQL_ALCHEMY_CONN_ENVIRONMENT_VARIABLE, f"sqlite:///{target_path}")
    install_legacy_sqlite_listener()
    target_engine = sqlalchemy_create_engine(f"sqlite:///{target_path}")
    other_engine = sqlalchemy_create_engine(f"sqlite:///{other_path}")

    with target_engine.connect() as connection:
        target_values = (
            connection.exec_driver_sql("PRAGMA journal_mode").scalar_one(),
            connection.exec_driver_sql("PRAGMA temp_store").scalar_one(),
            connection.exec_driver_sql("PRAGMA busy_timeout").scalar_one(),
        )
    with other_engine.connect() as connection:
        other_values = (
            connection.exec_driver_sql("PRAGMA journal_mode").scalar_one(),
            connection.exec_driver_sql("PRAGMA temp_store").scalar_one(),
            connection.exec_driver_sql("PRAGMA busy_timeout").scalar_one(),
        )

    assert target_values == ("wal", 2, 30_000)
    assert other_values != target_values
    assert other_values[0] == "delete"
    target_engine.dispose()
    other_engine.dispose()


def test_legacy_listener_supports_airflow_3_2_0(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Use the path-scoped fallback for Airflow 3.2.0's unreliable engine hook."""

    database_path = tmp_path / "metadata.db"
    monkeypatch.setattr(sqlite.metadata, "version", lambda _distribution: "3.2.0")
    monkeypatch.setenv(sqlite.SQL_ALCHEMY_CONN_ENVIRONMENT_VARIABLE, f"sqlite:///{database_path}")

    install_legacy_sqlite_listener()

    listener = sqlite._LEGACY_SQLITE_LISTENER
    assert listener is not None
    assert event.contains(Engine, "connect", listener)


def test_legacy_listener_is_noop_after_airflow_3_2_0(monkeypatch: pytest.MonkeyPatch) -> None:
    """Leave the class-level event registry unchanged where the engine hook is reliable."""

    monkeypatch.setattr(sqlite.metadata, "version", lambda _distribution: "3.2.2")
    monkeypatch.delenv(sqlite.SQL_ALCHEMY_CONN_ENVIRONMENT_VARIABLE, raising=False)

    install_legacy_sqlite_listener()

    assert sqlite._LEGACY_SQLITE_LISTENER is None


def test_legacy_listener_is_noop_without_airflow_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Avoid global installation when the Airflow distribution is unavailable."""

    def missing_distribution(distribution: str) -> str:
        """Simulate an environment without the Airflow core distribution."""

        raise sqlite.metadata.PackageNotFoundError(distribution)

    monkeypatch.setattr(sqlite.metadata, "version", missing_distribution)

    install_legacy_sqlite_listener()

    assert sqlite._LEGACY_SQLITE_LISTENER is None


@pytest.mark.parametrize(
    ("sql_alchemy_conn", "message"),
    [
        (None, "must contain a database URL: 'None'"),
        ("not a URL", "invalid database URL: 'not a URL'"),
        ("sqlite://", "must select a SQLite file: 'sqlite://'"),
        ("sqlite:///:memory:", "must select a SQLite file: 'sqlite:///:memory:'"),
        (
            "sqlite:///relative.db",
            "must select an absolute SQLite path: 'relative.db' from 'sqlite:///relative.db'",
        ),
    ],
)
def test_legacy_listener_rejects_invalid_database_urls(
    sql_alchemy_conn: str | None, message: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Wrap missing and malformed Airflow database values in semantic errors."""

    monkeypatch.setattr(sqlite.metadata, "version", lambda _distribution: "3.1.8")
    if sql_alchemy_conn is None:
        monkeypatch.delenv(sqlite.SQL_ALCHEMY_CONN_ENVIRONMENT_VARIABLE, raising=False)
    else:
        monkeypatch.setenv(sqlite.SQL_ALCHEMY_CONN_ENVIRONMENT_VARIABLE, sql_alchemy_conn)

    with pytest.raises(ValueError, match=message):
        install_legacy_sqlite_listener()

    assert sqlite._LEGACY_SQLITE_LISTENER is None


def test_legacy_listener_wraps_invalid_sqlite_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Name the configured URL and path when filesystem resolution fails."""

    database_path = tmp_path / "invalid.db"

    class InvalidPath:
        """Represent an absolute path that cannot be resolved."""

        def is_absolute(self) -> bool:
            """Report the configured path as absolute."""

            return True

        def resolve(self) -> Path:
            """Simulate an invalid filesystem path."""

            raise OSError("invalid path")

    monkeypatch.setattr(sqlite.metadata, "version", lambda _distribution: "3.1.8")
    monkeypatch.setenv(sqlite.SQL_ALCHEMY_CONN_ENVIRONMENT_VARIABLE, f"sqlite:///{database_path}")
    monkeypatch.setattr(sqlite, "Path", lambda _value: InvalidPath())

    with pytest.raises(ValueError, match="invalid SQLite path") as error:
        install_legacy_sqlite_listener()
    assert str(error.value) == (
        f"`{sqlite.SQL_ALCHEMY_CONN_ENVIRONMENT_VARIABLE}` contains an invalid SQLite path: "
        f"'{database_path}' from 'sqlite:///{database_path}'"
    )


def test_legacy_listener_ignores_a_configured_non_sqlite_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Skip registration when Airflow 3.1 is configured for another database backend."""

    monkeypatch.setattr(sqlite.metadata, "version", lambda _distribution: "3.1.8")
    monkeypatch.setenv(
        sqlite.SQL_ALCHEMY_CONN_ENVIRONMENT_VARIABLE,
        "postgresql://user:password@database/airflow",
    )

    install_legacy_sqlite_listener()

    assert sqlite._LEGACY_SQLITE_LISTENER is None


def test_sqlite_main_path_handles_memory_and_invalid_database_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Treat absent or unresolvable main database paths as outside the listener scope."""

    with sqlite3.connect(":memory:") as connection:
        assert sqlite._sqlite_main_path(connection) is None

    monkeypatch.setattr(sqlite, "Path", lambda _value: Path("/invalid\x00path"))
    with sqlite3.connect(tmp_path / "metadata.db") as connection:
        assert sqlite._sqlite_main_path(connection) is None


@pytest.mark.parametrize(
    ("profile", "message"),
    [
        (
            PragmaProfile("DELETE", "OFF", "MEMORY", 0, -1, 30_000, 8192),
            "fixed PRAGMA",
        ),
        (
            PragmaProfile("WAL", "OFF", "MEMORY", 257 * MIB, -1, 30_000, 8192),
            "mmap_size",
        ),
        (
            PragmaProfile("WAL", "OFF", "MEMORY", 0, 1, 30_000, 8192),
            "cache_size",
        ),
    ],
)
def test_invalid_profiles_are_rejected(profile: PragmaProfile, message: str) -> None:
    """Reject values before interpolating numeric PRAGMA operands."""

    with pytest.raises(ValueError, match=message):
        sqlite._pragma_statements(profile)


def test_legacy_listener_always_installs_on_the_v2_family(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Install the listener on 2.x regardless of any version gate."""

    from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily

    def core_absent(distribution: str) -> str:
        """Report the 3.x core distribution as absent."""

        raise sqlite.metadata.PackageNotFoundError(distribution)

    monkeypatch.setattr(sqlite.metadata, "version", core_absent)
    monkeypatch.setattr(sqlite, "installed_family", lambda: AirflowFamily.V2)
    monkeypatch.setenv(
        sqlite.SQL_ALCHEMY_CONN_ENVIRONMENT_VARIABLE, f"sqlite:///{tmp_path / 'meta.db'}"
    )

    install_legacy_sqlite_listener()

    assert sqlite._LEGACY_SQLITE_LISTENER is not None


def test_legacy_listener_skips_an_airflow_free_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Install nothing when the core is absent and no 2.x family is present."""

    def core_absent(distribution: str) -> str:
        """Report the 3.x core distribution as absent."""

        raise sqlite.metadata.PackageNotFoundError(distribution)

    monkeypatch.setattr(sqlite.metadata, "version", core_absent)
    monkeypatch.setattr(sqlite, "installed_family", lambda: None)

    install_legacy_sqlite_listener()

    assert sqlite._LEGACY_SQLITE_LISTENER is None
