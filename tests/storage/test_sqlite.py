"""Test SQLite profile scaling and SQLAlchemy engine integration."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine as sqlalchemy_create_engine
from sqlalchemy.engine import Engine

from pytest_airflow_in_a_box.storage import sqlite
from pytest_airflow_in_a_box.storage.sqlite import (
    PragmaProfile,
    calculate_profile,
    create_metadata_engine,
    write_local_settings,
)

MIB = 1024 * 1024


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


def test_write_local_settings_is_deterministic_and_importable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Write stable ASCII source exporting only the metadata-engine override."""

    settings_path = tmp_path / "config" / "airflow_local_settings.py"
    write_local_settings(settings_path)
    first_source = settings_path.read_text(encoding="ascii")
    write_local_settings(settings_path)

    assert settings_path.read_text(encoding="ascii") == first_source
    assert first_source.encode("ascii").decode("ascii") == first_source
    assert "password" not in first_source.lower()
    monkeypatch.syspath_prepend(str(settings_path.parent))
    monkeypatch.delitem(sys.modules, "airflow_local_settings", raising=False)
    importlib.invalidate_caches()
    local_settings = importlib.import_module("airflow_local_settings")
    assert local_settings.__all__ == ("create_metadata_engine",)
    assert local_settings.create_metadata_engine is create_metadata_engine
    monkeypatch.delitem(sys.modules, "airflow_local_settings")


def test_write_local_settings_rejects_relative_path() -> None:
    """Require bootstrap artifacts to have an unambiguous absolute location."""

    with pytest.raises(ValueError, match="must be absolute"):
        write_local_settings(Path("airflow_local_settings.py"))
