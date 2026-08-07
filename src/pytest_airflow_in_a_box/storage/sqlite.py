"""Tune SQLite metadata engines for disposable Airflow test databases.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/howto/customize-ui.html
    https://docs.sqlalchemy.org/en/20/core/events.html#sqlalchemy.events.PoolEvents.connect
    https://www.sqlite.org/pragma.html
"""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from functools import partial
from pathlib import Path

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine, make_url

MIB = 1024 * 1024
PAGE_SIZE = 8192
MAX_MMAP_SIZE = 256 * MIB
MAX_CACHE_SIZE_KIB = 128 * 1024
BUSY_TIMEOUT_MILLISECONDS = 30_000
FALLBACK_MEMORY_BYTES = 512 * MIB
LOCAL_SETTINGS_SOURCE = '''"""Configure the Airflow metadata engine for isolated tests."""

from pytest_airflow_in_a_box.storage import create_metadata_engine

__all__ = ("create_metadata_engine",)
'''


@dataclass(frozen=True)
class PragmaProfile:
    """Validated SQLite settings applied to each metadata connection.

    Parameters:
        journal_mode: str selecting SQLite's write-ahead log.
        synchronous: str disabling durability for disposable test data.
        temp_store: str keeping temporary objects in memory.
        mmap_size: int limiting memory-mapped I/O bytes per connection.
        cache_size: int selecting a negative KiB cache limit per connection.
        busy_timeout: int waiting for concurrent writers in milliseconds.
        page_size: int selecting bytes per database page.
    """

    journal_mode: str
    synchronous: str
    temp_store: str
    mmap_size: int
    cache_size: int
    busy_timeout: int
    page_size: int


def _cpu_count() -> int:
    """Detect logical CPUs with a conservative single-connection fallback.

    Returns:
        int containing at least one logical CPU.
    """

    detected = os.cpu_count()
    return detected if detected is not None and detected > 0 else 1


def _physical_memory_bytes() -> int:
    """Detect physical RAM using portable POSIX ``sysconf`` names.

    Returns:
        int containing detected bytes or a conservative fallback.
    """

    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        physical_pages = os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, OSError, ValueError):
        return FALLBACK_MEMORY_BYTES
    if (
        not isinstance(page_size, int)
        or isinstance(page_size, bool)
        or not isinstance(physical_pages, int)
        or isinstance(physical_pages, bool)
        or page_size <= 0
        or physical_pages <= 0
    ):
        return FALLBACK_MEMORY_BYTES
    return page_size * physical_pages


def calculate_profile() -> PragmaProfile:
    """Calculate bounded per-connection SQLite memory settings for this host.

    One third of physical RAM is the aggregate budget. Logical CPU count is a
    conservative estimate of concurrent metadata connections, and each share
    is split two-to-one between memory mapping and SQLite's page cache.

    Returns:
        PragmaProfile containing fixed and host-scaled SQLite settings.
    """

    per_connection_bytes = _physical_memory_bytes() // 3 // _cpu_count()
    mmap_size = min(MAX_MMAP_SIZE, per_connection_bytes * 2 // 3)
    cache_size_kib = min(MAX_CACHE_SIZE_KIB, max(1, per_connection_bytes // 3 // 1024))
    return PragmaProfile(
        journal_mode="WAL",
        synchronous="OFF",
        temp_store="MEMORY",
        mmap_size=mmap_size,
        cache_size=-cache_size_kib,
        busy_timeout=BUSY_TIMEOUT_MILLISECONDS,
        page_size=PAGE_SIZE,
    )


def _pragma_statements(profile: PragmaProfile) -> tuple[str, ...]:
    """Validate a profile and render only fixed-name PRAGMA statements.

    Parameters:
        profile: PragmaProfile containing candidate values.

    Returns:
        tuple[str, ...] containing validated SQL statements.

    Raises:
        ValueError: A fixed or numeric setting is outside the supported profile.
    """

    fixed_values = (
        profile.journal_mode,
        profile.synchronous,
        profile.temp_store,
        profile.busy_timeout,
        profile.page_size,
    )
    if fixed_values != ("WAL", "OFF", "MEMORY", BUSY_TIMEOUT_MILLISECONDS, PAGE_SIZE):
        raise ValueError("SQLite profile contains unsupported fixed PRAGMA values")
    if not 0 <= profile.mmap_size <= MAX_MMAP_SIZE:
        raise ValueError(
            f"SQLite `mmap_size` is outside the supported range: '{profile.mmap_size}'"
        )
    if not -MAX_CACHE_SIZE_KIB <= profile.cache_size < 0:
        raise ValueError(
            f"SQLite `cache_size` is outside the supported range: '{profile.cache_size}'"
        )
    return (
        f"PRAGMA page_size = {profile.page_size}",
        "PRAGMA journal_mode = WAL",
        "PRAGMA synchronous = OFF",
        "PRAGMA temp_store = MEMORY",
        f"PRAGMA mmap_size = {profile.mmap_size}",
        f"PRAGMA cache_size = {profile.cache_size}",
        f"PRAGMA busy_timeout = {profile.busy_timeout}",
    )


def _apply_pragmas(
    dbapi_connection: sqlite3.Connection,
    connection_record: object,
    *,
    statements: tuple[str, ...],
) -> None:
    """Apply validated SQLite settings to one new DBAPI connection.

    Parameters:
        dbapi_connection: sqlite3.Connection opened by SQLAlchemy.
        connection_record: object containing SQLAlchemy pool bookkeeping.
        statements: tuple[str, ...] containing validated fixed-name PRAGMAs.
    """

    del connection_record
    cursor = dbapi_connection.cursor()
    try:
        for statement in statements:
            cursor.execute(statement)
    finally:
        cursor.close()


def create_metadata_engine(
    sql_alchemy_conn: str,
    *,
    engine_args: dict[str, object],
    connect_args: dict[str, object],
) -> Engine:
    """Create Airflow's metadata engine and tune every SQLite connection.

    Parameters:
        sql_alchemy_conn: str containing a SQLAlchemy database URL.
        engine_args: dict[str, object] containing Airflow engine arguments.
        connect_args: dict[str, object] containing DBAPI connection arguments.

    Returns:
        sqlalchemy.engine.Engine configured like Airflow's default engine.
    """

    engine = create_engine(
        sql_alchemy_conn,
        connect_args=dict(connect_args),
        **dict(engine_args),
        future=True,
    )
    if make_url(sql_alchemy_conn).get_backend_name() != "sqlite":
        return engine

    statements = _pragma_statements(calculate_profile())
    event.listen(engine, "connect", partial(_apply_pragmas, statements=statements))
    return engine


def write_local_settings(path: Path) -> None:
    """Write deterministic Airflow metadata-engine customization source.

    Parameters:
        path: pathlib.Path receiving ``airflow_local_settings.py``.

    Raises:
        ValueError: The destination path is not absolute.
        OSError: The destination cannot be written.
    """

    if not path.is_absolute():
        raise ValueError(f"`path` must be absolute: '{path}'")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="ascii", newline="\n") as settings_file:
        settings_file.write(LOCAL_SETTINGS_SOURCE)


__all__ = ("PragmaProfile", "calculate_profile", "create_metadata_engine", "write_local_settings")
