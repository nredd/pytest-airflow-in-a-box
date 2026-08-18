"""Tune SQLite metadata engines for disposable Airflow test databases.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/howto/customize-ui.html
    https://docs.sqlalchemy.org/en/20/core/events.html#sqlalchemy.events.PoolEvents.connect
    https://www.sqlite.org/pragma.html
"""

from __future__ import annotations

import importlib
import os
import sqlite3
import sys
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from importlib import metadata
from importlib import util as importlib_util
from pathlib import Path
from threading import Lock

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.exc import ArgumentError

from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily, installed_family

MIB = 1024 * 1024
PAGE_SIZE = 8192
MAX_MMAP_SIZE = 256 * MIB
MAX_CACHE_SIZE_KIB = 128 * 1024
BUSY_TIMEOUT_MILLISECONDS = 30_000
FALLBACK_MEMORY_BYTES = 512 * MIB
AIRFLOW_CORE_DISTRIBUTION = AirflowFamily.V3.value
SQL_ALCHEMY_CONN_ENVIRONMENT_VARIABLE = "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
LOCAL_SETTINGS_SOURCE = '''"""Configure the Airflow metadata engine for isolated tests."""

from pytest_airflow_in_a_box.storage import (
    create_metadata_engine,
    install_legacy_sqlite_listener,
)

install_legacy_sqlite_listener()

__all__ = ("create_metadata_engine",)
'''
# Composes a configured cluster-policy module the same way Airflow's own
# `airflow.settings.import_local_settings` composes ours: `__all__` if present, else
# every non-dunder module attribute. Explicit `import_module` + `globals().update`
# rather than `from ... import *`, which needs a lint waiver this repo forbids.
USER_MODULE_BLOCK = """
from importlib import import_module

_user_module = import_module("{module_path}")
_user_names = getattr(_user_module, "__all__", None)
if _user_names is None:
    _user_names = tuple(name for name in vars(_user_module) if not name.startswith("__"))
globals().update({{name: getattr(_user_module, name) for name in _user_names}})
__all__ = tuple(sorted(set(__all__) | set(_user_names)))
"""
_LEGACY_LISTENER_LOCK = Lock()
_LEGACY_SQLITE_LISTENER: Callable[[object, object], None] | None = None


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


def _configured_sqlite_path() -> Path | None:
    """Resolve the SQLite file selected by Airflow's database environment variable.

    Returns:
        pathlib.Path containing the resolved SQLite file, or None for another backend.

    Raises:
        ValueError: The configured URL is missing, malformed, or not an absolute file URL.
    """

    sql_alchemy_conn = os.environ.get(SQL_ALCHEMY_CONN_ENVIRONMENT_VARIABLE)
    if not sql_alchemy_conn:
        raise ValueError(
            f"`{SQL_ALCHEMY_CONN_ENVIRONMENT_VARIABLE}` must contain a database URL: "
            f"'{sql_alchemy_conn}'"
        )
    try:
        url = make_url(sql_alchemy_conn)
    except (ArgumentError, TypeError, ValueError) as error:
        raise ValueError(
            f"`{SQL_ALCHEMY_CONN_ENVIRONMENT_VARIABLE}` contains an invalid database URL: "
            f"'{sql_alchemy_conn}'"
        ) from error
    if url.get_backend_name() != "sqlite":
        return None

    database_path = url.database
    if database_path is None or database_path == ":memory:":
        raise ValueError(
            f"`{SQL_ALCHEMY_CONN_ENVIRONMENT_VARIABLE}` must select a SQLite file: "
            f"'{sql_alchemy_conn}'"
        )
    path = Path(database_path)
    if not path.is_absolute():
        raise ValueError(
            f"`{SQL_ALCHEMY_CONN_ENVIRONMENT_VARIABLE}` must select an absolute SQLite path: "
            f"'{database_path}' from '{sql_alchemy_conn}'"
        )
    try:
        return path.resolve()
    except (OSError, RuntimeError, ValueError) as error:
        raise ValueError(
            f"`{SQL_ALCHEMY_CONN_ENVIRONMENT_VARIABLE}` contains an invalid SQLite path: "
            f"'{database_path}' from '{sql_alchemy_conn}'"
        ) from error


def _sqlite_main_path(dbapi_connection: sqlite3.Connection) -> Path | None:
    """Resolve the main database file attached to a SQLite connection.

    Parameters:
        dbapi_connection: sqlite3.Connection inspected before tuning.

    Returns:
        pathlib.Path containing the resolved main database file, or None when unavailable.
    """

    cursor = dbapi_connection.cursor()
    try:
        databases = cursor.execute("PRAGMA database_list").fetchall()
    finally:
        cursor.close()
    for _, name, database_path in databases:
        if name != "main" or not isinstance(database_path, str) or not database_path:
            continue
        try:
            return Path(database_path).resolve()
        except (OSError, RuntimeError, ValueError):
            return None
    return None


def _apply_legacy_pragmas(
    dbapi_connection: object,
    connection_record: object,
    *,
    configured_path: Path,
    statements: tuple[str, ...],
) -> None:
    """Tune only SQLite connections opened against the configured Airflow file.

    Parameters:
        dbapi_connection: object opened by a SQLAlchemy engine.
        connection_record: object containing SQLAlchemy pool bookkeeping.
        configured_path: pathlib.Path selecting the exact Airflow metadata file.
        statements: tuple[str, ...] containing validated fixed-name PRAGMAs.
    """

    if not isinstance(dbapi_connection, sqlite3.Connection):
        return
    if _sqlite_main_path(dbapi_connection) != configured_path:
        return
    _apply_pragmas(dbapi_connection, connection_record, statements=statements)


def install_legacy_sqlite_listener() -> None:
    """Install a process-global, path-scoped SQLite tuning fallback.

    Airflow 3.1 and 3.2.0 import ``airflow_local_settings.py`` before creating the
    metadata engine but do not apply the ``create_metadata_engine`` override reliably.
    Airflow 2.x has no ``create_metadata_engine`` override point at all, so every 2.x
    release takes this listener path.

    Raises:
        ValueError: Airflow has an invalid configured database URL or SQLite path.
    """

    try:
        installed_version = metadata.version(AIRFLOW_CORE_DISTRIBUTION)
    except metadata.PackageNotFoundError:
        installed_version = None
    if installed_version is not None:
        uses_fallback = installed_version.startswith(("3.1.", "3.2.0"))
        if not uses_fallback:
            return
    elif installed_family() is not AirflowFamily.V2:
        return

    global _LEGACY_SQLITE_LISTENER
    with _LEGACY_LISTENER_LOCK:
        if _LEGACY_SQLITE_LISTENER is not None:
            return
        configured_path = _configured_sqlite_path()
        if configured_path is None:
            return
        listener = partial(
            _apply_legacy_pragmas,
            configured_path=configured_path,
            statements=_pragma_statements(calculate_profile()),
        )
        event.listen(Engine, "connect", listener)
        _LEGACY_SQLITE_LISTENER = listener


def _reset_legacy_sqlite_listener_for_testing() -> None:
    """Remove the process-global legacy listener for isolated unit tests."""

    global _LEGACY_SQLITE_LISTENER
    with _LEGACY_LISTENER_LOCK:
        if _LEGACY_SQLITE_LISTENER is None:
            return
        event.remove(Engine, "connect", _LEGACY_SQLITE_LISTENER)
        _LEGACY_SQLITE_LISTENER = None


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


def local_settings_path(root: Path) -> Path:
    """Derive this run's generated ``airflow_local_settings.py`` location.

    Parameters:
        root: pathlib.Path containing this run's isolated Airflow home.

    Returns:
        pathlib.Path containing the generated ``airflow_local_settings.py``.
    """

    return root / "config" / "airflow_local_settings.py"


def write_local_settings(path: Path, *, user_module: str | None = None) -> None:
    """Write deterministic Airflow metadata-engine customization source.

    Parameters:
        path: pathlib.Path receiving ``airflow_local_settings.py``.
        user_module: str | None naming a dotted module composed into the generated
            source via ``import_module`` and ``globals().update``, or None to write the
            bare engine-tuning source.

    Raises:
        ValueError: The destination path is not absolute.
        OSError: The destination cannot be written.
    """

    if not path.is_absolute():
        raise ValueError(f"`path` must be absolute: '{path}'")
    source = LOCAL_SETTINGS_SOURCE
    if user_module is not None:
        source += USER_MODULE_BLOCK.format(module_path=user_module)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="ascii", newline="\n") as settings_file:
        settings_file.write(source)


def check_local_settings_collision(path: Path) -> None:
    """Fail loudly when a foreign ``airflow_local_settings`` would outrank this run's own.

    Airflow's own ``settings.prepare_syspath_for_config_and_plugins`` appends
    ``AIRFLOW_HOME/config`` to the END of ``sys.path``, while pytest's default import
    mode inserts a project's root at the FRONT. A project-root ``airflow_local_settings.py``
    therefore wins the plain ``import airflow_local_settings`` Airflow performs later,
    silently dropping this run's SQLite engine tuning. This mirrors that same
    ``sys.path.append`` and resolves the same module name before Airflow ever does, while
    the interpreter is still guaranteed not to have imported it yet this session.

    Parameters:
        path: pathlib.Path containing this run's generated ``airflow_local_settings.py``.

    Raises:
        pytest.UsageError: A foreign module would resolve first, the module could not be
            resolved at all, or the resolved module is an unresolvable namespace package.
    """

    config_dir = str(path.parent)
    if config_dir not in sys.path:
        sys.path.append(config_dir)
    importlib.invalidate_caches()
    spec = importlib_util.find_spec("airflow_local_settings")
    if spec is None:
        raise pytest.UsageError(
            f"`airflow_local_settings` did not resolve after generating '{path}'; check "
            "that its directory is not excluded from Python's module search"
        )
    if spec.origin is None:
        raise pytest.UsageError(
            "`airflow_local_settings` resolves to a namespace package with no single "
            f"origin file, so this run's generated '{path}' cannot be told apart from it"
        )
    if Path(spec.origin).resolve() != path.resolve():
        raise pytest.UsageError(
            f"`airflow_local_settings` resolves to '{spec.origin}', not this run's "
            f"generated '{path}'; move the foreign module aside, or set the "
            "`airflow_local_settings` ini option to its dotted module path instead"
        )


def validate_local_settings_module(value: str) -> None:
    """Validate an ini-configured dotted module path before it is composed.

    Parameters:
        value: str containing the candidate ``airflow_local_settings`` ini value.

    Raises:
        pytest.UsageError: The value is not an importable, resolvable dotted module path.
    """

    if "/" in value or "\\" in value or value.endswith(".py"):
        raise pytest.UsageError(
            "Ini option `airflow_local_settings` must be a dotted module path, not a "
            f"file path: '{value}'"
        )
    segments = value.split(".")
    if not all(segment.isidentifier() and segment.isascii() for segment in segments):
        raise pytest.UsageError(
            f"Ini option `airflow_local_settings` must be a dotted module path: '{value}'"
        )
    importlib.invalidate_caches()
    try:
        spec = importlib_util.find_spec(value)
    except (ImportError, ValueError) as error:
        raise pytest.UsageError(
            "Ini option `airflow_local_settings` names a module that cannot be imported: "
            f"'{value}'"
        ) from error
    if spec is None:
        raise pytest.UsageError(
            "Ini option `airflow_local_settings` names a module that cannot be imported: "
            f"'{value}'"
        )
    if spec.origin is None:
        raise pytest.UsageError(
            "Ini option `airflow_local_settings` resolves to a namespace package with no "
            f"single origin file: '{value}'"
        )


__all__ = (
    "PragmaProfile",
    "calculate_profile",
    "check_local_settings_collision",
    "create_metadata_engine",
    "install_legacy_sqlite_listener",
    "local_settings_path",
    "validate_local_settings_module",
    "write_local_settings",
)
