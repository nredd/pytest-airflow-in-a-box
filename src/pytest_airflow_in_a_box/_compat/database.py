"""Initialize the isolated Apache Airflow metadata database.

Airflow is imported only after bootstrap and capability validation are complete.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html
    https://airflow.apache.org/docs/apache-airflow/stable/installation/setting-up-the-database.html
"""

from __future__ import annotations

from pathlib import Path
from threading import Lock

from pytest_airflow_in_a_box._compat.capabilities import resolve_capabilities

DATABASE_INIT_LOCK_NAME = ".airflow-db-init.lock"
DATABASE_READY_SENTINEL_NAME = ".airflow-db-ready"

_INITIALIZATION_LOCK = Lock()
_DATABASE_INITIALIZED = False
_DATABASE_ENSURED = False


class DatabaseInitializationError(RuntimeError):
    """Report failure to initialize the isolated Airflow metadata database."""


def _initialize_airflow_database() -> None:
    """Call Airflow's public database initializer after bootstrap validation."""

    # Deferred to preserve pre-bootstrap safety for every compatibility import.
    from airflow.utils.db import initdb

    initdb()


def initialize_database() -> None:
    """Validate Airflow and initialize its metadata database once per process.

    Raises:
        AirflowCompatibilityError: The installed Airflow interface is unsupported.
        DatabaseInitializationError: Airflow cannot initialize the configured database.
    """

    resolve_capabilities()

    global _DATABASE_INITIALIZED
    with _INITIALIZATION_LOCK:
        if _DATABASE_INITIALIZED:
            return
        try:
            _initialize_airflow_database()
        except Exception as error:
            raise DatabaseInitializationError(
                "Could not initialize the isolated Airflow metadata database with "
                "`airflow.utils.db.initdb()`; verify the configured database URL and "
                f"filesystem permissions: {error}"
            ) from error
        _DATABASE_INITIALIZED = True


def ensure_database(run_root: Path) -> None:
    """Initialize the metadata database at most once across cooperating processes.

    An advisory ``flock`` below `run_root` elects exactly one initializer among
    concurrently racing processes (e.g. ``pytest-xdist`` workers); every other
    process blocks until the winner finishes, then observes the ready sentinel
    and returns without importing Airflow.

    Parameters:
        run_root: pathlib.Path of the session's disposable run root directory.

    Raises:
        AirflowCompatibilityError: The installed Airflow interface is unsupported.
        DatabaseInitializationError: The database cannot be initialized, or the
            cross-process coordination files below `run_root` cannot be used.
    """

    global _DATABASE_ENSURED
    with _INITIALIZATION_LOCK:
        if _DATABASE_ENSURED:
            return

    # Deferred import cost: POSIX-only module, matching Airflow's supported
    # platforms; keeps this module importable for platform-independent checks.
    import fcntl

    sentinel_path = run_root / DATABASE_READY_SENTINEL_NAME
    lock_path = run_root / DATABASE_INIT_LOCK_NAME
    try:
        with lock_path.open("ab") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                if not sentinel_path.exists():
                    initialize_database()
                    sentinel_path.touch()
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    except OSError as error:
        raise DatabaseInitializationError(
            "Could not coordinate cross-process Airflow metadata database "
            f"initialization below '{run_root}': {error}"
        ) from error
    with _INITIALIZATION_LOCK:
        _DATABASE_ENSURED = True


def _reset_database_for_testing() -> None:
    """Clear process-local initialization state for isolated unit tests."""

    global _DATABASE_ENSURED, _DATABASE_INITIALIZED
    with _INITIALIZATION_LOCK:
        _DATABASE_INITIALIZED = False
        _DATABASE_ENSURED = False


__all__ = ("DatabaseInitializationError", "ensure_database", "initialize_database")
