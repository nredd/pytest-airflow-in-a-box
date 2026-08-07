"""Initialize the isolated Apache Airflow metadata database.

Airflow is imported only after bootstrap and capability validation are complete.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html
    https://airflow.apache.org/docs/apache-airflow/stable/installation/setting-up-the-database.html
"""

from __future__ import annotations

from threading import Lock

from pytest_airflow_in_a_box._compat.capabilities import resolve_capabilities

_INITIALIZATION_LOCK = Lock()
_DATABASE_INITIALIZED = False


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


def _reset_database_for_testing() -> None:
    """Clear process-local initialization state for isolated unit tests."""

    global _DATABASE_INITIALIZED
    with _INITIALIZATION_LOCK:
        _DATABASE_INITIALIZED = False


__all__ = ("DatabaseInitializationError", "initialize_database")
