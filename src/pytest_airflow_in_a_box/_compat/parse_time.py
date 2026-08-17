"""Resolve Variable and Connection lookups that run at Dag parse time.

Airflow 3 routes every Variable and Connection lookup through the Task SDK, which
expects a supervisor to answer on the other end of
``airflow.sdk.execution_time.task_runner.SUPERVISOR_COMMS``. Dag parsing under test
has no supervisor, so a ``Variable.get()`` or ``BaseHook.get_connection()`` written at
Dag top level cannot see the rows ``airflow_variables`` / ``airflow_connections``
committed. The failure differs by release, from one root cause:

- Airflow 3.1's ``DEFAULT_SECRETS_SEARCH_PATH_WORKERS`` is the environment backend
  alone, so ``_get_variable`` / ``_get_connection`` fall through to a tail that does
  ``from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS`` and raises
  ``ImportError: cannot import name 'SUPERVISOR_COMMS'``.
- Airflow 3.2 and newer gained a three-way context detection in
  ``ensure_secrets_backend_loaded``, whose fallback chain omits the metastore backend.
  No ``ImportError``, but the lookup silently misses and raises
  ``AirflowNotFoundException`` (or returns the caller's default).

Both branches key off ``SUPERVISOR_COMMS`` being set: assigning it satisfies 3.1's
direct import and selects 3.2+'s client chain. So one shim covers every certified 3.x
release. Airflow 2.x reads the metastore directly at parse time and needs nothing.

Upstream intends to fix the root cause with a lazy-init ``InProcessExecutionAPI``
(apache/airflow#61630), which will move this surface. The capability contract guards
that: ``airflow.sdk.execution_time.context._get_variable`` / ``._get_connection`` are
required symbols, so a release that relocates them fails validation loudly instead of
letting this shim degrade into a silent no-op.

References:
    https://github.com/apache/airflow/issues/51816
    https://github.com/apache/airflow/issues/48554
    https://github.com/apache/airflow/pull/61630
    https://github.com/apache/airflow/blob/main/task-sdk/src/airflow/sdk/execution_time/context.py
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from pytest_airflow_in_a_box._compat.capabilities import resolve_capabilities
from pytest_airflow_in_a_box._compat.database import ensure_database
from pytest_airflow_in_a_box._compat.in_process import FakeSupervisorComms
from pytest_airflow_in_a_box._compat.seed import CONNECTION_FIELDS, open_seed_session

if TYPE_CHECKING:
    from pathlib import Path

    from sqlalchemy.orm import Session

LOGGER = logging.getLogger(__name__)

SUPERVISOR_COMMS_ATTRIBUTE = "SUPERVISOR_COMMS"


class ParseTimeComms(FakeSupervisorComms):
    """Answer parse-time Variable and Connection lookups from the metadata database.

    The database is opened lazily, on the first lookup a Dag actually issues, so a Dag
    folder whose files never touch Variables or Connections costs nothing. Airflow's
    environment secrets backend still runs ahead of this shim, exactly as it does at
    task time, so ``AIRFLOW_VAR_<KEY>`` and ``AIRFLOW_CONN_<CONN_ID>`` keep outranking
    seeded rows.

    Parameters:
        run_root: pathlib.Path of the session's disposable run root directory.
        xcoms: dict[str, Any] | None seeding XCom values by key.
        variables: dict[str, str] | None overriding Variable values by key, ahead of
            the metadata database.
        connections: dict[str, dict[str, Any]] | None overriding connection fields by
            connection id, ahead of the metadata database.
    """

    def __init__(
        self,
        *,
        run_root: Path,
        xcoms: dict[str, Any] | None = None,
        variables: dict[str, str] | None = None,
        connections: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(xcoms=xcoms, variables=variables, connections=connections)
        self.run_root = run_root
        self._session: Session | None = None

    def _metadata_session(self) -> Session:
        """Open, and thereafter reuse, the session backing metadata lookups.

        Returns:
            sqlalchemy.orm.Session connected to Airflow metadata.

        Raises:
            DatabaseInitializationError: The metadata database cannot be initialized.
            SeedPersistenceError: Airflow cannot provide a metadata session.
        """

        if self._session is None:
            ensure_database(self.run_root)
            self._session = open_seed_session("parse-time secrets")
        return self._session

    def close(self) -> None:
        """Close the metadata session, if one was ever opened."""

        if self._session is not None:
            self._session.close()
            self._session = None

    def _lookup_variable(self, key: str) -> str | None:
        """Resolve one Variable from the overrides, then the metadata database.

        Parameters:
            key: str naming the requested Variable.

        Returns:
            str | None containing the resolved value, or None when no row exists.
        """

        override = super()._lookup_variable(key)
        if override is not None:
            return override

        # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
        from airflow.models.variable import Variable
        from sqlalchemy import select

        session = self._metadata_session()
        # Read through the ORM rather than the column so Airflow's Fernet decryption
        # runs, matching how `_compat.seed` writes the row.
        row = session.scalars(select(Variable).where(Variable.key == key)).first()
        if row is None:
            return None
        # `Variable.val` is a decrypting synonym, so its declared type is the SQLAlchemy
        # descriptor rather than the string it yields on an instance.
        value: object = row.val
        return value if isinstance(value, str) else None

    def _lookup_connection(self, conn_id: str) -> dict[str, Any] | None:
        """Resolve one Connection from the overrides, then the metadata database.

        Parameters:
            conn_id: str naming the requested Connection.

        Returns:
            dict[str, Any] | None containing the resolved fields, or None when no row
            exists.
        """

        override = super()._lookup_connection(conn_id)
        if override is not None:
            return override

        # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
        from airflow.models.connection import Connection
        from sqlalchemy import select

        session = self._metadata_session()
        row = session.scalars(select(Connection).where(Connection.conn_id == conn_id)).first()
        if row is None:
            return None
        fields = {name: getattr(row, name, None) for name in sorted(CONNECTION_FIELDS)}
        return {name: value for name, value in fields.items() if value is not None}

    def _variable_hint(self, key: str) -> str:
        """Build the seeding hint carried by an unresolved Variable response.

        Parameters:
            key: str naming the requested Variable.

        Returns:
            str containing the actionable seeding hint.
        """

        return (
            f"Seed it before the Dag is parsed via `airflow_variables({{{key!r}: ...}})`, "
            f"from a session-scoped fixture when the Dag folder is parsed once per session"
        )

    def _connection_hint(self, conn_id: str) -> str:
        """Build the seeding hint carried by an unresolved Connection response.

        Parameters:
            conn_id: str naming the requested Connection.

        Returns:
            str containing the actionable seeding hint.
        """

        return (
            f"Seed it before the Dag is parsed via "
            f"`airflow_connections({{{conn_id!r}: {{'conn_type': ...}}}})`, from a "
            f"session-scoped fixture when the Dag folder is parsed once per session"
        )


def _task_runner_module() -> Any:
    """Import the Task SDK runner module without static attribute constraints.

    The module-global ``SUPERVISOR_COMMS`` must be replaced and restored, which a
    statically typed module reference would reject.

    Returns:
        Any containing the ``airflow.sdk.execution_time.task_runner`` module.
    """

    # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
    from airflow.sdk.execution_time import task_runner

    return task_runner


def _reset_secret_cache() -> None:
    """Drop Airflow's process-wide secrets cache.

    The cache is inert unless ``[secrets] use_cache`` is enabled, but a consumer who
    enables it would otherwise carry one parse's resolved values into the next, which
    is precisely the cross-test leak the seeding fixtures exist to prevent.
    """

    # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
    from airflow.sdk.execution_time.cache import SecretCache

    SecretCache.reset()


@contextmanager
def parse_time_supervision(comms: ParseTimeComms | None) -> Iterator[None]:
    """Answer Dag-parse-time Variable and Connection lookups for the enclosed block.

    Yields without touching anything when `comms` is None (the consumer opted out), on
    Airflow 2.x (which reads the metastore directly and needs no shim), or when a
    supervisor endpoint is already installed -- an in-process task run owns that
    endpoint and its seeded state must not be replaced mid-flight.

    Parameters:
        comms: ParseTimeComms | None answering supervisor messages during the block,
            or None to leave Airflow's own resolution untouched.

    Yields:
        None, with the shim installed for the duration of the block.

    Raises:
        AirflowCompatibilityError: The installed Airflow interface is unsupported.
    """

    if comms is None:
        yield
        return

    if not resolve_capabilities().has_task_sdk:
        yield
        return

    task_runner = _task_runner_module()
    absent = object()
    previous_comms = getattr(task_runner, SUPERVISOR_COMMS_ATTRIBUTE, absent)
    if previous_comms is not absent and previous_comms is not None:
        LOGGER.debug(
            "A supervisor endpoint is already installed; leaving parse-time secrets to it."
        )
        yield
        return

    _reset_secret_cache()
    task_runner.SUPERVISOR_COMMS = comms
    try:
        yield
    finally:
        if previous_comms is absent:
            del task_runner.SUPERVISOR_COMMS
        else:
            task_runner.SUPERVISOR_COMMS = previous_comms
        _reset_secret_cache()
        comms.close()


__all__ = ("ParseTimeComms", "parse_time_supervision")
