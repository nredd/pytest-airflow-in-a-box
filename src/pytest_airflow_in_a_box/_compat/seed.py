"""Seed and remove fixture-owned Variable and Connection metadata rows.

Ownership follows the ``dag`` compatibility module: a seed refuses to overwrite
a row it does not own, and teardown deletes exactly the primary keys it
inserted. Snapshot-and-restore is deliberately not used -- these rows are global
to a metadata database every ``xdist`` worker shares, so restoring a previous
value would silently revert a concurrent worker's write.

Airflow resolves Variables and Connections through
``DEFAULT_SECRETS_SEARCH_PATH``, which places the environment backend ahead of
the metastore backend. A seeded row is therefore unreachable while
``AIRFLOW_VAR_<KEY>`` or ``AIRFLOW_CONN_<CONN_ID>`` is set, so seeding refuses
rather than leaving a silently shadowed row behind.

Rows are built through the ORM constructors instead of ``Variable.set`` and the
Task SDK writers, which reroute through ``SUPERVISOR_COMMS`` when the in-process
runner has installed it and whose team-scoping keyword drifted across certified
releases.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html
    https://github.com/apache/airflow/blob/main/airflow-core/src/airflow/models/variable.py
    https://github.com/apache/airflow/blob/main/airflow-core/src/airflow/models/connection.py
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pytest_airflow_in_a_box._compat.capabilities import resolve_capabilities

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

# Mirrors `airflow.models.base.ID_LEN` and `airflow.models.connection.CONN_ID_MAX_LEN`
# without importing Airflow, which must stay deferred in this package.
IDENTIFIER_MAX_LENGTH = 250
# Mirrors `airflow.models.connection.RE_SANITIZE_CONN_ID`.
CONN_ID_PATTERN = re.compile(r"^[\w#!()\-.:/\\]+$")
CONNECTION_FIELDS = frozenset(
    {"conn_type", "description", "extra", "host", "login", "password", "port", "schema"}
)
# Parity with the DB-free seeding path in `_compat.in_process`.
DEFAULT_CONN_TYPE = "generic"
VARIABLE_ENV_PREFIX = "AIRFLOW_VAR_"
CONNECTION_ENV_PREFIX = "AIRFLOW_CONN_"


class SeedPersistenceError(RuntimeError):
    """Report failure to persist fixture-owned Variable or Connection rows."""


class SeedCleanupError(RuntimeError):
    """Report failure to remove fixture-owned Variable or Connection rows."""


@dataclass
class SeedRecord:
    """Track the metadata rows one seeding fixture owns for one test.

    Primary keys alone are not a stable identity on this database, so each id is
    recorded *paired with its own row's* natural key (issue #325). A pairing, not two
    parallel sets: matching id-set against key-set independently is a cross product,
    and a stranger's row carrying one owned id and a different owned key satisfies
    both halves. ``variable.id`` and ``connection.id``
    are plain ``Integer`` primary keys with no ``sqlite_autoincrement``, so on SQLite
    they are rowid aliases whose values are reused once the highest row is deleted --
    Airflow guards exactly this on three asset tables with
    ``{"sqlite_autoincrement": True}`` ("ensures PK values not reused") and guards
    neither of these. Every xdist worker shares one metadata database, so cleanup that
    deletes by bare primary key can take another worker's live seed.

    Parameters:
        session: sqlalchemy.orm.Session writing and removing owned rows.
        kind: str naming the seeded entity for failure diagnostics.
        variables: dict[int, str] mapping each owned ``variable`` primary key to that
            row's own ``key``.
        connections: dict[int, str] mapping each owned ``connection`` primary key to
            that row's own ``conn_id``.
    """

    session: Session
    kind: str
    variables: dict[int, str] = field(default_factory=dict)
    connections: dict[int, str] = field(default_factory=dict)


def open_seed_session(kind: str) -> Session:
    """Open a metadata session after validating the certified Airflow contract.

    Parameters:
        kind: str naming the seeded entity for failure diagnostics.

    Returns:
        sqlalchemy.orm.Session connected to Airflow metadata.

    Raises:
        SeedPersistenceError: Airflow cannot provide a metadata session.
    """

    try:
        resolve_capabilities()
        # Deferred because Airflow settings are bootstrap-sensitive.
        from airflow import settings

        session_factory = settings.Session
        if session_factory is None:
            raise RuntimeError("Airflow metadata session factory is not initialized")
        return session_factory.session_factory()
    except Exception as error:
        raise SeedPersistenceError(
            f"Could not open an Airflow metadata session to seed {kind}: {error}"
        ) from error


def _validate_identifier(value: Any, *, label: str) -> str:
    """Validate one Variable key or connection id shared constraint.

    Parameters:
        value: Any containing the prospective identifier.
        label: str naming the identifier in failure messages.

    Returns:
        str containing the validated identifier.

    Raises:
        TypeError: The identifier is not a string.
        ValueError: The identifier is empty or longer than Airflow allows.
    """

    if not isinstance(value, str):
        raise TypeError(f"`{label}` must be a string: '{value}'")
    if not value:
        raise ValueError(f"`{label}` must not be empty")
    if len(value) > IDENTIFIER_MAX_LENGTH:
        raise ValueError(
            f"`{label}` must be at most {IDENTIFIER_MAX_LENGTH} characters: '{value}'"
        )
    return value


def _reject_shadowing_env_var(identifier: str, *, prefix: str, fixture: str) -> None:
    """Refuse to seed a row Airflow's environment backend already outranks.

    Parameters:
        identifier: str containing the validated Variable key or connection id.
        prefix: str containing the Airflow environment variable prefix.
        fixture: str naming the seeding fixture in failure messages.

    Raises:
        ValueError: The corresponding environment variable is set.
    """

    name = f"{prefix}{identifier.upper()}"
    if name in os.environ:
        raise ValueError(
            f"`{name}` is set and outranks the metadata database, so `{fixture}` cannot seed "
            f"'{identifier}'; Airflow reads the environment secrets backend first"
        )


def validate_variables(variables: Mapping[str, str]) -> dict[str, str]:
    """Validate one whole batch of Variables before anything is written.

    Parameters:
        variables: Mapping[str, str] containing Variable values by key.

    Returns:
        dict[str, str] containing the validated batch.

    Raises:
        TypeError: The batch is not a mapping, or a key or value is not a string.
        ValueError: A key is empty, too long, or shadowed by an environment variable.
    """

    if not isinstance(variables, Mapping):
        raise TypeError(f"`variables` must be a mapping: '{variables}'")
    validated: dict[str, str] = {}
    for key, value in variables.items():
        validated_key = _validate_identifier(key, label="key")
        if not isinstance(value, str):
            raise TypeError(f"`variables['{validated_key}']` must be a string: '{value}'")
        _reject_shadowing_env_var(
            validated_key, prefix=VARIABLE_ENV_PREFIX, fixture="airflow_variables"
        )
        validated[validated_key] = value
    return validated


def _validate_connection_fields(conn_id: str, fields: Mapping[str, Any]) -> dict[str, Any]:
    """Validate one connection's flat field mapping.

    Parameters:
        conn_id: str containing the validated connection id.
        fields: Mapping[str, Any] containing connection fields.

    Returns:
        dict[str, Any] containing the validated fields with a default `conn_type`.

    Raises:
        TypeError: The fields are not a mapping, or `port` is not an integer.
        ValueError: A field is unknown, or `conn_type` or `extra` is malformed.
    """

    if not isinstance(fields, Mapping):
        raise TypeError(f"`connections['{conn_id}']` must be a mapping: '{fields}'")
    unknown = sorted(set(fields) - CONNECTION_FIELDS)
    if unknown:
        if "uri" in unknown:
            raise ValueError(
                f"`connections['{conn_id}']` does not accept `uri`; pass the flat fields "
                f"{sorted(CONNECTION_FIELDS)} instead"
            )
        names = ", ".join(f"`{name}`" for name in unknown)
        raise ValueError(
            f"`connections['{conn_id}']` has unknown fields {names}; supported fields are "
            f"{sorted(CONNECTION_FIELDS)}"
        )
    validated = dict(fields)
    conn_type = validated.get("conn_type", DEFAULT_CONN_TYPE)
    if not isinstance(conn_type, str):
        raise TypeError(f"`connections['{conn_id}']['conn_type']` must be a string: '{conn_type}'")
    if not conn_type:
        raise ValueError(f"`connections['{conn_id}']['conn_type']` must not be empty")
    validated["conn_type"] = conn_type
    port = validated.get("port")
    if port is not None and (isinstance(port, bool) or not isinstance(port, int)):
        raise TypeError(f"`connections['{conn_id}']['port']` must be an integer: '{port}'")
    extra = validated.get("extra")
    if extra is not None:
        _validate_extra(conn_id, extra)
    return validated


def _validate_extra(conn_id: str, extra: Any) -> None:
    """Require a JSON object string, matching `run_task(connections=...)`.

    Parameters:
        conn_id: str containing the validated connection id.
        extra: Any containing the prospective `extra` value.

    Raises:
        TypeError: The value is not a string.
        ValueError: The value is not a JSON object.
    """

    if not isinstance(extra, str):
        raise TypeError(
            f"`connections['{conn_id}']['extra']` must be a JSON object string: '{extra}'"
        )
    try:
        decoded: object = json.loads(extra)
    except json.JSONDecodeError as error:
        raise ValueError(
            f"`connections['{conn_id}']['extra']` must be valid JSON: '{extra}'"
        ) from error
    if not isinstance(decoded, dict):
        raise ValueError(f"`connections['{conn_id}']['extra']` must be a JSON object: '{extra}'")


def validate_connections(
    connections: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Validate one whole batch of Connections before anything is written.

    Parameters:
        connections: Mapping[str, Mapping[str, Any]] containing fields by connection id.

    Returns:
        dict[str, dict[str, Any]] containing the validated batch.

    Raises:
        TypeError: The batch, a connection id, or a field has the wrong type.
        ValueError: A connection id or field value is malformed or shadowed.
    """

    if not isinstance(connections, Mapping):
        raise TypeError(f"`connections` must be a mapping: '{connections}'")
    validated: dict[str, dict[str, Any]] = {}
    for conn_id, fields in connections.items():
        validated_id = _validate_identifier(conn_id, label="conn_id")
        if CONN_ID_PATTERN.match(validated_id) is None:
            raise ValueError(f"`conn_id` must match {CONN_ID_PATTERN.pattern}: '{validated_id}'")
        _reject_shadowing_env_var(
            validated_id, prefix=CONNECTION_ENV_PREFIX, fixture="airflow_connections"
        )
        validated[validated_id] = _validate_connection_fields(validated_id, fields)
    return validated


def _reject_existing_rows(record: SeedRecord, column: Any, keys: list[str]) -> None:
    """Refuse to overwrite rows already present in the metadata database.

    Parameters:
        record: SeedRecord identifying the owning fixture.
        column: Any containing the model's unique identifier column.
        keys: list[str] containing the prospective identifiers.

    Raises:
        ValueError: A row already exists for one of the identifiers.
    """

    # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
    from sqlalchemy import select

    existing = sorted(record.session.scalars(select(column).where(column.in_(keys))))
    if existing:
        names = ", ".join(f"'{name}'" for name in existing)
        raise ValueError(
            f"Airflow metadata already holds {record.kind} {names}; seeding never overwrites an "
            "existing row, so pick unseeded identifiers"
        )


def _persist(record: SeedRecord, rows: list[tuple[str, Any]], owned: dict[int, str]) -> None:
    """Commit one batch of owned rows and record each primary key with its natural key.

    Rows arrive already paired with their natural key so the mapping cannot be built
    from two independently-collected halves; cleanup predicates on the pair, because a
    primary key on its own can name another worker's row after SQLite reuses it (issue
    #325, see ``SeedRecord``). The pairing is written only after ``flush`` assigns the
    keys and is discarded with the session on failure, so it can never disagree with
    what was committed.

    Parameters:
        record: SeedRecord receiving the committed identities.
        rows: list[tuple[str, Any]] pairing each natural key with its unsaved Airflow
            model instance.
        owned: dict[int, str] receiving primary key to natural key for each row.

    Raises:
        SeedPersistenceError: The batch cannot be committed.
    """

    session = record.session
    try:
        session.add_all([row for _, row in rows])
        session.flush()
        owned.update({row.id: key for key, row in rows})
        session.commit()
    except Exception as error:
        session.rollback()
        raise SeedPersistenceError(
            f"Could not seed Airflow {record.kind}: {error}. Seeded identifiers are global to the "
            "metadata database, so give each test unique ones or group colliding tests with "
            "`@pytest.mark.xdist_group`"
        ) from error


def seed_variables(record: SeedRecord, variables: dict[str, str]) -> None:
    """Commit one validated batch of fixture-owned Variables.

    Parameters:
        record: SeedRecord owning the committed rows.
        variables: dict[str, str] containing validated Variable values by key.

    Raises:
        ValueError: A Variable already exists for one of the keys.
        SeedPersistenceError: The batch cannot be committed.
    """

    if not variables:
        return

    # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
    from airflow.models.variable import Variable

    _reject_existing_rows(record, Variable.key, list(variables))
    rows = [(key, Variable(key=key, val=value)) for key, value in variables.items()]
    _persist(record, rows, record.variables)


def seed_connections(record: SeedRecord, connections: dict[str, dict[str, Any]]) -> None:
    """Commit one validated batch of fixture-owned Connections.

    Parameters:
        record: SeedRecord owning the committed rows.
        connections: dict[str, dict[str, Any]] containing validated fields by connection id.

    Raises:
        ValueError: A Connection already exists for one of the connection ids.
        SeedPersistenceError: The batch cannot be committed.
    """

    if not connections:
        return

    # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
    from airflow.models.connection import Connection

    _reject_existing_rows(record, Connection.conn_id, list(connections))
    rows = [
        (conn_id, Connection(conn_id=conn_id, **fields)) for conn_id, fields in connections.items()
    ]
    _persist(record, rows, record.connections)


def cleanup_seeds(record: SeedRecord) -> None:
    """Delete every row this record owns and close its session.

    Parameters:
        record: SeedRecord identifying fixture-owned rows.

    Raises:
        SeedCleanupError: Airflow cannot remove all owned rows.
    """

    try:
        _cleanup_seeds(record)
    except Exception as error:
        record.session.rollback()
        raise SeedCleanupError(
            f"Could not remove seeded Airflow {record.kind}: {error}"
        ) from error
    finally:
        record.session.close()


DELETE_PAIR_CHUNK_SIZE = 200
"""Owned ``(primary key, natural key)`` pairs per cleanup ``DELETE``.

Each pair contributes one ``AND`` node to an ``OR`` chain, and SQLite caps an
expression tree at depth 1000 -- a single statement covering ~1000 pairs raises
``sqlite3.OperationalError: Expression tree is too large``. A seeding fixture can own
arbitrarily many rows (``airflow_variables`` accumulates across calls), so the
predicate is issued in chunks. 200 leaves generous headroom under the cap while
keeping the statement count trivial for the batch sizes tests actually use.
"""


def _delete_owned_rows(
    session: Session,
    model: Any,
    id_column: Any,
    key_column: Any,
    owned: dict[int, str],
) -> None:
    """Delete rows matching owned ``(primary key, natural key)`` pairs, in chunks.

    The predicate is an OR of per-row pairs rather than an id set ANDed with a key set
    (issue #325). The latter is a cross product: a stranger's row carrying one owned id
    and a *different* owned key satisfies both halves, which is the same deletion the
    pairing exists to prevent. Chunked per ``DELETE_PAIR_CHUNK_SIZE`` so a large owned
    batch cannot exceed SQLite's expression-tree depth cap.

    Parameters:
        session: sqlalchemy.orm.Session used for the deletes; not committed here.
        model: Any containing the SQLAlchemy mapped class to delete from.
        id_column: Any containing the model's primary key column.
        key_column: Any containing the model's unique natural-key column.
        owned: dict[int, str] mapping each owned primary key to its own natural key.
    """

    # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
    from sqlalchemy import and_, delete, or_

    pairs = sorted(owned.items())
    for start in range(0, len(pairs), DELETE_PAIR_CHUNK_SIZE):
        chunk = pairs[start : start + DELETE_PAIR_CHUNK_SIZE]
        session.execute(
            delete(model).where(
                or_(*(and_(id_column == row_id, key_column == key) for row_id, key in chunk))
            )
        )


def _cleanup_seeds(record: SeedRecord) -> None:
    """Delete only the rows this record inserted, matched on each id WITH its own key.

    The natural key is not redundant with the primary key here (issue #325): SQLite
    reuses a deleted rowid, and every xdist worker shares one metadata database, so an
    id this record inserted can name another worker's live row by teardown. The
    predicate shape and its chunking live in ``_delete_owned_rows``.

    Parameters:
        record: SeedRecord identifying fixture-owned rows.
    """

    if not record.variables and not record.connections:
        return

    # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
    from airflow.models.connection import Connection
    from airflow.models.variable import Variable

    session = record.session
    session.rollback()
    _delete_owned_rows(session, Variable, Variable.id, Variable.key, record.variables)
    _delete_owned_rows(session, Connection, Connection.id, Connection.conn_id, record.connections)
    session.commit()


__all__ = (
    "CONNECTION_ENV_PREFIX",
    "CONNECTION_FIELDS",
    "DEFAULT_CONN_TYPE",
    "VARIABLE_ENV_PREFIX",
    "SeedCleanupError",
    "SeedPersistenceError",
    "SeedRecord",
    "cleanup_seeds",
    "open_seed_session",
    "seed_connections",
    "seed_variables",
    "validate_connections",
    "validate_variables",
)
