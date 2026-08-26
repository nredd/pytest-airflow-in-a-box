"""Test the public Variable and Connection seeding fixtures.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html
    https://docs.sqlalchemy.org/en/20/orm/session_basics.html
"""

from __future__ import annotations

from typing import Any

import pytest
from airflow.models.connection import Connection
from airflow.models.variable import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState
from sqlalchemy import func, select

from pytest_airflow_in_a_box._compat import seed as compat_seed
from pytest_airflow_in_a_box.types import AirflowConnections, AirflowVariables, DagMaker

pytestmark = pytest.mark.db_test


def _row_count(model: Any, column: Any, value: str) -> int:
    """Count committed rows of one model carrying one identifier.

    Parameters:
        model: Any containing a SQLAlchemy mapped class.
        column: Any containing the model's unique identifier column.
        value: str containing the identifier to count.

    Returns:
        int containing the committed row count.
    """

    with create_session() as session:
        counted = session.scalar(select(func.count()).select_from(model).where(column == value))
    assert isinstance(counted, int)
    return counted


def _resolved_connection(conn_id: str) -> Connection:
    """Resolve one connection the way a live deployment resolves it.

    Parameters:
        conn_id: str containing the seeded connection id.

    Returns:
        airflow.models.connection.Connection resolved through the metastore backend.
    """

    connection = MetastoreBackend().get_connection(conn_id)
    assert connection is not None
    return connection


def test_seeded_variable_resolves_through_the_metastore_backend(
    airflow_variables: AirflowVariables,
) -> None:
    """Resolve a committed Variable the way a live deployment resolves it."""

    airflow_variables({"seed_answer": "42"})

    assert MetastoreBackend().get_variable("seed_answer") == "42"


def test_seeded_connection_resolves_every_supported_field(
    airflow_connections: AirflowConnections,
) -> None:
    """Resolve host, schema, login, port, encrypted password, and typed extra."""

    airflow_connections(
        {
            "seed_full_conn": {
                "conn_type": "http",
                "description": "seeded",
                "host": "127.0.0.1",
                "login": "tester",
                "password": "s3cret",
                "port": 8080,
                "schema": "synthetic",
                "extra": '{"region": "local"}',
            }
        }
    )

    connection = _resolved_connection("seed_full_conn")

    assert connection.conn_type == "http"
    assert connection.description == "seeded"
    assert connection.host == "127.0.0.1"
    assert connection.login == "tester"
    # Reading this back proves the run's Fernet key is pinned, since Airflow
    # encrypts the column on write.
    assert connection.password == "s3cret"
    assert connection.port == 8080
    assert connection.schema == "synthetic"
    assert connection.extra_dejson == {"region": "local"}


def test_seeded_connection_type_defaults_to_generic(
    airflow_connections: AirflowConnections,
) -> None:
    """Default the connection type exactly as the DB-free seeding path does."""

    airflow_connections({"seed_default_type": {"host": "127.0.0.1"}})

    assert _resolved_connection("seed_default_type").conn_type == compat_seed.DEFAULT_CONN_TYPE


def test_repeated_calls_accumulate_owned_rows(
    airflow_variables: AirflowVariables,
    airflow_connections: AirflowConnections,
) -> None:
    """Accumulate rows across calls instead of replacing the previous batch."""

    airflow_variables({"seed_first": "1"})
    airflow_variables({"seed_second": "2"})
    airflow_connections({"seed_conn_first": {"host": "first"}})
    airflow_connections({"seed_conn_second": {"host": "second"}})
    backend = MetastoreBackend()

    assert backend.get_variable("seed_first") == "1"
    assert backend.get_variable("seed_second") == "2"
    assert _resolved_connection("seed_conn_first").host == "first"
    assert _resolved_connection("seed_conn_second").host == "second"


def test_empty_batches_commit_nothing(
    airflow_variables: AirflowVariables,
    airflow_connections: AirflowConnections,
) -> None:
    """Accept an empty batch as a no-op rather than opening a write."""

    airflow_variables({})
    airflow_connections({})


def test_seeded_connection_reaches_a_persisted_task_run(
    airflow_connections: AirflowConnections, dag_maker: DagMaker
) -> None:
    """Resolve a seeded connection inside a real persisted task instance."""

    airflow_connections(
        {"seed_task_conn": {"conn_type": "http", "host": "127.0.0.1", "login": "tester"}}
    )

    with dag_maker(dag_id="seed_task_conn_dag"):
        PythonOperator(task_id="read", python_callable=_read_seeded_connection)

    ti = dag_maker.run_ti("read")

    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids="read", session=dag_maker.session) == "tester@127.0.0.1"


def _read_seeded_connection() -> str:
    """Resolve the seeded connection from inside a running task.

    Returns:
        str joining the seeded login and host.
    """

    # Deferred because this callable runs inside a task, after bootstrap.
    from airflow.sdk import BaseHook

    connection = BaseHook.get_connection("seed_task_conn")
    return f"{connection.login}@{connection.host}"


def test_seeded_rows_are_deleted_after_a_failing_test(pytester: pytest.Pytester) -> None:
    """Delete every owned row on teardown even when the test itself fails."""

    pytester.makeconftest(
        """
        from __future__ import annotations

        _COUNTERS = {
            "airflow_variables": ("Variable", "key", "teardown_variable"),
            "airflow_connections": ("Connection", "conn_id", "teardown_conn"),
        }


        def pytest_fixture_post_finalizer(fixturedef):
            expected = _COUNTERS.get(fixturedef.argname)
            if expected is None:
                return
            model_name, column_name, identifier = expected

            # Deferred because the consumer plugin checks post-bootstrap state.
            from airflow.models.connection import Connection
            from airflow.models.variable import Variable
            from airflow.utils.session import create_session
            from sqlalchemy import func, select

            model = {"Variable": Variable, "Connection": Connection}[model_name]
            column = getattr(model, column_name)
            with create_session() as verifier:
                remaining = verifier.scalar(
                    select(func.count()).select_from(model).where(column == identifier)
                )
            assert remaining == 0
        """
    )
    pytester.makepyfile(
        """
        import pytest

        pytestmark = pytest.mark.db_test


        def test_seeds_then_fails(airflow_variables, airflow_connections):
            airflow_variables({"teardown_variable": "value"})
            airflow_connections({"teardown_conn": {"host": "127.0.0.1"}})

            assert False
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(failed=1)


def test_seeding_fixtures_trigger_lazy_database_initialization(
    pytester: pytest.Pytester,
) -> None:
    """Initialize the metadata database for an unmarked seeding-only test."""

    pytester.makepyfile(
        """
        def test_unmarked(airflow_variables):
            airflow_variables({"lazy_variable": "value"})

            # Deferred because the consumer test checks post-bootstrap state.
            from airflow.secrets.metastore import MetastoreBackend

            assert MetastoreBackend().get_variable("lazy_variable") == "value"
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_seeding_refuses_an_existing_variable(airflow_variables: AirflowVariables) -> None:
    """Refuse to overwrite a Variable row the fixture does not own."""

    with create_session() as session:
        session.add(Variable(key="seed_preexisting", val="original"))
    try:
        with pytest.raises(ValueError, match="already holds Variables 'seed_preexisting'"):
            airflow_variables({"seed_preexisting": "replacement"})
        assert MetastoreBackend().get_variable("seed_preexisting") == "original"
    finally:
        with create_session() as session:
            session.query(Variable).filter(Variable.key == "seed_preexisting").delete()


def test_seeding_refuses_an_existing_connection(
    airflow_connections: AirflowConnections,
) -> None:
    """Refuse to overwrite a Connection row the fixture does not own."""

    with create_session() as session:
        session.add(Connection(conn_id="seed_preexisting_conn", conn_type="http", host="original"))
    try:
        with pytest.raises(ValueError, match="already holds Connections 'seed_preexisting_conn'"):
            airflow_connections({"seed_preexisting_conn": {"host": "hijacked"}})
        assert _resolved_connection("seed_preexisting_conn").host == "original"
    finally:
        with create_session() as session:
            session.query(Connection).filter(
                Connection.conn_id == "seed_preexisting_conn"
            ).delete()


def test_seeding_refuses_a_duplicate_within_one_test(
    airflow_variables: AirflowVariables,
) -> None:
    """Refuse a second seed of a key the same fixture already committed."""

    airflow_variables({"seed_duplicate": "first"})

    with pytest.raises(ValueError, match="already holds Variables 'seed_duplicate'"):
        airflow_variables({"seed_duplicate": "second"})


def test_seeding_refuses_a_shadowing_variable_environment_variable(
    airflow_variables: AirflowVariables, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse a Variable the environment secrets backend already outranks."""

    monkeypatch.setenv("AIRFLOW_VAR_SEED_SHADOWED", "environment")

    with pytest.raises(ValueError, match="`AIRFLOW_VAR_SEED_SHADOWED` is set and outranks"):
        airflow_variables({"seed_shadowed": "metadata"})

    assert _row_count(Variable, Variable.key, "seed_shadowed") == 0


def test_seeding_refuses_a_shadowing_connection_environment_variable(
    airflow_connections: AirflowConnections, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse a Connection the environment secrets backend already outranks."""

    monkeypatch.setenv("AIRFLOW_CONN_SEED_SHADOWED", "http://127.0.0.1")

    with pytest.raises(ValueError, match="`AIRFLOW_CONN_SEED_SHADOWED` is set and outranks"):
        airflow_connections({"seed_shadowed": {"host": "127.0.0.1"}})

    assert _row_count(Connection, Connection.conn_id, "seed_shadowed") == 0


@pytest.mark.parametrize(
    ("variables", "error", "match"),
    [
        (["not-a-mapping"], TypeError, "`variables` must be a mapping"),
        ({7: "value"}, TypeError, "`key` must be a string"),
        ({"": "value"}, ValueError, "`key` must not be empty"),
        ({"k" * 251: "value"}, ValueError, "`key` must be at most 250 characters"),
        ({"seed_bad_value": 42}, TypeError, "`variables\\['seed_bad_value'\\]` must be a string"),
    ],
)
def test_variable_validation_rejects_malformed_batches(
    variables: Any, error: type[Exception], match: str
) -> None:
    """Reject every malformed Variable batch before anything is written."""

    with pytest.raises(error, match=match):
        compat_seed.validate_variables(variables)


@pytest.mark.parametrize(
    ("connections", "error", "match"),
    [
        (["not-a-mapping"], TypeError, "`connections` must be a mapping"),
        ({7: {}}, TypeError, "`conn_id` must be a string"),
        ({"": {}}, ValueError, "`conn_id` must not be empty"),
        ({"c" * 251: {}}, ValueError, "`conn_id` must be at most 250 characters"),
        ({"bad id": {}}, ValueError, "`conn_id` must match"),
        ({"seed_c": "not-a-mapping"}, TypeError, "`connections\\['seed_c'\\]` must be a mapping"),
        ({"seed_c": {"unknown": 1}}, ValueError, "has unknown fields `unknown`"),
        ({"seed_c": {"uri": "http://x"}}, ValueError, "does not accept `uri`"),
        ({"seed_c": {"conn_type": 7}}, TypeError, "\\['conn_type'\\]` must be a string"),
        ({"seed_c": {"conn_type": ""}}, ValueError, "\\['conn_type'\\]` must not be empty"),
        ({"seed_c": {"port": "8080"}}, TypeError, "\\['port'\\]` must be an integer"),
        ({"seed_c": {"port": True}}, TypeError, "\\['port'\\]` must be an integer"),
        ({"seed_c": {"extra": {"a": 1}}}, TypeError, "must be a JSON object string"),
        ({"seed_c": {"extra": "{"}}, ValueError, "\\['extra'\\]` must be valid JSON"),
        ({"seed_c": {"extra": "[1]"}}, ValueError, "\\['extra'\\]` must be a JSON object"),
    ],
)
def test_connection_validation_rejects_malformed_batches(
    connections: Any, error: type[Exception], match: str
) -> None:
    """Reject every malformed Connection batch before anything is written."""

    with pytest.raises(error, match=match):
        compat_seed.validate_connections(connections)


def test_connection_validation_accepts_an_omitted_port_and_extra() -> None:
    """Accept an explicitly absent port and extra without inventing values."""

    validated = compat_seed.validate_connections({"seed_c": {"port": None, "extra": None}})

    assert validated == {
        "seed_c": {"conn_type": compat_seed.DEFAULT_CONN_TYPE, "port": None, "extra": None}
    }


def test_persistence_failure_rolls_back_and_reports_the_collision(
    airflow_variables: AirflowVariables, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Name the xdist remedy when a concurrent worker wins the unique constraint."""

    # Deferred because the failure under test is raised by SQLAlchemy.
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.orm import Session

    def fail_flush(self: Session, *args: Any, **kwargs: Any) -> None:
        del self, args, kwargs
        raise IntegrityError("INSERT", None, Exception("UNIQUE constraint failed"))

    monkeypatch.setattr(Session, "flush", fail_flush)

    with pytest.raises(compat_seed.SeedPersistenceError, match="xdist_group"):
        airflow_variables({"seed_raced": "value"})


def test_cleanup_failure_still_closes_the_session(monkeypatch: pytest.MonkeyPatch) -> None:
    """Report a cleanup failure and release the checked-out connection anyway."""

    record = compat_seed.SeedRecord(
        session=compat_seed.open_seed_session("Variables"), kind="Variables"
    )
    record.variables[-1] = "never_seeded"
    connection = record.session.connection()

    # Deferred because the failure under test is raised by SQLAlchemy.
    from sqlalchemy.orm import Session

    def fail_execute(self: Session, *args: Any, **kwargs: Any) -> None:
        del self, args, kwargs
        raise RuntimeError("delete refused")

    monkeypatch.setattr(Session, "execute", fail_execute)

    with pytest.raises(compat_seed.SeedCleanupError, match="Could not remove seeded Airflow"):
        compat_seed.cleanup_seeds(record)

    assert connection.closed


def test_cleanup_leaves_a_foreign_variable_sharing_an_owned_id(
    airflow_variables: AirflowVariables,
) -> None:
    """Skip an owned Variable id whose row now carries another seed's `key`.

    Regression test for issue #325: `variable.id` is a plain `Integer` primary key
    with no `sqlite_autoincrement`, so on SQLite it is a rowid alias whose value is
    reused once the highest row is deleted, and every xdist worker shares one metadata
    database. Deleting by bare primary key therefore took another worker's live seed.

    Parameters:
        airflow_variables: AirflowVariables seeding the bystander row this record must
            not delete.
    """

    airflow_variables({"seed_pk_reuse_bystander": "value"})
    with create_session() as session:
        bystander_id = session.scalar(
            select(Variable.id).where(Variable.key == "seed_pk_reuse_bystander")
        )
    assert bystander_id is not None

    record = compat_seed.SeedRecord(
        session=compat_seed.open_seed_session("Variables"), kind="Variables"
    )
    # The reused id: this record inserted it under its own key, and another seed's row
    # carries that id now.
    record.variables[bystander_id] = "seed_pk_reuse_owner"

    compat_seed.cleanup_seeds(record)

    assert _row_count(Variable, Variable.key, "seed_pk_reuse_bystander") == 1


def test_cleanup_pairs_each_owned_id_with_its_own_key(
    airflow_variables: AirflowVariables,
) -> None:
    """Skip a row holding one owned id and a *different* owned key.

    The predicate has to be an OR of per-row `(id, key)` pairs. Matching an id set
    against a key set independently is a cross product: with two or more owned rows, a
    stranger's row that reused owned id `A` while carrying owned key `B` satisfies both
    halves and gets deleted anyway, which is the same issue #325 failure the pairing is
    supposed to close.

    Parameters:
        airflow_variables: AirflowVariables seeding the bystander row this record must
            not delete.
    """

    airflow_variables({"seed_cross_product_bystander": "value"})
    with create_session() as session:
        bystander_id = session.scalar(
            select(Variable.id).where(Variable.key == "seed_cross_product_bystander")
        )
    assert bystander_id is not None

    record = compat_seed.SeedRecord(
        session=compat_seed.open_seed_session("Variables"), kind="Variables"
    )
    # This record owned `bystander_id` under a different key, and owned
    # `seed_cross_product_bystander` under a different id. Neither pair matches the
    # live row; only a cross product would.
    record.variables[bystander_id] = "seed_cross_product_owner"
    record.variables[bystander_id + 10_000] = "seed_cross_product_bystander"

    compat_seed.cleanup_seeds(record)

    assert _row_count(Variable, Variable.key, "seed_cross_product_bystander") == 1


def test_cleanup_chunks_a_batch_past_sqlite_expression_depth(
    airflow_variables: AirflowVariables,
) -> None:
    """Clean an owned batch larger than SQLite's expression-tree depth cap.

    The paired predicate contributes one `AND` node per owned row to an `OR` chain, and
    SQLite caps an expression tree at depth 1000 -- a single statement covering a batch
    that size raises `sqlite3.OperationalError: Expression tree is too large`, which
    surfaces as a `SeedCleanupError` in teardown rather than in the test body. The batch
    here is deliberately over `DELETE_PAIR_CHUNK_SIZE` and over the 1000 cap.

    Parameters:
        airflow_variables: AirflowVariables seeding the oversized owned batch.
    """

    size = compat_seed.DELETE_PAIR_CHUNK_SIZE * 6 + 1
    assert size > 1000
    airflow_variables({f"seed_bulk_{index}": "value" for index in range(size)})

    assert _row_count(Variable, Variable.key, "seed_bulk_0") == 1
    assert _row_count(Variable, Variable.key, f"seed_bulk_{size - 1}") == 1


def test_cleanup_leaves_a_foreign_connection_sharing_an_owned_id(
    airflow_connections: AirflowConnections,
) -> None:
    """Skip an owned Connection id whose row now carries another seed's `conn_id`.

    The `Connection` half of issue #325 -- `connection.id` has the same reusable
    `Integer` primary key as `variable.id`.

    Parameters:
        airflow_connections: AirflowConnections seeding the bystander row this record
            must not delete.
    """

    airflow_connections({"seed_pk_reuse_bystander_conn": {"host": "localhost"}})
    with create_session() as session:
        bystander_id = session.scalar(
            select(Connection.id).where(Connection.conn_id == "seed_pk_reuse_bystander_conn")
        )
    assert bystander_id is not None

    record = compat_seed.SeedRecord(
        session=compat_seed.open_seed_session("Connections"), kind="Connections"
    )
    record.connections[bystander_id] = "seed_pk_reuse_owner_conn"

    compat_seed.cleanup_seeds(record)

    assert _row_count(Connection, Connection.conn_id, "seed_pk_reuse_bystander_conn") == 1


def test_cleanup_of_an_unused_record_closes_without_writing() -> None:
    """Skip every delete when the fixture committed nothing."""

    record = compat_seed.SeedRecord(
        session=compat_seed.open_seed_session("Connections"), kind="Connections"
    )
    connection = record.session.connection()

    compat_seed.cleanup_seeds(record)

    assert connection.closed


def test_open_seed_session_reports_an_uninitialized_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report an unavailable Airflow session factory as a persistence failure."""

    # Deferred because Airflow settings are bootstrap-sensitive.
    from airflow import settings

    monkeypatch.setattr(settings, "Session", None)

    with pytest.raises(
        compat_seed.SeedPersistenceError, match="Could not open an Airflow metadata session"
    ):
        compat_seed.open_seed_session("Variables")
