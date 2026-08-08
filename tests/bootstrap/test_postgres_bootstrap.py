"""End-to-end Postgres bootstrap test gated on a real Docker daemon.

This is the only test that starts a real container. It is skipped unless the
``postgres`` extra is importable and Docker's daemon is reachable; the
injected-seam unit tests in ``tests/storage`` hold the coverage floor without
Docker. The outer ``--airflow-db-backend=postgres`` flag governs the inner
pytester session's metadata database, exercising the full owner path: select
provisioner, start the container, run ``initdb``, and tear the container down.

References:
    https://testcontainers-python.readthedocs.io/en/latest/modules/postgres/README.html
"""

from __future__ import annotations

import importlib.util
import json

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from pytest_airflow_in_a_box.storage.postgres import (
    _docker_is_available,
    _import_postgres_container,
)

_POSTGRES_EXTRA_INSTALLED = (
    importlib.util.find_spec("testcontainers") is not None
    and importlib.util.find_spec("psycopg2") is not None
)
try:
    _import_postgres_container()
except pytest.UsageError:
    _POSTGRES_EXTRA_INSTALLED = False
_POSTGRES_UNAVAILABLE = not (_POSTGRES_EXTRA_INSTALLED and _docker_is_available())

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(
        _POSTGRES_UNAVAILABLE,
        reason="requires the `postgres` extra and a running Docker daemon",
    ),
]


def test_postgres_backend_provisions_a_real_metadata_database(
    pytester: pytest.Pytester,
) -> None:
    """Bootstrap a real Postgres backend, initialize it, and tear it down."""

    record_path = pytester.path / "postgres-state.json"
    pytester.makepyfile(
        f"""
        import json
        import os
        from pathlib import Path

        import pytest

        from sqlalchemy import create_engine, text

        from pytest_airflow_in_a_box.plugin import get_bootstrap_state

        @pytest.mark.db_test
        def test_state(pytestconfig):
            state = get_bootstrap_state(pytestconfig)
            assert state.db_backend == "postgres"
            assert state.sql_alchemy_conn.startswith("postgresql")
            assert (
                os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
                == state.sql_alchemy_conn
            )
            assert (
                os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_ENABLED"] == "False"
            )
            engine = create_engine(state.sql_alchemy_conn)
            try:
                with engine.connect() as connection:
                    tables = {{
                        row[0]
                        for row in connection.execute(
                            text(
                                "SELECT tablename FROM pg_catalog.pg_tables "
                                "WHERE schemaname = 'public'"
                            )
                        )
                    }}
                    revision = connection.execute(
                        text("SELECT version_num FROM alembic_version")
                    ).fetchone()
            finally:
                engine.dispose()
            assert "dag" in tables
            assert revision is not None and revision[0]
            Path({str(record_path)!r}).write_text(
                json.dumps(state.sql_alchemy_conn), encoding="utf-8"
            )
        """
    )

    result = pytester.runpytest_subprocess("-q", "--airflow-db-backend=postgres")

    assert result.ret == 0
    result.assert_outcomes(passed=1)
    connection_url = json.loads(record_path.read_text(encoding="utf-8"))
    engine = create_engine(connection_url)
    try:
        with pytest.raises(OperationalError):
            engine.connect()
    finally:
        engine.dispose()
