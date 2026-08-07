"""Test the public metadata database session fixture.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/utils/session/
    https://docs.sqlalchemy.org/en/20/orm/session_basics.html
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session


def test_session_executes_sql(session: Session) -> None:
    """Discover the plugin fixture and execute SQL against Airflow's database."""

    assert session.scalar(text("SELECT 41 + 1")) == 42


def test_session_rolls_back_and_closes(pytester: pytest.Pytester) -> None:
    """Roll back uncommitted writes and close the checked-out connection on teardown."""

    pytester.makeconftest(
        """
        from __future__ import annotations

        import pytest
        from sqlalchemy import text

        _CONNECTION = None


        @pytest.fixture
        def observed_session(session):
            global _CONNECTION
            _CONNECTION = session.connection()
            session.execute(text(
                "CREATE TABLE fixture_rollback (value INTEGER NOT NULL)"
            ))
            session.commit()
            yield session


        def pytest_fixture_post_finalizer(fixturedef):
            if fixturedef.argname != "session" or _CONNECTION is None:
                return

            assert _CONNECTION.closed
            # Deferred because the consumer plugin is checking post-bootstrap state.
            from airflow.utils.session import create_session

            with create_session() as verifier:
                assert verifier.scalar(text(
                    "SELECT COUNT(*) FROM fixture_rollback"
                )) == 0
        """
    )
    pytester.makepyfile(
        """
        from sqlalchemy import text


        def test_uncommitted_write(observed_session):
            observed_session.execute(text(
                "INSERT INTO fixture_rollback (value) VALUES (1)"
            ))
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)
