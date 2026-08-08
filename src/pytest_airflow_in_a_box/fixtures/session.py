"""Provide rollback-isolated Apache Airflow metadata database sessions.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/utils/session/
    https://docs.sqlalchemy.org/en/20/orm/session_basics.html
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

import pytest

from pytest_airflow_in_a_box._compat import ensure_database
from pytest_airflow_in_a_box.bootstrap import get_bootstrap_state

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@pytest.fixture
def session(pytestconfig: pytest.Config) -> Iterator[Session]:
    """Yield an Airflow metadata session and roll back test changes.

    Parameters:
        pytestconfig: pytest.Config carrying the session's bootstrap state.

    Yields:
        sqlalchemy.orm.Session connected to the isolated metadata database.
    """

    ensure_database(get_bootstrap_state(pytestconfig).root)

    # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
    from airflow.utils.session import create_session

    with create_session() as database_session:
        try:
            yield database_session
        finally:
            database_session.rollback()


__all__ = ("session",)
