"""Public pytest fixtures registered by the plugin.

References:
    https://docs.pytest.org/en/stable/how-to/fixtures.html
"""

from __future__ import annotations

from pytest_airflow_in_a_box.fixtures.dag import dag_maker
from pytest_airflow_in_a_box.fixtures.dagbag import full_dag_bag
from pytest_airflow_in_a_box.fixtures.logging import cap_structlog
from pytest_airflow_in_a_box.fixtures.session import session
from pytest_airflow_in_a_box.fixtures.taskrun import run_task

__all__ = ("cap_structlog", "dag_maker", "full_dag_bag", "run_task", "session")
