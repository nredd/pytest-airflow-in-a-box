"""Exercise the public DB-free runner under pytest-xdist."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.compat


def test_run_task_is_xdist_safe(pytester: pytest.Pytester) -> None:
    """Run independent consumer tests concurrently on two workers."""

    pytester.makepyfile(
        """
        from airflow.sdk import DAG
        from airflow.providers.standard.operators.empty import EmptyOperator
        from airflow.utils.state import TaskInstanceState

        def _run(run_task, suffix):
            with DAG(dag_id=f"compat_xdist_{suffix}", schedule=None) as dag:
                EmptyOperator(task_id="empty")
            assert run_task(dag.get_task("empty")).state == TaskInstanceState.SUCCESS

        def test_first(run_task):
            _run(run_task, "first")

        def test_second(run_task):
            _run(run_task, "second")
        """
    )

    result = pytester.runpytest_subprocess("-q", "-n", "2")

    result.assert_outcomes(passed=2)
