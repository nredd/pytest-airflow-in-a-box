"""Exercise the public plugin surface under live pytest-xdist workers.

Every test here drives a nested consumer-style suite through
``runpytest_subprocess`` with a real ``-n`` flag, the way PLAN.md mandates for
genuine cross-process assertions. Cross-worker invariants travel through
artifact files the nested tests append to, the pattern from
``tests/bootstrap/test_bootstrap.py``.

References:
    https://pytest-xdist.readthedocs.io/en/stable/distribution.html
    https://github.com/nredd/pytest-airflow-in-a-box/issues/45
"""

from __future__ import annotations

import json
import platform
import sys
from pathlib import Path

import pytest

from pytest_airflow_in_a_box.storage.postgres import require_postgres_available

pytestmark = pytest.mark.compat

# Every nested run re-bootstraps Airflow from scratch; macOS and musl CI hosts
# need roughly double the glibc-Linux budget.
NESTED_RUN_TIMEOUT_SECONDS = (
    300 if sys.platform == "linux" and platform.libc_ver()[0] == "glibc" else 600
)

try:
    require_postgres_available()
except pytest.UsageError:
    _POSTGRES_UNAVAILABLE = True
else:
    _POSTGRES_UNAVAILABLE = False

_DAG_MAKER_SUITE = """
    import json
    import os
    import uuid
    from pathlib import Path

    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.utils.state import TaskInstanceState

    RECORD_DIR = Path({record_dir!r})


    def _run_and_record(dag_maker):
        with dag_maker():
            EmptyOperator(task_id="empty")
        ti = dag_maker.run_ti("empty")
        assert ti.state == TaskInstanceState.SUCCESS
        record = {{"worker": os.environ["PYTEST_XDIST_WORKER"], "dag_id": ti.dag_id}}
        (RECORD_DIR / f"{{uuid.uuid4().hex}}.json").write_text(
            json.dumps(record), encoding="utf-8"
        )


    def test_first_default_dag_id(dag_maker):
        _run_and_record(dag_maker)


    def test_second_default_dag_id(dag_maker):
        _run_and_record(dag_maker)
"""

_MIXED_SUITE = """
    import structlog
    from airflow.models.taskinstance import TaskInstance
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.sdk import DAG, task
    from airflow.utils.state import TaskInstanceState


    def _run_and_query(dag_maker, session):
        with dag_maker():
            EmptyOperator(task_id="empty")
        ti = dag_maker.run_ti("empty")
        rows = session.query(TaskInstance).filter(TaskInstance.dag_id == ti.dag_id).all()
        assert [(row.task_id, row.state) for row in rows] == [
            ("empty", TaskInstanceState.SUCCESS)
        ]


    def test_first_dag_rows_are_isolated(dag_maker, session):
        _run_and_query(dag_maker, session)


    def test_second_dag_rows_are_isolated(dag_maker, session):
        _run_and_query(dag_maker, session)


    def test_run_task_stays_db_free(run_task):
        with DAG(dag_id="mixed_run_task", schedule=None) as dag:
            EmptyOperator(task_id="empty")
        assert run_task(dag.get_task("empty")).state == TaskInstanceState.SUCCESS


    def test_cap_structlog_sees_task_events(cap_structlog, dag_maker):
        with dag_maker():

            @task
            def speak() -> None:
                structlog.get_logger("mixed_task").warning("mixed_event", answer=42)

            speak()
        dag_maker.run_ti("speak")
        assert "mixed_event" in cap_structlog
"""

_N_AUTO_SUITE = """
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.sdk import DAG
    from airflow.utils.state import TaskInstanceState


    def _run(run_task, suffix):
        with DAG(dag_id=f"auto_fanout_{suffix}", schedule=None) as dag:
            EmptyOperator(task_id="empty")
        assert run_task(dag.get_task("empty")).state == TaskInstanceState.SUCCESS


    def test_first(run_task):
        _run(run_task, "first")


    def test_second(run_task):
        _run(run_task, "second")


    def test_third(run_task):
        _run(run_task, "third")


    def test_dag_maker_runs_one_ti(dag_maker):
        with dag_maker():
            EmptyOperator(task_id="empty")
        assert dag_maker.run_ti("empty").state == TaskInstanceState.SUCCESS
"""


def _read_records(record_dir: Path) -> list[dict[str, object]]:
    """Read every JSON artifact a nested suite recorded.

    Parameters:
        record_dir: pathlib.Path containing one JSON file per nested test.

    Returns:
        list[dict[str, object]] containing the decoded records.
    """

    return [json.loads(path.read_text(encoding="utf-8")) for path in sorted(record_dir.iterdir())]


@pytest.mark.timeout(NESTED_RUN_TIMEOUT_SECONDS)
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


@pytest.mark.timeout(NESTED_RUN_TIMEOUT_SECONDS)
def test_dag_maker_default_ids_never_collide_across_workers(pytester: pytest.Pytester) -> None:
    """Run DB-backed `dag_maker` tests to SUCCESS on both workers with unique Dag ids."""

    record_dir = pytester.path / "records"
    record_dir.mkdir()
    source = _DAG_MAKER_SUITE.format(record_dir=str(record_dir))
    pytester.makepyfile(test_dag_maker_first=source, test_dag_maker_second=source)

    result = pytester.runpytest_subprocess("-q", "-n", "2", "--dist=loadfile")

    result.assert_outcomes(passed=4)
    records = _read_records(record_dir)
    assert len(records) == 4
    assert len({record["dag_id"] for record in records}) == 4
    assert len({record["worker"] for record in records}) == 2


@pytest.mark.timeout(NESTED_RUN_TIMEOUT_SECONDS)
def test_mixed_workload_shares_one_database(pytester: pytest.Pytester) -> None:
    """Combine `dag_maker`, `session`, `run_task`, and `cap_structlog` under two workers."""

    pytester.makepyfile(_MIXED_SUITE)

    result = pytester.runpytest_subprocess("-q", "-n", "2")

    result.assert_outcomes(passed=4)


@pytest.mark.db_test
@pytest.mark.postgres
@pytest.mark.skipif(
    _POSTGRES_UNAVAILABLE,
    reason="requires the `postgres` extra and a running Docker daemon",
)
@pytest.mark.timeout(NESTED_RUN_TIMEOUT_SECONDS)
def test_mixed_workload_shares_one_postgres_database(pytester: pytest.Pytester) -> None:
    """Rerun the mixed workload against real concurrent Postgres writers."""

    pytester.makepyfile(_MIXED_SUITE)

    result = pytester.runpytest_subprocess("-q", "-n", "2", "--airflow-db-backend=postgres")

    result.assert_outcomes(passed=4)


@pytest.mark.timeout(NESTED_RUN_TIMEOUT_SECONDS)
def test_n_auto_bootstrap_fans_out_at_real_core_count(pytester: pytest.Pytester) -> None:
    """Bootstrap and pass a cheap suite under `-n auto` at the host's real core count.

    `auto` worker counts vary per runner, so only outcome counts are asserted;
    the value is exercising `calculate_profile()` and worker fan-out for real.
    """

    pytester.makepyfile(_N_AUTO_SUITE)

    result = pytester.runpytest_subprocess("-q", "-n", "auto")

    result.assert_outcomes(passed=4)
