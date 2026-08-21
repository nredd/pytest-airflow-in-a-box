"""Drive real DagRuns through real executors, end to end.

The consumer contract for `run_dag(executor=...)`: a live api-server serving the Task
Execution API, workloads queued through the executor, and task bodies executing in
supervised worker subprocesses. Everything runs inside `pytester` subprocesses, which
bootstrap their own isolated `AIRFLOW_HOME` and their own api-server, so these never
contend with the outer suite over corpus `dag_id`s.

`SerialExecutor` is the user-authored executor -- the one Airflow 3 stopped shipping --
and `LocalExecutor` is Airflow's own, proving the path is not shaped around this
repository's toy.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html
    https://github.com/apache/airflow/issues/59074
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = [pytest.mark.compat, pytest.mark.requires_airflow3]

CORPUS = Path(__file__).parents[1] / "dags"


def test_serial_executor_runs_a_corpus_dag_in_worker_subprocesses(
    pytester: pytest.Pytester,
) -> None:
    """Execute a corpus Dag through a user-authored executor and keep every guarantee.

    The assertions are deliberately the same ones `tests/enduser/test_run_dag.py` makes
    of the in-process path: identical `DagRunResult` shape, ordering, and XCom values.
    That the two agree is the whole contract.

    Parameters:
        pytester: pytest.Pytester running the consumer suite in a subprocess.
    """

    pytester.makepyfile(
        """
        import sys
        from importlib import import_module


        def test_chained_through_serial_executor(
            dag_bag, run_dag, airflow_dags_folder
        ):
            # The corpus folder is exactly what `--dag-folder` selected, so the
            # executor package next to the Dag files is reachable without a path
            # interpolated into this generated module.
            sys.path.insert(0, str(airflow_dags_folder))
            executor = import_module("provider_package._executor").SerialExecutor

            result = run_dag(dag_bag.dags["chained"], executor=executor)

            assert result.success
            assert result.dag_id == "chained"
            assert result.states == {"produce": "success", "consume": "success"}
            assert result.order == ["produce", "consume"]
            assert result.xcoms == {"produce": 21, "consume": 42}
        """
    )

    result = pytester.runpytest_subprocess("-q", f"--dag-folder={CORPUS}")

    result.assert_outcomes(passed=1)


def test_local_executor_runs_a_corpus_dag(pytester: pytest.Pytester) -> None:
    """Execute a corpus Dag through Airflow's own multi-process executor, by alias.

    `LocalExecutor` reaches the same code by its core alias rather than by class, so
    this covers alias resolution through `ExecutorLoader` as well as a genuinely
    multi-process executor talking to the plugin's api-server.

    Passes `--airflow-executor-timeout` explicitly so the registered option name and the
    name the fixture reads cannot drift apart unnoticed.

    Parameters:
        pytester: pytest.Pytester running the consumer suite in a subprocess.
    """

    pytester.makepyfile(
        """
        def test_happy_path_through_local_executor(dag_bag, run_dag):
            result = run_dag(dag_bag.dags["happy_path"], executor="LocalExecutor")

            assert result.success
            assert result.states == {"greet": "success"}
        """
    )

    result = pytester.runpytest_subprocess(
        "-q", f"--dag-folder={CORPUS}", "--airflow-executor-timeout=180"
    )

    result.assert_outcomes(passed=1)


def test_a_failing_task_settles_scheduler_shaped(pytester: pytest.Pytester) -> None:
    """Report a raising task body as `failed` and block its downstream, as inline runs do.

    The task raises inside a worker subprocess, so the exception object never reaches
    the test process and `result.errors` cannot carry a traceback the way an in-process
    run does. The settled states still have to match the scheduler exactly.

    Parameters:
        pytester: pytest.Pytester running the consumer suite in a subprocess.
    """

    folder = pytester.mkdir("executor_dags")
    (folder / "boom.py").write_text(
        """
from airflow.sdk import dag, task


@dag(schedule=None, catchup=False)
def boom() -> None:
    @task
    def explode() -> None:
        raise RuntimeError("boom")

    @task
    def downstream() -> None:
        pass

    explode() >> downstream()


boom()
""",
        encoding="utf-8",
    )
    pytester.makepyfile(
        """
        def test_failure_states(dag_bag, run_dag):
            result = run_dag(dag_bag.dags["boom"], executor="LocalExecutor")

            assert not result.success
            assert result.states == {
                "explode": "failed",
                "downstream": "upstream_failed",
            }
        """
    )

    result = pytester.runpytest_subprocess("-q", f"--dag-folder={folder}")

    result.assert_outcomes(passed=1)


def test_a_dag_outside_the_dag_folder_is_refused_before_any_metadata_is_written(
    pytester: pytest.Pytester,
) -> None:
    """Name the dags-folder constraint, and leave the metadata database untouched.

    A Dag authored in the test module is the case users will hit -- `dag_maker` Dags are
    the same shape -- because no worker subprocess could ever re-import one. The refusal
    lands before `run_dag` persists anything, which the follow-up in-process `run_dag`
    proves: it would raise `ValueError: Dag metadata already exists` otherwise.

    Parameters:
        pytester: pytest.Pytester running the consumer suite in a subprocess.
    """

    pytester.makepyfile(
        """
        import pytest
        from airflow.sdk import DAG, task

        from pytest_airflow_in_a_box.taskinstance import ExecutorRunError


        def _build_dag():
            with DAG(dag_id="authored_in_a_test_module", schedule=None) as dag:
                @task
                def noop():
                    return 7

                noop()
            return dag


        def test_a_dag_authored_in_a_test_module_is_refused(run_dag):
            dag = _build_dag()

            with pytest.raises(ExecutorRunError, match="outside the Dag folder"):
                run_dag(dag, executor="LocalExecutor")

            # Nothing was persisted by the refused call, so the in-process path still
            # adopts the same `dag_id` cleanly.
            assert run_dag(dag).success
        """
    )

    result = pytester.runpytest_subprocess("-q", f"--dag-folder={CORPUS}")

    result.assert_outcomes(passed=1)


def test_a_sandbox_registered_alias_selects_the_executor(pytester: pytest.Pytester) -> None:
    """Compose `airflow_components.executor` with `run_dag(executor=...)`.

    The sandbox registers an alias into `ExecutorLoader` for one test, and `run_dag`
    resolves aliases through that same registry. Before this, a sandbox-registered alias
    had no configuration surface that could select it -- `airflow_executor` is resolved
    before the first Airflow import, when no alias exists yet.

    Parameters:
        pytester: pytest.Pytester running the consumer suite in a subprocess.
    """

    pytester.makepyfile(
        """
        import sys
        from importlib import import_module


        def test_alias_selects_the_executor(
            dag_bag, run_dag, airflow_components, airflow_dags_folder
        ):
            sys.path.insert(0, str(airflow_dags_folder))
            executor = import_module("provider_package._executor").SerialExecutor

            alias = airflow_components.executor(executor, alias="serial")
            result = run_dag(dag_bag.dags["happy_path"], executor=alias)

            assert alias == "serial"
            assert result.states == {"greet": "success"}
        """
    )

    result = pytester.runpytest_subprocess("-q", f"--dag-folder={CORPUS}")

    result.assert_outcomes(passed=1)
