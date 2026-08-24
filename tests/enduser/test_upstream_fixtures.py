"""Exercise the upstream `tests_common` parity fixtures as a consumer would."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.compat


def test_create_task_instance_and_dummy_dag_one_call_shapes(
    pytester: pytest.Pytester,
) -> None:
    """Drive the one-call factories through the installed plugin on any family."""

    pytester.makepyfile(
        """
        def test_task_instance_one_call(create_task_instance):
            ti = create_task_instance(dag_id="enduser_cti", task_id="probe")

            assert ti.dag_id == "enduser_cti"
            assert ti.task_id == "probe"
            assert ti.task is not None

        def test_dummy_dag_one_call(create_dummy_dag):
            dag, operator = create_dummy_dag(dag_id="enduser_cdd")

            assert dag.dag_id == "enduser_cdd"
            assert operator.task_id == "op1"
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=2)


def test_scheduler_side_handles_expose_and_resync_metadata(pytester: pytest.Pytester) -> None:
    """Drive `dag_model` and `sync_dagbag_to_db` through the public factory on any family."""

    pytester.makepyfile(
        """
        def test_handles(dag_maker):
            try:
                from airflow.providers.standard.operators.empty import EmptyOperator
            except ModuleNotFoundError:
                from airflow.operators.empty import EmptyOperator

            with dag_maker(dag_id="enduser_handles") as dag:
                EmptyOperator(task_id="original")

            assert dag_maker.serialized_dag is not None
            assert dag_maker.dag_model.dag_id == "enduser_handles"
            assert dag_maker.dag_model.is_paused is False

            EmptyOperator(task_id="added", dag=dag)
            reloaded = dag_maker.sync_dagbag_to_db()

            assert sorted(reloaded.task_ids) == ["added", "original"]
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_timetable_handle_run_ti_session_and_execution_date_alias(
    pytester: pytest.Pytester,
) -> None:
    """Drive the issue #261 parity surfaces through the installed plugin on any family.

    Covers the `dag_maker.timetable` scheduler-timetable handle (the migration
    target for upstream's `dag.timetable.infer_manual_data_interval` pattern),
    upstream's `run_ti(session=...)` routing, the Airflow 2 `execution_date`
    spelling on `create_task_instance`, and the documented `serialized_dag`
    escape hatch for scheduler-side Dag methods.
    """

    pytester.makepyfile(
        """
        from datetime import datetime, timedelta, timezone

        import pytest

        def test_timetable_handle_and_escape_hatch(dag_maker):
            try:
                from airflow.providers.standard.operators.empty import EmptyOperator
            except ModuleNotFoundError:
                from airflow.operators.empty import EmptyOperator

            with dag_maker(
                dag_id="enduser_timetable",
                schedule=timedelta(days=1),
                start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            ):
                EmptyOperator(task_id="probe")

            run_after = datetime(2024, 3, 4, tzinfo=timezone.utc)
            interval = dag_maker.timetable.infer_manual_data_interval(
                run_after=run_after
            )

            assert interval.end == run_after
            # The documented migration route for scheduler-side Dag methods the
            # authoring yield lacks (docs/adr/0002).
            for name in ("clear", "partial_subset", "set_task_instance_state"):
                assert hasattr(dag_maker.serialized_dag, name)

        def test_run_ti_session_routing(dag_maker, session):
            try:
                from airflow.providers.standard.operators.empty import EmptyOperator
            except ModuleNotFoundError:
                from airflow.operators.empty import EmptyOperator
            from airflow.utils.state import TaskInstanceState

            with dag_maker(dag_id="enduser_run_ti_session"):
                EmptyOperator(task_id="probe")

            ti = dag_maker.run_ti("probe", session=session)

            assert ti.state == TaskInstanceState.SUCCESS

        def test_execution_date_spelling(create_task_instance):
            execution_date = datetime(2018, 1, 1, tzinfo=timezone.utc)

            with pytest.warns(DeprecationWarning, match="execution_date"):
                ti = create_task_instance(
                    dag_id="enduser_execution_date", execution_date=execution_date
                )

            assert ti.dag_run.logical_date == execution_date
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=3)


def test_created_run_task_instances_keep_their_refreshed_tasks(
    pytester: pytest.Pytester,
) -> None:
    """Sort `dag_run.task_instances` and use each task, upstream's cleartasks shape.

    Regression test for issue #259 on both families: the refreshed instances must stay
    reachable through the returned DagRun after garbage collection, so consumer code
    holding only the run never sees `ti.task = None`.

    Parameters:
        pytester: pytest.Pytester driving the installed plugin in a subprocess.
    """

    pytester.makepyfile(
        """
        import gc

        def test_relationship_instances(dag_maker):
            try:
                from airflow.providers.standard.operators.empty import EmptyOperator
            except ModuleNotFoundError:
                from airflow.operators.empty import EmptyOperator

            with dag_maker(dag_id="enduser_run_parity"):
                EmptyOperator(task_id="first")
                EmptyOperator(task_id="second", retries=2)

            dag_run = dag_maker.create_dagrun()
            gc.collect()

            ti0, ti1 = sorted(dag_run.task_instances, key=lambda ti: ti.task_id)
            assert (ti0.task_id, ti1.task_id) == ("first", "second")
            for ti in (ti0, ti1):
                assert ti.task is not None
                assert ti.queue == ti.task.queue
                assert ti.dag_id == "enduser_run_parity"
            fetched = dag_run.get_task_instances(session=dag_maker.session)
            assert all(ti.task is not None for ti in fetched)
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


@pytest.mark.requires_airflow3
def test_testing_dag_bundle_and_no_logical_date_runs(pytester: pytest.Pytester) -> None:
    """Register the shared bundle and create a run without a logical date on 3.x."""

    pytester.makepyfile(
        """
        def test_shared_bundle_row(testing_dag_bundle):
            from airflow.models.dagbundle import DagBundleModel
            from airflow.utils.session import create_session

            with create_session() as session:
                assert session.get(DagBundleModel, "testing") is not None

        def test_explicit_none_logical_date(create_task_instance):
            ti = create_task_instance(dag_id="enduser_no_ld", logical_date=None)

            assert ti.dag_run.logical_date is None
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=2)
