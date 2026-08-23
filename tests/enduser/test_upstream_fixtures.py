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
