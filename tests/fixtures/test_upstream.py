"""Test the upstream ``tests_common`` parity fixtures against installed Airflow."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from pytest_airflow_in_a_box.fixtures.upstream import TESTING_BUNDLE_NAME
from pytest_airflow_in_a_box.types import CreateDummyDag, CreateTaskInstance, DagMaker

pytestmark = pytest.mark.db_test


def test_create_task_instance_defaults(create_task_instance: CreateTaskInstance) -> None:
    """Create one refreshed default task instance with a single call.

    Parameters:
        create_task_instance: CreateTaskInstance under test.
    """

    ti = create_task_instance(dag_id="cti_defaults")

    assert ti.dag_id == "cti_defaults"
    assert ti.task_id == "op1"
    assert ti.map_index == -1
    assert ti.hostname == ""
    assert ti.state is None
    assert ti.task is not None
    assert ti.task.task_id == "op1"
    # Upstream forwards `state=None` into DagRun creation, so the run carries
    # Airflow's column default rather than this plugin's `running` default.
    assert str(ti.dag_run.state) == "queued"


def test_create_task_instance_upstream_defaults_use_upstream_dag_id(
    create_task_instance: CreateTaskInstance,
) -> None:
    """Keep upstream's ``dag_id="dag"`` / ``task_id="op1"`` defaults verbatim.

    Parameters:
        create_task_instance: CreateTaskInstance under test.
    """

    ti = create_task_instance()

    assert ti.dag_id == "dag"
    assert ti.task_id == "op1"


def test_create_task_instance_assigns_every_instance_attribute(
    create_task_instance: CreateTaskInstance,
) -> None:
    """Land each task-instance attribute argument on the returned instance.

    Parameters:
        create_task_instance: CreateTaskInstance under test.
    """

    heartbeat = datetime(2024, 5, 4, 3, 2, 1, tzinfo=timezone.utc)

    ti = create_task_instance(
        dag_id="cti_attributes",
        task_id="op2",
        state="queued",
        external_executor_id="banana",
        hostname="worker-7",
        pid=4711,
        last_heartbeat_at=heartbeat,
        map_index=-1,
    )

    assert ti.task_id == "op2"
    assert str(ti.state) == "queued"
    assert ti.external_executor_id == "banana"
    assert ti.hostname == "worker-7"
    assert ti.pid == 4711
    assert ti.last_heartbeat_at == heartbeat


def test_create_task_instance_forwards_run_arguments(
    create_task_instance: CreateTaskInstance,
) -> None:
    """Forward run identity, type, state, and interval onto the created DagRun.

    Parameters:
        create_task_instance: CreateTaskInstance under test.
    """

    logical_date = datetime(2024, 2, 3, tzinfo=timezone.utc)
    interval = (logical_date - timedelta(days=1), logical_date)

    ti = create_task_instance(
        dag_id="cti_run_arguments",
        run_id="explicit_run_237",
        run_type="scheduled",
        dagrun_state="running",
        logical_date=logical_date,
        data_interval=interval,
    )
    dag_run = ti.dag_run

    assert ti.run_id == "explicit_run_237"
    assert str(dag_run.run_type) == "scheduled"
    assert str(dag_run.state) == "running"
    assert dag_run.logical_date == logical_date
    assert (dag_run.data_interval_start, dag_run.data_interval_end) == interval


def test_create_task_instance_explicit_none_logical_date(
    create_task_instance: CreateTaskInstance,
) -> None:
    """Create a run with no logical date at all from an explicit ``None``.

    Parameters:
        create_task_instance: CreateTaskInstance under test.
    """

    ti = create_task_instance(dag_id="cti_no_logical_date", logical_date=None)

    assert ti.dag_run.logical_date is None
    assert ti.dag_run.data_interval_start is None


def test_create_task_instance_omitted_logical_date_defaults_to_now(
    create_task_instance: CreateTaskInstance,
) -> None:
    """Keep the omission default: a current-UTC logical date with an interval.

    Parameters:
        create_task_instance: CreateTaskInstance under test.
    """

    ti = create_task_instance(dag_id="cti_default_logical_date")

    assert ti.dag_run.logical_date is not None
    assert ti.dag_run.data_interval_end is not None


def test_create_task_instance_accepts_the_execution_date_spelling(
    create_task_instance: CreateTaskInstance,
) -> None:
    """Map upstream's Airflow 2 ``execution_date`` spelling onto the logical date.

    Upstream 2.x-era suites call ``create_task_instance(execution_date=...)``;
    upstream ``tests_common`` preserves the spelling with a ``DeprecationWarning``
    and this fixture matches it (issue #261).

    Parameters:
        create_task_instance: CreateTaskInstance under test.
    """

    execution_date = datetime(2018, 1, 1, tzinfo=timezone.utc)

    with pytest.warns(DeprecationWarning, match="'execution_date' parameter is preserved"):
        ti = create_task_instance(dag_id="cti_execution_date", execution_date=execution_date)

    assert ti.dag_run.logical_date == execution_date


def test_create_task_instance_treats_none_execution_date_as_unset(
    create_task_instance: CreateTaskInstance,
) -> None:
    """Keep an explicit ``execution_date=None`` on the omission default.

    No Airflow 2 run can lack an execution date, so ``None`` in the 2.x spelling
    means "not supplied" -- never the 3.x no-logical-date sentinel, which would
    also raise ``ValueError`` on the 2.x family (issue #261).

    Parameters:
        create_task_instance: CreateTaskInstance under test.
    """

    ti = create_task_instance(dag_id="cti_none_execution_date", execution_date=None)

    assert ti.dag_run.logical_date is not None


def test_create_task_instance_rejects_both_date_spellings(
    create_task_instance: CreateTaskInstance,
) -> None:
    """Refuse to pick between ``logical_date`` and ``execution_date`` silently.

    Parameters:
        create_task_instance: CreateTaskInstance under test.
    """

    date = datetime(2018, 1, 1, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="Drop `execution_date`"):
        create_task_instance(
            dag_id="cti_both_dates",
            logical_date=date,
            execution_date=date,
        )


def test_create_task_instance_adopts_a_supplied_task(
    create_task_instance: CreateTaskInstance,
) -> None:
    """Bind a caller-constructed operator instead of building an ``EmptyOperator``.

    Parameters:
        create_task_instance: CreateTaskInstance under test.
    """

    from airflow.providers.standard.operators.python import PythonOperator

    adopted = PythonOperator(task_id="adopted", python_callable=lambda: None)

    ti = create_task_instance(dag_id="cti_adopted", task=adopted)

    assert ti.task_id == "adopted"
    assert ti.task is adopted
    assert adopted.start_from_trigger is False


def test_create_task_instance_forwards_dag_kwargs_and_serialized(
    create_task_instance: CreateTaskInstance,
    dag_maker: DagMaker,
) -> None:
    """Route ``**dag_kwargs`` to ``dag_maker``, including its ``serialized`` toggle.

    Parameters:
        create_task_instance: CreateTaskInstance under test.
        dag_maker: DagMaker instance shared with the factory via pytest's fixture cache.
    """

    ti = create_task_instance(dag_id="cti_dag_kwargs", max_active_runs=1, serialized=True)

    task = ti.task
    assert task is not None
    assert task.dag is not None
    assert task.dag.max_active_runs == 1
    assert dag_maker.serialized_dag is not None
    assert "op1" in dag_maker.serialized_dag.task_ids


def test_create_dummy_dag_returns_dag_and_operator_with_scheduled_run(
    create_dummy_dag: CreateDummyDag,
    dag_maker: DagMaker,
) -> None:
    """Author the single-operator Dag and create its default scheduled DagRun.

    Parameters:
        create_dummy_dag: CreateDummyDag under test.
        dag_maker: DagMaker instance sharing the metadata session.
    """

    from airflow.models.dagrun import DagRun

    dag, operator = create_dummy_dag(
        dag_id="cdd_scheduled",
        task_id="t1",
        pool="default_pool",
        email="redd@example.com",
    )

    assert dag.dag_id == "cdd_scheduled"
    assert operator.task_id == "t1"
    assert operator.email == "redd@example.com"
    dag_run = dag_maker.session.query(DagRun).filter(DagRun.dag_id == "cdd_scheduled").one()
    assert str(dag_run.run_type) == "scheduled"


def test_create_dummy_dag_skips_the_run_when_type_is_none(
    create_dummy_dag: CreateDummyDag,
    dag_maker: DagMaker,
) -> None:
    """Create no DagRun at all for ``with_dagrun_type=None``.

    Parameters:
        create_dummy_dag: CreateDummyDag under test.
        dag_maker: DagMaker instance sharing the metadata session.
    """

    from airflow.models.dagrun import DagRun

    dag, operator = create_dummy_dag(dag_id="cdd_no_run", with_dagrun_type=None)

    assert operator.task_id == "op1"
    runs = dag_maker.session.query(DagRun).filter(DagRun.dag_id == "cdd_no_run").count()
    assert runs == 0
    assert dag.dag_id == "cdd_no_run"


@pytest.mark.usefixtures("testing_dag_bundle")
def test_testing_dag_bundle_registers_the_shared_row() -> None:
    """Insert the shared ``testing`` bundle row upstream tests write against."""

    from airflow.models.dagbundle import DagBundleModel
    from airflow.utils.session import create_session

    with create_session() as session:
        assert session.get(DagBundleModel, TESTING_BUNDLE_NAME) is not None


@pytest.mark.usefixtures("testing_dag_bundle")
def test_testing_dag_bundle_satisfies_the_dag_model_foreign_key() -> None:
    """Accept a ``DagModel`` write against ``bundle_name="testing"``.

    The second consecutive test requesting the fixture also exercises its
    idempotent already-present path against the run-shared metadata database.
    """

    from airflow.models.dag import DagModel
    from airflow.utils.session import create_session

    dag_id = "testing_bundle_fk_probe"
    with create_session() as session:
        session.add(DagModel(dag_id=dag_id, bundle_name=TESTING_BUNDLE_NAME))
    try:
        with create_session() as session:
            written = session.get(DagModel, dag_id)
            assert written is not None
            assert written.bundle_name == TESTING_BUNDLE_NAME
    finally:
        with create_session() as session:
            probe = session.get(DagModel, dag_id)
            if probe is not None:
                session.delete(probe)
