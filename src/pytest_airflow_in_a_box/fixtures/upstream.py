"""Upstream ``tests_common`` parity fixtures composed over ``dag_maker``.

Upstream Airflow's core unit tests request ``create_task_instance``,
``create_dummy_dag``, and ``testing_dag_bundle`` by name; these fixtures mirror the
upstream names, parameters, and defaults so such tests call them the same way
(documented deviations live in the task-execution guide), and double as one-call
conveniences for plugin users. Everything here is composition over
``dag_maker`` and the compatibility layer -- no new persistence machinery.

References:
    https://github.com/apache/airflow/blob/main/devel-common/src/tests_common/pytest_plugin.py
    https://github.com/nredd/pytest-airflow-in-a-box/issues/237
"""

from __future__ import annotations

import logging
import warnings
from typing import TYPE_CHECKING, Any

import pytest

from pytest_airflow_in_a_box._compat import ensure_database
from pytest_airflow_in_a_box._compat.capabilities import require_v3
from pytest_airflow_in_a_box._compat.dag import (
    UNSET,
    UnsetType,
    coerce_run_type,
    empty_operator_class,
    ensure_shared_bundle,
)
from pytest_airflow_in_a_box.bootstrap import get_bootstrap_state

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from airflow.sdk import DAG

    # Deferred: eagerly importing `types.py` here would cycle back through
    # `fixtures/dag.py` -- `types.py` imports `dagcorpus.py` for its `DagCorpus`/...
    # re-exports, and `dagcorpus.py` imports the `fixtures.dagbag` submodule, which first
    # runs `fixtures/__init__.py`, pulling in `fixtures/dag.py` and (transitively) this
    # module. Safe under `from __future__ import annotations`: every use below is a type
    # annotation, never evaluated at runtime.
    from pytest_airflow_in_a_box.types import CreateDummyDag, CreateTaskInstance, DagMaker

LOGGER = logging.getLogger(__name__)

# Upstream's shared bundle row: core unit tests bulk-write Dag metadata against this
# exact name, and `DagModel.bundle_name` is a foreign key onto it.
TESTING_BUNDLE_NAME = "testing"


def _author_single_operator_dag(
    dag_maker: DagMaker,
    dag_id: str,
    task_id: str,
    operator_kwargs: dict[str, Any],
    task: Any | None,
    operator_attrs: dict[str, Any],
    dag_kwargs: dict[str, Any],
) -> tuple[DAG, Any]:
    """Author and persist one single-operator Dag through ``dag_maker``.

    Parameters:
        dag_maker: DagMaker owning persistence and cleanup.
        dag_id: str identifying the Dag.
        task_id: str identifying the built ``EmptyOperator``; ignored when ``task``
            carries its own identifier.
        operator_kwargs: dict[str, Any] forwarded to the ``EmptyOperator`` constructor,
            with ``None`` values dropped first: a ``None`` means "not requested" for
            every one of these parameters, and several (``task_display_name``,
            ``on_skipped_callback``) do not exist as constructor parameters on the
            oldest certified 2.x releases, so forwarding an unrequested ``None``
            would break there for nothing.
        task: Any | None containing an already-constructed operator to bind into the
            Dag instead of building an ``EmptyOperator``.
        operator_attrs: dict[str, Any] assigned onto the operator inside the authoring
            context, so persistence and serialization observe the values.
        dag_kwargs: dict[str, Any] forwarded to the authoring ``DAG`` constructor.

    Returns:
        tuple[airflow.sdk.DAG, Any] containing the persisted authoring Dag and its
        single operator.
    """

    requested_kwargs = {
        name: value for name, value in operator_kwargs.items() if value is not None
    }
    with dag_maker(dag_id, **dag_kwargs) as dag:
        if task is None:
            operator = empty_operator_class()(task_id=task_id, **requested_kwargs)
        else:
            operator = task
            operator.dag = dag
        for attr_name, attr_value in operator_attrs.items():
            setattr(operator, attr_name, attr_value)
    return dag, operator


@pytest.fixture
def create_dummy_dag(dag_maker: DagMaker) -> CreateDummyDag:
    """Return an upstream-parity factory for one-``EmptyOperator`` Dags.

    Parameters:
        dag_maker: DagMaker owning persistence and cleanup for every authored Dag.

    Returns:
        pytest_airflow_in_a_box.types.CreateDummyDag authoring single-operator Dags.
    """

    def factory(
        dag_id: str = "dag",
        task_id: str = "op1",
        task_display_name: str | None = None,
        max_active_tis_per_dag: int = 16,
        max_active_tis_per_dagrun: int | None = None,
        pool: str = "default_pool",
        executor_config: dict[str, Any] | None = None,
        trigger_rule: str = "all_done",
        on_success_callback: Callable[..., object] | None = None,
        on_execute_callback: Callable[..., object] | None = None,
        on_failure_callback: Callable[..., object] | None = None,
        on_retry_callback: Callable[..., object] | None = None,
        email: str | None = None,
        with_dagrun_type: str | None = "scheduled",
        **dag_kwargs: Any,
    ) -> tuple[DAG, Any]:
        """Author the Dag, persist it, and create its DagRun when requested.

        Parameters mirror ``pytest_airflow_in_a_box.types.CreateDummyDag.__call__``.

        Returns:
            tuple[airflow.sdk.DAG, Any] containing the persisted authoring Dag and its
            single ``EmptyOperator``.
        """

        dag, operator = _author_single_operator_dag(
            dag_maker,
            dag_id,
            task_id,
            operator_kwargs={
                "task_display_name": task_display_name,
                "max_active_tis_per_dag": max_active_tis_per_dag,
                "max_active_tis_per_dagrun": max_active_tis_per_dagrun,
                "pool": pool,
                "executor_config": executor_config,
                "trigger_rule": trigger_rule,
                "on_success_callback": on_success_callback,
                "on_execute_callback": on_execute_callback,
                "on_failure_callback": on_failure_callback,
                "on_retry_callback": on_retry_callback,
                "email": email,
            },
            task=None,
            operator_attrs={},
            dag_kwargs=dag_kwargs,
        )
        if with_dagrun_type is not None:
            dag_maker.create_dagrun(run_type=coerce_run_type(with_dagrun_type))
        return dag, operator

    return factory


@pytest.fixture
def create_task_instance(dag_maker: DagMaker) -> CreateTaskInstance:
    """Return an upstream-parity one-call TaskInstance factory.

    Parameters:
        dag_maker: DagMaker owning persistence and cleanup for every authored Dag.

    Returns:
        pytest_airflow_in_a_box.types.CreateTaskInstance creating task instances with
        their Dag and DagRun rows.
    """

    def factory(
        logical_date: datetime | UnsetType | None = UNSET,
        execution_date: datetime | UnsetType | None = UNSET,
        run_after: datetime | None = None,
        dagrun_state: str | None = None,
        state: str | None = None,
        run_id: str | None = None,
        run_type: str | None = None,
        data_interval: Any | None = None,
        external_executor_id: str | None = None,
        dag_id: str = "dag",
        task_id: str = "op1",
        task_display_name: str | None = None,
        max_active_tis_per_dag: int = 16,
        max_active_tis_per_dagrun: int | None = None,
        pool: str = "default_pool",
        executor_config: dict[str, Any] | None = None,
        trigger_rule: str = "all_done",
        on_success_callback: Callable[..., object] | None = None,
        on_execute_callback: Callable[..., object] | None = None,
        on_failure_callback: Callable[..., object] | None = None,
        on_retry_callback: Callable[..., object] | None = None,
        on_skipped_callback: Callable[..., object] | None = None,
        inlets: Any | None = None,
        outlets: Any | None = None,
        email: str | None = None,
        map_index: int = -1,
        hostname: str | None = None,
        pid: int | None = None,
        last_heartbeat_at: datetime | None = None,
        task: Any | None = None,
        start_from_trigger: bool = False,
        start_trigger_args: Any | None = None,
        **dag_kwargs: Any,
    ) -> Any:
        """Author, persist, run-create, and return one refreshed task instance.

        Parameters mirror ``pytest_airflow_in_a_box.types.CreateTaskInstance.__call__``.

        Returns:
            airflow.models.taskinstance.TaskInstance refreshed from its authoring task.

        Raises:
            ValueError: Both ``logical_date`` and ``execution_date`` were passed.
        """

        # An explicit `execution_date=None` means "not supplied", never the 3.x
        # no-logical-date sentinel: `execution_date` is the Airflow 2 spelling, and no
        # Airflow 2 run can lack an execution date -- upstream defaulted `None` to the
        # current date. `logical_date=None` stays the only way to request a
        # logical-date-less run.
        if not isinstance(execution_date, UnsetType) and execution_date is not None:
            if not isinstance(logical_date, UnsetType):
                raise ValueError(
                    "`execution_date` is the Airflow 2 spelling of `logical_date`; "
                    "passing both would pick one silently. Drop `execution_date`."
                )
            # Upstream `tests_common` preserves the Airflow 2 spelling with the same
            # warning category; the mapping keeps 2.x-era suites running unchanged.
            warnings.warn(
                "'execution_date' parameter is preserved only for backward "
                "compatibility with Airflow 2 test suites. Use logical_date instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            logical_date = execution_date
        _, operator = _author_single_operator_dag(
            dag_maker,
            dag_id,
            task_id,
            operator_kwargs={
                "task_display_name": task_display_name,
                "max_active_tis_per_dag": max_active_tis_per_dag,
                "max_active_tis_per_dagrun": max_active_tis_per_dagrun,
                "pool": pool,
                "executor_config": executor_config,
                "trigger_rule": trigger_rule,
                "on_success_callback": on_success_callback,
                "on_execute_callback": on_execute_callback,
                "on_failure_callback": on_failure_callback,
                "on_retry_callback": on_retry_callback,
                "on_skipped_callback": on_skipped_callback,
                "inlets": inlets,
                "outlets": outlets,
                "email": email,
            },
            task=task,
            operator_attrs={
                "start_from_trigger": start_from_trigger,
                "start_trigger_args": start_trigger_args,
            },
            dag_kwargs=dag_kwargs,
        )
        # `state` is forwarded even when None, exactly as upstream does: an explicit
        # None reaches Airflow's DagRun constructor, which skips state assignment and
        # leaves the column default (`queued`). Filtering it here would silently turn
        # upstream's default into this plugin's `running` default.
        dag_run_kwargs: dict[str, Any] = {"state": dagrun_state}
        if run_type is not None:
            dag_run_kwargs["run_type"] = coerce_run_type(run_type)
        if data_interval is not None:
            dag_run_kwargs["data_interval"] = data_interval
        dag_run = dag_maker.create_dagrun(
            run_id=run_id,
            logical_date=logical_date,
            run_after=run_after,
            **dag_run_kwargs,
        )
        ti: Any = dag_maker.create_ti(str(operator.task_id), dag_run=dag_run)
        ti.state = state
        ti.external_executor_id = external_executor_id
        ti.map_index = map_index
        ti.hostname = hostname or ""
        ti.pid = pid
        if last_heartbeat_at is not None:
            ti.last_heartbeat_at = last_heartbeat_at
        dag_maker.session.commit()
        return ti

    return factory


@pytest.fixture
def testing_dag_bundle(request: pytest.FixtureRequest) -> None:
    """Register the shared ``testing`` Dag bundle row upstream tests write against.

    Idempotent and shared: the row is created once per metadata database and
    deliberately never deleted afterward -- a conditional teardown delete would race
    another ``pytest-xdist`` worker's in-flight ``DagModel.bundle_name`` reference,
    and the database is disposable per run. Upstream's fixture of the same name
    performs a refcount-guarded delete instead; the deviation is documented in the
    fixture reference.

    Parameters:
        request: pytest.FixtureRequest containing bootstrap state.
    """

    require_v3(
        "testing_dag_bundle",
        "Airflow 2.x has no Dag bundle models, so there is no `dag_bundle` row to "
        "register and no `DagModel.bundle_name` foreign key to satisfy. Drop the "
        "fixture on the 2.x family.",
    )
    ensure_database(get_bootstrap_state(request.config).root)
    ensure_shared_bundle(TESTING_BUNDLE_NAME)


__all__ = ("create_dummy_dag", "create_task_instance", "testing_dag_bundle")
