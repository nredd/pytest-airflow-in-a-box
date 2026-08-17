"""Evaluate asset/dataset schedule conditions and create consumer DagRuns.

``_evaluate_v3_dag`` is adapted from the DagRun-creation body of Apache Airflow's
``SchedulerJobRunner._create_dag_runs_asset_triggered`` and the readiness evaluation in
``DagModel.dags_needing_dagruns``. ``_evaluate_v2_dag`` is adapted analogously from
``SchedulerJobRunner._create_dag_runs_dataset_triggered``. Both drop scheduler-operational
concerns that do not apply to one evaluated test Dag inside an isolated single-process
test database: row locking, ``max_active_runs`` throttling, paused/stale/import-error Dag
filters, and batching. Both also simplify the attached "consumed events" query to omit
the real scheduler's lower bound at the previous asset/dataset-triggered run of the same
consumer, which only matters across repeated evaluations of the same consumer Dag across
multiple producer runs; a single evaluation per consumer (the common test shape) is
unaffected. Aliased asset/dataset consumers (``DagScheduleAssetAliasReference`` /
``DagScheduleDatasetAliasReference``) are out of scope for this first cut and rejected
explicitly rather than silently evaluated with an incomplete `consumed_*_events`.
``_evaluate_v3``, ``_evaluate_v2``, and ``evaluate_asset_schedules`` are independently
authored.

The 2.x path additionally requires release 2.10 or newer: `DatasetTriggeredTimetable`
only gained a `dataset_condition` attribute in 2.10 (`airflow/timetables/simple.py`).
2.9.3 evaluates readiness off `dag.dataset_triggers` instead, and 2.7.3/2.8.4 carry no
condition object at all -- three more distinct shapes `_evaluate_v2_dag` does not
reimplement. `_evaluate_v2_dag` rejects an older release explicitly rather than
crashing with an unlabeled `AttributeError`.

References:
    https://github.com/apache/airflow/blob/3.3.0/airflow-core/src/airflow/jobs/scheduler_job_runner.py
    https://github.com/apache/airflow/blob/3.3.0/airflow-core/src/airflow/models/dag.py
    https://github.com/apache/airflow/blob/3.3.0/airflow-core/src/airflow/assets/evaluation.py
    https://github.com/apache/airflow/blob/2.10.5/airflow/jobs/scheduler_job_runner.py
    https://github.com/apache/airflow/blob/2.10.5/airflow/timetables/simple.py
    https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html
"""

from __future__ import annotations

from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from pytest_airflow_in_a_box._compat.capabilities import (
    AirflowFamily,
    AssetUniqueKeyLocation,
    resolve_capabilities,
)

# `DatasetTriggeredTimetable.dataset_condition` was introduced in this release; see the
# module docstring for what predates it.
DATASET_CONDITION_REQUIRED_ABOVE: tuple[int, int, int] = (2, 10, 0)

if TYPE_CHECKING:
    from airflow.models.dagrun import DagRun
    from sqlalchemy.orm import Session


def _resolve_dag_ids(dag_ids: str | Collection[str] | None) -> tuple[str, ...] | None:
    """Normalize the caller-supplied ``dag_ids`` argument to a tuple or ``None``.

    Parameters:
        dag_ids: str | Collection[str] | None naming the Dags to evaluate.

    Returns:
        tuple[str, ...] | None containing normalized Dag identifiers, or ``None`` to
        evaluate every Dag carrying a pending queue row.
    """

    if dag_ids is None:
        return None
    if isinstance(dag_ids, str):
        return (dag_ids,)
    return tuple(dag_ids)


def _resolve_scheduler_dag(dag_id: str, session: Session) -> Any:
    """Load one Dag's persisted scheduler representation.

    Parameters:
        dag_id: str identifying the Dag.
        session: sqlalchemy.orm.Session used to query metadata.

    Returns:
        Any containing the persisted scheduler Dag.

    Raises:
        ValueError: No serialized Dag is persisted for ``dag_id``.
    """

    from airflow.models.serialized_dag import SerializedDagModel

    dag = SerializedDagModel.get_dag(dag_id, session=session)
    if dag is None:
        raise ValueError(f"No serialized Dag is persisted for dag_id '{dag_id}'")
    return dag


def _pending_v3_dag_ids(session: Session) -> tuple[str, ...]:
    """Return every Dag id carrying a pending asset queue row.

    Parameters:
        session: sqlalchemy.orm.Session used to query metadata.

    Returns:
        tuple[str, ...] containing distinct target Dag identifiers.
    """

    from airflow.models.asset import AssetDagRunQueue
    from sqlalchemy import select

    return tuple(sorted(session.scalars(select(AssetDagRunQueue.target_dag_id).distinct()).all()))


def _asset_unique_key_type(capabilities: Any) -> Any:
    """Resolve the asset-condition unique-key type for the installed release.

    Parameters:
        capabilities: pytest_airflow_in_a_box._compat.capabilities.AirflowCapabilities
            resolved for the installed Airflow.

    Returns:
        Any containing the ``AssetUniqueKey``/``SerializedAssetUniqueKey`` type.
    """

    if capabilities.asset_unique_key_location is AssetUniqueKeyLocation.SERIALIZATION:
        from airflow.serialization.definitions.assets import SerializedAssetUniqueKey

        return SerializedAssetUniqueKey
    from airflow.sdk.definitions.asset import AssetUniqueKey

    return AssetUniqueKey


def _evaluate_v3_dag(dag_id: str, session: Session) -> DagRun | None:
    """Evaluate one Airflow 3.x Dag's asset condition and create its DagRun if ready.

    Parameters:
        dag_id: str identifying the consumer Dag.
        session: sqlalchemy.orm.Session used to query and persist metadata.

    Returns:
        airflow.models.dagrun.DagRun | None containing the created run, or ``None``
        when the Dag has no pending queue rows or its asset condition is unsatisfied.

    Raises:
        ValueError: No serialized Dag is persisted for ``dag_id``, the Dag is not
            scheduled by an ``Asset``, or the Dag is scheduled through an
            ``AssetAlias``, which is out of scope for this first cut.
    """

    from airflow.assets.evaluation import AssetEvaluator
    from airflow.models.asset import (
        AssetDagRunQueue,
        AssetEvent,
        DagScheduleAssetAliasReference,
        DagScheduleAssetReference,
    )
    from airflow.models.dagrun import DagRun
    from airflow.timetables.simple import AssetTriggeredTimetable
    from airflow.utils.state import DagRunState
    from airflow.utils.types import DagRunTriggeredByType, DagRunType
    from sqlalchemy import delete, select

    dag = _resolve_scheduler_dag(dag_id, session)
    if not isinstance(dag.timetable, AssetTriggeredTimetable):
        raise ValueError(
            f"Dag '{dag_id}' is not scheduled by an `Asset`: "
            f"timetable is '{type(dag.timetable).__name__}'"
        )
    # An alias-scheduled consumer still passes the `isinstance` check above and can
    # still evaluate ready (`AssetEvaluator` resolves aliases), but the "consumed
    # events" query below only joins `DagScheduleAssetReference`, so it would create a
    # real, `QUEUED` `DagRun` with a silently empty `consumed_asset_events` instead of
    # the documented out-of-scope rejection.
    if session.scalar(
        select(DagScheduleAssetAliasReference).where(
            DagScheduleAssetAliasReference.dag_id == dag_id
        )
    ):
        raise ValueError(
            f"Dag '{dag_id}' is scheduled through an `AssetAlias`, which "
            "`evaluate_asset_schedules` does not yet support"
        )

    queued: Any = session.scalars(
        select(AssetDagRunQueue).where(AssetDagRunQueue.target_dag_id == dag_id)
    ).all()
    if not queued:
        return None

    unique_key = _asset_unique_key_type(resolve_capabilities())
    statuses = {unique_key.from_asset(adrq.asset): True for adrq in queued}
    if not AssetEvaluator(session).run(dag.timetable.asset_condition, statuses):
        return None

    triggered_date = max(adrq.created_at for adrq in queued)
    asset_events = (
        session.scalars(
            select(AssetEvent)
            .join(
                DagScheduleAssetReference,
                AssetEvent.asset_id == DagScheduleAssetReference.asset_id,
            )
            .where(
                DagScheduleAssetReference.dag_id == dag_id,
                AssetEvent.timestamp <= triggered_date,
            )
            .order_by(AssetEvent.timestamp, AssetEvent.id)
        )
        .unique()
        .all()
    )

    dag_run = dag.create_dagrun(
        run_id=DagRun.generate_run_id(
            run_type=DagRunType.ASSET_TRIGGERED, logical_date=None, run_after=triggered_date
        ),
        logical_date=None,
        data_interval=None,
        run_after=triggered_date,
        run_type=DagRunType.ASSET_TRIGGERED,
        triggered_by=DagRunTriggeredByType.ASSET,
        state=DagRunState.QUEUED,
        session=session,
    )
    dag_run.consumed_asset_events.extend(asset_events)
    session.execute(delete(AssetDagRunQueue).where(AssetDagRunQueue.target_dag_id == dag_id))
    session.flush()
    return dag_run


def _evaluate_v3(dag_ids: tuple[str, ...] | None, session: Session) -> tuple[DagRun, ...]:
    """Evaluate asset schedules across the Airflow 3.x family.

    Parameters:
        dag_ids: tuple[str, ...] | None naming the Dags to evaluate, or ``None`` to
            evaluate every Dag carrying a pending queue row.
        session: sqlalchemy.orm.Session used to query and persist metadata.

    Returns:
        tuple[airflow.models.dagrun.DagRun, ...] containing every created run.
    """

    targets = dag_ids if dag_ids is not None else _pending_v3_dag_ids(session)
    created = (_evaluate_v3_dag(dag_id, session) for dag_id in targets)
    return tuple(dag_run for dag_run in created if dag_run is not None)


def _pending_v2_dag_ids(session: Session) -> tuple[str, ...]:
    """Return every Dag id carrying a pending dataset queue row.

    Parameters:
        session: sqlalchemy.orm.Session used to query metadata.

    Returns:
        tuple[str, ...] containing distinct target Dag identifiers.
    """

    from importlib import import_module

    from sqlalchemy import select

    # Imported dynamically: `airflow.models.dataset` does not exist on a 3.x-only
    # install, so a literal import fails static resolution for `ruff`/`ty` even though
    # this function only ever runs on an actual 2.x installation.
    dataset_dag_run_queue = import_module("airflow.models.dataset").DatasetDagRunQueue

    return tuple(
        sorted(session.scalars(select(dataset_dag_run_queue.target_dag_id).distinct()).all())
    )


def _evaluate_v2_dag(dag_id: str, session: Session) -> DagRun | None:
    """Evaluate one Airflow 2.x Dag's dataset condition and create its DagRun if ready.

    Parameters:
        dag_id: str identifying the consumer Dag.
        session: sqlalchemy.orm.Session used to query and persist metadata.

    Returns:
        airflow.models.dagrun.DagRun | None containing the created run, or ``None``
        when the Dag has no pending queue rows or its dataset condition is
        unsatisfied.

    Raises:
        ValueError: No serialized Dag is persisted for ``dag_id``, the Dag is not
            scheduled by a ``Dataset``, the Dag is scheduled through a
            ``DatasetAlias``, which is out of scope for this first cut, or the
            installed 2.x release predates the 2.10 ``dataset_condition`` timetable
            attribute this evaluation reads.
    """

    from importlib import import_module

    from airflow.utils.state import DagRunState
    from sqlalchemy import delete, select

    capabilities = resolve_capabilities()
    if (
        capabilities.family is AirflowFamily.V2
        and capabilities.release < DATASET_CONDITION_REQUIRED_ABOVE
    ):
        # 2.7.3/2.8.4 have no condition object at all, and 2.9.3 evaluates readiness
        # off `dag.dataset_triggers` instead of `timetable.dataset_condition` -- three
        # more distinct shapes this evaluator does not (yet) reimplement.
        certified = ".".join(str(part) for part in DATASET_CONDITION_REQUIRED_ABOVE)
        installed = ".".join(str(part) for part in capabilities.release)
        raise ValueError(
            f"`evaluate_asset_schedules` requires Airflow '{certified}' or newer on "
            f"the 2.x family; installed release is '{installed}'"
        )

    # Imported dynamically: `airflow.models.dataset`,
    # `airflow.timetables.simple.DatasetTriggeredTimetable`, and
    # `DagRunType.DATASET_TRIGGERED` do not exist on a 3.x-only install (`ruff`'s
    # `AIR301` also flags the timetable as removed in 3.0), even though this function
    # only ever runs on an actual 2.x installation. The attribute name is held in a
    # variable, not a literal at the `getattr` call site, so neither `ty` nor `ruff`'s
    # `B009` statically resolve it against the installed 3.x symbol.
    dataset_models = import_module("airflow.models.dataset")
    dag_schedule_dataset_reference = dataset_models.DagScheduleDatasetReference
    dataset_dag_run_queue = dataset_models.DatasetDagRunQueue
    dataset_event = dataset_models.DatasetEvent
    dag_schedule_dataset_alias_reference = dataset_models.DagScheduleDatasetAliasReference
    timetable_name = "DatasetTriggeredTimetable"
    dataset_triggered_timetable = getattr(
        import_module("airflow.timetables.simple"), timetable_name
    )
    run_type_name = "DATASET_TRIGGERED"
    dataset_triggered_run_type = getattr(
        import_module("airflow.utils.types").DagRunType, run_type_name
    )

    dag = _resolve_scheduler_dag(dag_id, session)
    if not isinstance(dag.timetable, dataset_triggered_timetable):
        raise ValueError(
            f"Dag '{dag_id}' is not scheduled by a `Dataset`: "
            f"timetable is '{type(dag.timetable).__name__}'"
        )
    # See the matching `DagScheduleAssetAliasReference` check in `_evaluate_v3_dag`:
    # an alias-scheduled consumer still passes the `isinstance` check above and can
    # still evaluate ready, but the "consumed events" query below only joins
    # `DagScheduleDatasetReference`, so it would create a real, `QUEUED` `DagRun` with
    # a silently empty `consumed_dataset_events` instead of the documented rejection.
    # `DagScheduleDatasetAliasReference` is 2.10+ only, matching the release gate above.
    if session.scalar(
        select(dag_schedule_dataset_alias_reference).where(
            dag_schedule_dataset_alias_reference.dag_id == dag_id
        )
    ):
        raise ValueError(
            f"Dag '{dag_id}' is scheduled through a `DatasetAlias`, which "
            "`evaluate_asset_schedules` does not yet support"
        )

    queued: Any = session.scalars(
        select(dataset_dag_run_queue).where(dataset_dag_run_queue.target_dag_id == dag_id)
    ).all()
    if not queued:
        return None

    statuses = {adrq.dataset.uri: True for adrq in queued}
    if not dag.timetable.dataset_condition.evaluate(statuses):
        return None

    exec_date = max(adrq.created_at for adrq in queued)
    dataset_events = (
        session.scalars(
            select(dataset_event)
            .join(
                dag_schedule_dataset_reference,
                dataset_event.dataset_id == dag_schedule_dataset_reference.dataset_id,
            )
            .where(
                dag_schedule_dataset_reference.dag_id == dag_id,
                dataset_event.timestamp <= exec_date,
            )
            .order_by(dataset_event.timestamp, dataset_event.id)
        )
        .unique()
        .all()
    )

    data_interval = dag.timetable.data_interval_for_events(exec_date, dataset_events)
    dag_run = dag.create_dagrun(
        run_id=dag.timetable.generate_run_id(
            run_type=dataset_triggered_run_type,
            logical_date=exec_date,
            data_interval=data_interval,
            session=session,
            events=dataset_events,
        ),
        run_type=dataset_triggered_run_type,
        execution_date=exec_date,
        data_interval=data_interval,
        state=DagRunState.QUEUED,
        external_trigger=False,
        session=session,
    )
    dag_run.consumed_dataset_events.extend(dataset_events)
    session.execute(
        delete(dataset_dag_run_queue).where(dataset_dag_run_queue.target_dag_id == dag_id)
    )
    session.flush()
    return dag_run


def _evaluate_v2(dag_ids: tuple[str, ...] | None, session: Session) -> tuple[DagRun, ...]:
    """Evaluate dataset schedules across the Airflow 2.x family.

    Parameters:
        dag_ids: tuple[str, ...] | None naming the Dags to evaluate, or ``None`` to
            evaluate every Dag carrying a pending queue row.
        session: sqlalchemy.orm.Session used to query and persist metadata.

    Returns:
        tuple[airflow.models.dagrun.DagRun, ...] containing every created run.
    """

    targets = dag_ids if dag_ids is not None else _pending_v2_dag_ids(session)
    created = (_evaluate_v2_dag(dag_id, session) for dag_id in targets)
    return tuple(dag_run for dag_run in created if dag_run is not None)


def evaluate_asset_schedules(
    dag_ids: str | Collection[str] | None = None,
    *,
    session: Session | None = None,
) -> tuple[DagRun, ...]:
    """Evaluate consumer Dags' asset/dataset conditions and create ready DagRuns.

    Mirrors, without a live scheduler, what Apache Airflow's own scheduler loop does
    once a producer task's outlet events are persisted: check whether a consumer Dag's
    ``Asset``/``Dataset`` condition is satisfied by its queued events, and if so create
    its ``QUEUED`` ``DagRun`` with the satisfying events attached. Returns the raw ORM
    DagRun rather than a settled result -- chain into
    ``pytest_airflow_in_a_box.taskinstance.execute_dag_run`` to run it.

    ``dag_ids=None`` sweeps every Dag with a pending queue row database-wide, the same
    scope `pytest_airflow_in_a_box.db.clear_db` documents as serial-only: it is not safe
    while parallel xdist workers are mutating the shared database, since another
    worker's still-pending queue rows are indistinguishable from this caller's.

    Parameters:
        dag_ids: str | Collection[str] | None naming the consumer Dags to evaluate. A
            bare `str` evaluates one Dag. `None` evaluates every Dag carrying at least
            one pending queue row database-wide -- serial-only, see above.
        session: sqlalchemy.orm.Session | None used to query and persist metadata.
            Required: this function operates database-wide, with no caller-supplied ORM
            object to derive an implicit session from.

    Returns:
        tuple[airflow.models.dagrun.DagRun, ...] containing one entry per consumer Dag
        whose condition was satisfied, in evaluation order. A Dag with no pending queue
        rows or an unsatisfied condition contributes no entry.

    Raises:
        ValueError: ``session`` is omitted, an explicitly named Dag has no persisted
            serialized representation, or an explicitly named Dag is not scheduled by
            an `Asset`/`Dataset`.
    """

    if session is None:
        raise ValueError(
            "`evaluate_asset_schedules` requires a metadata `session`, for example "
            "`dag_maker.session` or the `session` fixture"
        )
    targets = _resolve_dag_ids(dag_ids)
    if resolve_capabilities().family is AirflowFamily.V2:
        return _evaluate_v2(targets, session)
    return _evaluate_v3(targets, session)


__all__ = ("evaluate_asset_schedules",)
