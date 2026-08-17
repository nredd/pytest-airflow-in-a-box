"""Probe-double coverage for the Airflow 2.x branches of `_compat.asset_schedule`.

The 2.x branches are unreachable from a 3.x install, so these tests hold the 100%
gate with fake capability contracts and fake `airflow` modules -- the real behavior
is certified end to end by the 2.x compat CI legs. `select()`/`delete()` need real
SQLAlchemy-mapped classes to construct (a bare `SimpleNamespace` raises
`ArgumentError`), so the fake `DatasetDagRunQueue`/`DatasetEvent`/
`DagScheduleDatasetReference`/`DatasetModel` classes below are genuine declarative
models backed by an in-memory SQLite engine, matching `test_dag_v2_compat.py`'s
established fake-probe technique one level deeper.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, relationship

from pytest_airflow_in_a_box._compat import asset_schedule as schedule_module
from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily


class _Base(DeclarativeBase):
    """Local declarative base, isolated from Airflow's own metadata."""


class FakeDatasetModel(_Base):
    """Stand in for `airflow.models.dataset.DatasetModel`."""

    __tablename__ = "fake_dataset"

    id = Column(Integer, primary_key=True)
    uri = Column(String, nullable=False)


class FakeDatasetDagRunQueue(_Base):
    """Stand in for `airflow.models.dataset.DatasetDagRunQueue`."""

    __tablename__ = "fake_dataset_dag_run_queue"

    dataset_id = Column(Integer, ForeignKey("fake_dataset.id"), primary_key=True)
    target_dag_id = Column(String, primary_key=True)
    created_at = Column(DateTime, nullable=False)
    dataset = relationship("FakeDatasetModel")


class FakeDatasetEvent(_Base):
    """Stand in for `airflow.models.dataset.DatasetEvent`."""

    __tablename__ = "fake_dataset_event"

    id = Column(Integer, primary_key=True)
    dataset_id = Column(Integer, ForeignKey("fake_dataset.id"), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    source_task_id = Column(String, nullable=True)


class FakeDagScheduleDatasetReference(_Base):
    """Stand in for `airflow.models.dataset.DagScheduleDatasetReference`."""

    __tablename__ = "fake_dag_schedule_dataset_reference"

    dag_id = Column(String, primary_key=True)
    dataset_id = Column(Integer, ForeignKey("fake_dataset.id"), primary_key=True)


class FakeDatasetTriggeredTimetable:
    """Stand in for `airflow.timetables.simple.DatasetTriggeredTimetable`."""


@pytest.fixture
def fake_dataset_module(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Install the fake `airflow.models.dataset` module for one test.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing the fake module.

    Returns:
        Any exposing the fake dataset ORM classes.
    """

    fake_module = SimpleNamespace(
        DatasetDagRunQueue=FakeDatasetDagRunQueue,
        DatasetEvent=FakeDatasetEvent,
        DagScheduleDatasetReference=FakeDagScheduleDatasetReference,
    )
    monkeypatch.setitem(sys.modules, "airflow.models.dataset", fake_module)
    return fake_module


@pytest.fixture
def fake_dataset_triggered_timetable(monkeypatch: pytest.MonkeyPatch) -> type:
    """Attach a fake `DatasetTriggeredTimetable` to the real `timetables.simple` module.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing the fake attribute.

    Returns:
        type containing the fake timetable class.
    """

    import airflow.timetables.simple as real_module

    monkeypatch.setattr(
        real_module, "DatasetTriggeredTimetable", FakeDatasetTriggeredTimetable, raising=False
    )
    return FakeDatasetTriggeredTimetable


@pytest.fixture
def fake_dataset_triggered_run_type(monkeypatch: pytest.MonkeyPatch) -> object:
    """Attach a fake `DagRunType.DATASET_TRIGGERED` member to the real enum.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing the fake attribute.

    Returns:
        object containing the fake run-type sentinel.
    """

    from airflow.utils.types import DagRunType

    sentinel = object()
    monkeypatch.setattr(DagRunType, "DATASET_TRIGGERED", sentinel, raising=False)
    return sentinel


@pytest.fixture
def in_memory_session() -> Any:
    """Build a Session over a fresh in-memory SQLite database.

    Returns:
        sqlalchemy.orm.Session bound to an isolated in-memory engine.
    """

    engine = create_engine("sqlite://")
    _Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


class _FakeDagRun:
    """Record `create_dagrun` outcomes and accept consumed events."""

    def __init__(self) -> None:
        self.consumed_dataset_events: list[Any] = []


def _fake_dag(
    *,
    timetable_cls: type,
    condition_result: bool,
    created: dict[str, Any],
) -> Any:
    """Build a fake scheduler Dag exposing the 2.x timetable/`create_dagrun` surface.

    Parameters:
        timetable_cls: type instantiated as the fake Dag's `timetable`.
        condition_result: bool returned by the fake dataset condition's `evaluate`.
        created: dict[str, Any] recording the `create_dagrun` call's kwargs.

    Returns:
        Any standing in for the persisted scheduler Dag.
    """

    def create_dagrun(**kwargs: Any) -> _FakeDagRun:
        created["kwargs"] = kwargs
        return _FakeDagRun()

    timetable = timetable_cls()
    timetable.dataset_condition = SimpleNamespace(evaluate=lambda _statuses: condition_result)
    timetable.data_interval_for_events = lambda exec_date, events: ("interval", exec_date, events)
    timetable.generate_run_id = lambda **_kwargs: "fake-run-id"
    return SimpleNamespace(timetable=timetable, create_dagrun=create_dagrun)


@pytest.mark.usefixtures("fake_dataset_module")
def test_pending_v2_dag_ids_lists_distinct_target_dag_ids(in_memory_session: Any) -> None:
    """Report distinct target Dag ids across every queued row."""

    dataset = FakeDatasetModel(uri="dataset://compat/v2")
    in_memory_session.add(dataset)
    in_memory_session.flush()
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    in_memory_session.add_all(
        [
            FakeDatasetDagRunQueue(dataset_id=dataset.id, target_dag_id="b", created_at=now),
            FakeDatasetDagRunQueue(dataset_id=dataset.id, target_dag_id="a", created_at=now),
        ]
    )
    in_memory_session.flush()

    assert schedule_module._pending_v2_dag_ids(in_memory_session) == ("a", "b")


@pytest.mark.usefixtures(
    "fake_dataset_module", "fake_dataset_triggered_timetable", "fake_dataset_triggered_run_type"
)
def test_evaluate_v2_dag_rejects_a_dag_not_scheduled_by_a_dataset(
    monkeypatch: pytest.MonkeyPatch, in_memory_session: Any
) -> None:
    """Reject a Dag whose timetable is not `DatasetTriggeredTimetable`."""

    monkeypatch.setattr(
        schedule_module,
        "_resolve_scheduler_dag",
        lambda *_args: SimpleNamespace(timetable=object()),
    )

    with pytest.raises(ValueError, match="is not scheduled by a `Dataset`"):
        schedule_module._evaluate_v2_dag("consumer", in_memory_session)


@pytest.mark.usefixtures(
    "fake_dataset_module", "fake_dataset_triggered_timetable", "fake_dataset_triggered_run_type"
)
def test_evaluate_v2_dag_returns_none_when_queue_is_empty(
    monkeypatch: pytest.MonkeyPatch, in_memory_session: Any
) -> None:
    """Return `None` for a consumer with no pending queue rows."""

    dag = _fake_dag(timetable_cls=FakeDatasetTriggeredTimetable, condition_result=True, created={})
    monkeypatch.setattr(schedule_module, "_resolve_scheduler_dag", lambda *_args: dag)

    assert schedule_module._evaluate_v2_dag("consumer", in_memory_session) is None


@pytest.mark.usefixtures(
    "fake_dataset_module", "fake_dataset_triggered_timetable", "fake_dataset_triggered_run_type"
)
def test_evaluate_v2_dag_returns_none_when_condition_unsatisfied(
    monkeypatch: pytest.MonkeyPatch, in_memory_session: Any
) -> None:
    """Leave the queue row untouched when the dataset condition is unsatisfied."""

    dataset = FakeDatasetModel(uri="dataset://compat/unsatisfied")
    in_memory_session.add(dataset)
    in_memory_session.flush()
    in_memory_session.add(
        FakeDatasetDagRunQueue(
            dataset_id=dataset.id,
            target_dag_id="consumer",
            created_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
    )
    in_memory_session.flush()

    created: dict[str, Any] = {}
    dag = _fake_dag(
        timetable_cls=FakeDatasetTriggeredTimetable, condition_result=False, created=created
    )
    monkeypatch.setattr(schedule_module, "_resolve_scheduler_dag", lambda *_args: dag)

    assert schedule_module._evaluate_v2_dag("consumer", in_memory_session) is None
    assert created == {}
    remaining = in_memory_session.query(FakeDatasetDagRunQueue).count()
    assert remaining == 1


@pytest.mark.usefixtures(
    "fake_dataset_module", "fake_dataset_triggered_timetable", "fake_dataset_triggered_run_type"
)
def test_evaluate_v2_dag_creates_a_dagrun_when_satisfied(
    monkeypatch: pytest.MonkeyPatch, in_memory_session: Any
) -> None:
    """Create the consumer's DagRun, attach matched events, and drain the queue."""

    dataset = FakeDatasetModel(uri="dataset://compat/satisfied")
    in_memory_session.add(dataset)
    in_memory_session.flush()
    created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    in_memory_session.add(
        FakeDatasetDagRunQueue(
            dataset_id=dataset.id, target_dag_id="consumer", created_at=created_at
        )
    )
    in_memory_session.add(
        FakeDagScheduleDatasetReference(dag_id="consumer", dataset_id=dataset.id)
    )
    in_memory_session.add(
        FakeDatasetEvent(
            dataset_id=dataset.id,
            timestamp=created_at,
            source_task_id="emit",
        )
    )
    in_memory_session.flush()

    created: dict[str, Any] = {}
    dag = _fake_dag(
        timetable_cls=FakeDatasetTriggeredTimetable, condition_result=True, created=created
    )
    monkeypatch.setattr(schedule_module, "_resolve_scheduler_dag", lambda *_args: dag)

    result = schedule_module._evaluate_v2_dag("consumer", in_memory_session)

    assert result is not None
    assert len(result.consumed_dataset_events) == 1
    assert result.consumed_dataset_events[0].source_task_id == "emit"
    # SQLite drops tzinfo on round-trip; the value is still the same instant.
    assert created["kwargs"]["execution_date"].replace(tzinfo=timezone.utc) == created_at
    assert created["kwargs"]["run_id"] == "fake-run-id"
    remaining = in_memory_session.query(FakeDatasetDagRunQueue).count()
    assert remaining == 0


@pytest.mark.usefixtures("fake_dataset_module", "fake_dataset_triggered_timetable")
def test_evaluate_v2_sweeps_every_pending_dag_id(
    monkeypatch: pytest.MonkeyPatch, in_memory_session: Any
) -> None:
    """Evaluate every pending Dag id when `dag_ids` is `None`."""

    dataset = FakeDatasetModel(uri="dataset://compat/sweep")
    in_memory_session.add(dataset)
    in_memory_session.flush()
    in_memory_session.add(
        FakeDatasetDagRunQueue(
            dataset_id=dataset.id,
            target_dag_id="swept_consumer",
            created_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
    )
    in_memory_session.flush()

    evaluated: list[str] = []

    def fake_evaluate_v2_dag(dag_id: str, session: Any) -> None:
        del session
        evaluated.append(dag_id)

    monkeypatch.setattr(schedule_module, "_evaluate_v2_dag", fake_evaluate_v2_dag)

    assert schedule_module._evaluate_v2(None, in_memory_session) == ()
    assert evaluated == ["swept_consumer"]


def test_evaluate_asset_schedules_dispatches_to_the_v2_family(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Route to `_evaluate_v2` when the resolved family is V2."""

    monkeypatch.setattr(
        schedule_module, "resolve_capabilities", lambda: SimpleNamespace(family=AirflowFamily.V2)
    )
    calls: list[Any] = []
    monkeypatch.setattr(
        schedule_module,
        "_evaluate_v2",
        lambda dag_ids, session: calls.append((dag_ids, session)) or (),
    )

    fake_session: Any = object()
    result = schedule_module.evaluate_asset_schedules("consumer", session=fake_session)

    assert result == ()
    assert calls == [(("consumer",), fake_session)]
