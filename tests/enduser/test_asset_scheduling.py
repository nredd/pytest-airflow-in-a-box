"""Exercise asset/dataset-triggered cross-Dag scheduling.

`Asset`/`Dataset`, `DAG`/`BaseOperator`, and the outlet-emitting operator all resolve
dynamically ON PURPOSE, matching the shared corpus Dags in `tests/dags/asset_outlet.py`
and `tests/dags/asset_consumer.py`: the class is named `Asset` on 3.x
(`airflow.sdk`) and `Dataset` on 2.x (`airflow.datasets`), with no common import path.
"""

from __future__ import annotations

import os
from importlib import import_module
from typing import TYPE_CHECKING, Any

import pytest
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from pytest_airflow_in_a_box._compat.asset_schedule import DATASET_CONDITION_REQUIRED_ABOVE
from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily, resolve_capabilities
from pytest_airflow_in_a_box.assets import evaluate_asset_schedules
from pytest_airflow_in_a_box.types import DagMaker

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

# `evaluate_asset_schedules`'s 2.x path requires 2.10+ (see `asset_schedule`'s module
# docstring for why); every test below evaluates a real schedule, so the whole module
# skips rather than failing on the certified releases that predate it.
_capabilities = resolve_capabilities()
_unsupported_v2_release = (
    _capabilities.family is AirflowFamily.V2
    and _capabilities.release < DATASET_CONDITION_REQUIRED_ABOVE
)
pytestmark = [
    pytest.mark.compat,
    pytest.mark.skipif(
        _unsupported_v2_release,
        reason=(
            "`evaluate_asset_schedules` requires Airflow 2.10+ on the 2.x family; "
            f"installed release is {_capabilities.release}"
        ),
    ),
]


# Shared with the sibling contract modules; see `tests/enduser/_authoring.py`.
_resolve = import_module("_authoring")._resolve

_authoring = _resolve("airflow.sdk", "airflow.models")
DAG = _authoring.DAG
BaseOperator = _authoring.BaseOperator
try:
    Asset = import_module("airflow.sdk").Asset
except ImportError:  # Airflow 2.x: datasets predate assets.
    Asset = import_module("airflow.datasets").Dataset

EmptyOperator = _resolve(
    "airflow.providers.standard.operators.empty", "airflow.operators.empty"
).EmptyOperator

# `ASSET_TRIGGERED` (3.x) and `DATASET_TRIGGERED` (2.x) are two distinct `DagRunType`
# members with no common spelling; resolve dynamically like `Asset`/`Dataset` above.
# The 2.x member name is held in a variable, not a literal at the `getattr` call site,
# so `ty` does not statically resolve it against the installed 3.x enum.
_v2_run_type_name = "DATASET_TRIGGERED"
_TRIGGERED_RUN_TYPE = getattr(DagRunType, "ASSET_TRIGGERED", None) or getattr(
    DagRunType, _v2_run_type_name
)


class EmitAssetOperator(BaseOperator):
    """Emit one outlet event through the plain `outlets=` declaration.

    Does not touch `context["outlet_events"]` to attach `.extra` metadata (unneeded by
    these tests) ON PURPOSE: that context key was added in Airflow 2.10 and this module
    otherwise runs on 2.7+, where touching it would raise `KeyError` before the outlet
    event -- which Airflow records from `outlets=` alone -- is ever reached.
    """

    def execute(self, context: Any) -> None:
        del context


@pytest.mark.db_test
def test_asset_triggered_dagrun_is_created(dag_maker: DagMaker) -> None:
    """Evaluate a satisfied consumer condition and create its queued DagRun.

    The consumer must be persisted before the producer's task runs: Airflow queues an
    asset event against a consumer Dag only if a `DagScheduleAssetReference` already
    names it as a subscriber at persist time, the same ordering a live deployment
    requires (both Dags are deployed before either runs).
    """

    asset = Asset(uri="asset://compat/cross-dag")
    with dag_maker(dag_id="compat_asset_triggering_consumer", schedule=[asset]):
        EmptyOperator(task_id="consume")
    with dag_maker(dag_id="compat_asset_triggering_producer"):
        EmitAssetOperator(task_id="emit", outlets=[asset])
    dag_maker.run_ti("emit")

    (consumer_run,) = evaluate_asset_schedules(
        "compat_asset_triggering_consumer", session=dag_maker.session
    )

    assert consumer_run.dag_id == "compat_asset_triggering_consumer"
    assert consumer_run.run_type == _TRIGGERED_RUN_TYPE
    assert consumer_run.state == DagRunState.QUEUED
    consumed = (
        consumer_run.consumed_asset_events
        if hasattr(consumer_run, "consumed_asset_events")
        else consumer_run.consumed_dataset_events
    )
    assert [event.source_task_id for event in consumed] == ["emit"]


@pytest.mark.db_test
def test_asset_triggered_dagrun_waits_for_every_condition(dag_maker: DagMaker) -> None:
    """Leave an unsatisfied compound condition without a created consumer DagRun."""

    first = Asset(uri="asset://compat/cross-dag-a")
    second = Asset(uri="asset://compat/cross-dag-b")
    with dag_maker(dag_id="compat_asset_partial_consumer", schedule=(first & second)):
        EmptyOperator(task_id="consume")
    with dag_maker(dag_id="compat_asset_partial_producer"):
        EmitAssetOperator(task_id="emit", outlets=[first])
    dag_maker.run_ti("emit")  # Only `first` is produced; `second` never arrives.

    assert (
        evaluate_asset_schedules("compat_asset_partial_consumer", session=dag_maker.session) == ()
    )


def test_evaluate_asset_schedules_requires_a_session() -> None:
    """Reject evaluation with no metadata session to query or persist through."""

    with pytest.raises(ValueError, match="requires a metadata `session`"):
        evaluate_asset_schedules("compat_asset_triggering_consumer")


@pytest.mark.db_test
@pytest.mark.skipif(
    bool(os.environ.get("PYTEST_XDIST_WORKER")),
    reason="a `dag_ids=None` sweep is documented as serial-only, like `clear_db`",
)
def test_asset_triggered_dagrun_sweeps_every_pending_consumer(dag_maker: DagMaker) -> None:
    """Evaluate every Dag carrying a pending queue row when `dag_ids` is omitted."""

    asset = Asset(uri="asset://compat/sweep")
    with dag_maker(dag_id="compat_asset_sweep_consumer", schedule=[asset]):
        EmptyOperator(task_id="consume")
    with dag_maker(dag_id="compat_asset_sweep_producer"):
        EmitAssetOperator(task_id="emit", outlets=[asset])
    dag_maker.run_ti("emit")

    (consumer_run,) = evaluate_asset_schedules(session=dag_maker.session)

    assert consumer_run.dag_id == "compat_asset_sweep_consumer"


@pytest.mark.db_test
def test_asset_triggered_dagrun_accepts_a_dag_id_collection(dag_maker: DagMaker) -> None:
    """Evaluate exactly the Dags named in an explicit collection."""

    asset = Asset(uri="asset://compat/collection")
    with dag_maker(dag_id="compat_asset_collection_consumer", schedule=[asset]):
        EmptyOperator(task_id="consume")
    with dag_maker(dag_id="compat_asset_collection_producer"):
        EmitAssetOperator(task_id="emit", outlets=[asset])
    dag_maker.run_ti("emit")

    (consumer_run,) = evaluate_asset_schedules(
        ["compat_asset_collection_consumer"], session=dag_maker.session
    )

    assert consumer_run.dag_id == "compat_asset_collection_consumer"


@pytest.mark.db_test
@pytest.mark.requires_airflow3
def test_asset_triggered_dagrun_rejects_an_asset_alias_consumer(dag_maker: DagMaker) -> None:
    """Reject a Dag scheduled through an `AssetAlias` rather than a bare `Asset`.

    Airflow-3-only: dual-authoring `AssetAlias`/`DatasetAlias` construction for a
    corner case already covered by the 2.x probe-double in
    `tests/compat/test_asset_schedule_v2_compat.py` is not worth the complexity here.
    """

    from airflow.sdk import AssetAlias

    with dag_maker(dag_id="compat_asset_alias_consumer", schedule=AssetAlias(name="compat-alias")):
        EmptyOperator(task_id="consume")

    with pytest.raises(ValueError, match="is scheduled through an `AssetAlias`"):
        evaluate_asset_schedules("compat_asset_alias_consumer", session=dag_maker.session)


@pytest.mark.db_test
def test_asset_triggered_dagrun_rejects_an_unresolvable_dag_id(session: Session) -> None:
    """Reject a `dag_id` with no persisted serialized Dag."""

    with pytest.raises(ValueError, match="No serialized Dag is persisted"):
        evaluate_asset_schedules("compat_asset_never_persisted", session=session)


@pytest.mark.db_test
def test_asset_triggered_dagrun_rejects_a_dag_not_scheduled_by_an_asset(
    dag_maker: DagMaker,
) -> None:
    """Reject a Dag whose timetable is not asset/dataset-triggered."""

    with dag_maker(dag_id="compat_asset_unscheduled_consumer"):
        EmptyOperator(task_id="consume")

    with pytest.raises(ValueError, match="is not scheduled by a"):
        evaluate_asset_schedules("compat_asset_unscheduled_consumer", session=dag_maker.session)


@pytest.mark.db_test
def test_asset_triggered_dagrun_waits_with_no_producer_run(dag_maker: DagMaker) -> None:
    """Leave a consumer with no queued events unevaluated to a DagRun."""

    asset = Asset(uri="asset://compat/never-produced")
    with dag_maker(dag_id="compat_asset_unproduced_consumer", schedule=[asset]):
        EmptyOperator(task_id="consume")

    assert (
        evaluate_asset_schedules("compat_asset_unproduced_consumer", session=dag_maker.session)
        == ()
    )
