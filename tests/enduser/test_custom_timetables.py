"""Exercise the custom-timetable and weight-strategy registration consumer contract.

Issue #114's "Done" bullet: registering and round-tripping a custom timetable is one
line of user code. Every test here is `requires_airflow3` -- the component sandbox the
registration rides on is 3.x-only by design.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/114
    https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html
"""

from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest

if TYPE_CHECKING:
    from pytest_airflow_in_a_box.types import ComponentRegistry, DagMaker

pytestmark = pytest.mark.compat

CORPUS = Path(__file__).parents[1] / "dags"


@pytest.mark.requires_airflow3
@pytest.mark.need_serialized_dag
def test_dag_maker_schedules_a_custom_timetable_end_to_end(
    dag_maker: DagMaker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Author, persist, and run against a custom timetable with zero plugin wiring."""

    monkeypatch.syspath_prepend(str(CORPUS))
    provider: Any = import_module("provider_package")

    with dag_maker(schedule=provider.ExampleTimetable(hours=2)) as dag:
        from airflow.providers.standard.operators.empty import EmptyOperator

        EmptyOperator(task_id="scheduled")

    dag_run = dag_maker.create_dagrun()

    assert dag_run.dag_id == dag.dag_id
    serialized_dag = dag_maker.serialized_dag
    assert serialized_dag is not None
    timetable = cast("Any", serialized_dag).timetable
    assert type(timetable) is provider.ExampleTimetable
    assert timetable.hours == 2


@pytest.mark.requires_airflow3
def test_serialization_round_trip_is_one_line(
    airflow_components: ComponentRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Register and round-trip a custom timetable in a single registry call."""

    monkeypatch.syspath_prepend(str(CORPUS))
    provider: Any = import_module("provider_package")

    airflow_components.serialization_round_trip(provider.ExampleTimetable(hours=3))


@pytest.mark.requires_airflow3
@pytest.mark.need_serialized_dag
def test_weight_strategy_survives_dag_serialization(
    airflow_components: ComponentRegistry,
    dag_maker: DagMaker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Serialize an operator bound to a registered custom weight strategy.

    Registration is the whole enablement: Airflow's
    `_encode_priority_weight_strategy` refuses an unregistered custom strategy at
    `SerializedDagModel.write_dag` time on every certified 3.x release, and decoding
    re-instantiates the class with no arguments.
    """

    monkeypatch.syspath_prepend(str(CORPUS))
    weight_strategy_module: Any = import_module("provider_package._weight_strategy")
    strategy_class = weight_strategy_module.ExampleWeightStrategy

    airflow_components.priority_weight_strategy(strategy_class)

    with dag_maker():
        from airflow.providers.standard.operators.empty import EmptyOperator

        EmptyOperator(task_id="weighted", weight_rule=strategy_class())

    serialized_dag = dag_maker.serialized_dag
    assert serialized_dag is not None
    weight_rule = serialized_dag.get_task("weighted").weight_rule
    assert type(weight_rule) is strategy_class
