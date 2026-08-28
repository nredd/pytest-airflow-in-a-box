# Timetables

Test a custom timetable as three separate contracts:

| Contract | Test |
| --- | --- |
| Scheduling behavior | Call the timetable directly |
| Serialized state | Round-trip one instance with `airflow_components` |
| Production discovery | Test the package or plugin that registers the class |

Keeping those boundaries separate makes failures specific: logic tests need no Airflow
registration, while a discovery test should not also be your scheduling algorithm test.

## The logic needs no plugin

Call `next_dagrun_info` and `infer_manual_data_interval` on the timetable instance you
constructed. These methods need no fixture, metadata database, or registration:

```python
import pendulum
from airflow.timetables.base import DataInterval, TimeRestriction


def test_workday_intervals():
    timetable = WorkdayTimetable()
    monday = pendulum.datetime(2026, 1, 5, tz="UTC")
    tuesday = pendulum.datetime(2026, 1, 6, tz="UTC")
    expected = DataInterval(start=monday, end=tuesday)

    automatic = timetable.next_dagrun_info(
        last_automated_data_interval=DataInterval(
            start=pendulum.datetime(2026, 1, 2, tz="UTC"),
            end=pendulum.datetime(2026, 1, 3, tz="UTC"),
        ),
        restriction=TimeRestriction(earliest=None, latest=None, catchup=True),
    )
    manual = timetable.infer_manual_data_interval(run_after=tuesday)

    assert automatic is not None
    assert automatic.data_interval == expected
    assert automatic.run_after == tuesday
    assert manual == expected
```

Cover the boundary conditions your timetable owns: first run, catchup on and off, earliest and
latest limits, daylight-saving transitions, and an empty schedule. Assert the complete
`DataInterval`, not only `run_after`.

On Airflow 3.2 and newer, the authoring Dag's timetable does not expose scheduler methods. Call
your concrete timetable directly for logic tests. After `dag_maker` persists a Dag, use
`dag_maker.timetable` when the assertion specifically needs the scheduler-side object.

## What registration is actually for

Airflow serializes a custom timetable as a qualified class name plus the mapping returned by
`serialize()`. Deserialization must find the registered class and reconstruct the same state.
Test that contract in one call:

```python
def test_timetable_round_trips(airflow_components):
    airflow_components.serialization_round_trip(WorkdayTimetable(hours=2))
```

This registers the class for the test, encodes and decodes the instance, and rejects a changed
class or payload. If the class defines `__eq__`, equality must survive too. Pass an instance,
not a class; the encoder needs state to serialize. To register a class without asserting a
round trip, use `airflow_components.timetable(WorkdayTimetable)`.

Define registered timetable classes at module scope. Airflow resolves them by qualified name,
so a class defined inside a test function cannot be reconstructed.

## Static shape checks

Run `check_component(MyTimetable).raise_for_problems()` as an earlier preflight. The canonical
list of timetable checks and their limits lives under
[Checking components](custom-components.md#timetables); this page does not repeat it. A clean
report proves shape, not scheduling behavior, state preservation, or production discovery.

## dag_maker registers for you

For a Dag authored in the test, pass a custom timetable instance through `schedule=`.
`dag_maker` registers it before persistence and exposes the reconstructed scheduler timetable:

```python
def test_dag_uses_my_timetable(dag_maker):
    with dag_maker(schedule=WorkdayTimetable(hours=2)):
        EmptyOperator(task_id="scheduled")

    assert type(dag_maker.timetable) is WorkdayTimetable
    assert dag_maker.timetable.hours == 2
```

The same automatic path handles a custom timetable nested inside `AssetOrTimeSchedule`.
Built-in timetables under `airflow.timetables` need no registration, and a class already
registered by the test environment is left alone. Passing a custom timetable class instead of
an instance raises an immediate `TypeError` naming the fix.

Automatic registration is limited to `dag_maker` on Airflow 3. `dag_bag` and `run_dag` do not
register timetables from repository files. Register the class explicitly before `run_dag`
persists it, or load the real plugin or package for a production-shaped discovery test.

## Production discovery

The `dag_maker` and `airflow_components` paths use a reversible test-only plugin. They prove
that Airflow can serialize the class once registered, not that your deployment will discover
it.

Ship the timetable through an `AirflowPlugin`, then test that packaging boundary with the
session `airflow_plugins_folder` setting or an
[`airflow_isolated`](custom-components-wiring.md#isolated-entry-point-discovery) test. The
[Registration and packaging](custom-components-wiring.md) page explains when to use each
channel.

## Caveats

- Direct timetable logic and `check_component` work on Airflow 2 and 3.
- `airflow_components` and `dag_maker` automatic registration require Airflow 3. On Airflow 2,
  custom-timetable serialization remains unsupported by these fixtures.
- Registering the same class twice is harmless; Airflow's lookup is keyed by qualified name.
