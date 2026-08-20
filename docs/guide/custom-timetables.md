# Custom timetables

Upstream's timetable documentation says a custom timetable "must be a subclass of
`Timetable`, and be registered as a part of a plugin" -- and offers zero testing
guidance. This page splits the problem the way it actually splits: the timetable's
LOGIC needs nothing from this plugin, while registration -- the part that normally only
breaks inside a running scheduler -- is one line.

## The logic needs no plugin

`next_dagrun_info` and `infer_manual_data_interval` are pure functions of a
`DataInterval` and a `TimeRestriction`. Call them directly -- no registration, no
fixture, no metadata database, not even this plugin:

```python
import pendulum
from airflow.timetables.base import DataInterval, TimeRestriction


def test_next_run_lands_on_the_next_workday():
    timetable = WorkdayTimetable()
    info = timetable.next_dagrun_info(
        last_automated_data_interval=DataInterval(
            start=pendulum.datetime(2026, 1, 2, tz="UTC"),
            end=pendulum.datetime(2026, 1, 3, tz="UTC"),
        ),
        restriction=TimeRestriction(earliest=None, latest=None, catchup=True),
    )
    assert info.run_after == pendulum.datetime(2026, 1, 6, tz="UTC")
```

Test the scheduling behavior this way first. It is fast, deterministic, and covers the
part of a timetable that actually contains decisions.

## What registration is actually for

Serialization. Airflow persists a Dag by encoding its timetable to a qualified class
name and reconstructs it by looking that name up through the plugins manager --
`encode_timetable` refuses an unregistered custom timetable outright on Airflow 3.1,
and `decode_timetable` raises `TimetableNotRegistered` on every release. In production
that lookup works because your deployment ships an `AirflowPlugin` listing the class in
its `timetables` attribute; in a test process nothing has loaded any such plugin.

The [`airflow_components`](custom-components.md#runtime-component-registration)
registry closes that gap without you writing a plugin at all -- it synthesizes a
throwaway `AirflowPlugin` carrying the class, registers it into the live plugins
manager, and reverts everything when the test finishes:

```python
def test_timetable_round_trips(airflow_components):
    airflow_components.serialization_round_trip(WorkdayTimetable(hours=2))
```

That single call registers the class, runs the timetable conformance checks (see
[Timetable checks](custom-components.md#timetable-checks)), and asserts
`decode_timetable(encode_timetable(...))` reconstructs the instance -- catching "not
registered", serialize/deserialize asymmetry, and (when the class defines its own
`__eq__`) equality problems in one shot. To register without the round-trip assertion,
call `airflow_components.timetable(WorkdayTimetable)` instead.

## dag_maker registers for you

Passing a custom timetable instance straight to `dag_maker(schedule=...)` registers it
automatically before serialization -- the round trip through `SerializedDagModel` just
works, without the test touching `airflow_components` or knowing plugins are involved:

```python
@pytest.mark.need_serialized_dag
def test_dag_scheduled_by_my_timetable(dag_maker):
    with dag_maker(schedule=WorkdayTimetable(hours=2)):
        EmptyOperator(task_id="scheduled")
    assert dag_maker.serialized_dag.timetable.hours == 2
```

Built-in timetables (`CronTriggerTimetable`, asset-triggered schedules, and everything
else defined under the `airflow.` namespace) never trigger registration -- they decode
without it, and a test passing one pays no sandbox cost at all.

## Priority weight strategies

The same registration story applies to `PriorityWeightStrategy` subclasses: Dag
serialization refuses an operator's custom `weight_rule` unless the class is
registered, and decoding re-instantiates it with no arguments (state must live on the
class -- upstream's documented contract):

```python
def test_operator_uses_my_weight_strategy(airflow_components, dag_maker):
    airflow_components.priority_weight_strategy(FixedWeightStrategy)
    with dag_maker():
        EmptyOperator(task_id="weighted", weight_rule=FixedWeightStrategy())
```

## Caveats

- Only a module-scope class can register: Airflow matches a custom timetable by
  qualified name, so a class defined inside a test function can never resolve -- the
  `timetable-local-qualname` conformance check refuses it up front with an explanation
  instead of letting `TimetableNotRegistered` surface later
- Registration (like all of `airflow_components`) requires Airflow 3.x; on the 2.x
  family `dag_maker` behaves exactly as before and custom-timetable serialization is
  unsupported
- Registering the same class twice, or both calling `timetable()` and passing an
  instance to `dag_maker(schedule=...)`, is harmless -- the lookup mapping is keyed by
  qualified name and deduplicates
