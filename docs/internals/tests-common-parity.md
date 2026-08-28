# Upstream `tests_common` parity

`tests_common` is Apache Airflow's shared internal test-support package. It lives in the
repository's [`devel-common`](https://github.com/apache/airflow/tree/main/devel-common)
workspace, whose
[`apache-airflow-devel-common` package](https://github.com/apache/airflow/blob/main/devel-common/pyproject.toml)
is explicitly private and not published to PyPI. Its live
[`pytest_plugin.py`](https://github.com/apache/airflow/blob/main/devel-common/src/tests_common/pytest_plugin.py)
provides the fixtures and lifecycle machinery used by Airflow's own pytest suites, including
tests run through [Breeze](https://github.com/apache/airflow/tree/main/dev/breeze), Airflow's
Docker Compose development and CI environment.

This plugin descended from `devel-common`'s `tests_common` plugin circa 2025, during an Airflow
2-to-3 migration and the release-candidate phase of Airflow 3. Airflow's own Breeze-driven tests
proved the approach;
`pytest-airflow-in-a-box` turns that internal pattern into a supported, compatibility-tested
plugin available to every Airflow project. This page maps calls that work unchanged, calls that
need a one-line rewrite, and deliberate differences. For ordinary Dag tests, start with
[a whole `DagRun`, real state](../guide/ladder.md#a-whole-dagrun-real-state).

## The `tests_common` stand-in experiment

[Issue #227](https://github.com/nredd/pytest-airflow-in-a-box/issues/227) asked a deliberately
adversarial question: how much of Airflow's own core test suite would survive if
`pytest-airflow-in-a-box` replaced its private `tests_common` pytest plugin? Selected model,
serialization, and timetable tests ran two ways. The **hybrid** kept upstream's fixtures but
disabled its database initialization, leaving this plugin to own `AIRFLOW_HOME` and the metadata
database. The **stand-in** replaced upstream's plugin registration with a marker-only shim, so
every missing fixture and behavioral difference became a measurable failure.

Three rounds compared those runs with the stock harness, repeated the stand-in across Airflow
2.9.3 through 3.4.0.dev, implemented the highest-value gaps, and reran the matrix to measure the
change. The
[permanent record](https://github.com/nredd/pytest-airflow-in-a-box/discussions/245) contains the
method, result tables, reproduction patches, and raw logs. The durable findings were:

- **The foundation held.** The plugin bootstrapped every tested release without a compatibility
  failure, including the uncertified development release through live probing. In the hybrid
  serial experiment, replacing Airflow's database bootstrap while retaining its fixtures
  preserved the baseline outcomes and reduced wall time by 10–40%.
- **The gaps were concentrated contracts, not broad incompatibility.** Failures clustered around
  a small fixture surface and `dag_maker` semantics. Successive rounds directly produced the
  upstream keyword routing, one-call factories, scheduler handles, run defaults, borrowed-session
  teardown, and Dag-scoped cleanup documented below.
- **Parity has a boundary.** Assertions tied to Airflow's repository paths or plugin directory
  were harness assumptions, not compatibility failures. Upstream-only fixtures such as
  `testing_team` and `mock_supervisor_comms` remain intentionally unsupported until they serve a
  downstream use case; the [scope decision](https://github.com/nredd/pytest-airflow-in-a-box/discussions/283)
  records the alternatives. The parallel experiments also drove the worker-environment drift
  policy and the explicit xdist collision guidance elsewhere in this guide.

## Upstream harness keywords

`dag_maker(...)` forwards unknown keywords to the authoring `DAG` constructor. Three
upstream-compatible keywords instead configure persistence: `session`, `bundle_name`, and
`bundle_version`. Calls using them need no rewrite:

```python
from airflow.models.dag import DagModel


def test_upstream_style(dag_maker, session):
    with dag_maker("upstream_style", session=session):

        @task
        def answer():
            return 42

        answer()

    assert dag_maker.session is session
    assert session.get(DagModel, "upstream_style") is not None
```

- `session=` supplies the session for persistence, `create_dagrun`, and `create_ti`;
  `dag_maker.session` returns it. The fixture never closes a supplied session, and cleanup uses
  a separate one. Persistence commits the supplied session, including other staged changes, so
  it narrows the rollback guarantee described under
  [the disposable database](test-environments.md#the-disposable-metadata-database).
- `bundle_name=` replaces the generated per-Dag bundle name. Generated names isolate workers
  from bundle-row contention; an override gives up that protection. A shared bundle row is
  removed after its final Dag reference.
- `bundle_version=` is written to the 3.x `dag` and `dag_version` metadata. Both bundle
  keywords are accepted but ignored on the certified 2.x family, which has no Dag bundles.

## Scheduler-side handles

The context always yields the mutable *authoring* Dag. Scheduler-side state lives on explicit
factory handles instead (the design decision is recorded in
[ADR 0002](../adr/0002-authoring-yield-with-scheduler-handles.md)):

```python
def test_scheduler_state(dag_maker):
    with dag_maker("scheduler_state") as dag:
        EmptyOperator(task_id="original")

    assert dag_maker.serialized_dag.task_ids == ["original"]
    assert dag_maker.dag_model.is_paused is False

    EmptyOperator(task_id="added", dag=dag)
    reloaded = dag_maker.sync_dagbag_to_db()

    assert sorted(reloaded.task_ids) == ["added", "original"]
```

- `serialized_dag` is the persisted scheduler representation after each successful context
  exit. Persistence always serializes, so the upstream `serialized=` keyword and
  `need_serialized_dag` marker are accepted but do not change behavior.
- `dag_model` is the live `DagModel` row on `dag_maker.session`, typed as the structural
  `DagModelRow` protocol. It exposes committed scheduler metadata such as `is_paused` and
  `next_dagrun*`; mutations are visible to Airflow.
- `sync_dagbag_to_db()` re-persists the current authoring Dag, refreshes `serialized_dag`, and
  returns it. It commits the session, including other staged changes in a supplied `session=`.
  On 3.x, a resync may create a `DagVersion`; existing `DagRun`s retain their original version.
  The method also works on the certified 2.x family.
- `timetable` is the persisted scheduler Dag's timetable, typed as `SchedulerTimetable`. It is
  the object `create_dagrun` uses to infer `data_interval`. On Airflow 3.2+, the authoring
  timetable no longer exposes scheduler methods; this handle does on every certified release.

### Migrating scheduler-side Dag calls

Upstream's `dag_maker` yields a serialized-Dag proxy. This plugin yields the authoring Dag, so
move scheduler-side calls to a factory handle:

| Upstream pattern on the yield | Migration target |
| --- | --- |
| `dag.timetable.infer_manual_data_interval(...)` | `dag_maker.timetable.infer_manual_data_interval(...)` |
| `dag.create_dagrun(...)` | `dag_maker.create_dagrun(...)` |
| `dag.clear(...)` | `dag_maker.serialized_dag.clear(..., session=dag_maker.session)` |
| `dag.partial_subset(...)` | `dag_maker.serialized_dag.partial_subset(...)` |
| `dag.set_task_instance_state(...)` | `dag_maker.serialized_dag.set_task_instance_state(..., session=dag_maker.session)` |

`serialized_dag` is the installed release's real scheduler Dag, so its public methods remain
reachable. Their signatures follow that Airflow release; for example,
`partial_subset(exclude_original=...)` exists only where Airflow provides it. Private
attributes such as `_time_restriction` are outside the plugin contract.

`create_dagrun_after` has no equivalent. It is deferred in
[#261](https://github.com/nredd/pytest-airflow-in-a-box/issues/261) until consumer demand
justifies its version-specific run-info handling.

## Upstream one-call factories

`create_task_instance` and `create_dummy_dag` match the parameters and defaults of the same
fixtures in `tests_common.pytest_plugin`. Upstream-style calls therefore work unchanged. They
are also the shortest path to a task instance when Dag content is irrelevant:

```python
def test_one_call(create_task_instance):
    ti = create_task_instance(dag_id="one_call", state="queued", pool="default_pool")

    assert ti.task_id == "op1"
    assert ti.pool == "default_pool"
```

Both compose `dag_maker`, which owns and cleans up their Dag, `DagRun`, and task-instance rows.
They pass `**dag_kwargs`, including `serialized=`, through unchanged. On Airflow 3,
`testing_dag_bundle` registers the shared `testing` bundle row used by upstream bulk metadata
writes.

## Deliberate deviations

These differences come from the plugin's persistence and xdist guarantees:

- `create_task_instance` returns a plain ORM `TaskInstance`; `ti.task` is the authoring
  operator. It adds no `ti.run()` wrapper. Execute with `dag_maker.run_ti` or `run_task`.
- `testing_dag_bundle` never deletes the shared row at teardown: a conditional delete would
  race another `pytest-xdist` worker's in-flight `DagModel.bundle_name` reference, and the
  per-run metadata database is disposable anyway.
- `dag_maker(start_date=None)` disables the default start date. Upstream replaces `None` with
  `DEFAULT_DATE`.
- For a non-manual run with `schedule=None`, the default `logical_date` falls back to the Dag's
  `start_date`, then the current UTC date. Upstream crashes on the missing run information.
- When Airflow's automated interval inference rejects a trigger-style or custom timetable, the
  plugin uses the manual interval shape that every timetable implements.
- On a serial run, reusing one `dag_id` across factory calls—in the same test, or after a
  previous test in the same process leaked its cleanup—replaces the earlier metadata,
  matching upstream's silent re-sync. `ValueError` remains for a `dag_id` whose metadata this
  process never persisted (foreign rows, another worker's live registration) and for any
  collision on a `pytest-xdist` worker, where a leftover is indistinguishable from another
  worker's in-flight row.
- On Airflow 2, `run_after` raises `ValueError` instead of being silently discarded, matching
  `dag_maker.create_dagrun`.
- `create_task_instance(execution_date=...)`, the spelling used by Airflow 2 suites, works on
  both families. It maps to `logical_date` with a `DeprecationWarning`; passing both spellings
  raises `ValueError`. `dag_maker.create_dagrun` does not accept the alias:
  `dag_run_kwargs={"execution_date": ...}` is rejected with a message naming `logical_date`.
- Keywords routed to `dag_maker`—`session=`, `bundle_name=`, and `bundle_version=`—follow
  `dag_maker(...)`'s accepted values.

The upstream `dag_id="dag"` default is unchanged. Concurrent tests that rely on it contend on
the shared metadata database like any repeated `dag_id`; pass explicit identifiers or follow
[the xdist guidance](../guide/ladder.md#testing-a-dag-defined-elsewhere).
