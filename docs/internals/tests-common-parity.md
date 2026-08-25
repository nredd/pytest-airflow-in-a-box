# Upstream tests_common parity

For a suite porting off Airflow's internal `tests_common` harness. This page is the call-site
parity contract: which keywords route where, which upstream calls need a one-line rewrite, and
every place this plugin deliberately differs. Everyday Dag testing does not need it -- start at
[Real DagRuns and real state](../guide/task-execution.md).

## Upstream harness keywords

`dag_maker(...)` forwards unknown keyword arguments to the authoring `DAG` constructor, with
three exceptions mirroring upstream's harness contract: `session`, `bundle_name`, and
`bundle_version` route to the persistence layer instead of the constructor. Those call sites
stay unchanged:

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

- `session=` supplies the metadata session used for every write the context makes
  (persistence, `create_dagrun`, `create_ti`), and `dag_maker.session` returns it. The fixture
  never closes a supplied session; teardown cleanup opens its own. Persistence *commits* on it,
  which narrows what the rollback-isolated `session` fixture guarantees -- see
  [Sessions](../guide/database.md#sessions)
- `bundle_name=` overrides the derived per-Dag bundle row name. The derived name is unique per
  Dag on purpose -- it is the mitigation for cross-worker bundle-row contention under
  `pytest-xdist` -- so supplying your own opts out of that isolation. A shared row is still
  cleaned up once the last Dag referencing it is gone
- `bundle_version=` is recorded on the persisted 3.x metadata rows (`dag`, `dag_version`). Both
  bundle keywords are accepted and ignored on the certified 2.x family, which predates bundles

## Scheduler-side handles

The context always yields the mutable *authoring* Dag; scheduler-side state is exposed through
opt-in handles on the factory instead (the design decision is recorded in
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

- `serialized_dag` returns the persisted scheduler representation after every successful
  context exit. Every Dag serializes as part of persistence, so the `serialized=` keyword and
  the `need_serialized_dag` marker are accepted for upstream compatibility but no longer change
  behavior
- `dag_model` returns the live `DagModel` ORM row on `dag_maker.session` (typed as the
  structural `DagModelRow` protocol). Reads observe committed scheduler metadata --
  `is_paused`, the `next_dagrun*` columns -- and mutations are visible to Airflow
- `sync_dagbag_to_db()` mirrors upstream's mutate-then-resync shape: re-persists the current
  authoring Dag, refreshes `serialized_dag`, and returns it. It commits the metadata session
  (on a borrowed `session=` that includes anything the caller had staged, exactly like
  persistence at context exit). On 3.x a resync may record a new DagVersion; DagRuns created
  before the resync keep their original version. Works on the certified 2.x family too, through
  that family's writers
- `timetable` returns the persisted scheduler Dag's timetable (typed as the structural
  `SchedulerTimetable` protocol) -- the exact object `create_dagrun` infers `data_interval`
  through. On Airflow 3.2+ the *authoring* Dag's timetable lost the scheduling methods, so
  `dag.timetable.infer_manual_data_interval(...)` raises `AttributeError` on the yielded Dag;
  this handle carries the method on every certified release

### Migrating scheduler-side Dag calls

Upstream's `dag_maker` yields a proxy over the serialized Dag, so upstream tests call
scheduler-side methods directly on the yield. This plugin's context always yields the authoring
Dag (ADR 0002), so those call sites move to a factory handle -- each is a one-line rewrite:

| Upstream pattern on the yield | Migration target |
| --- | --- |
| `dag.timetable.infer_manual_data_interval(...)` | `dag_maker.timetable.infer_manual_data_interval(...)` |
| `dag.create_dagrun(...)` | `dag_maker.create_dagrun(...)` |
| `dag.clear(...)` | `dag_maker.serialized_dag.clear(..., session=dag_maker.session)` |
| `dag.partial_subset(...)` | `dag_maker.serialized_dag.partial_subset(...)` |
| `dag.set_task_instance_state(...)` | `dag_maker.serialized_dag.set_task_instance_state(..., session=dag_maker.session)` |

`serialized_dag` is the installed release's real scheduler Dag object, so everything on that
class is reachable -- the rows above are the patterns upstream suites actually hit. Signatures
beyond `infer_manual_data_interval` follow the installed Airflow release (e.g.
`partial_subset(exclude_original=...)` exists only where upstream added it); private attributes
such as `_time_restriction` are deliberately not part of any plugin contract.
`create_dagrun_after` has no equivalent -- deliberately deferred in
[#261](https://github.com/nredd/pytest-airflow-in-a-box/issues/261) until consumer demand
justifies its per-version run-info machinery (see ADR 0002's amendment).

## Upstream one-call factories

`create_task_instance` and `create_dummy_dag` mirror upstream Airflow's
`tests_common.pytest_plugin` fixtures of the same names -- same parameters and defaults -- so
upstream-style tests call them the same way, and they double as the shortest path to "give me a
task instance" when the Dag's content does not matter:

```python
def test_one_call(create_task_instance):
    ti = create_task_instance(dag_id="one_call", state="queued", pool="default_pool")

    assert ti.task_id == "op1"
    assert ti.pool == "default_pool"
```

Both are composition over `dag_maker`: the Dag, DagRun, and task-instance rows are owned and
cleaned up exactly as `dag_maker`'s are, and `**dag_kwargs` (including `serialized=`) route to
`dag_maker` unchanged. `testing_dag_bundle` registers the shared `testing` Dag bundle row
upstream core tests bulk-write metadata against (Airflow 3.x only).

## Deliberate deviations

All nine are rooted in this plugin's own persistence machinery rather than upstream's:

- `create_task_instance` returns the plain ORM `TaskInstance` with `ti.task` carrying the
  *authoring* operator -- there is no `ti.run()` wrapper; execute through `dag_maker.run_ti` or
  `run_task` instead
- `testing_dag_bundle` never deletes the shared row at teardown: a conditional delete would
  race another `pytest-xdist` worker's in-flight `DagModel.bundle_name` reference, and the
  per-run metadata database is disposable anyway
- An explicit `start_date=None` to `dag_maker(...)` opts out of the default-`start_date`
  injection entirely; upstream silently replaces it with `DEFAULT_DATE`
- A non-manual run whose timetable schedules nothing (`schedule=None`) degrades its default
  `logical_date` to the Dag's `start_date` and then the current UTC date; upstream crashes on
  the `None` run info there
- Likewise a non-manual run on a trigger-style or custom timetable degrades whitelist-refused
  automated `data_interval` inference to the manual shape every timetable implements
- On a serial run, reusing one `dag_id` across factory calls -- in the same test, or after a
  previous test in the same process leaked its cleanup -- replaces the earlier metadata,
  matching upstream's silent re-sync. `ValueError` remains for a `dag_id` whose metadata this
  process never persisted (foreign rows, another worker's live registration) and for any
  collision on a `pytest-xdist` worker, where a leftover is indistinguishable from another
  worker's in-flight row
- `run_after` on the Airflow 2.x family raises `ValueError` instead of upstream's silent drop,
  matching `dag_maker.create_dagrun`
- `create_task_instance(execution_date=...)` -- the Airflow 2 spelling 2.x-era upstream suites
  use -- is accepted on both families and mapped onto `logical_date` with a
  `DeprecationWarning`, exactly as upstream preserves it; passing both spellings raises
  `ValueError`. `dag_maker.create_dagrun` deliberately does *not* grow the alias:
  `dag_run_kwargs={"execution_date": ...}` keeps its loud rejection, whose message names the
  `logical_date` remedy
- `dag_maker`-routed keywords upstream supports (`session=`, `bundle_name=`, `bundle_version=`)
  follow whatever `dag_maker(...)` itself accepts

Upstream's `dag_id="dag"` default is kept verbatim, so two concurrent tests relying on it
contend on the shared metadata database exactly like any repeated `dag_id` -- see
[the xdist caveat](../guide/task-execution.md#testing-a-dag-defined-elsewhere), or pass explicit
identifiers.
