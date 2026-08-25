# Deciding which failures are yours

Test the Airflow code *you* wrote. That means your Dags, and it also means the custom
operators, hooks, sensors, decorators, and connection types those Dags lean on -- writing your
own components is squarely in scope here.

What is not in scope is Airflow's own machinery: the mechanisms a stock install already
provides and Airflow's own suite already covers.

The fastest way to tell whether a test earns its runtime: if it fails, is the bug yours?

## In scope

- Your callables -- the Python inside a `@task` or an operator's `python_callable`. Fastest
  through the DB-free [`run_task`](db-free-execution.md) fixture
- Your custom components -- a `BaseOperator` subclass you wrote, a custom hook, a sensor's
  `poke`, a `@task`-style decorator, a custom connection type. `execute()` and `poke()` are
  your code; check the component's shape with [`check_component`](custom-components.md), drive
  them through `run_task` for the DB-free path, hand-drive `execute()` against a real Task SDK
  context via [`task_context`](db-free-execution.md), or use `dag_maker` + `run_ti` when the
  component needs persisted state
- Wiring and task relations -- trigger rules, branching, mapped fan-out, and the data flowing
  between your tasks. See
  [Task relations](../why/dagbag-callable-gap.md#task-relations-trigger-rules-branching-and-cross-task-xcom)
- Cross-Dag relations -- your producer's outlet actually triggering your consumer. See
  [Cross-Dag relations](../why/dagbag-callable-gap.md#cross-dag-relations-asset-triggered-downstream-dags)
- DagRun-to-DagRun relations -- `depends_on_past`, backfill-ish sequences, anything where run
  N-1 conditions run N. See
  [DagRun relations](../why/dagbag-callable-gap.md#dagrun-relations-depends-on-past-and-backfill-ish-sequences)
- Retry and failure behavior you configured -- attempt-dependent logic, `on_failure_callback`,
  the `trigger_rule` that decides whether your alert task fires
- Rendered templates -- that *your* Jinja produces the string you expect, via
  [`render_task`](db-free-execution.md#rendering-template-fields-without-running)
- Hook and connection wiring -- that your operator reaches the connection you seeded, via
  [`airflow_connections`](seeding.md)
- Corpus-level habits -- top-level Variable access, import-time I/O, unbounded `expand`, and
  the rest of the [smoke checks](smoke-tests.md)

## Out of scope

Airflow mechanisms. A toy two-task Dag asserting that xcom transports a value, that the
scheduler honors a timetable, that a *stock* operator survives serialization, or that
`BaseHook.get_connection` reads the metadata DB -- these cost runtime on every CI run and
assert nothing about your code. When one passes it tells you nothing; when one fails you have
found an Airflow bug, and the fix is an upstream issue, not a change to your Dag.

Note the "stock" qualifier. It is what carves the two always-on
[smoke catalog](smoke-tests.md) items out of this list:

- `test_dag_serialization_roundtrip` -- worth running precisely because *your* operator's
  constructor arguments are the part that can fail to serialize. Airflow's serializer is not
  the subject; your Dag files are
- `test_schedule_sanity` -- computes `next_dagrun_info` for every scheduled Dag in your
  corpus. The subject is *your* `schedule=`, `start_date`, and any timetable you wrote, not
  the timetable machinery

Both run once `--airflow-smoke` is on, over every Dag in your corpus, with no test body to
write. Neither asserts anything about a stock component in isolation. Drop either with the
`airflow_smoke_disable` ini option if your corpus makes it worthless.

Same for the plugin itself. `dag_maker` persisting a Dag, `clear_db` truncating tables, the
REST API fixture booting -- those are covered in `tests/` here, across the full compatibility
matrix.

There is also an upper bound. This plugin is not a provider- or core-development harness:
building a provider package for distribution, or changing Airflow itself, wants Airflow's own
Breeze environment and `tests_common` -- the harness Airflow's core test suite runs on, aimed
at testing Airflow itself and not published as a package for testing Dag-author code. The line
is roughly "a component that lives in your repo alongside your Dags" versus "a component you
are shipping to other people as part of Airflow".

## Same fixture, different subject

The line is not "avoid xcom". It is what the assertion is *about*:

- In scope -- `branch` pushed `"quarantine"`, so `notify` ran with `trigger_rule="all_done"`
  and `load` was skipped. The subject is your branching logic
- Out of scope -- a task pushed `{"a": 1}` and a downstream task pulled `{"a": 1}`. The subject
  is xcom

Both use the same `dag_maker` + `run()` shape. Only the first can fail because you were wrong.

## The one exception: a pre-upgrade regression suite

Re-asserting mechanism behavior is the point when you are about to change the mechanism. Pin a
suite against your current Airflow, capture how it behaves today, then run the same suite on
the target version and diff -- that is a deliberate, temporary artifact with an expiry date,
not a permanent part of your test suite.

The plugin ships the machinery for exactly this workflow:

- [Migration outcome diff](migration/outcome-diff.md) -- record outcomes on one Airflow, compare
  against another
- [Migration diff orchestrator](migration/orchestrator.md) -- `airflow-migration-diff` runs
  both disposable environments and prints the categorized diff in one command
- [Migration-strict mode](migration/strict.md) -- the cheaper structural layer to run first
- The `requires_airflow2` / `requires_airflow3` [markers](../reference/markers.md), which
  auto-skip on the other family so one suite stays green on both sides of the bump

Delete the mechanism assertions once the upgrade lands.

## See also

[What a dagbag test and a callable test miss](../why/dagbag-callable-gap.md) is the
complement to this page: this one is about tests that should not exist, that one is about the
seams a dagbag import test plus a direct `task.function` call never reaches. The
[cookbook](cookbook.md) holds the lookup-shaped recipes.
