# Whose fail is it anyway?

Test the Airflow behavior your team owns. A useful failure should identify code your team can
change.

A `DagBag` import test proves the file parses. Calling `task.function(...)` proves the Python
callable works. Those checks still do not verify how Airflow executes the Dag: trigger rules,
branch skips, rendered templates, serialization, mapped expansion, cross-task data, and
relations between runs remain untested.

## The failures worth catching

- A branch skips one path, but the downstream task's `trigger_rule` prevents it from running.
- A `template_fields` entry is missing, so `{{ ds }}` ships literally.
- A constructor argument is not JSON-serializable, so the scheduler cannot load the Dag.
- A producer emits an asset, but the intended consumer is not actually triggered.
- `depends_on_past` blocks the second run because the first never reached the expected state.
- Attempt-dependent logic reads the wrong `try_number`.
- A top-level Variable or connection lookup turns every scheduler parse into network or
  database I/O.

The [Quickstart](../quickstart.md#verify-branching-behavior) gives branching one
canonical example. The [Cookbook](cookbook.md) covers assets, templates, hooks, and retries.

## In scope

- TaskFlow callables, operator `python_callable`s, and custom operators, hooks, sensors, and
  decorators.
- Task wiring: trigger rules, branching, mapped fan-out, and cross-task data.
- Cross-Dag and cross-run behavior: assets, `depends_on_past`, and backfill-like sequences.
- Retry and failure behavior you configured, including callbacks and attempt-dependent logic.
- Rendered templates, connection resolution, and serialization inputs.
- Extension points you own: connection types, timetables, listeners, executors, policies, and
  triggers.
- Corpus-wide rules such as unique `dag_id`s, no import-time I/O, and bounded expansion.

Use the [fidelity ladder](ladder.md) to choose the cheapest runner that exposes the state your
assertion needs. Use [Smoke Tests](smoke-tests.md) for properties of the whole Dag folder.

## Out of scope

Do not retest stock Airflow mechanisms in isolation. Airflow's own suite owns assertions that
XCom transports a value, a stock timetable schedules, a stock operator serializes, or
`BaseHook.get_connection` reads the metastore.

The stock qualifier matters. Asserting that *your* branch selected `quarantine`, causing
`notify` to run under `all_done`, is in scope. Asserting only that a value pushed to XCom can be
pulled back is not. Both tests may use `dag_maker`; the subject is different.

The plugin itself is also out of scope for consumers: this repository already tests its
fixtures, cleanup, and API bootstrap across the compatibility matrix. Provider and Airflow
core development belong in Breeze with upstream `tests_common`.

The exception is a pre-upgrade regression suite. While moving from Airflow 2 to 3, mechanism
behavior is temporarily yours because the subject is the upgrade. Delete those assertions
after cutover; the [migration guide](migration.md) is a bridge, not a second home.

## Why not `dag.test()`?

`DAG.test()` is a useful debugger, not a pytest harness. On Airflow 3 it clears task instances
for the logical date, catches task-body exceptions so the loop can continue, and returns an ORM
`DagRun`. You must inspect that run yourself to make an assertion. It also supplies no isolated
`AIRFLOW_HOME`, disposable database, result snapshot, fixtures, or xdist coordination.

`dag.test(use_executor=True)` queues real workloads, but the test process does not serve the
Task Execution API those workers report to. The plugin's
[`executor=` rung](ladder.md#executor-driven-runs) starts that server and drives the run.

## Why not `DebugExecutor`?

It does not exist on Airflow 3. The equivalent of “run one task in process under a debugger” is
[`run_task`](ladder.md#one-operator-no-database); for a whole Dag, use
[`run_dag`](ladder.md#a-whole-dagrun-real-state).

## Why not a hand-rolled `conftest.py`?

Airflow reads `AIRFLOW_HOME`, its cfg file, and `AIRFLOW__*` at first import. This plugin
bootstraps from `pytest_load_initial_conftests`, before pytest imports consumer conftests. A
consumer `conftest.py` cannot establish that boundary, especially when a test module imports
Airflow at module scope.

Airflow 3 also has no public testing API. This plugin contains moving private interfaces behind
capability probes and verifies the consumer contract across certified releases. A hand-rolled
harness discovers those changes when an upgrade breaks it.

## Scale is a different axis

One-Dag tests cannot detect two templates claiming the same `dag_id`, import-time I/O paid for
every scheduler parse, or an unbounded mapped task. `--airflow-smoke` checks those properties
over the set. See [Smoke Tests](smoke-tests.md); then use the [Cookbook](cookbook.md) for
individual handoffs that need a worked recipe.
