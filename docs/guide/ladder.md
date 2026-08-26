# The fidelity ladder

Deciding how to run one unit of your Dag code is a cost question, not a taste question. Each
rung buys a class of assertion the rung below structurally cannot make, and charges for it in
setup, runtime, and blast radius.

The climbing rule, one sentence: **stand on the lowest rung that can still fail for the reason
you care about.** If the test would fail identically one rung down, you are paying for fidelity
you are not asserting on.

| Rung | Fixture | Metadata DB | Page |
| --- | --- | --- | --- |
| 0 | `render_task` | no | [One operator, no database](db-free-execution.md) |
| 1 | `run_task` / `task_context` | no | [One operator, no database](db-free-execution.md) |
| 2 | `dag_maker` + `run_ti` | yes | [Real `DagRun`s and real state](task-execution.md) |
| 3 | `dag_maker.run()` / `run_dag` | yes | [Real `DagRun`s and real state](task-execution.md) |
| 4 | `run_dag(..., executor=...)` | yes | [Real `DagRun`s and real state](task-execution.md) |

## Rung 0 -- `render_task`

Proves: your `template_fields` resolve to the string you expect.

Costs: nothing. No DB, no `execute()`, no Airflow ORM import.

Cannot prove: anything about the operator body -- `render_task` never calls `execute()`.

## Rung 1 -- `run_task` / `task_context`

Proves: your `execute()` runs against a real `RuntimeTaskInstance`, with real template
rendering and a real `airflow.sdk.get_current_context()`. Retry *classification* is reachable
only here, via `try_number=` plus the operator's own `retries`.

Costs: the Task SDK in-process runner. Airflow 3.x only.

Cannot prove: anything involving a second task or real metadata --
[where this rung stops](db-free-execution.md#where-this-rung-stops) itemizes the gaps.

## Rung 2 -- `dag_maker` + `run_ti`

Proves: one task instance against real metadata -- real `DagRun` row, real `XCom` table, real
mapped expansion at a given `map_index`, real deferral through a persisted `Trigger` row.

Costs: a lazy DB migration on first request, plus an authored Dag in the test body.

Cannot prove: ordering, or how a run settles. One instance is one instance.

## Rung 3 -- `dag_maker.run()` / `run_dag`

Proves: `result.order`, `result.states` including
`upstream_failed` propagation, and mid-run mapped expansion. `run_dag` additionally proves your
*real* file in `dags/` does this, under its real `dag_id`.

Costs: with `run_dag` the real `dag_id` becomes a shared metadata key, so two `pytest-xdist`
workers running the same Dag can tear each other's metadata down. See
[the xdist caveat](task-execution.md#testing-a-dag-defined-elsewhere).

Cannot prove: retries. Every instance is attempted exactly once
([whole-`DagRun` execution](task-execution.md#whole-dagrun-execution) has the consequences).

## Rung 4 -- `executor=`

Proves: your task body survives re-import in a subprocess, and your executor round-trips
through a live Task Execution API. `dag.test(use_executor=True)` cannot reach this rung -- see
[why not `dag.test()`](../why/index.md#dagtest).

Costs: the Dag must be a file in your Dag folder, `result.errors` degrades to best-effort, and
each instance carries a timeout ([executor-driven runs](task-execution.md#executor-driven-runs)
has all three in full).

Cannot prove: an executor's own concurrency. Instances are dispatched one at a time to keep
`result.order` meaningful.

## Off the ladder

Two tools are not fidelity increments:

- [`run_trigger`](deferrable-operators.md) -- defer, fire, resume. Spans rungs 1 and 2;
  that page lists what is not modeled
- [The live REST API](rest-api.md) -- a different thing entirely, for code *you* wrote that
  resolves `conf.get("api", "base_url")` or calls `/api/v2`

## Corpus checking is a different axis

The ladder varies fidelity over one unit of code. [Smoke checks](smoke-tests.md),
[per-file collection](smoke-tests.md#one-pytest-item-per-dag-file), and [Dag coverage](smoke-tests.md#dag-coverage) vary *breadth*
over every unit at fixed parse-only fidelity, asserting whole-corpus properties no single-Dag
test can phrase at any rung.
