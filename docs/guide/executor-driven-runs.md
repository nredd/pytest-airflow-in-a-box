# Executor-driven runs

This is rung 4 on [the fidelity ladder](ladder.md), one level up from
[a whole `DagRun`](dagrun-execution.md): pass `executor=` to run your task bodies through a
real Airflow executor instead of the pytest process itself -- workloads queued, heartbeats
pumped, task bodies executing in supervised worker subprocesses that report back to a live
Task Execution API:

```python
def test_orders_dag_through_our_executor(dag_bag, run_dag):
    dag = dag_bag.dags["orders"]

    result = run_dag(dag, executor="my_company.executors.MyExecutor")

    assert result.success
```

`executor=` takes an alias registered through
[`airflow_components.executor`](custom-components-wiring.md#runtime-component-registration), a dotted
import path, a `BaseExecutor` subclass, or an instance you built yourself. The api-server the
workers report to starts lazily on the first executor-driven call, exactly as `api_client`
starts it, and `--apps core,execution` means it serves `/execution` as well as `/api/v2`.

This is the piece `dag.test(use_executor=True)` cannot offer -- see
[why not `dag.test()`](../why/index.md#dagtest). This plugin already ships a live
api-server, so pointing an executor at it is the whole trick.

The result is the same `DagRunResult`, with the same ordering, mapped-task expansion, and
final-state semantics -- both paths share one driver. Three things differ, all of them inherent
to tasks running in another process:

- **The Dag must be a file inside your Dag folder.** Each task is re-imported from that file
  in a worker subprocess, so a `dag_maker` Dag -- defined in a test body -- can never qualify
  and is refused by name, before any metadata is written. Use `dag_bag`.
- **`result.errors` is best-effort.** A task's exception is raised inside the worker, so only
  what the executor itself attaches to a failure reaches your test. `result.states` stays
  authoritative, and the traceback is in the worker's log under the run's logs folder --
  [`--airflow-home-retention=all`](../internals/test-environments.md#the-isolated-airflow_home) keeps it around.
- **Instances are dispatched one at a time**, in dependency order, so an executor's own
  concurrency is not exercised. This is what keeps `result.order` meaningful.

`run_triggerer=` cannot be combined with `executor=`: resuming a deferred task is a
triggerer's job, and an executor-driven run leaves a deferring instance `deferred`.

An instance that never reaches a final state fails the run naming the stuck task, rather than hanging.
`--airflow-executor-timeout` (or the `airflow_executor_timeout` ini option) sets that budget
per instance; it defaults to 300 seconds, which is generous for a worker subprocess that has
to start up and parse a Dag file.

On Airflow 2.x `executor=` fails with an actionable error (the Task Execution API is an
AIP-72 / 3.x thing); drop it and the in-process path works there unchanged.

Writing an executor to test is easier than it sounds -- Airflow 3 removed `SequentialExecutor`
from core, so a serial one is about fifteen lines. See
[Custom components](custom-components.md#a-worked-executor) for the whole thing.
