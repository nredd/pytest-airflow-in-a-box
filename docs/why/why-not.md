# Why not `dag.test()`, `DebugExecutor`, or your own `conftest.py`

Three things stand between a Dag repo and a real test suite. Two of them are what people
reach for first, and one of them is what you end up maintaining.

Already have a dagbag import test and a pile of `task.function(...)` calls? That pair is
covered separately, in [what a dagbag test and a callable test
miss](dagbag-callable-gap.md).

## `dag.test()`

Airflow's own debug entry point. It runs one Dag end to end in-process, and it is a debug
harness, not a test harness.

What it does, from `DAG.test` on Airflow 3:

- It clears existing task instances for the logical date before running
  (`SerializedDAG.clear_dags(..., dag_run_state=False)`). Anything you set up on that run
  is gone
- It catches every exception a task body raises, logs it, and keeps looping
  (`except Exception: log.exception("Task failed; ti=%s", ti)`). The call itself does not
  fail, so a bare `dag.test()` in a test body asserts nothing. You have to fish the state
  out of the returned `DagRun` yourself
- It returns that ORM `DagRun` and nothing else. No execution order, no per-task drill-down,
  no pulled XComs

It is also not a pytest plugin: no fixtures, no isolated `AIRFLOW_HOME`, no disposable
metadata DB, no xdist story.

`dag.test(use_executor=True)` looks like the exception, and is not. It queues a real
`workloads.ExecuteTask` onto a real executor, but nothing inside the test process serves the
Task Execution API those supervised workers report to, so the workloads have nowhere to land
(apache/airflow#59074). That is the gap `executor=` fills here, by standing the api-server up
itself -- see [real DagRuns and real state](../guide/task-execution.md).

## `DebugExecutor`

It does not exist on Airflow 3. `airflow.executors.debug_executor` is gone; importing it
raises `ModuleNotFoundError`.

Every 2.x-era blog post recommending `AIRFLOW__CORE__EXECUTOR=DebugExecutor` plus a manual
run is describing an interface that is no longer there. The name survives in this plugin in
exactly one place, `--airflow-doctor`'s Airflow 2.x SQLite compatibility check, because 2.x
still gates SQLite on a single-threaded executor.

If you landed here from that search: the Airflow 3 equivalent of "run my task in-process
under a debugger" is [`run_task`](../guide/db-free-execution.md), and the equivalent of
"run my whole Dag" is [`run_dag`](../guide/task-execution.md).

## A hand-rolled `conftest.py`

The real competitor. Everyone writes one, and the reason it does not work is timing, not
features.

Airflow reads `AIRFLOW_HOME`, its cfg file, and `AIRFLOW__*` at first import. Set them any
later and you are configuring an Airflow that already booted. So the question is only which
code runs first:

- This plugin bootstraps from `pytest_load_initial_conftests`, during argument parsing
- pytest's own conftest collector is `@hookimpl(trylast=True)` on the *same* hook
  (`_pytest/config/__init__.py`), so it runs after every other implementation of it

Your `conftest.py` is imported by that collector. It structurally cannot win the race. The
usual workarounds -- a `pytest.ini` `env` block, a wrapper shell script, `-p` a local plugin
module -- each move the problem rather than solve it, and none of them survives someone
importing `airflow` at the top of a test module. This plugin fails that case loudly instead:
`load_initial_state` raises a `pytest.UsageError` when `airflow` is already in `sys.modules`.
See [who owns `AIRFLOW__*`](../internals/bootstrap-env-ownership.md).

The cost of the hand-roll is not "impossible". It is that you now own a compat layer. Airflow
3's testing surface is private: `_compat/` here is 12.5k lines of shims over private modules
across Airflow 3.1-3.3, each gated by a capability probe, and it is the only reason anything
above it survives a minor bump. See [what `_compat/` absorbs](../internals/compat-layer.md).
You find out your hand-roll broke when the upgrade lands.
