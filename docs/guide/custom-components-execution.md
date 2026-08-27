# Execution components

Static [`check_component`](custom-components.md) checks for the three pluggable kinds that
run your workloads: executors, `XCom` backends, and weight strategies.

## Executor checks

- `executor-missing-override` -- `sync`, `_process_workloads`, or `end` is not
  overridden. None is abstract: `sync`'s default silently does nothing, and the other
  two's defaults raise `NotImplementedError`. `terminate` shares that raising default but
  is not checked -- no certified 3.x scheduler path ever calls it, SIGTERM included
- `executor-stale-attribute` -- the executor sets `is_single_threaded`,
  `supports_pickling`, `change_sensitivity`, or `execute_async`. All four are still
  documented in older material but do not exist on `BaseExecutor` in Airflow 3.1-3.3, so
  Airflow silently ignores them
- `executor-flag-wrong-type` -- the sentry integration flag uses the wrong name or type
  for the installed release. 3.1 has `supports_sentry: bool`; 3.2 renamed it to
  `sentry_integration: str`, unchanged through 3.3

A clean report says the shape is sound. To find out whether the executor actually *runs*
anything, drive a real `DagRun` through it with
[`run_dag(dag, executor=...)`](ladder.md#executor-driven-runs).

## A worked executor

Airflow 3 removed `SequentialExecutor` from core, so "run my tasks one at a time" is a
real reason to write an executor today. The whole thing:

```python
from airflow.executors.base_executor import BaseExecutor


class SerialExecutor(BaseExecutor):
    """Run one workload at a time, to completion, in the calling process."""

    is_local = True

    def sync(self) -> None:
        """Nothing async to reconcile: `_process_workloads` already settled it."""

    def _process_workloads(self, workload_items) -> None:
        for workload in workload_items:
            key = workload.ti.key
            self.queued_tasks.pop(key, None)
            try:
                BaseExecutor.run_workload(workload)
            except Exception as error:
                self.fail(key, error)
            else:
                self.success(key)

    def end(self) -> None:
        """No workload is ever left in flight to wait for."""

    def terminate(self) -> None:
        """No workload is ever left in flight to kill."""
```

Two things that are easy to get wrong, and that `check_component` will tell you about:

- **Do not set `is_single_threaded`.** It reads like the right knob for a serial executor
  and `BaseExecutor` no longer looks at it, which is exactly what `executor-stale-attribute`
  is for
- **Key on `workload.ti.key`, not `workload.key`.** `ExecuteTask` grew a `key` property
  after 3.1, and the task-instance key underneath it is both portable and what
  `BaseExecutor.queue_workload` keys `queued_tasks` on

`BaseExecutor.run_workload` is Airflow 3.3 and newer. On 3.1 and 3.2, call
`airflow.sdk.execution_time.supervisor.supervise(ti=workload.ti,
dag_rel_path=workload.dag_rel_path, bundle_info=workload.bundle_info,
token=workload.token, server=..., log_path=workload.log_path)` instead, resolving
`server` from `[core] execution_api_server_url`.

## `XCom` backend checks

- `xcom-orm-deserialize-removed` -- the backend defines `orm_deserialize_value`, which
  does not exist anywhere on `BaseXCom` in Airflow 3. Nothing calls it; it ships silently
  inert
- `xcom-backend-signature` -- the class is not a real `BaseXCom` subclass (only reachable
  by forcing `kind=ComponentKind.XCOM`), or an overridden `serialize_value` /
  `deserialize_value` cannot accept the real base's call shape. `set()` calls
  `cls.serialize_value(value=..., key=..., task_id=..., dag_id=..., run_id=...,
  map_index=...)` -- every argument by keyword -- and `get_one()` / `get_all()` call
  `cls.deserialize_value(result)` positionally; both are `@staticmethod` on the real
  base, so dropping that decorator silently shifts every argument by one position

## Weight strategy checks

- `weight-strategy-abstract` -- the class still has unimplemented abstract methods (for
  example `get_weight`). `PriorityWeightStrategy` is a real `abc.ABC`, so Python refuses
  to instantiate an incomplete subclass, but only when something actually tries to --
  which for a `weight_rule` class reference can be long after Dag parsing
- `weight-strategy-hash-of-none` -- the effective `__hash__`, compared by identity against
  the installed `PriorityWeightStrategy.__hash__`, is either `None` or still the
  un-subclassed base's own. `PriorityWeightStrategy` defines `__eq__` without `__hash__`;
  on the certified 3.1.x base that makes every instance unhashable, and on 3.2+ the base
  defines `__hash__` as `return hash(None)`, making every instance hash equal. The same
  Python rule fires just as easily on a subclass that defines `__eq__` without `__hash__`.
  Either way, a `set` or `dict` keyed on strategy instances cannot dedupe correctly
