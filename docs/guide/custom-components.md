# `check_component`

You wrote a `Timetable`, a listener, or a `BaseExecutor` subclass, the class definition
imported fine, and the suite is green. That proves nothing: `BaseExecutor` is not an ABC,
`Timetable` is a `typing.Protocol`, and a listener or a policy hookimpl carries no base
class at all. Nothing about any of them is enforced when the class is defined. A shape
bug ships silently and first fails in a live scheduler, worker, or Dag processor.

`check_component` runs static conformance checks against that shape -- no metadata
database, no cache, no Airflow bootstrap, so it is safe in a plain unit test or a
pre-commit hook:

```python
from pytest_airflow_in_a_box.components import check_component


def test_my_timetable_conforms():
    check_component(MyTimetable).raise_for_problems()
```

Scope, because "custom component" is an overloaded phrase: this page covers the ten
*pluggable extension kinds* Airflow discovers by registration -- timetable, listener,
executor, XCom backend, weight strategy, notifier, secrets backend, policy, plugin,
provider. Your own `BaseOperator` subclass, hook, sensor, or `@task` decorator is not one
of them and has no shape to check; test those by running them, through
[`run_task`](db-free-execution.md) or [`dag_maker`](task-execution.md).

## The report

`check_component` accepts a bare class or an already-built instance interchangeably and
never constructs one itself, so it is safe on a component whose constructor is not
side-effect-free or takes required arguments. It returns a `ComponentReport`:

```python
report = check_component(MyExecutor)
report.ok  # bool: no problems found
report.problems  # tuple[ComponentProblem, ...] -- (code, message, hint) each
report.summary()  # human-readable, one line per problem
report.certification  # CertificationTier | None -- PROBED on an uncertified release
report.raise_for_problems()  # raises ComponentContractError when not ok
```

Checks are additive: each reports what it finds and never raises on the component itself,
so a wrong or overly strict check cannot fail an otherwise-passing suite. Only
`raise_for_problems()` (or asserting `.ok` yourself) turns a report into a test failure.

## Kind detection

Pass `kind=ComponentKind.TIMETABLE` / `.LISTENER` / `.EXECUTOR` / `.XCOM` /
`.WEIGHT_STRATEGY` / `.NOTIFIER` / `.SECRETS_BACKEND` / `.POLICY` / `.PLUGIN` /
`.PROVIDER` to force a check set, or omit it and let `check_component` classify the
component itself:

- **Timetable** -- nominal `Timetable` inheritance. A purely duck-typed timetable that
  never inherits `Timetable` needs the explicit `kind=`; structural `isinstance` checks
  are not used because `Timetable` declares data attributes as well as methods, which
  makes `issubclass` against it always raise `TypeError`
- **Listener** -- at least one `@hookimpl`-decorated method
- **Executor** -- `BaseExecutor` subclassing
- **XCom backend** -- `airflow.sdk.bases.xcom.BaseXCom` subclassing
- **Weight strategy** -- `airflow.task.priority_strategy.PriorityWeightStrategy`
  subclassing
- **Notifier** -- `airflow.sdk.bases.notifier.BaseNotifier` subclassing
- **Secrets backend** -- `airflow.secrets.base_secrets.BaseSecretsBackend` subclassing
- **Policy** -- at least one `airflow.policies.hookimpl`-decorated method
- **Plugin** -- the same MRO name-and-module duck typing Airflow's own `is_valid_plugin`
  uses (`base.__name__ == "AirflowPlugin" and "plugins_manager" in base.__module__`)
  rather than `issubclass`, since core and the Task SDK reach the plugin base through
  different symlinked paths, and Python treats the two as distinct classes
- **Provider** -- a non-class callable named exactly `get_provider_info`

A component matching none of these, with no `kind` given, returns a clean, empty report
rather than raising. Every kind except timetable requires Airflow 3.x and reports no
problems on 2.x.

## Timetable checks

- `timetable-local-qualname` -- a timetable defined inside a function or method carries
  `<locals>` in `__qualname__`. Airflow's `find_registered_custom_timetable` matches a
  custom timetable by qualified name, so a `<locals>` class can never match; every DagRun
  using it raises `TimetableNotRegistered` permanently, not just in a test
- `timetable-missing-protocol-method` -- `infer_manual_data_interval` or
  `next_dagrun_info` is not overridden. Both default to `raise NotImplementedError()`;
  every other Protocol member (the data attributes, `serialize`/`deserialize`,
  `validate`, the partition hooks) has a usable default
- `timetable-serialize-pair-incomplete` -- exactly one of `serialize`/`deserialize` is
  overridden. The default `deserialize` reconstructs the class with `cls()`, silently
  dropping whatever state a custom `serialize` emits
- `timetable-serialize-not-json` -- an instance's `serialize()` does not return a
  JSON-serializable mapping. Only checked against an already-built instance; a bare class
  skips this one check, since calling `serialize()` needs a real instance and
  `check_component` never constructs one

The scheduling logic itself needs none of this -- see [custom
timetables](custom-timetables.md).

## Listener checks

- `listener-no-matching-hookspec` -- a hookimpl method's name matches no hookspec
  registered by either listener manager. pluggy silently ignores it; the method never
  fires, with no warning. The single most common real-world listener bug
- `listener-unknown-argument` -- a hookimpl method declares an argument name its matching
  hookspec does not have. pluggy hard-errors on this at registration time
- `listener-core-manager-only` / `listener-sdk-manager-only` -- a hookimpl matches a
  hookspec registered by only one manager. `airflow.listeners.listener` registers
  lifecycle, taskinstance, dagrun, asset, and import-error hookspecs;
  `airflow.sdk.listener` registers only lifecycle and taskinstance. Register a listener
  with only one manager and half its hooks are silently unreachable

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
anything, drive a real DagRun through it with
[`run_dag(dag, executor=...)`](task-execution.md#executor-driven-runs).

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

## XCom backend checks

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

## Notifier checks

- `notifier-missing-notify` -- `notify` is not overridden. Its default raises
  `NotImplementedError()` unconditionally; `on_success_callback` / `on_failure_callback`
  run on the Dag processor, sync-only, and call `notify` directly, so implementing only
  `async_notify` does not help there. Covers apache/airflow#64649, where a minimal
  `BaseNotifier` used as a callback crashed under `airflow dags test`
- `notifier-template-fields-unresolvable` -- an instance's `template_fields` names an
  attribute the instance does not carry. `_update_context` does a plain `getattr(self, f)`
  for every entry, raising `AttributeError` the first time the notifier actually fires.
  Instance-only, for the same reason as `timetable-serialize-not-json`

## Secrets backend checks

- `secrets-backend-raises-on-miss` -- an overridden `get_conn_value`, `get_connection`,
  `get_variable`, or `get_config` is annotated to return something that does not admit
  `None` (recognizing `X | None`, `Optional[X]`, and `Union[X, None]` alike). All four
  must return `None` on a miss, not raise -- raising the backing client's own not-found
  error instead is the common bug. `check_component` never calls a secrets backend for
  real, since a genuine miss needs credentials it cannot fabricate safely, so this reads
  the override's declared return annotation; an unannotated override is not flagged

## Policy checks

- `policy-unknown-hookspec` -- a policy hookimpl method's name matches no hookspec in
  `airflow.policies`. pluggy silently ignores it; the method never fires
- `policy-argument-name-mismatch` -- a policy hookimpl method declares an argument its
  matching hookspec does not have. pluggy hard-errors on this at registration time.
  `task_instance_mutation_hook` gained a `dag_run` parameter in Airflow 3.3, so a hook
  written for a newer release breaks registration entirely on an older one; this reads
  the live installed hookspec, not a hardcoded table

Both checks model a policy registered as an `@hookimpl`-decorated class through the
`airflow.policy` entry point -- the shape `ComponentKind.POLICY`'s classifier requires. A
plain module-level function in `airflow_local_settings.py`, the older and still-common
way, is a different mechanism: `make_plugin_from_local_settings` loads it through a
generated shim that calls it positionally and deliberately tolerates a name or arity
mismatch. Forcing `kind=ComponentKind.POLICY` on such a function finds nothing either,
since a plain function is never `@hookimpl`-marked. See [keeping your own
`airflow_local_settings.py`](../internals/test-environments.md#cluster-policies-and-airflow_local_settingspy).

## Plugin checks

- `plugin-name-missing` -- the plugin does not set `name`. `AirflowPlugin.validate()`
  raises `AirflowPluginException` for exactly this, but only when Airflow's own
  `is_valid_plugin` calls it during real discovery; this checker never calls `validate()`
  itself, since doing so risks raising out of `check_component`

## Providers, if you are shipping one

Writing a provider package means shipping Airflow integration code to other people, which
is past the boundary [deciding which failures are yours](testing-scope.md) draws -- that
job wants Breeze and upstream `tests_common`. The checks exist because a provider still
starts life as a directory in your own repo:

- `provider-info-schema` -- the callable's return value fails the shipped
  `provider_info.schema.json`, read from the installed `airflow` package so it always
  matches the resolved release, or the callable raises when called
- `provider-package-name-mismatch` -- the returned dict's `package-name` disagrees with
  the owning distribution's canonical name. `ProvidersManager` raises `ValueError` at
  discovery for this, not a warning
- `provider-no-entry-point` -- the owning distribution registers no
  `apache_airflow_provider` entry point at all, so `ProvidersManager` never calls this
  function and the provider is silently undiscovered

Pass the `get_provider_info` callable itself, not its return value:
`check_component(get_provider_info)`. The last two checks need the callable's module
attributed to a real installed distribution, done by matching the distribution's recorded
file list, falling back to the source root(s) named in its `.pth` file for an editable
install. That root is narrower than the project checkout on purpose: a `src/`-layout
package is exposed only under `src/`, so a sibling `tests/` directory is not attributed
to it. A callable that cannot be attributed -- one defined in a test file, say -- is
silently skipped by both.

To prove the entry point itself *resolves*, rather than that it exists, run the test
under [`airflow_isolated`](isolated-tests.md).

## Next

A clean report proves the shape, never that a run can load the component. That is
[wiring components into the run](custom-components-wiring.md).
