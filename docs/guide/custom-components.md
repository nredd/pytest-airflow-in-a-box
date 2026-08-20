# Custom components

`check_component` runs pure, static conformance checks against a custom `Timetable`,
listener, `BaseExecutor`, XCom backend, weight strategy, notifier, secrets backend,
policy, plugin, or provider -- no metadata database, no cache, no Airflow bootstrap.
Safe in a plain unit test or a pre-commit hook:

```python
from pytest_airflow_in_a_box.components import check_component


def test_my_timetable_conforms():
    check_component(MyTimetable).raise_for_problems()
```

`BaseExecutor` is not an ABC and `Timetable` is a `typing.Protocol`, and a listener carries
no base class at all -- nothing about any of the three is enforced when the class is
defined. A shape bug ships silently and only fails once a scheduler actually exercises it.

## The report

`check_component` accepts a bare class or an already-built instance interchangeably and
never constructs one itself, so it is safe to call on a component whose constructor is not
side-effect-free or takes required arguments. It returns a `ComponentReport`:

```python
report = check_component(MyExecutor)
report.ok  # bool: no problems found
report.problems  # tuple[ComponentProblem, ...] -- (code, message, hint) each
report.summary()  # human-readable, one line per problem
report.raise_for_problems()  # raises ComponentContractError when not ok
```

Checks are additive: each reports the problems it finds and never raises on the component
itself, so a wrong or overly strict check cannot fail an otherwise-passing suite -- only
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
- **Plugin** -- the same MRO name-and-module duck typing Airflow's own
  `is_valid_plugin` uses (`base.__name__ == "AirflowPlugin" and "plugins_manager" in
  base.__module__`) rather than `issubclass`, since the shared plugin base is reachable
  via different symlinked paths from core and the Task SDK, which Python treats as
  distinct classes
- **Provider** -- a non-class callable named exactly `get_provider_info`, the
  conventional name every real Airflow provider uses. Pass the callable itself, not its
  return value: `check_component(get_provider_info)`

A component matching none of these (with no `kind` given) returns a clean, empty report
rather than raising. The XCom backend, weight strategy, notifier, secrets backend,
policy, plugin, and provider checks all require Airflow 3.x and report no problems on
2.x.

## Timetable checks

- `timetable-local-qualname` -- a timetable defined inside a function or method carries
  `<locals>` in `__qualname__`. Airflow's `find_registered_custom_timetable` matches a
  custom timetable by qualified name, so a `<locals>` class can never match; every DagRun
  using it raises `TimetableNotRegistered` permanently, not just in a test
- `timetable-missing-protocol-method` -- `infer_manual_data_interval` or `next_dagrun_info`
  is not overridden. Both default to `raise NotImplementedError()`; every other Protocol
  member (the data attributes, `serialize`/`deserialize`, `validate`, the partition hooks)
  has a usable default
- `timetable-serialize-pair-incomplete` -- exactly one of `serialize`/`deserialize` is
  overridden. The default `deserialize` reconstructs the class with `cls()`, silently
  dropping whatever state a custom `serialize` emits
- `timetable-serialize-not-json` -- an instance's `serialize()` does not return a
  JSON-serializable mapping. Only checked against an already-built instance; a bare class
  skips this one check, since calling `serialize()` needs a real instance and
  `check_component` never constructs one

## Listener checks

- `listener-no-matching-hookspec` -- a hookimpl method's name matches no hookspec
  registered by either listener manager. pluggy silently ignores it; the method never
  fires, with no warning -- the single most common real-world listener bug
- `listener-unknown-argument` -- a hookimpl method declares an argument name its matching
  hookspec does not have. pluggy hard-errors on this at registration time
- `listener-core-manager-only` / `listener-sdk-manager-only` -- a hookimpl matches a
  hookspec registered by only one manager. `airflow.listeners.listener` registers
  lifecycle, taskinstance, dagrun, asset, and import-error hookspecs;
  `airflow.sdk.listener` registers only lifecycle and taskinstance. Register a listener
  with only one manager and half its hooks are silently unreachable

Listener checks require Airflow 3.x and report no problems on 2.x, whose listener
architecture predates the Task SDK's separate manager entirely.

## Executor checks

- `executor-missing-override` -- `sync` or `_process_workloads` is not overridden. Neither
  is abstract: `sync`'s default silently does nothing, and `_process_workloads`'s default
  raises `NotImplementedError`
- `executor-stale-attribute` -- the executor sets `is_single_threaded`, `supports_pickling`,
  `change_sensitivity`, or `execute_async`. All four are still documented in older material
  but do not exist on `BaseExecutor` in Airflow 3.1-3.3, so Airflow silently ignores them
- `executor-flag-wrong-type` -- the sentry integration flag uses the wrong name or type for
  the installed release. 3.1 has `supports_sentry: bool`; 3.2 renamed it to
  `sentry_integration: str`, unchanged through 3.3

Executor checks also require Airflow 3.x and report no problems on 2.x.

## XCom backend checks

- `xcom-orm-deserialize-removed` -- the backend defines `orm_deserialize_value`, which
  does not exist anywhere on `BaseXCom` in Airflow 3. Nothing calls it; it ships silently
  inert
- `xcom-backend-signature` -- the class is not a real `BaseXCom` subclass (only reachable
  by forcing `kind=ComponentKind.XCOM`), or an overridden `serialize_value`/
  `deserialize_value` cannot accept the real base's call shape. `set()` calls
  `cls.serialize_value(value=..., key=..., task_id=..., dag_id=..., run_id=...,
  map_index=...)` -- every argument by keyword -- and `get_one()`/`get_all()` call
  `cls.deserialize_value(result)` positionally; both are `@staticmethod` on the real
  base, so dropping that decorator silently shifts every argument by one position

## Weight strategy checks

- `weight-strategy-abstract` -- the class still has unimplemented abstract methods (for
  example `get_weight`). `PriorityWeightStrategy` is a real `abc.ABC`, so Python refuses
  to instantiate an incomplete subclass, but only when something actually tries to --
  which for a `weight_rule` class reference can be long after Dag parsing
- `weight-strategy-hash-of-none` -- the effective `__hash__` (compared by identity
  against the installed `PriorityWeightStrategy.__hash__`, not by who defines it) is
  either `None` or still the un-subclassed base's own. `PriorityWeightStrategy` defines
  `__eq__` without `__hash__`; on the certified 3.1.x base that makes every instance
  unhashable, and on 3.2+ the base instead defines `__hash__` as `return hash(None)`,
  making every instance hash equal. The same automatic Python rule fires just as easily
  on a user subclass that defines `__eq__` without `__hash__` too -- identity
  comparison catches that case as readily as an untouched base, where checking "who
  defines `__hash__`" alone would miss it. Either way, a `set` or `dict` keyed on
  strategy instances cannot dedupe or key on them correctly

## Notifier checks

- `notifier-missing-notify` -- `notify` is not overridden. Its default raises
  `NotImplementedError()` unconditionally; `on_success_callback`/`on_failure_callback`
  run on the Dag processor, sync-only, and call `notify` directly, so implementing only
  `async_notify` does not help there. Covers apache/airflow#64649, where a minimal
  `BaseNotifier` used as a callback crashed under `airflow dags test`
- `notifier-template-fields-unresolvable` -- an instance's `template_fields` names an
  attribute the instance does not carry. `_update_context` does a plain
  `getattr(self, f)` for every entry, raising `AttributeError` the first time the
  notifier actually fires. Only checked against an already-built instance, for the same
  reason as `timetable-serialize-not-json`

## Secrets backend checks

- `secrets-backend-raises-on-miss` -- an overridden `get_conn_value`, `get_connection`,
  `get_variable`, or `get_config` is annotated to return something that does not admit
  `None` (recognizing `X | None`, `Optional[X]`, and `Union[X, None]` alike). All four
  must return `None` on a miss, not raise -- a very common bug is raising the backing
  client's own not-found error instead. `get_conn_value` is included because it is the
  *default* override point Airflow's own docs and shipped backends actually use, not a
  rarely-touched internal. `check_component` never calls a secrets backend for real,
  since a genuine miss needs real credentials this module cannot fabricate safely, so
  this reads the override's own declared return annotation; an unannotated override is
  not flagged

## Policy checks

- `policy-unknown-hookspec` -- a policy hookimpl method's name matches no hookspec in
  `airflow.policies`. pluggy silently ignores it; the method never fires
- `policy-argument-name-mismatch` -- a policy hookimpl method declares an argument its
  matching hookspec does not have. pluggy hard-errors on this at registration time.
  `task_instance_mutation_hook` gained a `dag_run` parameter in Airflow 3.3, so a hook
  written for -- or copied from -- a newer release breaks registration entirely on an
  older one; this reads the live, installed hookspec, so it reflects whatever the
  resolved Airflow actually declares rather than a hardcoded table

## Plugin checks

- `plugin-name-missing` -- the plugin does not set `name`. `AirflowPlugin.validate()`
  raises `AirflowPluginException` for exactly this, but only when Airflow's own
  `is_valid_plugin` calls it during real discovery; this checker never calls
  `validate()` itself, since doing so risks raising out of `check_component`

## Provider checks

Pass the `get_provider_info` callable itself, not its return value -- these checks call
it internally, the same way `ProvidersManager` calls the real entry point as
`entry_point.load()()`.

- `provider-info-schema` -- the callable's return value fails the shipped
  `provider_info.schema.json` (read from the installed `airflow` package directly, so it
  always validates against whichever schema the resolved release actually ships), or the
  callable raises when called
- `provider-package-name-mismatch` -- the returned dict's `package-name` disagrees with
  the owning distribution's own canonical name. `ProvidersManager` raises `ValueError` at
  discovery when these disagree -- not a warning
- `provider-no-entry-point` -- the owning distribution registers no
  `apache_airflow_provider` entry point at all. Without one, `ProvidersManager` never
  calls this function, and the provider is not discovered, silently

The last two need `check_component` to attribute the callable's module to a real
installed distribution, which it does by matching the distribution's own recorded file
list, falling back to the precise source root(s) named in the distribution's own `.pth`
file for an editable (`pip install -e .`) install -- the standard way a provider author
develops their own package. That precise root is deliberately narrower than the whole
project checkout: a `src/`-layout package is editable-exposed only under `src/`, so a
sibling directory sharing the same project root (a `tests/` folder, another package in
a workspace) is not attributed to it. A callable that cannot be attributed to any
installed distribution this way (for example one defined directly in a test file, not
part of an installed package) is silently skipped by both checks. The underlying
distribution index is built once per process and reused, not rescanned on every call.
