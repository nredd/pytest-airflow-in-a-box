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

Both checks cover a policy registered as an `@hookimpl`-decorated class through the
`airflow.policy` plugin entry point -- the shape `ComponentKind.POLICY`'s classifier
requires. A plain module-level function in `airflow_local_settings.py`, the older and
still-common way to write cluster policies, is a different surface entirely:
`make_plugin_from_local_settings` loads it through a dynamically generated shim that
calls it positionally and deliberately tolerates a name or arity mismatch, rather than
registering it with pluggy directly -- a mismatch that hard-errors on the plugin
entry-point path this check models is often silently accepted on that one. Forcing
`kind=ComponentKind.POLICY` on such a function does not help either: neither check finds
anything to say, since a plain function is never `@hookimpl`-marked and both checks
require that marker to find a hookimpl to examine at all.

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

To prove the entry point itself *resolves* -- not just that it exists -- run a test under
[`airflow_isolated`](isolated-tests.md), which registers a synthetic distribution and exercises
real `entry_points()` discovery in a one-shot child process.

## Making a component reachable

A clean `check_component` report only proves the component's own shape is sound -- it
says nothing about whether a run can actually load it. Five ini options, resolved and
applied before the first Airflow import, wire a checked component into the generated
`AIRFLOW_HOME` itself:

- `airflow_plugins_folder` -- a directory whose entries are symlinked individually into
  the run's `plugins/` directory, so an `AirflowPlugin` (and, through it, a bundled
  timetable, listener, or macro) is discoverable. Symlinking per entry, not the directory
  itself, means an edit to an existing file stays live and adding or removing a plugin
  source between runs never leaves a stale copy behind. `plugins/` is always created,
  even when this is left unconfigured -- an empty directory makes Airflow's own plugin
  load a genuine no-op, not an error
- `airflow_executor` -- written to `[core] executor`, overriding this plugin's own 2.x
  `SequentialExecutor` default when configured
- `airflow_xcom_backend` -- written to `[core] xcom_backend`
- `airflow_secrets_backend` / `airflow_secrets_backend_kwargs` -- written to
  `[secrets] backend` / `backend_kwargs`; the kwargs entry (and the whole `[secrets]`
  section) is dropped unless a backend is also configured

```ini
[pytest]
airflow_plugins_folder = plugins
airflow_executor = tests.support.executors.FakeExecutor
airflow_xcom_backend = tests.support.xcom.RecordingXCom
airflow_secrets_backend = tests.support.secrets.FakeSecretsBackend
```

All five are ini-only, with no command-line flag: they are project facts, not
per-invocation knobs. That matters beyond plumbing for `airflow_xcom_backend`
specifically -- Airflow's Task SDK resolves the configured XCom backend at MODULE IMPORT
TIME (`airflow.sdk.execution_time.xcom`, imported transitively by every Dag through
`airflow.sdk.definitions.xcom_arg`), so a per-test override would be a lie. A run-wide
ini applied before Airflow imports at all is the only honest version, and the same
before-first-import guarantee is why all five live in `bootstrap.py`/`airflow_cfg.py`
rather than a fixture.

## Runtime component registration

The five ini options above are run-wide, fixed before the first Airflow import -- right
for a component every test needs, wrong for one only a single test wants. The
`airflow_components` fixture yields a `ComponentRegistry` that registers a plugin,
listener, policy, secrets backend, executor, timetable, or priority weight strategy
directly into Airflow's live, process-global registries for the duration of one test,
then reverts every one of them:

```python
def test_my_listener_fires(airflow_components, dag_maker):
    airflow_components.listener(MyListener)
    with dag_maker() as dag:
        ...
    dag_maker.run()
    assert MyListener.calls
```

Every method runs `check_component` first and raises `ComponentContractError` on any
conformance problem, so a broken component is never silently registered and left to be
wondered about later -- you cannot register a listener with an unmatched hookspec and
then be surprised it never fires. A problem with the registration itself, rather than the
component, raises `ComponentSandboxError` instead: an unsupported dual-registration
request, an executor class with no importable module-level path, an unknown policy
hookspec name, or -- on a certified Apache Airflow release -- live cache-clearable names
that no longer match what this plugin has certified.

On an Airflow release newer than the last certified one (a fresh upstream minor or
patch), the sandbox degrades instead of failing: capabilities resolve by live probing,
unknown plugins-manager caches are snapshot/cleared generically by introspection, and
drift from the last certified tables is logged rather than raised. State isolation
still holds byte-for-byte; what the degraded tier gives up is byte-verified vetting of
Airflow's internals. The degraded tier is surfaced three ways: one
`UncertifiedAirflowWarning` per session at configure time, a `DEGRADED:` bullet in
[`--airflow-doctor`](../reference/diagnostics.md), and a machine-readable
`ComponentReport.certification` field (`CertificationTier.PROBED`) on every
`check_component` report.

- `plugin(component)` -- registers into every plugins-manager half the installed release
  has (both core and Task SDK from 3.2 on; core only on 3.1.x, which carries no Task SDK
  plugin-loading surface at all)
- `listener(component, *, core=True, task=True)` -- registers with the core and/or Task
  SDK listener manager. Requesting `task=True` on 3.1.x (no Task SDK listener manager at
  all) raises rather than silently registering core-only; pass `task=False` explicitly on
  a test that intentionally spans both families. The conformance check's manager-scope
  findings follow the requested scope: a hookimpl whose hookspec exists on only one
  manager (core-only `on_dag_run_success`, say) is accepted whenever that manager is
  among the requested ones, and refused only when it could never fire
- `policy(**hookname_to_callable)` -- builds a policy plugin from hookspec-named
  callables (`task_policy`, `dag_policy`, `task_instance_mutation_hook`,
  `pod_mutation_hook`, `get_airflow_context_vars`, `get_dagbag_import_timeout`) and
  registers it directly with Airflow's policy plugin manager. Never writes an
  `airflow_local_settings.py` file, so per-test policies are fully decoupled from the
  `airflow_local_settings` collision guard above. A registered
  `task_instance_mutation_hook` also flips the `is_noop` dispatch gate to False for the
  test (reverted at teardown) -- Airflow short-circuits on that flag, so the hookimpl
  would otherwise register but never fire
- `secrets_backend(component, *, first=True)` -- inserts into the secrets backend search
  path, at the front (checked before every other configured backend) by default
- `executor(component, *, alias="test") -> str` -- registers an executor class under
  `alias`, returning it for use with
  `airflow_config({("core", "executor"): alias})` within the same test. The alias
  exists only in this process for this one test, so the `airflow_executor` ini can
  never name it: that option is resolved before the first Airflow import, when no
  alias exists yet, and takes a real dotted class path. `component` must be defined at
  module scope somewhere importable -- Airflow resolves it later by dotted import
  path, and a class defined inside a test function has none
- `timetable(component)` -- registers a custom timetable class through a synthesized
  throwaway `AirflowPlugin`, which is exactly what makes Airflow's serialization round
  trip resolve the class by qualname. Accepts the class or an instance (an instance
  registers its class, plus runs the instance-only conformance checks). See the
  [custom timetables guide](custom-timetables.md)
- `priority_weight_strategy(component)` -- the same synthesized-plugin registration for
  a `PriorityWeightStrategy` subclass, which is what lets Dag serialization accept an
  operator's custom `weight_rule` at all. Deserialization re-instantiates the class
  with no arguments, so state must live on the class
- `serialization_round_trip(component)` -- registers a timetable INSTANCE via
  `timetable()`, then asserts `decode_timetable(encode_timetable(...))` reconstructs
  it: one call covers "not registered", serialize/deserialize asymmetry, and (when the
  class defines its own `__eq__`) equality problems
- `round_trip(component)` -- classifies `component` as exactly one of plugin, listener,
  executor, secrets backend, timetable, or priority weight strategy (the same
  classification `check_component`'s own auto-detection uses) and calls that method
  with its defaults, except a listener's `task` flag, which follows the installed
  release's Task SDK availability so a 3.1.x install round-trips core-only rather than
  raising. Not a substitute for `policy()`, which has no bare-component form to
  classify

`airflow_components` is unavailable on the Airflow 2.x family, which predates the Task
SDK's own plugin and listener managers entirely.

## How the two channels compose

The ini options and the `airflow_components` sandbox install the same component
families through two channels with different lifetimes. The contract between them:

- **The ini options are the session substrate.** `airflow_plugins_folder`,
  `airflow_executor`, `airflow_xcom_backend`, and `airflow_secrets_backend` /
  `airflow_secrets_backend_kwargs` are resolved into the generated `AIRFLOW_HOME` and
  the pre-import environment before the first Airflow import, and stay fixed for the
  whole run.
- **`airflow_components` is a per-test overlay on that substrate.** Every registration
  mutates Airflow's live process-global registries for one test, and teardown reverts
  each registry to whatever the substrate seeded -- never to empty. An ini-configured
  executor, secrets backend, or plugins-folder component is live before the sandbox
  snapshots, so it is exactly what restoration reinstates; the next test sees the
  substrate again, sandbox registrations gone.
- **`airflow_config` environment overrides sit between the two.** A
  `with airflow_config(...)` block -- or the [`airflow_config` ini
  option](configuration.md), whose context is the whole session -- outranks the
  generated `airflow.cfg` for its context's duration, because the environment outranks
  every file on each `conf.get()`. It changes what Airflow *reads*, never the live
  registries the sandbox manages.

One consequence worth spelling out: `airflow_executor` writes `[core] executor` into
the generated `airflow.cfg`, while an `airflow_config` ini line `core.executor = ...`
becomes the `AIRFLOW__CORE__EXECUTOR` environment variable -- so when both are set,
the `airflow_config` line wins for the whole session. That is deliberate:
`core.executor` is intentionally not on the `airflow_config` denylist, and the
environment channel is defined to outrank the file channel. Set one or the other.

## Hazards at the sandbox seam

The live-mutation channel has sharp edges that are documented contract, each pinned by
a test, not accidents to be discovered:

- **Teardown restores secrets backend *instances*, not configuration.** The sandbox
  restores `airflow.configuration.secrets_backend_list` to the exact pre-test instance
  objects by slice assignment; it never calls upstream's `ensure_secrets_loaded()`
  rebuild. A substrate backend that accumulated state during a test (an open client, a
  populated cache) is resurrected as that same live object in every later test, not
  reconstructed fresh from configuration.
- **`ensure_secrets_loaded()` hides nothing today, by upstream heuristic.**
  `airflow.configuration.ensure_secrets_loaded()` returns the live
  `secrets_backend_list` UNLESS the list holds exactly two entries -- its two built-in
  defaults -- in which case it rebuilds a fresh list from configuration instead
  (without touching the module global). A sandbox-registered backend always grows the
  list past two (the two defaults plus the registration; one more with an ini
  backend), so it stays visible through `ensure_secrets_loaded()` with or without an
  ini backend configured. That visibility rests on upstream's `len(...) == 2`
  heuristic, which this plugin does not own; see PROVENANCE.md.
- **Plugins-folder modules reload across sandboxed tests; old bindings go stale.**
  Teardown removes a plugins-folder module from `sys.modules` and clears the
  plugins-manager caches, so the next plugin access rescans the folder and executes
  the file again, producing a NEW module object with new class objects. Anything
  holding the previous load's objects -- a class a test imported and kept, the
  restored listener instance on a manager -- is bound to the old ones, and an
  identity or `isinstance` comparison across a sandboxed test boundary compares
  classes from different loads. Compare by name across tests, never by identity.
- **Each sandboxed test costs two plugins-folder rescans.** The sandbox clears the
  plugins-manager caches at construction (so a stale pre-test load cannot win) and
  again at teardown (so nothing the test computed lingers), and each clear makes the
  next plugin access rescan the folder. `dag_maker(schedule=...)`'s first call in a
  test adds one more wrinkle: its registered-timetable lookup probe runs BEFORE the
  sandbox exists, so the plugin load that probe may trigger is immediately discarded
  by the sandbox construction clear and repeated afterward.
- **An ini plugins-folder listener survives teardown through the snapshot, by
  construction ordering.** Sandbox construction clears the plugins-manager caches,
  THEN resolves the listener managers -- integrating plugins-folder listeners when a
  manager is built here for the first time -- and only then snapshots them, so the
  ini-seeded listener is inside the snapshot teardown restores. This ordering is
  pinned as contract by test; the cached listener-manager getters themselves are
  never cleared, which is what keeps the restored manager (and its substrate
  listeners) alive across tests.
