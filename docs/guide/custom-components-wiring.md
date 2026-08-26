# Wiring components into the run

A clean [`check_component`](custom-components.md) report proves the component's shape. It
says nothing about whether a test process can load it -- in production your deployment
ships an `AirflowPlugin`, a configured backend, or an entry point, and in a bare test
process nothing has loaded any of that. Two channels close the gap, with different
lifetimes: ini options fix the session substrate before the first Airflow import, and the
`airflow_components` fixture overlays one test on top of it.

## Session substrate: five ini options

Resolved and applied before the first Airflow import, into the generated `AIRFLOW_HOME`:

- `airflow_plugins_folder` -- a directory whose entries are symlinked individually into
  the run's `plugins/` directory, so an `AirflowPlugin` (and through it a bundled
  timetable, listener, or macro) is discoverable. Symlinking per entry, not the directory
  itself, keeps an edit to an existing file live and never leaves a stale copy behind.
  `plugins/` is always created, even unconfigured -- an empty directory makes Airflow's
  plugin load a genuine no-op, not an error
- `airflow_executor` -- written to `[core] executor`, overriding this plugin's own 2.x
  `SequentialExecutor` default
- `airflow_xcom_backend` -- written to `[core] xcom_backend`
- `airflow_secrets_backend` / `airflow_secrets_backend_kwargs` -- written to
  `[secrets] backend` / `backend_kwargs`. The kwargs entry, and the whole `[secrets]`
  section, is dropped unless a backend is also configured

```ini
[pytest]
airflow_plugins_folder = plugins
airflow_executor = tests.support.executors.FakeExecutor
airflow_xcom_backend = tests.support.xcom.RecordingXCom
airflow_secrets_backend = tests.support.secrets.FakeSecretsBackend
```

All five are ini-only, with no command-line flag: they are project facts, not
per-invocation knobs. That matters most for `airflow_xcom_backend` -- Airflow's Task SDK
resolves the configured XCom backend at MODULE IMPORT TIME
(`airflow.sdk.execution_time.xcom`, imported transitively by every Dag through
`airflow.sdk.definitions.xcom_arg`), so a per-test override would be a lie. A run-wide ini
applied before Airflow imports at all is the only honest version, which is why all five
live in `bootstrap.py` / `airflow_cfg.py` rather than a fixture.

## Runtime component registration

Run-wide is right for a component every test needs and wrong for one a single test wants.
The `airflow_components` fixture yields a `ComponentRegistry` that registers a component
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
wondered about later -- you cannot register a listener with an unmatched hookspec and then
be surprised it never fires. A problem with the *registration* rather than the component
raises `ComponentSandboxError` instead: an unsupported dual-registration request, an
executor class with no importable module-level path, an unknown policy hookspec name, or
-- on a certified Apache Airflow release -- live cache-clearable names that no longer
match what this plugin has certified.

The methods:

- `plugin(component)` -- registers into every plugins-manager half the installed release
  has (both core and Task SDK from 3.2 on; core only on 3.1.x, which carries no Task SDK
  plugin-loading surface at all)
- `listener(component, *, core=True, task=True)` -- registers with the core and/or Task
  SDK listener manager. Requesting `task=True` on 3.1.x raises rather than silently
  registering core-only; pass `task=False` explicitly on a test that intentionally spans
  both families. The conformance check's manager-scope findings follow the requested
  scope: a hookimpl whose hookspec exists on only one manager (core-only
  `on_dag_run_success`, say) is accepted whenever that manager is among the requested
  ones, and refused only when it could never fire
- `policy(**hookname_to_callable)` -- builds a policy plugin from hookspec-named callables
  (`task_policy`, `dag_policy`, `task_instance_mutation_hook`, `pod_mutation_hook`,
  `get_airflow_context_vars`, `get_dagbag_import_timeout`) and registers it with Airflow's
  policy plugin manager. Never writes an `airflow_local_settings.py` file, so per-test
  policies are fully decoupled from the collision guard in [keeping your own
  `airflow_local_settings.py`](cluster-policies.md). A registered
  `task_instance_mutation_hook` also flips the `is_noop` dispatch gate to False for the
  test, reverted at teardown -- Airflow short-circuits on that flag, so the hookimpl would
  otherwise register but never fire
- `secrets_backend(component, *, first=True)` -- inserts into the secrets backend search
  path, at the front by default
- `executor(component, *, alias="test") -> str` -- registers an executor class under
  `alias` and returns it, for `ExecutorLoader.load_executor(alias)` /
  `ExecutorLoader.lookup_executor_name_by_str(alias)` and for
  [`run_dag(dag, executor=alias)`](task-execution.md#executor-driven-runs) within the same
  test. `component` must be defined at module scope somewhere importable -- Airflow
  resolves it later by dotted import path, and a class defined inside a test function has
  none
- `timetable(component)` -- registers a custom timetable class through a synthesized
  throwaway `AirflowPlugin`, which is what makes serialization's qualname lookup resolve.
  Accepts a class or an instance; an instance registers its class and additionally runs
  the instance-only conformance checks. See [custom timetables](custom-timetables.md)
- `priority_weight_strategy(component)` -- the same synthesized-plugin registration for a
  `PriorityWeightStrategy` subclass, which is what lets Dag serialization accept an
  operator's custom `weight_rule` at all. Deserialization re-instantiates with no
  arguments, so state must live on the class
- `serialization_round_trip(component)` -- registers a timetable INSTANCE via
  `timetable()`, then asserts `decode_timetable(encode_timetable(...))` reconstructs it:
  one call covers "not registered", serialize/deserialize asymmetry, and (when the class
  defines its own `__eq__`) equality problems
- `round_trip(component)` -- classifies `component` as exactly one of plugin, listener,
  executor, secrets backend, timetable, or priority weight strategy, using the same
  classification `check_component`'s auto-detection uses, and calls that method with its
  defaults -- except a listener's `task` flag, which follows the installed release's Task
  SDK availability so a 3.1.x install round-trips core-only rather than raising. Not a
  substitute for `policy()`, which has no bare-component form to classify

No *Airflow configuration* surface can select an executor alias. The `airflow_executor`
ini is resolved before the first Airflow import, when no alias exists yet, and takes a
real dotted class path; a `core.executor` override is silently ignored too, because
`ExecutorLoader._get_executor_names()` memoizes its config parse into `_executor_names`
(which the sandbox itself already forced while snapshotting the loader), and a bare
single-part value resolves against Airflow's built-in core executor names, never the alias
map. `run_dag` sidesteps all of that by asking the alias registry directly.

`airflow_components` is unavailable on the Airflow 2.x family, which predates the Task
SDK's own plugin and listener managers entirely.

### On an uncertified Airflow release

On a release newer than the last certified one, a fresh upstream minor or patch, the
sandbox degrades instead of failing: capabilities resolve by live probing, unknown
plugins-manager caches are snapshot and cleared generically by introspection, and drift
from the certified tables is logged rather than raised. State isolation still holds
byte-for-byte; what the degraded tier gives up is byte-verified vetting of Airflow's
internals. It is surfaced three ways: one `UncertifiedAirflowWarning` per session at
configure time, a `DEGRADED:` bullet in
[`--airflow-doctor`](../reference/diagnostics.md), and
`ComponentReport.certification == CertificationTier.PROBED` on every report.

## How the two channels compose

- **The ini options are the session substrate.** Resolved into the generated
  `AIRFLOW_HOME` and the pre-import environment before the first Airflow import, fixed for
  the whole run
- **`airflow_components` is a per-test overlay on that substrate.** Every registration
  mutates the live process-global registries for one test, and teardown reverts each
  registry to whatever the substrate seeded -- never to empty. An ini-configured executor,
  secrets backend, or plugins-folder component is live before the sandbox snapshots, so it
  is exactly what restoration reinstates
- **`airflow_config` environment overrides sit between the two.** A `with
  airflow_config(...)` block, or the [`airflow_config` ini option](configuration.md) whose
  context is the whole session, outranks the generated `airflow.cfg` for its duration,
  because the environment outranks every file on each `conf.get()`. It changes what
  Airflow *reads*, never the live registries the sandbox manages

One consequence worth spelling out: `airflow_executor` writes `[core] executor` into the
generated `airflow.cfg`, while an `airflow_config` ini line `core.executor = ...` becomes
the `AIRFLOW__CORE__EXECUTOR` environment variable -- so with both set, the
`airflow_config` line wins for the whole session. Deliberate: `core.executor` is
intentionally not on the `airflow_config` denylist, and the environment channel is defined
to outrank the file channel. Set one or the other.

## Hazards at the sandbox seam

Sharp edges of the live-mutation channel. Each is a documented contract pinned by a test,
not an accident to discover:

- **Teardown restores secrets backend *instances*, not configuration.** The sandbox
  restores `airflow.configuration.secrets_backend_list` to the exact pre-test instance
  objects by slice assignment; it never calls upstream's `ensure_secrets_loaded()` rebuild.
  A substrate backend that accumulated state during a test -- an open client, a populated
  cache -- is resurrected as that same live object in every later test
- **`ensure_secrets_loaded()` hides nothing today, by upstream's heuristic.** It returns the
  live `secrets_backend_list` UNLESS the list holds exactly two entries, its two built-in
  defaults, in which case it rebuilds from configuration instead without touching the
  module global. A sandbox-registered backend always grows the list past two, so it stays
  visible with or without an ini backend configured. That rests on upstream's
  `len(...) == 2` heuristic, which this plugin does not own; see `PROVENANCE.md`
- **Plugins-folder modules reload across sandboxed tests; old bindings go stale.** Teardown
  clears the plugins-manager caches, so the next plugin access rescans the folder, and
  upstream's directory loader unconditionally re-executes each file via `module_from_spec`,
  producing a NEW module object with new class objects. (Teardown's `sys.modules` handling
  is secondary: a plugins-folder key the test introduced is deleted, a pre-existing one is
  restored.) Anything holding the previous load's objects is bound to the old ones.
  Compare by name across tests, never by identity
- **Each sandboxed test costs two plugins-folder rescans.** The sandbox clears the caches
  at construction, so a stale pre-test load cannot win, and again at teardown, so nothing
  the test computed lingers. `dag_maker(schedule=...)`'s first call adds a wrinkle: its
  registered-timetable lookup probe runs BEFORE the sandbox exists, so the plugin load
  that probe triggers is immediately discarded by the construction clear and repeated
- **An ini plugins-folder listener survives teardown through the listener snapshot.** The
  listener-manager getters are `functools.cache`d and deliberately never cleared, so the
  manager persists across tests, and sandbox construction resolves the managers -- building
  them if nothing had done so yet -- before snapshotting, putting the ini-seeded listener
  inside the snapshot teardown restores
