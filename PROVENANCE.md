# Provenance

`pytest-airflow-in-a-box` is an independent project informed by public Apache Airflow source,
documentation, issue discussions, and mailing-list discussions.

`src/pytest_airflow_in_a_box/_compat/taskrun.py::run_task_instance` is adapted from Apache Airflow
`devel-common/src/tests_common/test_utils/taskinstance.py` at commit
`2d374f71bc81202204ac0208df07b07c280668fa`, introduced by merge
`960973bfd8341040150ac312302cd795bf72bc20`. The local version defers Airflow imports, resolves an
optional task, preserves dependency flags on both execution paths, implements Airflow 3.2+
`mark_success`, commits before Execution API access, and refreshes the original ORM task instance.
The ASF license header is retained in that module.

`src/pytest_airflow_in_a_box/_compat/asset_schedule.py::_evaluate_v3_dag` is adapted from the
DagRun-creation body of Apache Airflow's `SchedulerJobRunner._create_dag_runs_asset_triggered`
and the readiness evaluation in `DagModel.dags_needing_dagruns`
(`airflow-core/src/airflow/jobs/scheduler_job_runner.py` and
`airflow-core/src/airflow/models/dag.py`) at commit `1438ea3587031417cc85d74323235cf087a058fb`
(tag `3.3.0`). `_evaluate_v2_dag` is adapted analogously from
`SchedulerJobRunner._create_dag_runs_dataset_triggered` (`airflow/jobs/scheduler_job_runner.py`)
at commit `b93c3db6b1641b0840bd15ac7d05bc58ff2cccbf` (tag `2.10.5`). Both drop row locking,
`max_active_runs` throttling, paused/stale/import-error Dag filters, and batching -- scheduler-
operational concerns that do not apply to one evaluated test Dag in an isolated single-process
test database -- and simplify the attached-events query to omit the real scheduler's lower bound
at the previous asset/dataset-triggered run of the same consumer. Neither file's license header
is copied verbatim; both are reimplementations against the same public/private model surface.

`src/pytest_airflow_in_a_box/fixtures/upstream.py`'s `create_task_instance`, `create_dummy_dag`,
and `testing_dag_bundle` fixtures mirror the names, parameter lists, defaults, and observable
semantics of the fixtures of the same names in Apache Airflow
`devel-common/src/tests_common/pytest_plugin.py` (read at Airflow `main`, August 2026). The
bodies are independently authored compositions over this project's own `dag_maker` and
compatibility layer -- no upstream function body is copied -- and deliberately deviate where
documented: the returned `TaskInstance` is the plain ORM object (no `TaskInstanceWrapper`),
`None`-valued operator arguments are dropped before construction, and the shared `testing`
bundle row is never deleted at teardown.

`ordered_task_instances`, all DagMaker extensions, and `evaluate_asset_schedules` (the family
dispatcher in `asset_schedule.py`) are independently authored for this project.

`src/pytest_airflow_in_a_box/_compat/components.py`'s timetable, listener, and executor
conformance checks, plus `_compat/capabilities.py::_probe_executor_contract` and
`_probe_sdk_listener_manager_available`, are independently authored -- no Apache Airflow function
body is copied or adapted. Their embedded facts (which `BaseExecutor` methods and attributes exist
under which names and types on which release, which `Timetable` methods lack a usable default, and
which hookspec modules each listener manager registers) were transcribed by reading Apache Airflow
source directly, not derived from documentation. Verified against
`airflow-core/src/airflow/timetables/base.py`, `airflow-core/src/airflow/executors/base_executor.py`,
`airflow-core/src/airflow/listeners/listener.py`,
`airflow-core/src/airflow/listeners/spec/{dagrun,asset,importerrors}.py`,
`shared/listeners/src/airflow_shared/listeners/spec/{lifecycle,taskinstance}.py` (symlinked
unchanged into both `airflow-core/src/airflow/_shared/listeners` and
`task-sdk/src/airflow/sdk/_shared/listeners`), and `task-sdk/src/airflow/sdk/listener.py`, all at
commit `1438ea3587031417cc85d74323235cf087a058fb` (tag `3.3.0`). The executor's sentry-flag rename
and `execute_async` removal were additionally verified against `base_executor.py` at commit
`54bd5d8cd9f6f477cc83445737614dec81c4323c` (tag `3.1.0`) and commit
`3bc3ccfacc3dec9f359a3b153bfd4fc706c661ba` (tag `3.2.0`). `task-sdk/src/airflow/sdk/listener.py`
does not exist at all at the `3.1.0` commit -- confirmed both by installing the real `3.1.0` wheel
and by the absence of the path in the `3.1.0` tag's own source tree -- so
`sdk_listener_manager_available` certifies `False` for every 3.1.x release and `True` from `3.2`
onward, alongside the rest of that release's Task SDK listener-architecture changes. The same
`3.1.0` wheel install additionally shows `lifecycle`/`taskinstance` living directly at
`airflow.listeners.spec.{lifecycle,taskinstance}` on 3.1.x, with no `_shared` split at all --
`airflow.listeners.listener.ListenerManager.__init__` builds all five hookspecs from
`airflow.listeners.spec` itself
(https://github.com/apache/airflow/blob/54bd5d8cd9f6f477cc83445737614dec81c4323c/airflow-core/src/airflow/listeners/listener.py) --
and the installed `3.2.0` wheel confirms the old path is fully removed there, replaced by the
`_shared` one; `_CORE_LISTENER_SPEC_MODULES` lists both candidates so each release resolves the
one that exists.

`src/pytest_airflow_in_a_box/_compat/components.py`'s XCom backend, weight strategy, notifier,
secrets backend, policy, plugin, and provider conformance checks, plus
`_compat/capabilities.py::_probe_task_instance_mutation_hook_supports_dag_run`, are independently
authored -- no Apache Airflow function body is copied or adapted. Their embedded facts were
transcribed by reading Apache Airflow source directly (and, for the two cross-release behavioral
facts below, by running the real installed code), not derived from documentation. Verified against
`airflow-core/src/airflow/policies.py`, `airflow-core/src/airflow/plugins_manager.py`,
`airflow-core/src/airflow/task/priority_strategy.py`, `task-sdk/src/airflow/sdk/bases/xcom.py`,
`task-sdk/src/airflow/sdk/bases/notifier.py`, `airflow-core/src/airflow/secrets/base_secrets.py`,
`airflow-core/src/airflow/providers_manager.py`, and
`airflow-core/src/airflow/provider_info.schema.json`, all at commit
`1438ea3587031417cc85d74323235cf087a058fb` (tag `3.3.0`), cross-checked against the same paths at
commit `54bd5d8cd9f6f477cc83445737614dec81c4323c` (tag `3.1.0`) and commit
`3bc3ccfacc3dec9f359a3b153bfd4fc706c661ba` (tag `3.2.0`) by installing the real `3.1.0` and `3.2.0`
wheels (via each release's own `constraints-<python>.txt`) into isolated environments and
introspecting the live classes and functions directly, the same methodology the timetable/listener/
executor checks above used.

`airflow.policies.task_instance_mutation_hook` carries only a `task_instance` parameter on the
installed 3.1.0 and 3.2.0 wheels and gains `dag_run` only on the installed 3.3.0 -- confirmed by
`inspect.signature` against all three, plus the shipped `2.11.2` tag's `airflow/policies.py`
(https://github.com/apache/airflow/blob/2.11.2/airflow/policies.py) for the 2.x contract row, which
carries no `dag_run` either. `PriorityWeightStrategy.__hash__` genuinely varies by release rather
than being a fixed fact: the installed 3.1.0 wheel defines `__eq__` without `__hash__`, so Python's
own class-creation rule sets `__hash__` to `None` on that class -- every instance is unhashable,
confirmed by actually constructing one and calling `hash()` on it, which raises `TypeError` -- while
the installed 3.2.0 and 3.3.0 wheels both define `__hash__` explicitly as `return hash(None)`,
confirmed the same way, so `weight-strategy-hash-of-none` reads `PriorityWeightStrategy.__hash__`
live rather than hardcoding either behavior as a per-release constant.

`airflow.policies.make_plugin_from_local_settings` wraps an `airflow_local_settings.py`
module-level policy function in a dynamically generated shim (`_make_shim_fn`) that calls it
positionally and deliberately tolerates a name or arity mismatch that pluggy's own hookimpl
registration would hard-error on for the plugin entry-point path -- confirmed by reading
`airflow-core/src/airflow/policies.py` directly at commit
`1438ea3587031417cc85d74323235cf087a058fb` (tag `3.3.0`). `docs/guide/custom-components.md`'s
Policy checks section cites this as the reason `policy-unknown-hookspec`/
`policy-argument-name-mismatch` -- which model the plugin entry-point path pluggy actually
validates -- do not cover a plain `airflow_local_settings.py` function.

`airflow._shared.plugins_manager.plugins_manager.is_valid_plugin`'s duck-typing match (`base.__name__
== "AirflowPlugin" and "plugins_manager" in base.__module__`) exists verbatim on the installed 3.2.0
and 3.3.0 wheels; the installed 3.1.0 wheel's `airflow.plugins_manager.is_valid_plugin` instead uses
plain `issubclass(plugin_obj, AirflowPlugin)`, predating the `_shared` split that introduced the
duck-typing match (confirmed by the module comment: core and Task SDK access the same shared source
via different symlinked paths, which Python treats as distinct classes, so `issubclass` alone would
reject a provider's plugin genuinely inheriting the SDK's `AirflowPlugin`).
`_compat/components.py::_is_plugin`/`_check_plugin_name_missing` use the duck-typing match
unconditionally on every certified release rather than branching by release, since it is a strict
superset of `issubclass` for a component that genuinely inherits the one real `AirflowPlugin` that
exists on 3.1.x, and never calls `AirflowPlugin.validate()` itself (unlike the real
`is_valid_plugin`), since that can raise `AirflowPluginException` and a `check_component` classifier
or checker must never raise on the checked component.

`airflow._shared.providers_discovery.providers_discovery.discover_all_providers_from_packages`
(inline as `ProvidersManager._discover_all_providers_from_packages` on the installed 3.1.0, before
the `_shared` split) raises `ValueError` when a discovered distribution's canonicalized name
disagrees with the `get_provider_info()` dict's own `package-name` field, and
`ProvidersManager._create_provider_info_schema_validator` resolves the validator via
`jsonschema.validators.validator_for(schema)`, both confirmed identical on all three installed
wheels; `_check_provider_info_schema` mirrors that exact validator construction and reads the
schema from the installed `airflow` package directly via `importlib.resources.files("airflow")`
rather than embedding a copy, so it always validates against whichever schema the resolved release
actually ships (the schema's own optional fields grew between `3.1.0` and `3.2.0`, confirmed by
diffing the two installed copies).

`BaseSecretsBackend.get_conn_value`'s own docstring ("If the client your secrets backend uses
already returns a python dict, you should override ``get_connection`` instead") and the installed
`airflow.secrets.environment_variables.EnvironmentVariablesBackend` overriding `get_conn_value`
rather than `get_connection` are both confirmed on the installed 3.3.0; `_SECRETS_BACKEND_GETTERS`
includes `get_conn_value` on that basis, not `get_connection` alone.

`_distribution_editable_roots` reads a `.pth` file's own line content -- confirmed against this
project's own real `uv pip install -e .` install (`pytest_airflow_in_a_box.pth`, whose one line is
the absolute path to `src/`, not the project root PEP 610's `direct_url.json` would otherwise
suggest) -- rather than PEP 610's `direct_url.json` project root, since the latter over-attributes:
a `src/`-layout package (this project included) is editable-exposed only under `src/`, and a
sibling directory sharing the same project root (`tests/`, another package in a workspace) is not
actually reachable through the installed `.pth` redirect at all.

`src/pytest_airflow_in_a_box/_compat/components.py`'s runtime component sandbox (the seam
functions, cache-enumeration certification, and every snapshot/restore function backing
`pytest_airflow_in_a_box.fixtures.components.airflow_components`) is independently authored --
no Apache Airflow function body is copied or adapted, except `build_policy_plugin`'s
`staticmethod(hookimpl(fn, specname=name))` line, which reproduces one line of
`airflow.policies.make_plugin_from_local_settings` verbatim because it is the only correct way
to turn a bare callable into a pluggy hookimpl matching an arbitrary hookspec name; the
surrounding function is otherwise independently authored (it deliberately drops that upstream
function's silent per-name `hasattr(pm.hook, name)` tolerance and its automatic argument-mismatch
shimming, both traded for a loud `check_component` failure instead). Every embedded fact below
was transcribed by reading Apache Airflow source directly, or by running the real installed
code, not derived from documentation, the same methodology the timetable/listener/executor/
plugin/policy/secrets-backend checks above used. Verified against
`airflow-core/src/airflow/plugins_manager.py`, `task-sdk/src/airflow/sdk/plugins_manager.py`,
`shared/plugins_manager/src/airflow_shared/plugins_manager/plugins_manager.py`,
`shared/module_loading/src/airflow_shared/module_loading/__init__.py` (symlinked unchanged into
both `airflow-core/src/airflow/_shared/module_loading` and
`task-sdk/src/airflow/sdk/_shared/module_loading`), `airflow-core/src/airflow/listeners/listener.py`,
`task-sdk/src/airflow/sdk/listener.py`, `airflow-core/src/airflow/settings.py`,
`airflow-core/src/airflow/policies.py`, `airflow-core/src/airflow/configuration.py`, and
`airflow-core/src/airflow/executors/executor_loader.py`, all at commit
`1438ea3587031417cc85d74323235cf087a058fb` (tag `3.3.0`), cross-checked against the installed
`apache-airflow-core==3.1.0` wheel's `airflow/plugins_manager.py` and `airflow/utils/entry_points.py`,
and the paired installed `apache-airflow-task-sdk==1.1.0` wheel (the exact `apache-airflow-task-sdk`
range `apache-airflow-core==3.1.0`'s own `METADATA` declares:
`Requires-Dist: apache-airflow-task-sdk<1.2.0,>=1.1.0`), both downloaded and introspected directly
rather than assumed, the same wheel-introspection methodology the capability probes above used.

`airflow.plugins_manager` exposes exactly eleven `functools.cache`-decorated functions and
`airflow.sdk.plugins_manager` exposes exactly three on the installed 3.3.0, matching issue #113's
own verified list exactly; `CERTIFIED_CORE_PLUGINS_MANAGER_CACHES` and
`CERTIFIED_SDK_PLUGINS_MANAGER_CACHES` in `_compat/capabilities.py` encode both sets keyed by
`PluginsManagerShape.CACHED_FUNCTIONS`. The eleven are NOT release-invariant across the
`CACHED_FUNCTIONS` bucket: the `apache-airflow-core==3.2.0` and `==3.2.1` wheels' `airflow/plugins_manager.py`
each carry exactly nine (`get_deadline_references_plugins` and `get_windows_plugins` absent), the
`==3.2.2` wheel carries ten (`get_deadline_references_plugins` added, `get_windows_plugins` still
absent), and the `==3.3.0`/`==3.3.1` wheels carry all eleven -- every count read directly from
the downloaded wheels' `@cache`-decorated definitions, never assumed. `CertifiedCaches` encodes
that as nine `required` names plus two `optional` ones (each commented with its introduction
release), and `_verify_and_clear_cache_functions` verifies superset-of-required /
subset-of-required-plus-optional rather than strict symmetric difference. The Task SDK's three
names are all present from the `apache-airflow-task-sdk==1.2.0` wheel (the release paired with
core 3.2.0, the first `CACHED_FUNCTIONS` release) onward, confirmed by reading that wheel's
`airflow/sdk/plugins_manager.py` directly, so the SDK row carries no optional names. The installed `apache-airflow-core==3.1.0` wheel's
`airflow/plugins_manager.py` instead carries the entire plugin cache as nineteen plain
module-level globals (`plugins`, `loaded_plugins`, `import_errors`, `macros_modules`,
`admin_views`, `flask_blueprints`, `fastapi_apps`, `fastapi_root_middlewares`, `external_views`,
`react_apps`, `menu_links`, `flask_appbuilder_views`, `flask_appbuilder_menu_links`,
`global_operator_extra_links`, `operator_extra_links`, `registered_operator_link_classes`,
`timetable_classes`, `hook_lineage_reader_classes`, `priority_weight_strategy_classes`), each
`None`-sentineled except `loaded_plugins` (`set()`) and `import_errors` (`{}`), gated by
`ensure_plugins_loaded`'s own `if plugins is not None: return` short-circuit -- confirmed absent
any `@cache`/`@functools.cache` decorator anywhere in that file, and confirmed the paired
`apache-airflow-task-sdk==1.1.0` wheel ships no `plugins_manager.py` or `listener.py` at all
(consistent with PROVENANCE's existing `sdk_listener_manager_available` finding for 3.1.x), so
`CERTIFIED_SDK_PLUGINS_MANAGER_CACHES[PluginsManagerShape.MODULE_GLOBALS]` is the empty set. These
nineteen names populate `CERTIFIED_CORE_PLUGINS_MANAGER_CACHES[PluginsManagerShape.MODULE_GLOBALS]`.
`_verify_and_reset_module_globals` verifies this row by presence only, never by symmetric
difference like the `CACHED_FUNCTIONS` row does -- see that function's own docstring for why a
plain module global carries no structural marker a bare `.cache_clear()` probe can discover, and
why 3.1.x being a permanently closed release line makes presence-only verification complete
rather than merely weaker.

Since issue #212 these certified tables are an assurance tier, not the interface: they
hard-fail only on the `CertificationTier.CERTIFIED` tier (a release with a row in
`_CERTIFIED_CAPABILITIES`). An uncertified 3.x release at or above the certified floor
resolves on the `PROBED` tier -- `_verify_contract` is skipped, capabilities are pure runtime
observations, and the sandbox clears every *observed* cache-clearable name generically
(introspection discovers uncertified upstream caches exactly as reliably as certified ones)
while logging drift from the last certified tables instead of raising. The degraded tier's
exact guarantee boundary: `functools.cache` state is still snapshot/cleared byte-for-byte
because `.cache_clear` is structurally discoverable; an uncertified plain module global is
structurally invisible, so on the `MODULE_GLOBALS` shape only the certified names reset --
that undiscoverability is precisely the semantic vetting the `PROBED` tier gives up. The
weekly canary runs `pytest_airflow_in_a_box.certification.certification_probe_report` against
the newest upstream release and fails on either an uncertified release or cache-name drift, so
re-certification (extending `SUPPORTED_RELEASES`, `_CERTIFIED_CAPABILITIES`, and these cache
tables by wheel introspection) is filed before users ever rely on the degraded tier.

`_get_grouped_entry_points` (`functools.cache`-decorated, identical body on every certified
release) lives once, at `airflow.utils.entry_points`, on the installed `apache-airflow-core==3.1.0`
wheel; 3.2 moved it to `airflow._shared.module_loading` and gave the Task SDK its own
independently-`functools.cache`d copy at `airflow.sdk._shared.module_loading`, confirmed distinct
module objects (`is` identity) on the installed 3.3.0. `SharedModuleLoading.SINGLE`/`DUPLICATED`
and `CERTIFIED_SHARED_MODULE_LOADING_CACHES` encode this; `_shared_module_loading_modules` returns
one module for `SINGLE` and two for `DUPLICATED`.

`airflow.executors.executor_loader` declares exactly five module-level globals holding executor
lookup state (`_alias_to_executors_per_team`, `_module_to_executors_per_team`,
`_classname_to_executors_per_team`, `_team_name_to_executors`, `_executor_names`), confirmed by
`vars()` introspection against the installed 3.3.0 -- matching issue #113's own count exactly --
alongside the class-level `ExecutorLoader.executors` dict upstream's own test reset leaks between
runs. `register_executor` injects a constructed `ExecutorName` directly into all five rather than
going through `ExecutorLoader._get_executor_names()`'s `core.executor` config-string parsing,
confirmed live by actually registering an alias this way and resolving it through
`ExecutorLoader.lookup_executor_name_by_str`/`load_executor` end to end against the installed
3.3.0, not merely by reading the source. The `_per_team` split is a 3.2.0 change: the
`apache-airflow-core==3.1.0` and `==3.1.8` wheels' `airflow/executors/executor_loader.py` (identical
shape on both, read directly) instead declare the flat `_alias_to_executors`, `_module_to_executors`,
`_classname_to_executors` (each `dict[str, ExecutorName]`, the classname key derived upstream as
`module_path.split(".")[-1]`), a SCALAR-valued `_team_name_to_executors: dict[str | None, ExecutorName]`
(upstream's own `_get_executor_names` population loop assigns per team, never appends), and the
same `_executor_names` list -- `ExecutorLoaderSnapshotV31` and `register_executor`'s flat branch
transcribe exactly that, and `_executor_loader_is_per_team` probes the live module for the
`_alias_to_executors_per_team` name rather than consulting a release table. `ExecutorName`'s
constructor signature (`module_path`, `alias=None`, `team_name=None`) is identical on the 3.1.0,
3.1.8, and installed 3.3.0 `executor_utils.py`, so no per-release construction branch exists.
`airflow.settings` on the 3.1.0/3.1.8 wheels has no `get_policy_plugin_manager` at all; the policy
manager lives in the module global `POLICY_PLUGIN_MANAGER` (`None` until
`configure_policy_plugin_manager()` runs during `settings.initialize()`), which
`policy_plugin_manager()`'s `ImportError` fallback reads. The 3.1.x plugin list is reached via
`ensure_plugins_loaded()` populating the `plugins` module global, both already certified in the
`MODULE_GLOBALS` row; `_live_plugin_list` branches on `hasattr(module, "_get_plugins")` for the same
structural-probe reason. `integrate_macros_plugins` attaches per-plugin macros modules to the SAME
parent on every certified 3.x release -- 3.1.x's core implementation imports
`airflow.sdk.execution_time.macros` directly and both 3.2+ halves pass it as
`target_macros_module` to the shared implementation, all with the
`airflow.sdk.execution_time.macros.` `sys.modules` prefix (lowercased by `make_module`) plus a raw
`plugin.name` `setattr` on the parent, confirmed by reading the 3.1.0, 3.1.8, and 3.2.0 core wheels
and the `apache-airflow-task-sdk==1.2.0` wheel -- `snapshot_macros_module_keys` /
`restore_macros_module_keys` revert the `setattr` half, which upstream never removes.

`airflow.configuration.secrets_backend_list` is a plain module-level list `initialize_secrets_backends()`
builds once; `restore_secrets_backend_list` restores it by slice assignment (`secrets_backend_list[:] = before`)
rather than rebinding the module attribute, so any other code holding a reference to the same list
object (a real, if currently unconfirmed-elsewhere, risk `airflow.configuration.ensure_secrets_loaded`'s
own `len(...) == 2` reload heuristic exists to paper over) keeps seeing the same object with restored
contents. `ensure_secrets_loaded` is deliberately never called by this plugin's restore path for exactly
that heuristic's own reason: it belongs to upstream and may change. The heuristic's exact shape,
confirmed by reading the installed 3.3.0's `configuration.py` directly: with its default
`default_backends` argument (`DEFAULT_SECRETS_SEARCH_PATH`, two entries --
`EnvironmentVariablesBackend` and `MetastoreBackend`), `ensure_secrets_loaded()` returns the live
`secrets_backend_list` unless `len(secrets_backend_list) == 2`, in which case it returns a FRESH
`initialize_secrets_backends()` list built from configuration without rebinding the module global;
with a non-default `default_backends` (the worker path) it always rebuilds. A sandbox-registered
backend therefore stays visible through `ensure_secrets_loaded()` on the installed 3.3.0 --
`register_secrets_backend` always grows the list past two (the two defaults plus the registration,
one more when `airflow_secrets_backend` seeds a custom backend at position zero) -- which
`tests/fixtures/test_component_channel_interactions.py` pins with and without an ini backend across
the certified matrix, so a heuristic drift on any certified release fails a test here rather than
silently hiding registrations.
A note on restore semantics that same file pins as contract: because the restore reinstates the
snapshot's exact INSTANCE objects, an ini-seeded substrate backend survives sandbox teardown as the
same live object (state included), never as a freshly-constructed one, and an ini-seeded
plugins-folder LISTENER survives teardown because the cached, never-cleared listener managers are
resolved (built, if nothing had yet) at sandbox construction before the listener snapshot is taken,
so a listener the plugins-folder scan integrated is part of the restored snapshot -- documented in
`docs/guide/custom-components.md`'s "Hazards at the sandbox seam".

`airflow.listeners.listener.get_listener_manager` and `airflow.sdk.listener.get_listener_manager` are
BOTH `functools.cache`-decorated on the installed 3.3.0 (confirmed by `hasattr(get_listener_manager,
"cache_clear")`), exactly like `airflow.settings.get_policy_plugin_manager` is -- yet none of the three
is ever cleared by `clear_plugins_manager_caches`, which is scoped narrowly to
`airflow.plugins_manager`/`airflow.sdk.plugins_manager`/the shared module-loading modules only.
Clearing any of the three getters would force the next call to construct a brand-new manager and
(for the two listener getters) re-run `integrate_listener_plugins` against whatever the plugins-manager
cache holds at that later moment -- losing the manager's identity and already-registered hookspecs for
no reason `ListenerManager.clear()` (confirmed to exist and to unregister every currently-registered
pluggy plugin without touching hookspecs, by reading `airflow._shared.listeners.listener.ListenerManager`
directly) does not already serve better. `airflow.settings.get_policy_plugin_manager` returns a bare
`pluggy.PluginManager`, which (unlike `ListenerManager`) exposes `unregister()` directly, confirmed by
reading `pluggy`'s own `PluginManager` and by round-tripping `register`/`unregister` against the
installed 3.3.0's real policy plugin manager end to end.

`src/pytest_airflow_in_a_box/_compat/components.py`'s timetable/weight-strategy registration
(`build_component_plugin`, `invalidate_component_lookup_caches`, `register_timetable`,
`register_weight_strategy`, `timetable_round_trip`) is independently authored. Its embedded facts were
transcribed by reading Apache Airflow source directly: `encode_timetable`/`decode_timetable` are
defined in `airflow/serialization/serialized_objects.py` at commit
`54bd5d8cd9f6f477cc83445737614dec81c4323c` (tag `3.1.0`, where `encode_timetable` raises
`_TimetableNotRegistered` for an unregistered custom timetable at encode time) and re-exported from
`airflow.serialization.encoders`/`decoders` into the same `serialized_objects` location from tag
`3.2.0` onward (confirmed against the installed 3.3.0), so one import location covers every certified
3.x release. The lookup each registration must satisfy: on 3.2+, `decode_timetable` resolves through
`plugins_manager.get_timetables_plugins()` and `_encode_priority_weight_strategy` through
`plugins_manager.get_priority_weight_strategy_plugins()` -- both `functools.cache` functions building
`{qualname(cls): cls}` from every registered plugin's `timetables`/`priority_weight_strategies` list
(confirmed by reading the installed 3.3.0's `plugins_manager.py`, and the reason
`invalidate_component_lookup_caches` clears exactly those two derived caches and never `_get_plugins`,
which holds the appended plugin itself); on 3.1.x the same lookups read the `timetable_classes`/
`priority_weight_strategy_classes` module globals, populated by `initialize_*_plugins()` functions
that no-op while the global is non-None, which is why the 3.1 invalidation resets them to `None`.
Upstream's `qualname()` helper (`airflow/_shared/module_loading.py`, installed 3.3.0) keys a class by
`__module__.__name__`, not `__qualname__`.

`src/pytest_airflow_in_a_box/_compat/dagbag.py::list_dag_file_paths`/`build_partial_dag_bag`
(issue #243) call, rather than adapt, two of Airflow's own file-discovery/import entry points per
certified `DagBagLocation`; `build_partial_dag_bag`'s per-file stat-accumulation loop adapts the
body of `DagBag.collect_dags`. On `DagBagLocation.DAG_PROCESSING` (3.2+), `list_dag_file_paths`
calls `get_importer_registry().list_dag_files()`
(`airflow/dag_processing/importers/base.py::DagImporterRegistry.list_dag_files`, line 222 on the
installed 3.3.0, delegating to `AbstractDagImporter.list_dag_files`, line 128) -- the exact function
`DagBag.collect_dags` itself calls internally (confirmed by reading
`airflow/dag_processing/dagbag.py::collect_dags`, line 442 on the installed 3.3.0, directly: `registry
= get_importer_registry(); files_to_parse = registry.list_dag_files(dag_folder, safe_mode=safe_mode)`).
On `DagBagLocation.MODELS` (3.1.x), it calls `airflow.utils.file.list_py_file_paths` instead --
confirmed present with an identical signature and body on the installed `apache-airflow-core==3.1.0`
wheel's `airflow/utils/file.py`, line 244, delegating to `find_dag_file_paths` (line 268) and
`find_path_from_directory` (line 223), predating the importer-registry split entirely. Both walk-only
functions still exist unmodified on the installed 3.3.0 (`list_py_file_paths` at
`airflow/utils/file.py`, mirrored at `find_dag_file_paths`), so calling either unconditionally on
every release was considered and rejected: doing so on 3.2+ would silently miss any Dag file that
only a plugin-registered custom `DagImporter` (an importer-registry-only concept) surfaces, a
regression `list_dag_file_paths` avoids by dispatching on the same certified location `build_dag_bag`
already does.

`build_partial_dag_bag`'s per-file loop -- construct with `collect_dags=False`, call
`process_file(path, only_if_updated=True, safe_mode=False)` per shard path, and hand-build a
`DagBagShardStat` per file -- is adapted from the body of `DagBag.collect_dags` on both certified
locations (`airflow/dag_processing/dagbag.py`, line 442, and `airflow/models/dagbag.py`, line 574 on
the installed 3.1.0 wheel; both confirmed to construct `FileLoadStat`-equivalent entries only inside
this method, meaning a `collect_dags=False` instance never populates `dagbag_stats` at all). The
adaptation drops `only_if_updated`'s practical effect (always `True`, but a fan-out shard's `DagBag`
is always freshly constructed, so `self.file_last_changed` starts empty and every file is parsed
regardless), the `include_examples`-driven `example_dags` extension present only on `MODELS`, and the
per-release-divergent relative-file computation: 3.1.0's `collect_dags` computes
`filepath.replace(settings.DAGS_FOLDER, "")`, while 3.2.0 (confirmed identical on the installed
`apache-airflow-core==3.2.0` wheel's `airflow/dag_processing/dagbag.py`, line 494 -- this is a
3.2-module-split-era change, not one introduced later within `DAG_PROCESSING`'s own lifetime) and
3.3.0 both compute `Path(filepath).relative_to(Path(self.dag_folder)).as_posix()`, falling back to
`Path(filepath).as_posix()` on `ValueError`. `build_partial_dag_bag` uses the latter, 3.2+ shape
unconditionally on every certified release: the `file` field is consumed only for slowpoke/budget
display strings by `smoke.py`'s corpus checks, never as a correctness-critical merge key (`import_errors`
and `dags` key on the file's real absolute path and `dag_id` respectively, both untouched by this
choice), so one consistent computation is a deliberate simplification, not a compatibility gap.
`DagBag.process_file`'s own signature and return behavior (a plain list of bagged Dags, `import_errors`
populated internally and keyed on the file's relative fileloc, which falls back to the raw absolute
path whenever `bundle_path` is unset -- confirmed unset by both `build_dag_bag` and
`build_partial_dag_bag` alike) is identical on the installed 3.1.0, 3.2.0, and 3.3.0, so no
per-release branch was needed there.

No proprietary source code, credentials, hostnames, internal paths, or private repository history
may be included in this project.

## References

- Apache Airflow: https://github.com/apache/airflow
- Apache Airflow license: https://github.com/apache/airflow/blob/main/LICENSE
- Adapted task-instance helper: https://github.com/apache/airflow/blob/2d374f71bc81202204ac0208df07b07c280668fa/devel-common/src/tests_common/test_utils/taskinstance.py
- Adapted asset-triggered scheduling (3.x): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/jobs/scheduler_job_runner.py
- Adapted asset-triggered readiness evaluation (3.x): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/models/dag.py
- Adapted dataset-triggered scheduling (2.x): https://github.com/apache/airflow/blob/b93c3db6b1641b0840bd15ac7d05bc58ff2cccbf/airflow/jobs/scheduler_job_runner.py
- Timetable Protocol (3.3.0): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/timetables/base.py
- BaseExecutor (3.3.0): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/executors/base_executor.py
- BaseExecutor (3.2.0): https://github.com/apache/airflow/blob/3bc3ccfacc3dec9f359a3b153bfd4fc706c661ba/airflow-core/src/airflow/executors/base_executor.py
- BaseExecutor (3.1.0): https://github.com/apache/airflow/blob/54bd5d8cd9f6f477cc83445737614dec81c4323c/airflow-core/src/airflow/executors/base_executor.py
- Core listener manager (3.3.0): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/listeners/listener.py
- Core-only listener hookspecs (3.3.0): https://github.com/apache/airflow/tree/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/listeners/spec
- Shared lifecycle/taskinstance listener hookspecs (3.3.0): https://github.com/apache/airflow/tree/1438ea3587031417cc85d74323235cf087a058fb/shared/listeners/src/airflow_shared/listeners/spec
- Task SDK listener manager (3.3.0): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/task-sdk/src/airflow/sdk/listener.py
- Policies, including `task_instance_mutation_hook` and `make_plugin_from_local_settings` (3.3.0): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/policies.py
- Policies (3.1.0): https://github.com/apache/airflow/blob/54bd5d8cd9f6f477cc83445737614dec81c4323c/airflow-core/src/airflow/policies.py
- Policies (2.11.2): https://github.com/apache/airflow/blob/2.11.2/airflow/policies.py
- Plugins manager, `is_valid_plugin` duck typing (3.3.0): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/shared/plugins_manager/src/airflow_shared/plugins_manager/plugins_manager.py
- Plugins manager, `is_valid_plugin` via `issubclass` (3.1.0): https://github.com/apache/airflow/blob/54bd5d8cd9f6f477cc83445737614dec81c4323c/airflow-core/src/airflow/plugins_manager.py
- PriorityWeightStrategy (3.3.0): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/task/priority_strategy.py
- PriorityWeightStrategy (3.1.0): https://github.com/apache/airflow/blob/54bd5d8cd9f6f477cc83445737614dec81c4323c/airflow-core/src/airflow/task/priority_strategy.py
- BaseXCom (3.3.0): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/task-sdk/src/airflow/sdk/bases/xcom.py
- BaseNotifier, covering apache/airflow#64649 (3.3.0): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/task-sdk/src/airflow/sdk/bases/notifier.py
- BaseSecretsBackend (3.3.0): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/secrets/base_secrets.py
- Provider discovery, package-name validation and schema (3.3.0): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/shared/providers_discovery/src/airflow_shared/providers_discovery/providers_discovery.py
- Provider discovery, inline on `ProvidersManager` (3.1.0): https://github.com/apache/airflow/blob/54bd5d8cd9f6f477cc83445737614dec81c4323c/airflow-core/src/airflow/providers_manager.py
- Provider info schema (3.3.0): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/provider_info.schema.json
- Plugins manager, core cache functions (3.3.0): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/plugins_manager.py
- Plugins manager, Task SDK cache functions (3.3.0): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/task-sdk/src/airflow/sdk/plugins_manager.py
- Plugins manager, nine core cache functions (3.2.0): https://github.com/apache/airflow/blob/3bc3ccfacc3dec9f359a3b153bfd4fc706c661ba/airflow-core/src/airflow/plugins_manager.py
- Executor loader, flat pre-per-team globals (3.1.0): https://github.com/apache/airflow/blob/54bd5d8cd9f6f477cc83445737614dec81c4323c/airflow-core/src/airflow/executors/executor_loader.py
- ExecutorName (3.1.0): https://github.com/apache/airflow/blob/54bd5d8cd9f6f477cc83445737614dec81c4323c/airflow-core/src/airflow/executors/executor_utils.py
- Settings, `POLICY_PLUGIN_MANAGER` module global (3.1.0): https://github.com/apache/airflow/blob/54bd5d8cd9f6f477cc83445737614dec81c4323c/airflow-core/src/airflow/settings.py
- Plugins manager, `ensure_plugins_loaded`/`integrate_macros_plugins` (3.1.0): https://github.com/apache/airflow/blob/54bd5d8cd9f6f477cc83445737614dec81c4323c/airflow-core/src/airflow/plugins_manager.py
- Shared module loading, `_get_grouped_entry_points` (3.3.0): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/shared/module_loading/src/airflow_shared/module_loading/__init__.py
- Settings, `get_policy_plugin_manager` and `task_instance_mutation_hook.is_noop` (3.3.0): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/settings.py
- Configuration, `secrets_backend_list` and `ensure_secrets_loaded` (3.3.0): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/configuration.py
- Executor loader, the five module globals and `ExecutorLoader.executors` (3.3.0): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/executors/executor_loader.py
- apache/airflow#64649 (notifier crash running Dags locally through tests): https://github.com/apache/airflow/issues/64649
- pytest plugin documentation: https://docs.pytest.org/en/stable/how-to/writing_plugins.html
- `apache-airflow-core` 3.1.0 wheel (module-globals plugins-manager shape; downloaded and introspected directly, not browsed on GitHub): https://pypi.org/project/apache-airflow-core/3.1.0/#files
- `apache-airflow-task-sdk` 1.1.0 wheel (no `plugins_manager.py`/`listener.py` at all, paired with `apache-airflow-core` 3.1.0 per its own `Requires-Dist`; downloaded and introspected directly): https://pypi.org/project/apache-airflow-task-sdk/1.1.0/#files
- Dag processing, `DagBag.collect_dags`/`process_file` and the importer registry (3.3.0, installed): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/dag_processing/dagbag.py and https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/dag_processing/importers/base.py
- Dag processing, `DagBag.collect_dags`' relative-fileloc computation (3.2.0, downloaded and introspected directly): https://pypi.org/project/apache-airflow-core/3.2.0/#files
- Models `DagBag.collect_dags`/`process_file` and `airflow.utils.file.list_py_file_paths`/`find_dag_file_paths` (3.1.0, downloaded and introspected directly): https://pypi.org/project/apache-airflow-core/3.1.0/#files
