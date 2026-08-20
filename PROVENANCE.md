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
- apache/airflow#64649 (notifier crash running Dags locally through tests): https://github.com/apache/airflow/issues/64649
- pytest plugin documentation: https://docs.pytest.org/en/stable/how-to/writing_plugins.html
