# CLI and INI options

The plugin owns 20 pytest command-line flags and 42 ini options. This page is the complete
catalog. All command-line flags are spelled exactly as registered; none has a short or
alternate alias.

Put persistent values in any standard pytest configuration file: `pytest.ini`, `tox.ini`, or
`setup.cfg` under `[pytest]` (or `[tool:pytest]`), or `pyproject.toml` under
`[tool.pytest.ini_options]`. For a flag with an ini twin, the flag wins for that invocation.
The plugin requires no configuration.

## Plugin-owned CLI flags

### Paths, storage, and execution

| Flag | Alias | Value and default | INI twin | Effect and detail |
| --- | --- | --- | --- | --- |
| `--dag-folder=PATH` | none | path; automatic scratch `dags/` fallback | `airflow_dags_folder` | Selects the folder parsed by `dag_bag`, `dag_corpus`, `run_dag`, and smoke checks. [Dag folder options](../guide/smoke-tests.md#the-two-dag-folder-options) |
| `--collect-dag-folder=PATH` | none | path; unset | `airflow_collect_dags_folder` | Recursively collects each eligible Dag file as an import-check item; those items activate the database. [Dag-file collection](smoke.md#dag-file-collection-contract) |
| `--airflow-home=PATH` | none | base path; automatic storage ladder | `airflow_home` | Creates the unique run directory below `PATH`; it does not select the exact `AIRFLOW_HOME` name. [Storage ladder](../internals/test-environments.md#the-isolated-airflow_home) |
| `--allow-network-airflow-home` | none | switch; off | `allow_network_airflow_home` | Permits an explicitly selected network or unclassified storage base despite SQLite's locking risk. [Storage boundary](../internals/test-environments.md#the-isolated-airflow_home) |
| `--airflow-home-retention=POLICY` | none | `all`, `failed`, or `none`; `failed` | `airflow_home_retention_policy` | Chooses which run directories survive teardown; Postgres containers always stop. [Retention](../internals/test-environments.md#the-isolated-airflow_home) |
| `--airflow-home-retention-count=N` | none | positive integer; `3` | `airflow_home_retention_count` | Keeps at most `N` retained run directories per storage base. [Retention](../internals/test-environments.md#the-isolated-airflow_home) |
| `--airflow-db-backend=BACKEND` | none | `sqlite` or `postgres`; `sqlite` | `airflow_db_backend` | Selects the disposable metadata backend. Postgres needs the `postgres` extra and Docker. [Database backends](../internals/test-environments.md#the-disposable-metadata-database) |
| `--airflow-executor-timeout=SECONDS` | none | positive number; `300` | `airflow_executor_timeout` | Bounds each task instance in an executor-driven run. Executor mode is Airflow 3 only and starts the API. [Executor-driven runs](../guide/ladder.md#executor-driven-runs) |
| `--airflow-parse-secrets=POLICY` | none | `metastore` or `off`; `metastore` | `airflow_parse_secrets` | Controls the Variable/Connection shim during plugin-owned Dag parses. It is a no-op on Airflow 2. [Parse-time secrets](../internals/test-environments.md#parse-time-secret-resolution) |

### Smoke and corpus control

| Flag | Alias | Value and default | INI twin | Effect and detail |
| --- | --- | --- | --- | --- |
| `--airflow-smoke` | none | switch; off | `airflow_smoke` | Enables the generated smoke catalog. Its default checks initialize the database lazily. [Smoke Tests](../guide/smoke-tests.md) |
| `--airflow-smoke-update` | none | switch; off | none | Writes configured Dag serialization snapshots instead of comparing them. It does not itself enable the catalog. [Snapshot contract](smoke.md#catalog) |
| `--airflow-dag-bag-fanout` | none | switch; off | `airflow_dag_bag_fanout` | Parses a large portable corpus in subprocess shards. It never changes the live `dag_bag` fixture. [Fan-out](smoke.md#performance-and-parallelism) |

### Diagnostics and reports

| Flag | Alias | Value and default | INI twin | Effect and detail |
| --- | --- | --- | --- | --- |
| `--airflow-doctor` | none | switch; off | none | Prints the bootstrapped environment and exits before collection, workers, or tests. [Airflow Doctor](diagnostics.md) |
| `--airflow-report-dir=PATH` | none | path; unset | `airflow_report_dir` | Creates `pytest.log` and `pytest.xml` unless explicit pytest logging/JUnit destinations win. [Report artifacts](../guide/ci/github-action.md#report-artifacts) |

### Migration runs

| Flag | Alias | Value and default | INI twin | Effect and detail |
| --- | --- | --- | --- | --- |
| `--airflow-migration-strict` | none | switch; off | `airflow_migration_strict` | Promotes Airflow's 2-to-3 deprecations during test phases. It is a reported no-op outside Airflow 2. [Strict mode](../guide/migration.md#migration-strict-mode) |
| `--airflow-record=PATH` | none | path; unset | none | Writes one outcome artifact at session finish; the controller writes under xdist. [Artifact schema](migration.md#artifact-schema) |
| `--airflow-baseline=PATH` | none | path; unset | none | Compares live outcomes with a recorded artifact and prints migration categories. [Categories](migration.md#categories) |
| `--airflow-baseline-select=MODE` | none | `passing`, `failing`, or `new`; unset | none | Collects only the chosen baseline bucket; requires `--airflow-baseline`. [Migration workflow](../guide/migration.md#diffing-outcomes-across-the-upgrade) |
| `--airflow-baseline-xfail=PATH` | none | prior-live artifact path; unset | none | Non-strict-xfails known regressions; requires `--airflow-baseline`. [Migration workflow](../guide/migration.md#diffing-outcomes-across-the-upgrade) |
| `--airflow-baseline-allow-incomplete` | none | switch; off | `airflow_baseline_allow_incomplete` | Accepts an incomplete baseline or prior-live artifact. [Incomplete artifacts](migration.md#artifact-schema) |

`airflow-migration-diff` is a separate console script, not a pytest plugin flag. Its options
are listed under [Orchestrator options](migration.md#orchestrator-options); arguments after its
`--` separator are forwarded unchanged to both pytest runs.

## Plugin-owned INI options

Types use pytest's ini grammar: `string` is a scalar, `bool` is a boolean, and `linelist` is
one entry per line (an array of strings in `pyproject.toml`). Empty strings and empty lists mean
"not configured."

### Core

| Option | Type | Default | Effect |
| --- | --- | --- | --- |
| `airflow_home` | string | `""` | Base for the isolated run directory; see the [storage ladder](../internals/test-environments.md#the-isolated-airflow_home). |
| `airflow_db_backend` | string | `sqlite` | `sqlite` or `postgres`; see [database backends](../internals/test-environments.md#the-disposable-metadata-database). |
| `airflow_dags_folder` | string | `""` | Folder parsed by `dag_bag`, `dag_corpus`, `run_dag`, and smoke checks. |
| `airflow_collect_dags_folder` | string | `""` | Folder collected as per-file Dag import items. |
| `airflow_parse_secrets` | string | `metastore` | `metastore` or `off`; see [parse-time secrets](../internals/test-environments.md#parse-time-secret-resolution). |
| `airflow_executor` | string | `""` | Value written to `[core] executor` before Airflow import. |
| `airflow_executor_timeout` | string | `300` | Positive seconds allowed per task instance in an executor-driven run. |
| `airflow_local_settings` | string | `""` | Dotted module composed into generated `airflow_local_settings.py`; see [cluster policies](../internals/test-environments.md#cluster-policies-and-airflow_local_settingspy). |
| `airflow_plugins_folder` | string | `""` | Directory whose entries are symlinked into the run's plugins directory; see [registration](../guide/custom-components-wiring.md#session-configuration). |
| `airflow_xcom_backend` | string | `""` | Value written to `[core] xcom_backend` before Airflow import. |
| `airflow_secrets_backend` | string | `""` | Value written to `[secrets] backend` before Airflow import. |
| `airflow_secrets_backend_kwargs` | string | `""` | Value written to `[secrets] backend_kwargs` before Airflow import. |
| `airflow_pools` | linelist | `[]` | `name = slots` entries seeded before the pool-reference smoke check. |
| `airflow_environments` | linelist | `[]` | `name = path` sentinels consumed by the [`environment` marker](markers.md#environment-gate-environmentname). |
| `airflow_config` | linelist | `[]` | Early `section.key = value` overrides; grammar and ownership rules are [below](#airflow_config). |
| `allow_network_airflow_home` | bool | `False` | Allows an explicit Airflow storage base on a network or unclassified filesystem. |
| `airflow_worker_env_drift` | string | `error` | `error` or `repair` when a worker or isolated child inherits mutated bootstrap-owned variables; see [environment ownership](../internals/test-environments.md#pytest-xdist-and-environment-ownership). |
| `airflow_migration_strict` | bool | `False` | Enables [migration-strict mode](../guide/migration.md#migration-strict-mode). |
| `airflow_report_dir` | string | `""` | Directory receiving derived `pytest.log` and `pytest.xml` artifacts. |
| `airflow_baseline_allow_incomplete` | bool | `False` | Allows incomplete baseline and prior-live artifacts. |

### Retention

| Option | Type | Default | Effect |
| --- | --- | --- | --- |
| `airflow_home_retention_policy` | string | `failed` | `all`, `failed`, or `none`; chooses which run directories survive teardown. |
| `airflow_home_retention_count` | string | `3` | Positive maximum retained directories per storage base. |

### Smoke catalog

These values are consumed by the smoke catalog or its shared corpus builder. The
[smoke reference](smoke.md) defines every generated item and the interactions among sampling,
snapshots, selection, and fan-out.

| Option | Type | Default | Effect |
| --- | --- | --- | --- |
| `airflow_smoke` | bool | `False` | Enables the generated catalog. |
| `airflow_dag_parse_timeout` | string | `30` | Positive per-file parse timeout in seconds; also pins `core.dagbag_import_timeout` and scales the slowpoke budget. |
| `airflow_dag_parse_slowpoke_ratio` | string | `0.75` | Fraction in `(0, 1]` above which a parse is logged as slow. |
| `airflow_dag_id_pattern` | string | `""` | Optional regular expression every `dag_id` must match. |
| `airflow_required_dag_tags` | linelist | `[]` | Tags every Dag must carry. |
| `airflow_smoke_disable` | linelist | `[]` | Exact generated item names removed from the catalog. |
| `airflow_forbid_default_owner` | bool | `False` | Enables the stock-`airflow` owner policy. |
| `airflow_forbid_top_level_variable_access` | bool | `True` | Enables import-time Variable and Connection lookup checks. |
| `airflow_forbid_top_level_io` | bool | `True` | Enables import-time calls into known I/O-module checks. |
| `airflow_top_level_io_modules` | linelist | `[]` | Replaces, rather than extends, the built-in module-prefix list. |
| `airflow_forbid_runtime_varying_dag_args` | bool | `True` | Enables the runtime-varying Dag/task constructor argument check. |
| `airflow_forbid_catchup` | bool | `False` | Enables the no-catchup policy. |
| `airflow_forbid_unbounded_expand` | bool | `False` | Requires runtime-mapped tasks to set `max_active_tis_per_dag`. |
| `airflow_dag_snapshot_dir` | string | `""` | Directory containing committed serialization snapshots. |
| `airflow_serialization_sample_size` | string | `0` | Non-negative number of Dags sampled; `0` means every Dag. |
| `airflow_serialization_sample_seed` | string | `0` | Integer seed for deterministic sample selection. |
| `airflow_dag_bag_fanout` | bool | `False` | Enables subprocess parsing for the portable corpus. |
| `airflow_dag_bag_fanout_workers` | string | `0` | Non-negative worker count; `0` selects a CPU-based default. |
| `airflow_dag_bag_fanout_min_files` | string | `200` | Non-negative file threshold below which fan-out is skipped. |
| `airflow_dag_bag_fanout_timeout` | string | `600` | Positive whole-fan-out timeout in seconds before serial fallback. |

### Warnings

| Option | Type | Default | Effect |
| --- | --- | --- | --- |
| `airflow_default_filterwarnings` | linelist | `ignore:No path_separator found in configuration:DeprecationWarning` | Warning filters replayed inside metadata bootstrap and prepended to pytest's `filterwarnings`. Defining this option, even as empty, replaces the default. |

Lines use pytest's `filterwarnings` syntax and are parsed during configuration, so malformed
values are usage errors rather than failures during database bootstrap.

## Relevant pytest and companion-plugin flags

These flags are not owned by `pytest-airflow-in-a-box`. They appear here because the plugin
changes their defaults, reads them, or coordinates behavior around them. See pytest's
[complete command-line reference](https://docs.pytest.org/en/stable/reference/reference.html#command-line-flags)
for everything else.

| Flag | Relationship to this plugin |
| --- | --- |
| `-o NAME=VALUE`, `--override-ini=NAME=VALUE` | Overrides any ini value above for one run. |
| `-p no:pytest_airflow_in_a_box` | Disables this plugin for the invocation; see the [Quickstart](../quickstart.md#where-next). |
| `--basetemp=PATH` | Its parent becomes the caller-selected rung of the [Airflow storage ladder](../internals/test-environments.md#the-isolated-airflow_home). |
| `-n N`, `--numprocesses=N`, `--dist=loadgroup` | Owned by pytest-xdist. `loadgroup` is required for the plugin's `xdist_group` colocation; see [smoke parallelism](smoke.md#performance-and-parallelism). |
| `-m EXPR`, `-k EXPR`, `--deselect=NODEID` | Select generated smoke items through normal pytest collection; see [smoke selection](smoke.md#selection-and-configuration). These also pass through `airflow-migration-diff -- ...` to both family runs. |
| `--cov=SOURCE`, `--no-cov` | Owned by pytest-cov. Doctor reads them only to diagnose whether the Dag folder is measured; see [coverage verdicts](diagnostics.md#coverage-verdicts). |
| `--log-file=PATH`, `--log-file-level=LEVEL`, `--log-level=LEVEL`, `--junit-xml=PATH` | Explicit destinations or levels override values derived from `--airflow-report-dir`; see [report artifacts](../guide/ci/github-action.md#report-artifacts). |
| `--tb=STYLE`, `-r CHARS`, `--durations=N` | Pytest built-ins whose untouched defaults become `short`, `a`, and `20`. Any non-default explicit value survives. |
| `-q`, `--no-header` | Suppress the plugin's `AIRFLOW_HOME` session-header line along with pytest's normal header. |
| `--max-warnings=N` | Pytest raises this failure after retention is decided; use `--airflow-home-retention=all` when investigating such a run. [Known retention edge](../internals/test-environments.md#the-isolated-airflow_home) |

The plugin also re-registers two pytest builtin ini names. User values still win:

| INI option | Pytest default | Plugin default |
| --- | --- | --- |
| `tmp_path_retention_policy` | `all` | `failed` |
| `tmp_path_retention_count` | `3` | `3` |

## `airflow_config`

Use `airflow_config` for repo-wide Airflow configuration that must exist before consumer
conftests or Dag files import Airflow:

```ini
[pytest]
airflow_config =
    core.dag_ignore_file_syntax = glob
    core.dagbag_import_timeout = 120
```

```toml
[tool.pytest.ini_options]
airflow_config = [
    "core.dag_ignore_file_syntax = glob",
    "core.dagbag_import_timeout = 120",
]
```

### Grammar

Each line is `section.key = value`. The parser splits the value on the first `=` and the
section/key on the last `.`, then strips all three fields. An empty value means an empty
string. Duplicate lines that resolve to the same `AIRFLOW__SECTION__KEY` variable are errors,
including names that collide after underscore mangling. Every line is validated before any
override is applied.

The plugin applies these values as environment variables during
`pytest_load_initial_conftests`, after bootstrap but before consumer conftests. They reach
xdist workers and never modify generated `airflow.cfg`.

### Bootstrap-owned keys

`airflow_config` rejects keys the plugin must own to preserve isolation: the database URL and
pool flag, Dag and plugins folders, log folder, XCom backend, secrets backend and kwargs, and
the rest of bootstrap's environment surface. Each error names the supported plugin option.
`core.executor` is deliberately allowed.

`core.dagbag_import_timeout` is allowed on ordinary runs but rejected while the smoke catalog
is enabled. Set `airflow_dag_parse_timeout` then, because it controls Airflow's import timeout,
the plugin watchdog, and the slowpoke threshold together.

For test- and fixture-scoped alternatives, see
[configuration scopes](../internals/test-environments.md#overriding-configuration) and the
[Fixtures reference](fixtures.md#configuration-and-paths).
