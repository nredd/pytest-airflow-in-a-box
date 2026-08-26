# INI options

Every option below goes in the standard pytest ini locations: `pytest.ini` / `tox.ini` /
`setup.cfg` under `[pytest]` (or `[tool:pytest]`), or `pyproject.toml` under
`[tool.pytest.ini_options]`. Where a `--airflow-*` CLI flag has an ini twin, the flag wins for
one invocation and the ini value pins the repo default. The plugin needs *zero* ini
configuration -- every option has a working default, and there is no required section.

Types: `string` and `bool` are pytest's usual ini scalars; `linelist` is one entry per line in
ini files and an array of strings in `pyproject.toml`.

## Core

| Option | Type | Default | Does | CLI twin |
| --- | --- | --- | --- | --- |
| `airflow_home` | string | `""` | Base directory the isolated per-run `AIRFLOW_HOME` is provisioned under. The [`airflow_home` fixture](fixtures.md) returns the per-run root created below it | `--airflow-home` |
| `airflow_db_backend` | string | `sqlite` | Metadata database backend: `sqlite` or `postgres` | `--airflow-db-backend` |
| `airflow_dags_folder` | string | `""` | Directory parsed by the [`dag_bag` fixture](fixtures.md) | `--dag-folder` |
| `airflow_collect_dags_folder` | string | `""` | Directory whose Dag files are collected as import-check test items | `--collect-dag-folder` |
| `airflow_parse_secrets` | string | `metastore` | Parse-time Variable and Connection resolution: `metastore` or `off` | `--airflow-parse-secrets` |
| `airflow_executor` | string | `""` | Executor written to `[core] executor` before the first Airflow import | -- |
| `airflow_executor_timeout` | string | `300` | Seconds one task instance may take to settle during an executor-driven `run_dag` before the run fails naming the stuck instance | `--airflow-executor-timeout` |
| `airflow_local_settings` | string | `""` | Dotted module path composed into the generated `airflow_local_settings.py` | -- |
| `airflow_plugins_folder` | string | `""` | Directory whose entries are symlinked into the run's `plugins/` directory | -- |
| `airflow_xcom_backend` | string | `""` | XCom backend written to `[core] xcom_backend` before the first Airflow import | -- |
| `airflow_secrets_backend` | string | `""` | Secrets backend written to `[secrets] backend` before the first Airflow import | -- |
| `airflow_secrets_backend_kwargs` | string | `""` | Secrets backend kwargs written to `[secrets] backend_kwargs` | -- |
| `airflow_pools` | linelist | `[]` | Pools seeded before `test_pool_references_exist` runs, as `name = slots` lines | -- |
| `airflow_environments` | linelist | `[]` | Test environment sentinel paths as `name = path` lines, consumed by the [`environment` marker](markers.md) | -- |
| `allow_network_airflow_home` | bool | `False` | Allow an explicit Airflow storage base on a network filesystem | `--allow-network-airflow-home` |
| `airflow_worker_env_drift` | string | `error` | Response when an xdist worker or isolated child inherits an Airflow environment another plugin mutated: `error` or `repair` | -- |
| `airflow_migration_strict` | bool | `False` | Promote Airflow's 2->3 deprecation categories to test-phase errors on a 2.x run; a no-op on 3.x | `--airflow-migration-strict` |
| `airflow_report_dir` | string | `""` | Directory receiving the `pytest.log` and `pytest.xml` report artifacts | `--airflow-report-dir` |
| `airflow_baseline_allow_incomplete` | bool | `False` | Accept a baseline or prior-live artifact recorded from an incomplete session | `--airflow-baseline-allow-incomplete` |

## Retention

| Option | Type | Default | Does | CLI twin |
| --- | --- | --- | --- | --- |
| `airflow_home_retention_policy` | string | `failed` | Which runs keep the isolated Airflow run directory: `all`, `failed`, or `none` | `--airflow-home-retention` |
| `airflow_home_retention_count` | string | `3` | Maximum retained Airflow run directories per storage base; older ones are garbage-collected whenever a run is retained | `--airflow-home-retention-count` |

## Smoke catalog

All read only when the catalog is enabled (`airflow_smoke` / `--airflow-smoke`); see the
[diagnostics reference](diagnostics.md) for the catalog itself.

| Option | Type | Default | Does | CLI twin |
| --- | --- | --- | --- | --- |
| `airflow_smoke` | bool | `False` | Enable the bundled opt-in `smoke` test catalog | `--airflow-smoke` |
| `airflow_dag_parse_timeout` | string | `30` | Per-file Dag parse timeout in seconds for the smoke integrity test. Also pins `core.dagbag_import_timeout` and scales the slowpoke budget | -- |
| `airflow_dag_parse_slowpoke_ratio` | string | `0.75` | Fraction of the parse timeout above which a file is a slowpoke | -- |
| `airflow_dag_parse_budget_ratio` | string | `10` | Fail Dag files parsing slower than this multiple of the corpus median; `0` disables | -- |
| `airflow_dag_id_pattern` | string | `""` | Regex every collected `dag_id` must match | -- |
| `airflow_required_dag_tags` | linelist | `[]` | Tags every collected Dag must carry | -- |
| `airflow_smoke_disable` | linelist | `[]` | Bundled smoke item names to drop from the catalog (e.g. `test_schedule_sanity`) | -- |
| `airflow_forbid_default_owner` | bool | `False` | Fail Dags whose tasks are owned by the stock `airflow` owner | -- |
| `airflow_forbid_top_level_variable_access` | bool | `True` | Fail Dag files that fetch Variables or Connections at import time | -- |
| `airflow_forbid_top_level_io` | bool | `True` | Fail Dag files that call into known I/O modules at import time | -- |
| `airflow_top_level_io_modules` | linelist | `[]` | Module prefixes the top-level I/O check flags; *replaces* the built-in list | -- |
| `airflow_forbid_catchup` | bool | `True` | Fail Dags that enable catchup | -- |
| `airflow_forbid_unbounded_expand` | bool | `True` | Fail mapped tasks expanding over runtime data without `max_active_tis_per_dag` | -- |
| `airflow_dag_snapshot_dir` | string | `""` | Directory of committed Dag serialization snapshots the catalog diffs against (regenerate with `--airflow-smoke-update`) | -- |
| `airflow_serialization_sample_size` | string | `0` | Number of Dags the serialization checks cover; `0` covers every Dag | -- |
| `airflow_serialization_sample_seed` | string | `0` | Seed for deterministic selection of the serialization sample | -- |
| `airflow_dag_bag_fanout` | bool | `False` | Fan the smoke corpus's Dag parse out across subprocess workers for large corpora | `--airflow-dag-bag-fanout` |
| `airflow_dag_bag_fanout_workers` | string | `0` | Subprocess worker count for fan-out; `0` auto-selects a CPU-based default | -- |
| `airflow_dag_bag_fanout_min_files` | string | `200` | Minimum discovered Dag file count below which fan-out is skipped even when enabled | -- |
| `airflow_dag_bag_fanout_timeout` | string | `600` | Seconds the whole fan-out may take before falling back to a serial parse | -- |

## Warnings

| Option | Type | Default | Does | CLI twin |
| --- | --- | --- | --- | --- |
| `airflow_default_filterwarnings` | linelist | `ignore:No path_separator found in configuration:DeprecationWarning` | Warning filters covering the plugin's own metadata-database bootstrap. That bootstrap runs inside its own reset warnings context that plain `filterwarnings` ini lines cannot reach, so this option's lines are both prepended below your `filterwarnings` lines *and* replayed inside that context. Redefining it -- even to empty -- replaces the default wholesale, the escape hatch for warnings-as-errors suites | -- |

Lines use pytest's `filterwarnings` syntax and are parsed eagerly at configure time, so a
malformed value is a usage error rather than an explosion mid-bootstrap.

## Re-registered pytest builtins

The plugin re-registers two builtin ini defaults (last registration wins; an explicit user
value always beats either default):

| Option | pytest default | plugin default |
| --- | --- | --- |
| `tmp_path_retention_policy` | `all` | `failed` |
| `tmp_path_retention_count` | `3` | `3` |

It also rewrites three display options while they still equal pytest's parser default:
`--tb=short`, `-ra`, `--durations=20`. An explicit `--tb=auto` is indistinguishable from the
parser default and is overridden; any other explicit value survives.

## `airflow_config`

Repo-wide Airflow configuration as `section.key = value` lines, each applied as an
`AIRFLOW__SECTION__KEY` environment variable:

```ini
# pytest.ini
[pytest]
airflow_config =
    core.dag_ignore_file_syntax = glob
    core.dagbag_import_timeout = 120
```

```toml
# pyproject.toml
[tool.pytest.ini_options]
airflow_config = [
    "core.dag_ignore_file_syntax = glob",
    "core.dagbag_import_timeout = 120",
]
```

### Grammar

One option per line. The value splits on the *first* `=` (values legitimately contain more:
connection URLs, query strings); section and key split on the *last* `.` (Airflow section
names may contain dots, keys may not). Section, key, and value are each stripped. An empty
value means the empty string -- there is no line syntax for "make this option absent", which
the programmatic `airflow_config` form spells `None`. Two lines resolving to one variable are
an error, including `a.b.k` and `a_b.k`, which both mangle to `AIRFLOW__A_B__K` and which
Airflow cannot address separately. Every line is validated before any override is applied, so
a malformed entry never reaches the environment half-applied.

### When it applies

From `pytest_load_initial_conftests`, immediately after bootstrap -- before any consumer
conftest is imported and therefore before any Dag parse. This is the *point* of the option:
nothing you write in a `conftest.py` can beat the first `DagBag` build, because pytest's own
conftest-collecting hookimpl is `trylast`. Under xdist the overrides reach every worker, which
re-applies the identical inherited values. Everything stays in the environment; nothing is
written into the generated `airflow.cfg`.

### Bootstrap-owned keys are rejected

A line naming an option the plugin's bootstrap owns is a usage error naming the supported knob
instead. The denied set is *derived* from bootstrap's own environment-name list, so it cannot
go stale; setting one of these would either be scrubbed by bootstrap's environment install,
break the metadata-database isolation, or desynchronize an xdist controller from its workers
(workers cross-check the inherited environment and would abort with a drift error naming a
variable you never typed). It covers the metadata database URL and pool flag,
`core.dags_folder`, `core.plugins_folder`, `logging.base_log_folder`, `core.xcom_backend`, the
`secrets.backend` pair, and the rest of bootstrap's environment surface -- each rejection names
the ini option or flag to use instead (e.g. `airflow_db_backend`, `airflow_dags_folder`,
`airflow_secrets_backend`).

`core.executor` is deliberately *not* denied: bootstrap does not own it, and
[`--airflow-doctor`](diagnostics.md) already tells you to override it.

One key is rejected only *conditionally*: `core.dagbag_import_timeout` is fine on an ordinary
run, but an error while the smoke catalog is enabled, because the catalog pins that same
variable from `airflow_dag_parse_timeout` -- which also scales the per-file parse watchdog and
the slowpoke budget. Set `airflow_dag_parse_timeout` instead; it is the one knob driving all
three.

For per-test and session-fixture override forms (`airflow_config` the context manager,
`airflow_configure` the fixture), see the [fixtures reference](fixtures.md).
