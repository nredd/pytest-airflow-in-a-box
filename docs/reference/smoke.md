# Smoke catalog and corpus mechanics

The [Smoke Tests guide](../guide/smoke-tests.md) helps you choose between the catalog,
per-file collection, and coverage. This page defines every generated item and the shared
`DagCorpus` that makes corpus-wide checks portable.

## Catalog

Enable the catalog with `--airflow-smoke` or `airflow_smoke = true`. Each enabled check is a
separate `::smoke::<item>` node carrying the `smoke` and `timeout` markers:

| Item | Enabled when | Contract |
| --- | --- | --- |
| `test_dag_bag_integrity` | Always | Import succeeds, `dag_id`s are unique, and every file stays within the parse timeout. Files above the slowpoke threshold warn; all timings are logged slowest-first. |
| `test_dag_serialization_roundtrip` | Always | Every selected Dag serializes and deserializes through the installed scheduler representation. |
| `test_schedule_sanity` | Always | Every selected, scheduled Dag can compute its first automated run after deserialization. |
| `test_pool_references_exist` | Always | Every task names an existing pool. `airflow_pools` can seed additional `name = slots` rows. |
| `test_no_top_level_variable_access` | `airflow_forbid_top_level_variable_access = true` (default) | AST and runtime interception find no Variable or Connection lookup during import. |
| `test_no_top_level_io` | `airflow_forbid_top_level_io = true` (default) | Statically resolvable calls into configured I/O modules do not run during import. |
| `test_forbid_catchup` | `airflow_forbid_catchup = true` | No scheduled Dag enables catchup. |
| `test_no_unbounded_expand` | `airflow_forbid_unbounded_expand = true` | A task mapped over runtime data sets `max_active_tis_per_dag`; literal expansions are already bounded. |
| `test_dag_id_pattern` | `airflow_dag_id_pattern` is set | Every `dag_id` matches the configured regular expression. |
| `test_required_dag_tags` | `airflow_required_dag_tags` is nonempty | Every Dag carries every required tag. |
| `test_forbid_default_owner` | `airflow_forbid_default_owner = true` | No task uses Airflow's stock `airflow` owner. |
| `test_dag_serialization_snapshot` | `airflow_dag_snapshot_dir` is set | Every selected Dag's normalized serialization matches its committed JSON snapshot. |

`test_dag_bag_integrity` and `test_pool_references_exist` also carry `db_test`. More broadly,
the corpus parse can initialize the metadata database when parse-time secret resolution uses
the metastore. No catalog item starts the REST API.

The top-level secrets check combines an AST pass with lookups intercepted during parsing, so it
can find calls hidden behind helpers. If the corpus reuses a `DagBag` parsed without
instrumentation, it logs that runtime findings are unavailable and retains the AST results.
The I/O check is intentionally conservative: it reports only calls that resolve through the
file's top-level imports to a configured module. `airflow_top_level_io_modules` replaces the
built-in module list instead of extending it.

Exact option types, defaults, and validation rules live in
[CLI and INI options](ini-options.md#smoke-catalog).

## Selection and configuration

Normal pytest selection applies after the synthetic catalog is added: `-m smoke`, `-m "not
smoke"`, `-k`, and `--deselect ::smoke::<item>` all work. A bare run, directory positional,
or `testpaths` run includes the catalog. An explicit test file or node ID excludes it unless a
`-m` expression explicitly mentions `smoke` and can select a real smoke item.

`airflow_smoke_disable` removes exact item names before construction. Unknown names are usage
errors, even when file selection would otherwise exclude the catalog. When the catalog remains
in scope, each disabled check's own configuration is still validated. Disabling every
serialization consumer—the round-trip and schedule checks, plus snapshots when
configured—skips serialization entirely.

The catalog and `dag_corpus` resolve their folder in this order:

1. `--dag-folder` (a relative path uses the invocation directory)
2. `airflow_dags_folder` (a relative path uses pytest's root)
3. the isolated `AIRFLOW_HOME/dags` scratch folder

Per-file collection uses the separate `--collect-dag-folder` /
`airflow_collect_dags_folder` channel described below.

## Serialization cost

The round-trip, schedule, and snapshot checks share one serialization cache.
`airflow_serialization_sample_size` selects Dags by a deterministic SHA-256 rank of the seed
and `dag_id`; `0` or a size at least as large as the corpus selects every Dag. The final sample
is ordered by `dag_id`.

Sampling applies only to a smoke-only corpus. If any surviving test requests `dag_corpus`, the
builder serializes every Dag because it cannot predict which fields that test will inspect.
For an enabled snapshot check, update mode is rejected when a nonzero sample is active: a
partial update could leave the committed set inconsistent.

Set `airflow_dag_snapshot_dir` to compare one `<dag_id>.json` file per selected Dag. Relative
paths resolve from pytest's root. `--airflow-smoke-update` rewrites those snapshots after
removing run-dependent file-location fields; it does not enable the catalog or snapshot check
by itself. Missing snapshots, serialization failures, and unified diffs fail the snapshot item.

Use `--log-cli-level=INFO` to stream parse and serialization progress. Captured output cannot
survive an outer process kill.

## Dag-file collection contract

`--collect-dag-folder` (or `airflow_collect_dags_folder`) recursively collects each `.py` file
whose name does not begin with `_`. Every file gets a `dag-import` item; import errors and files
declaring no Dags fail independently. Each item carries `db_test`, not `smoke`. When pytest's
normal Python collector also owns a file below this folder, its duplicate items are removed.

A module-level literal adds pinned parameter cases without importing the module during
collection:

```python
PYTEST_DAG_CASES = {
    "dev": {"environment": "dev"},
    "prod": {"environment": "prod"},
}
```

Each case becomes `dag-params[<name>]` and validates its mapping against every Dag in that file.
A malformed declaration is a collection error; import failures, unknown parameter names, and
schema-invalid values fail the generated item. This collection path is independent of the
catalog and parses files again when both are enabled.

## Performance and parallelism

`DagCorpus` is the read-mostly, JSON-portable result shared by the catalog and the public
`dag_corpus` fixture:

| Field | Contents |
| --- | --- |
| `dags` | Read-only mapping of `dag_id` to tags, task metadata, scheduling flags, file location, and optional serialization outcome |
| `import_errors` | Read-only mapping of file path to import failure |
| `dagbag_stats` | Immutable per-file duration, Dag-count, and task-count records |
| `runtime_lookups` | Intercepted secret lookups; `None` means interception was unavailable, while `()` means none occurred |
| `producer_pid`, `producer_worker` | Process that built the shared artifact |

Treat each Dag's nested `serialized` dictionary as read-only. Use `dags_under(corpus,
dag_folder, subdir)` for a read-only subtree view without reparsing.

Within one process, the builder reuses an existing live `dag_bag` parse. Under xdist, the first
consumer takes a file lock and publishes a versioned JSON artifact inside the run root; other
workers decode it. With `--dist loadgroup`, the plugin co-locates the catalog with surviving
`dag_corpus` consumers, otherwise with one eligible `dag_bag` consumer, otherwise with itself.
It never replaces an explicit user `xdist_group`. Other distribution modes cannot honor this
grouping and may emit `SmokeColocationWarning` for avoidable decoding or parsing.

For very large repositories, `--airflow-dag-bag-fanout` /
`airflow_dag_bag_fanout = true` shards one Airflow-aware file listing across subprocesses and
merges their portable results, including duplicate-ID detection and parse timings. Fan-out:

- skips below `airflow_dag_bag_fanout_min_files` (default `200`);
- uses the configured worker count, or a CPU-based count bounded by the file count;
- falls back to one serial parse after a spawn failure, timeout, child crash, or invalid output;
- is skipped for a smoke-only nonzero serialization sample, because a shard cannot see the
  complete `dag_id` set; and
- never backs the live `dag_bag` fixture, whose return value must remain a real Airflow
  `DagBag`.

The [canonical option rows](ini-options.md#smoke-catalog) define the fan-out timeout, worker,
threshold, sampling, and parse-time defaults.
