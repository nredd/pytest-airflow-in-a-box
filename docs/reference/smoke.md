# Smoke catalog and corpus mechanics

The [Smoke Tests guide](../guide/smoke-tests.md) chooses among catalog checks, per-file
collection, and coverage. This page records the exact catalog and its scaling behavior.

## Catalog

Enabled by `--airflow-smoke` or `airflow_smoke = true`:

| Item | Contract |
| --- | --- |
| `test_dag_bag_integrity` | No import errors, duplicate IDs, or per-file parse timeout; slow files warn and are logged slowest-first |
| `test_dag_serialization_roundtrip` | Every selected Dag survives scheduler serialization |
| `test_schedule_sanity` | Every selected scheduled Dag computes its next run |
| `test_pool_references_exist` | Every task pool exists; custom pools come from `airflow_pools = name = slots` lines |
| `test_no_top_level_variable_access` | AST and runtime interception find no Variable or Connection lookup during import |
| `test_no_top_level_io` | Statically resolvable calls into configured network/database modules do not run at import |

Optional items appear only when their ini option is configured:

| Option | Item / contract |
| --- | --- |
| `airflow_forbid_catchup` | Scheduled Dags do not enable catchup |
| `airflow_forbid_unbounded_expand` | Runtime-mapped tasks set `max_active_tis_per_dag` |
| `airflow_dag_id_pattern` | Every `dag_id` matches the regex |
| `airflow_required_dag_tags` | Every Dag carries every required tag |
| `airflow_forbid_default_owner` | No task uses the stock `airflow` owner |
| `airflow_dag_snapshot_dir` | Serialized structure matches committed snapshots; update with `--airflow-smoke-update` |

The top-level I/O check is deliberately conservative: it flags a call only when the callee
resolves through the file's top-level imports to a listed module. Aliased indirection may escape
rather than create false positives. `airflow_top_level_io_modules` replaces the built-in list.

## Selection and configuration

Every catalog item carries `smoke` and `timeout`; database-backed items also carry `db_test`.
`-m smoke`, `-m "not smoke"`, `-k`, and `--deselect ::smoke::<name>` work normally.

An explicit test file or node ID suppresses the synthetic catalog. A directory positional,
bare run, or `testpaths` run keeps it. An explicit `-m` expression mentioning `smoke` overrides
file/node scoping; unrelated marker expressions do not.

`airflow_smoke_disable` removes named items before construction. When all serialization-backed
items are disabled, the builder skips serialization entirely. A sample size of zero means every
Dag, not none.

Exact defaults and types are in [INI options](ini-options.md#smoke-catalog).

## Serialization cost

The round-trip, schedule, and snapshot checks share one serialized-Dag cache.
`airflow_serialization_sample_size` selects a deterministic sample by hashing `dag_id` with
`airflow_serialization_sample_seed`; `0` selects all Dags. Sampling is incompatible with
`--airflow-smoke-update`, which must regenerate every snapshot.

`--log-cli-level=INFO` streams progress for an expensive serialization pass. Captured logs do
not survive an outer process kill.

## Dag-file collection contract

`--collect-dag-folder` walks `.py` files recursively, skips names beginning with `_`, and adds a
`db_test`-marked `dag-import` item. Files that pytest's normal Python collector also owns are
deduplicated after collection.

A module-level literal can create pinned parameter items:

```python
PYTEST_DAG_CASES = {
    "dev": {"environment": "dev"},
    "prod": {"environment": "prod"},
}
```

The literal is parsed without importing the module. Unknown parameter names and schema-invalid
values fail against every Dag declared by that file.

## Performance and parallelism

`DagCorpus` stores read-only portable metadata, import errors, parse timings, intercepted
lookups, and serialized records. Within a process, an existing `dag_bag` parse is reused. Under
xdist, the first consumer takes a file lock, parses once, and publishes a versioned JSON
artifact; other workers decode it instead of reparsing.

Under `--dist loadgroup`, the plugin co-locates the catalog with every surviving `dag_corpus`
consumer, otherwise one eligible `dag_bag` anchor, otherwise the catalog itself. Explicit user
groups are never overwritten. Plain `-n auto` uses `load`, where `xdist_group` is inert; a
`SmokeColocationWarning` identifies avoidable extra decoding or parsing.

For thousands of files, enable subprocess fan-out:

```ini
[pytest]
airflow_dag_bag_fanout = true
```

The builder walks once, shards files across workers, and merges portable results. Fan-out skips
below `airflow_dag_bag_fanout_min_files` (default 200) and falls back to serial parsing after
spawn, timeout, crash, or decode failure. It is incompatible with a nonzero serialization
sample because a shard lacks the complete `dag_id` set.

Fan-out cannot produce the real `DagBag` promised by `dag_bag`; it applies only to the portable
corpus. Prefer `dag_corpus` whenever static metadata is sufficient.
