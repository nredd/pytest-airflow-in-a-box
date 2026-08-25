# Smoke checks over every Dag

Properties of the *set* of Dags, asserted with no test body to write: no two files claim the
same `dag_id`, no file fetches a Variable at import time, no scheduled Dag has `catchup=True`,
no mapped task expands over runtime data without a concurrency cap. A per-Dag test cannot
phrase any of those, because none of them are about one Dag.

Turn the catalog on:

```console
pytest --airflow-smoke --dag-folder=dags/
```

or persistently via the `airflow_smoke` ini option. Ten items collect by default; four more
appear once you configure the ini option that enables them.

## Why the default items are in scope

[Deciding which failures are yours](testing-scope.md) rules out asserting on Airflow's own
mechanisms, and two always-on items look like exactly that. The
[stock carve-out](testing-scope.md#out-of-scope) is why they are not:

- `test_dag_serialization_roundtrip` -- the subject is *your* operator's constructor
  arguments, which are the part that fails to serialize and takes the whole Dag out of the
  scheduler. Airflow's serializer is not the subject
- `test_schedule_sanity` -- the subject is *your* `schedule=`, `start_date`, and any timetable
  you wrote, not the timetable machinery

Neither asserts anything about a stock component in isolation. Drop either with
`airflow_smoke_disable` if you disagree.

## Overlap with `--collect-dag-folder`

Both parse the same corpus, and the import-failure message is byte-identical
(`smoke.py:1069` vs `collection.py:341`). Enabling both parses the Dag folder **twice**.

Rule: `--airflow-smoke` when you want corpus-wide policy and one row per check;
[`--collect-dag-folder`](dag-collection.md) when you want one selectable pytest item per Dag
file. Pick one unless you genuinely want both shapes of report.

## Selecting and disabling items

Every item carries `smoke` (plus `timeout`, and `db_test` on the Dag-integrity and
pool-reference checks), so `-m smoke` / `-m "not smoke"` select exactly the bundled catalog.

Explicit selection is honored: pointing pytest at a file or node ID (`pytest tests/test_x.py`,
`pytest tests/test_x.py::test_one`) runs *only* that selection and drops the catalog, while
directory positionals (`pytest tests/`), bare runs, and `testpaths`-driven runs keep it --
unless an explicit `-m` expression mentions `smoke` and would itself select a real smoke item
(`-m smoke`, `-m "smoke and db_test"`), in which case that unambiguous opt-in overrides the
file/node-ID scoping and the catalog stays in. An `-m` expression that never mentions `smoke`
(`-m db_test`, `-m "not slow"`) does not, even if it happens to match some smoke item's other
marks -- otherwise an unrelated filter on an explicitly scoped run could silently pull in the
whole catalog. `-k` and `--deselect ::smoke::<name>` apply to the items as usual.

Drop an item permanently with `airflow_smoke_disable`, a list of item names. Unlike
`--deselect`, which filters after collection, a disabled item is never synthesized at all --
if every serialization-backed item (`test_dag_serialization_roundtrip`, `test_schedule_sanity`,
and, when configured, `test_dag_serialization_snapshot`) is disabled, the corpus builder skips
calling Airflow's Dag serializer entirely, which `airflow_serialization_sample_size` alone
cannot do (`0` means every Dag, not none):

```ini
[pytest]
airflow_smoke_disable =
    test_dag_serialization_roundtrip
    test_schedule_sanity
```

## The catalog

On by default whenever the catalog is enabled:

- `test_dag_bag_integrity` -- fails on import errors and per-file parse timeouts
  (`airflow_dag_parse_timeout`, default `30` seconds, exported as
  `AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT` so Airflow hard-kills runaway files); warns with
  `SlowDagParseWarning` on files above `airflow_dag_parse_slowpoke_ratio` (default `0.75`) of
  the timeout without failing the run; logs a slowest-first parse-timing table
- `test_dag_serialization_roundtrip` -- every parsed Dag survives Airflow's scheduler
  serialization round trip. A Dag that does not is dropped by the scheduler entirely. Logs a
  slowest-first per-Dag timing table and carries a corpus-scaled `pytest-timeout` deadline
  (floored at 30 seconds, so a tuned-down parse timeout cannot starve the serialization pass)
  so a pathological Dag is named before an outer CI timeout
- `test_no_duplicate_dag_ids` -- no two Dag files declare the same `dag_id`. The scheduler
  keeps one of them and you do not get to pick which
- `test_schedule_sanity` -- every scheduled Dag computes its next run without raising
- `test_pool_references_exist` -- every task's pool exists in the metadata database (`db_test`).
  A fresh metadata database only knows Airflow's stock pools; seed consumer-defined ones with
  `airflow_pools`, as `name = <positive slot count>` lines, seeded right before this item runs:
  ```ini
  [pytest]
  airflow_pools =
      batch = 4
      critical = 1
  ```
  Seeding is idempotent -- a pool already present with the configured slot count is left alone,
  so the item stays safe to run more than once against the same database (every worker under
  `pytest-xdist --dist each`, or a rerun after failure). A name that already exists with a
  *different* slot count (including Airflow's own `default_pool`) fails the item
- `test_no_top_level_variable_access` -- no Dag file fetches a Variable or Connection at import
  time, where it would run on every scheduler parse loop. Two mechanisms merge into one report:
  an AST scan over exactly the files Airflow parsed (direct calls like `Variable.get(...)`,
  `Connection.get(...)`, `BaseHook.get_connection(...)`, with exact file and line), and runtime
  interception that patches the secrets entry points while the shared corpus fills its `DagBag`,
  which also catches lookups hidden behind helper functions. The `dag_bag` parse is instrumented
  the same way whenever the catalog is enabled, so the runtime pass survives either parse
  ordering; a `DagBag` that was somehow parsed without instrumentation degrades the check to
  AST-only with a logged note. Disable with `airflow_forbid_top_level_variable_access = false`
- `test_no_top_level_io` -- no Dag file calls into a known network or database module at import
  time. AST-only, and deliberately conservative: a call is flagged only when its callee provably
  resolves through the file's own top-level imports to a listed module (`requests.get(...)`,
  `boto3.client(...)`, `create_engine(...)`), so aliased indirection escapes by design rather
  than risking false positives. `airflow_top_level_io_modules` *replaces* the built-in module
  list (copy it to extend it); disable with `airflow_forbid_top_level_io = false`
- `test_dag_parse_budget` -- no Dag file's parse duration exceeds
  `max(ratio x corpus median, 1.0s)`, with `airflow_dag_parse_budget_ratio` defaulting to `10`.
  Relative to the run's own median, so it is independent of absolute CI speed; the one-second
  floor keeps tiny fast corpora from failing on timing jitter, and fewer than three parsed files
  pass trivially. `airflow_dag_parse_budget_ratio = 0` disables the check
- `test_forbid_catchup` -- no scheduled Dag enables `catchup`, which backfills every missed
  interval the moment the Dag is unpaused; unscheduled Dags are skipped, since with no timetable
  there is nothing to backfill. Disable with `airflow_forbid_catchup = false`
- `test_no_unbounded_expand` -- no mapped task expands over runtime data (XCom or task output)
  without `max_active_tis_per_dag`; one oversized upstream result would otherwise fan out into an
  unbounded number of concurrent task instances. Literal expansions are bounded by construction
  and pass. Disable with `airflow_forbid_unbounded_expand = false`

Four more collect only once their ini option is configured, so defaults stay zero-config:

- `airflow_dag_id_pattern` -- every `dag_id` matches the given regex
- `airflow_required_dag_tags` -- every Dag carries the listed tags
- `airflow_forbid_default_owner` -- no task is owned by the stock `airflow` owner
- `airflow_dag_snapshot_dir` -- every Dag's serialized structure (topology, schedule, params,
  task attrs) matches its committed snapshot in the configured directory; regenerate with
  `--airflow-smoke-update`

## Bounding serialization cost

The serialization-backed checks (`test_dag_serialization_roundtrip`, `test_schedule_sanity`,
`test_dag_serialization_snapshot`) share one serialized-Dag cache, so the corpus is parsed and
the selected Dags serialized once per run. Two knobs bound the cost on large generated corpora:

- `airflow_serialization_sample_size` (default `0`, meaning every Dag) -- serialize only a
  deterministic sample of N Dags, selected by hashing each `dag_id` with
  `airflow_serialization_sample_seed` (default `0`); the same corpus and seed always select the
  same sample, and `test_schedule_sanity` skips Dags outside it. Incompatible with
  `--airflow-smoke-update`, which must regenerate every snapshot. Bounds the cost, but `0` still
  means every Dag -- use `airflow_smoke_disable` above to eliminate it entirely
- `--log-cli-level=INFO` streams per-Dag serialization progress live; captured-only logs do not
  survive a hard outer kill

For a corpus large enough that the parse itself dominates, see
[fanning the parse out](../internals/dag-corpus.md#fanning-the-parse-out-across-subprocess-workers).

## Writing your own corpus check

The catalog is not the only consumer of the parsed corpus. `dag_corpus` is a public,
session-scoped fixture handing back the same portable `DagCorpus` the catalog builds, so a
repository-defined check reuses the same parse instead of paying for its own:

```python
def test_every_dag_has_an_owner_tag(dag_corpus):
    for dag_id, dag in dag_corpus.dags.items():
        assert dag.tags, f"{dag_id} has no tags"
```

See [Corpus parsing, parallelism, and `dag_corpus`](../internals/dag-corpus.md) for the record
fields, the read-only guarantees, and how the parse is shared across `pytest-xdist` workers.
