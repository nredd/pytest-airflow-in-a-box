# Smoke tests

A bundled catalog of zero-boilerplate checks against the configured Dag folder, synthesized with
no files written. Off unless configured:

```console
pytest --airflow-smoke --dag-folder=dags/
```

or persistently via the `airflow_smoke` ini option. Every item carries `smoke` (plus `timeout`,
and `db_test` on the Dag-integrity and pool-reference checks), so `-m smoke` / `-m "not smoke"`
select exactly the bundled catalog. Explicit selection is honored: pointing pytest at a file or
node ID (`pytest tests/test_x.py`, `pytest tests/test_x.py::test_one`) runs *only* that
selection and drops the catalog, while directory positionals (`pytest tests/`), bare runs, and
`testpaths`-driven runs keep it -- *unless* an explicit `-m` expression would itself select a
smoke item (e.g. `-m smoke`, `-m "smoke and db_test"`), in which case that unambiguous opt-in
overrides the file/node-ID scoping and the catalog stays in. `-k` and `--deselect
::smoke::<name>` apply to the items as usual:

Under `pytest-xdist`, bundled items remain independently schedulable across workers. The first item
to need the corpus parses it once and publishes a serialized artifact below the isolated run root;
the other workers reuse that artifact instead of reparsing every Dag. The `smoke` marker itself has
no scheduling effect, so user-authored smoke tests remain fully parallel too.

A test using the `full_dag_bag` fixture in the same worker process shares that parse too: if
`full_dag_bag` already parsed in this process, the corpus builder reuses that live `DagBag` instead
of parsing again (the catalog is always collected last, so this is the common case). While the
catalog is enabled this way, `airflow_dag_parse_timeout` also governs `full_dag_bag`'s own parse, so
a Dag file that exceeds it lands in `full_dag_bag.import_errors` instead of `full_dag_bag.dags`.
Treat a shared `DagBag` as read-only: a consumer's mutation is visible to the catalog's checks too.

- `test_dag_bag_integrity` -- fails on import errors and per-file parse timeouts
  (`airflow_dag_parse_timeout`, default `30` seconds, exported as
  `AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT` so Airflow hard-kills runaway files); warns with
  `SlowDagParseWarning` on files above `airflow_dag_parse_slowpoke_ratio` (default `0.75`) of the
  timeout without failing the run; logs a slowest-first parse-timing table
- `test_dag_serialization_roundtrip` -- every parsed Dag survives Airflow's scheduler
  serialization round trip; logs a slowest-first per-Dag timing table and carries a corpus-scaled
  `pytest-timeout` deadline (floored at 30 seconds, so a tuned-down parse timeout cannot starve
  the serialization pass) so a pathological Dag is named before an outer CI timeout
- `test_no_duplicate_dag_ids` -- no two Dag files declare the same `dag_id`
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

The serialization-backed checks (`test_dag_serialization_roundtrip`, `test_schedule_sanity`,
`test_dag_serialization_snapshot`) share the producer's serialized-Dag cache across workers, so
the corpus is parsed and the selected Dags are serialized once per run. Two ini options bound the
cost on large generated corpora:

- `airflow_serialization_sample_size` (default `0`, meaning every Dag) -- serialize only a
  deterministic sample of N Dags, selected by hashing each `dag_id` with
  `airflow_serialization_sample_seed` (default `0`); the same corpus and seed always select the
  same sample, and `test_schedule_sanity` skips Dags outside it. Incompatible with
  `--airflow-smoke-update`, which must regenerate every snapshot
- run with `--log-cli-level=INFO` to stream per-Dag serialization progress live; captured-only
  logs do not survive a hard outer kill

Four additional policy checks appear only when their ini is configured, so defaults stay
zero-config:

- `airflow_dag_id_pattern` -- every `dag_id` matches the given regex
- `airflow_required_dag_tags` -- every Dag carries the listed tags
- `airflow_forbid_default_owner` -- no task is owned by the stock `airflow` owner
- `airflow_dag_snapshot_dir` -- every Dag's serialized structure (topology, schedule, params,
  task attrs) matches its committed snapshot in the configured directory; regenerate with
  `--airflow-smoke-update`
