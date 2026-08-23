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
`testpaths`-driven runs keep it -- *unless* an explicit `-m` expression mentions `smoke` and
would itself select a real smoke item (e.g. `-m smoke`, `-m "smoke and db_test"`), in which
case that unambiguous opt-in overrides the file/node-ID scoping and the catalog stays in. An
`-m` expression that never mentions `smoke` (`-m db_test`, `-m "not slow"`) does not, even if
it happens to match some smoke item's other marks -- otherwise an unrelated filter on an
explicitly scoped run could silently pull in the whole catalog. `-k` and `--deselect
::smoke::<name>` apply to the items as usual:

Persistently drop any bundled item from the catalog with `airflow_smoke_disable`, a list of
item names (e.g. `test_schedule_sanity`, `test_dag_serialization_snapshot`). Unlike `--deselect`,
which filters after collection, a disabled item is never synthesized at all -- if every
serialization-backed item (`test_dag_serialization_roundtrip`, `test_schedule_sanity`, and, when
configured, `test_dag_serialization_snapshot`) is disabled, the corpus builder skips calling the
Airflow DAG serializer entirely, which `airflow_serialization_sample_size` alone cannot do (`0`
means every Dag, not none):

```ini
[pytest]
airflow_smoke_disable =
    test_dag_serialization_roundtrip
    test_schedule_sanity
```

Under `pytest-xdist`, bundled items remain independently schedulable across workers. The first item
to need the corpus takes an exclusive lock on the isolated run root, parses the Dag folder once, and
publishes a serialized artifact there; every other worker *blocks on that lock* and then decodes the
artifact rather than reparsing. A whole run therefore pays at most one corpus parse no matter how
many workers or bundled items it has -- the cost of a cold worker is a short wait, not a second
parse. The `smoke` marker itself has no scheduling effect, so user-authored smoke tests remain fully
parallel too.

A test using the `dag_bag` fixture in the same worker process shares that parse too: if
`dag_bag` already parsed in this process, the corpus builder reuses that live `DagBag` instead
of parsing again (the catalog is always collected last, so this is the common case). While the
catalog is enabled this way, `airflow_dag_parse_timeout` also governs `dag_bag`'s own parse, so
a Dag file that exceeds it lands in `dag_bag.import_errors` instead of `dag_bag.dags`.
Treat a shared `DagBag` as read-only: a consumer's mutation is visible to the catalog's checks too.

That same-process reuse depends on the corpus builder and a `dag_bag` consumer landing on the
same worker, which plain load-balanced scheduling does not guarantee. Under `--dist loadgroup`, if
this run also has a test that uses `dag_bag` and would survive an active `-m` expression, the
plugin puts the whole catalog and one such consumer into a shared `xdist_group`, forcing them onto
the same worker -- instead of the corpus builder and `dag_bag` independently parsing the same
folder in parallel on two workers. Only one consumer joins the group, not every one in the run, so a
suite with many `dag_bag` tests does not have all of their execution serialized onto a single
worker just to save one parse. An item that already carries its own explicit `xdist_group` is never
chosen or overwritten. `-k` deselection is not predicted the way `-m` is, so a `dag_bag`
consumer dropped only by `-k` may still be chosen.

Consumers are detected through pytest's full fixture *closure*, not just a test's own signature: a
test taking a project fixture that itself declares `dag_bag` (`def subdir_bag(dag_bag): ...`)
anchors the catalog exactly like a direct consumer. A fixture that reaches the bag through
`request.getfixturevalue("dag_bag")`, or that builds its own `DagBag` instead of deriving from this
one, is invisible to that detection and cannot anchor anything.

Note that `--dist loadgroup` is *not* the default anywhere: `--dist` defaults to `no`, and a plain
`-n auto` promotes it to `load`, under which `xdist_group` is inert and co-location never happens.
Whenever the plugin wants to co-locate the catalog and cannot, it says so with a
`SmokeColocationWarning` naming the reason and the extra full Dag parse it costs -- whether that is
a run distributing without `loadgroup`, or a `loadgroup` run in which every `dag_bag` consumer is
ineligible because each already carries its own `xdist_group` or each is about to be deselected by
`-m`. Silence it with `-W ignore::pytest_airflow_in_a_box.smoke.SmokeColocationWarning`, or promote
it to a hard failure with `-W error::pytest_airflow_in_a_box.smoke.SmokeColocationWarning` on a
suite that treats parallel-efficiency regressions as bugs. A smoke-only run with no `dag_bag`
consumer at all warns nothing and loses nothing: the catalog owns the only Dag parse in the run
either way, and keeps distributing across workers as described above.

### Fanning the parse out across subprocess workers

For a large corpus (thousands of files), the corpus builder's own single-process parse
can become the dominant cost. `airflow_dag_bag_fanout` (default off) fans the parse out
across subprocess workers instead: the corpus builder walks the Dag folder once from its
true configured root -- so `.airflowignore` resolution is identical to the serial
path -- then shards the discovered files across `airflow_dag_bag_fanout_workers`
(default `0`, auto-detected from the CPU count) subprocesses, each importing only its own
files, and merges their results:

```ini
[pytest]
airflow_dag_bag_fanout = true
```

Each worker reproduces the parse environment, not only `.airflowignore`: it inherits the
parent's `sys.path` (any `pythonpath` ini entries, rootdir/conftest insertions) and runs
with the same working directory, so a Dag file importing a sibling package -- a common
`dags/` + shared-package repo layout -- resolves identically whether or not fan-out is on.

Below `airflow_dag_bag_fanout_min_files` (default `200`) files, fan-out is skipped even
when enabled -- subprocess spawn and a fresh Airflow import per worker cost real time a
small corpus would never recoup. `airflow_dag_bag_fanout_timeout` (default `600`
seconds) bounds the whole fanned-out parse; any fan-out failure -- a shard that could not
be spawned, timed out, crashed, or wrote an undecodable result -- logs a warning and
falls back to the existing single-process parse rather than failing the run. Fan-out
itself activates independently of serialization, but only ever *serializes* when
`airflow_serialization_sample_size` is `0` (serialize every Dag): a seed-keyed sample
needs the whole corpus's `dag_id` set, which no single worker's shard has on its own; a
nonzero sample size falls back to the serial path entirely. Within that, fan-out still
skips the Dag serializer whenever no still-collected smoke item needs a serialized Dag at
all (e.g. every serialization-backed item disabled via `airflow_smoke_disable`), the same
short-circuit the serial path already applies.

Does not extend to the `dag_bag` fixture: its public contract is the real Airflow
`DagBag` class, and there is no format that hands one of those back across a process
boundary. A `dag_bag`-only test on a large corpus still pays for one full parse per
xdist worker that runs it -- the colocation described above (forcing the catalog and one
`dag_bag` consumer onto the same worker under `--dist loadgroup`) remains the mitigation
for that case.

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

Five anti-pattern checks target the Dag habits that only bite in production. They are on by
default whenever the catalog is enabled, and each has its own ini to disable or tune it:

- `test_no_top_level_variable_access` -- no Dag file fetches a Variable or Connection at import
  time, where it would run on every scheduler parse loop. Two mechanisms merge into one report:
  an AST scan over exactly the files Airflow parsed (direct calls like `Variable.get(...)`,
  `Connection.get(...)`, `BaseHook.get_connection(...)`, with exact file and line), and runtime
  interception that patches the secrets entry points while the shared corpus fills its `DagBag`,
  which also catches lookups hidden behind helper functions. The `dag_bag` parse is
  instrumented the same way whenever the catalog is enabled, so the runtime pass survives either
  parse ordering; a `DagBag` that was somehow parsed without instrumentation degrades the check
  to AST-only with a logged note. Disable with
  `airflow_forbid_top_level_variable_access = false`
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
  interval the moment the Dag is unpaused; unscheduled Dags are skipped, since with no
  timetable there is nothing to backfill. Disable with `airflow_forbid_catchup = false`
- `test_no_unbounded_expand` -- no mapped task expands over runtime data (XCom or task output)
  without `max_active_tis_per_dag`; one oversized upstream result would otherwise fan out into
  an unbounded number of concurrent task instances. Literal expansions are bounded by
  construction and pass. Disable with `airflow_forbid_unbounded_expand = false`

The serialization-backed checks (`test_dag_serialization_roundtrip`, `test_schedule_sanity`,
`test_dag_serialization_snapshot`) share the producer's serialized-Dag cache across workers, so
the corpus is parsed and the selected Dags are serialized once per run. Two ini options bound the
cost on large generated corpora:

- `airflow_serialization_sample_size` (default `0`, meaning every Dag) -- serialize only a
  deterministic sample of N Dags, selected by hashing each `dag_id` with
  `airflow_serialization_sample_seed` (default `0`); the same corpus and seed always select the
  same sample, and `test_schedule_sanity` skips Dags outside it. Incompatible with
  `--airflow-smoke-update`, which must regenerate every snapshot. Bounds the cost, but `0`
  still means every Dag -- use `airflow_smoke_disable` above to eliminate it entirely
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
