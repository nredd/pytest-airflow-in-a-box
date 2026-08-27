# Smoke Tests

Checks over the whole Dag corpus, in three shapes: a bundled catalog of corpus-wide policy
checks (`--airflow-smoke`), one selectable pytest item per Dag file
(`--collect-dag-folder`), and coverage over the Dag folder (`pytest-cov`, no plugin
support needed). All of them ride the same shared parse, described at the
[bottom of this page](#corpus-parsing-and-parallelism).

The catalog asserts properties of the *set* of Dags, with no test body to write: no two
files claim the same `dag_id`, no file fetches a Variable at import time, and -- opt-in -- no scheduled Dag
has `catchup=True`, no mapped task expands over runtime data without a concurrency cap. A
per-Dag test cannot phrase any of those, because none of them are about one Dag.

Turn the catalog on:

```console
pytest --airflow-smoke --dag-folder=dags/
```

or persistently via the `airflow_smoke` ini option. Six items collect by default; six more
appear once you configure the ini option that enables them.

## Why the default items are in scope

[Deciding which failures are yours](testing-scope.md) rules out asserting on Airflow's own
mechanisms, and two always-on items look like exactly that. The
[stock carve-out](testing-scope.md#out-of-scope) is why they are not:

- `test_dag_serialization_roundtrip` -- the subject is *your* operator's constructor
  arguments, which are the part that fails to serialize and takes the whole Dag out of the
  scheduler. Airflow's serializer is not the subject
- `test_schedule_sanity` -- the subject is *your* `schedule=`, `start_date`, and any timetable
  you wrote, not the timetable code Airflow ships

Neither asserts anything about a stock component in isolation. Drop either with
`airflow_smoke_disable` if you disagree.

## Selecting and disabling items

Every item carries `smoke` (plus `timeout`, and `db_test` on the Dag-integrity and
pool-reference checks), so `-m smoke` / `-m "not smoke"` select exactly the bundled catalog.

Explicit selection is honored: pointing pytest at a file or node ID (`pytest tests/test_x.py`,
`pytest tests/test_x.py::test_one`) runs *only* that selection and drops the catalog, while
directory positionals (`pytest tests/`), bare runs, and `testpaths`-driven runs keep it --
unless an explicit `-m` expression mentions `smoke` and would itself select a real smoke item
(`-m smoke`, `-m "smoke and db_test"`), in which case that unambiguous opt-in overrides the
file/node-ID scoping and the catalog stays in. An `-m` expression that never mentions `smoke`
(`-m db_test`, `-m "not slow"`) does not override that scoping, even if it happens to match
some smoke item's other marks -- otherwise an unrelated filter on an explicitly scoped run
could silently pull in the whole catalog. `-k` and `--deselect ::smoke::<name>` apply to the items as usual.

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
  the timeout without failing the run; logs a slowest-first parse-timing table. There is no
  separate item for a duplicate `dag_id`: the scheduler keeps one of the two files and you do
  not get to pick which, and that collision already surfaces here as an ordinary import error,
  naming both files
- `test_dag_serialization_roundtrip` -- every parsed Dag survives Airflow's scheduler
  serialization round trip. A Dag that does not is dropped by the scheduler entirely. Logs a
  slowest-first per-Dag timing table and carries a corpus-scaled `pytest-timeout` deadline
  (floored at 30 seconds, so a tuned-down parse timeout cannot starve the serialization pass)
  so a pathological Dag is named before an outer CI timeout
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
  and also catches lookups hidden behind helper functions. The `dag_bag` parse is instrumented
  the same way whenever the catalog is enabled, so the runtime pass survives either parse
  ordering; a `DagBag` that was somehow parsed without instrumentation degrades the check to
  AST-only with a logged note. Disable with `airflow_forbid_top_level_variable_access = false`
- `test_no_top_level_io` -- no Dag file calls into a known network or database module at import
  time. AST-only, and deliberately conservative: a call is flagged only when its callee provably
  resolves through the file's own top-level imports to a listed module (`requests.get(...)`,
  `boto3.client(...)`, `create_engine(...)`), so aliased indirection escapes by design rather
  than risking false positives. `airflow_top_level_io_modules` *replaces* the built-in module
  list (copy it to extend it); disable with `airflow_forbid_top_level_io = false`

Six more collect only once their ini option is configured, so defaults stay zero-config:

- `airflow_forbid_catchup = true` -- no scheduled Dag enables `catchup`, which backfills every
  missed interval the moment the Dag is unpaused; unscheduled Dags are skipped, since with no
  timetable there is nothing to backfill (`test_forbid_catchup`)
- `airflow_forbid_unbounded_expand = true` -- no mapped task expands over runtime data (`XCom`
  or task output) without `max_active_tis_per_dag`; one oversized upstream result would
  otherwise fan out into an unbounded number of concurrent task instances. Literal expansions
  are bounded by construction and pass (`test_no_unbounded_expand`)
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
[fanning the parse out](#fanning-the-parse-out-across-subprocess-workers).

## One pytest item per Dag file

Every `*.py` file in your Dag folder becomes its own named, selectable pytest item that fails
if the file does not import or defines no Dags. One broken file is one red row you can `-k`,
shard across `pytest-xdist`, and read as a per-file line in JUnit XML -- instead of one
aggregated assert that tells you the corpus is broken and nothing else.

```console
pytest --collect-dag-folder=dags/
```

or persistently via the `airflow_collect_dags_folder` ini option. Off unless configured.

Rule of thumb: `--collect-dag-folder` when you want per-file granularity in the report;
`--airflow-smoke` when you want corpus-wide policy -- duplicate `dag_id`s, no top-level
Variable access, `catchup=True` anywhere (opt-in) -- which no per-file item can phrase.
Enabling both parses the Dag folder **twice** (the import-failure message is byte-identical,
`collection.py:341` vs `smoke.py:1027`), so pick one unless you want both shapes of report.

### What the ten-line `DagBag` conftest test misses

Nearly every Dag repo has this test:

```python
def test_no_import_errors():
    assert DagBag(dag_folder="dags/", include_examples=False).import_errors == {}
```

What it costs you:

- One item, one assert. Fifty broken files are one failure, and you cannot `-k` a single file,
  shard the corpus, or get a JUnit row per file
- No Dag-free-file check. A file that imports clean and declares zero Dags passes that assert
  and then quietly does not exist as far as the scheduler is concerned. Collected items fail
  with `Dag file defines no Dags`
- Top-level `Variable.get()` resolves differently than it will in production. Airflow 3 routes
  those lookups through the Task SDK, and a bare `DagBag()` parse has no supervisor answering:
  3.2+ silently misses and returns whatever default your call passed, 3.1 raises `ImportError`
  on `SUPERVISOR_COMMS`. Collected items parse through the plugin's parse-time shim instead
  (`--airflow-parse-secrets`, default `metastore`), so the lookup reads the rows you seeded --
  see [Parse-time secret resolution](../internals/parse-time-secrets.md)

### The two Dag folder options

They are different options and this is the canonical statement of the difference:

| Option / ini | Feeds |
| --- | --- |
| `--dag-folder` / `airflow_dags_folder` | The `dag_bag` fixture (and through it [`run_dag`](ladder.md#testing-a-dag-defined-elsewhere)), the smoke catalog, and `dag_corpus` |
| `--collect-dag-folder` / `airflow_collect_dags_folder` | Dag-file collection, this section |

They are usually the same directory. Set both if you want both features, and cover both under
`--cov` if you measure [Dag coverage](#dag-coverage).

### What gets collected

- Files directly below the configured folder or any subdirectory of it, with a `.py` suffix
- Files whose name starts with `_` are skipped
- Each collects a `dag-import` item, auto-marked `db_test`
- Files also matching pytest's `python_files` patterns (`test_*.py`), or passed directly on the
  command line, are collected by pytest's default Python collector too; those duplicates are
  pruned after collection

### Pinned param cases

A Dag file may pin param cases through a module-level literal, read by AST without importing
the file:

```python
PYTEST_DAG_CASES = {
    "dev": {"environment": "dev"},
    "prod": {"environment": "prod"},
}
```

Each case collects as a sibling `dag-params[dev]` item that validates the pinned values against
every Dag the file declares. A key the Dag never declared as a param fails the case, naming the
declared ones; a value that fails its param schema fails the case with Airflow's own validation
error. That catches a `params={...}` rename which a plain import check cannot see.

Cost, stated plainly: `PYTEST_DAG_CASES` is a test-only literal living in a production Dag
file. A repo with a no-test-artifacts-in-`dags/` policy can only use the import half of this
feature. The literal is inert at runtime -- nothing but the collector reads it -- but it is
still shipped code.

## Dag coverage

A green suite over a 300-file `dags/` folder tells you nothing about how many of those files a
test ever touched. Coverage does, and it needs no plugin support -- one `pytest-cov` flag
naming the Dag folder is the whole answer:

```console
pytest --cov=dags --cov-report=term-missing
```

`pytest-cov` is not a dependency of this plugin; add it to your project's dev dependencies (or,
in CI provisioned by the bundled [GitHub Action](ci/github-action.md), list it in the file
passed to the Action's `requirements-file` input so it lands in the same venv).

Why it works: every parse path in the plugin -- `dag_bag`, the catalog's corpus builder, and
Dag-file collection items -- funnels through one plain in-process `DagBag` constructor.
Airflow's importer mangles the *module name* (`unusual_prefix_<hash>_<stem>`), but the code
objects carry the real on-disk file path, and `coverage.py` attributes lines by code-object
filename, not module name, so Dag files land in the report under their real paths. Task
callables execute in-process too -- both the DB-backed runners and the DB-free `run_task` --
so operator and TaskFlow bodies are measured, not just top-level Dag definitions, and none of
`coverage.py`'s subprocess settings (`concurrency`, `sigterm`, `patch = subprocess`) are
needed.

The one path that is not in-process:
[`run_dag(dag, executor=...)`](ladder.md#executor-driven-runs) re-imports the Dag
*file* in a fresh supervised worker subprocess and executes the task body there. Lines that
only ever execute inside that worker are not recorded, so a task body reached *only* through
an `executor=` run reads as uncovered. Nothing in this plugin wires up coverage's subprocess
support to fix that. Cover the body from an in-process rung -- `run_task`, or `run_dag`
without `executor=` -- and keep the `executor=` test for what it uniquely proves: that the
body survives re-import in a subprocess and that your executor round-trips through a real API.
The Dag file's top-level lines stay covered regardless, since the parent process parsed it.

Footguns:

- `--cov=src` (or `--cov=<your package>`) *alone* silently excludes the Dag folder.
  `pytest-cov` reports only files below its configured sources, so Dag files vanish from the
  report with no warning and the total looks fine. Pass one `--cov` per measured folder:
  `--cov=dags --cov=src`
- There are TWO Dag folder options, and they can point at different directories. Coverage sees
  only what `--cov` names, so cover every folder your tests actually parse. See
  [the two Dag folder options](#the-two-dag-folder-options)
- When neither `--dag-folder` nor `airflow_dags_folder` is set, `dag_bag` falls back to the
  empty scratch `dags/` directory below the disposable bootstrap run root. Never feed that path
  to `--cov`: it is temporary, per-run, and deleted at session end

`pytest --airflow-doctor` prints a "Dag coverage" section covering exactly these: the resolved
Dag and collection folders, whether `pytest-cov` is installed and active, and whether the Dag
folder sits inside a configured `--cov` source, with a copy-pasteable fix when it does not. See
[Diagnosing a run](../reference/diagnostics.md).

For merging coverage data recorded in different checkouts or containers (matrix legs, an `xml`
upload), set `relative_files = true` under `[tool.coverage.run]` in your `pyproject.toml`.
Under `pytest-xdist`, `pytest-cov` combines per-worker data files automatically, so totals are
unaffected; the single elected corpus parse (below) does not distort them either -- parsing is
where Dag-file lines execute, and the electing worker records them.

## Corpus parsing and parallelism

Parsing a Dag folder is the expensive thing this plugin does, and every consumer above wants
the same parse. The shared value is `pytest_airflow_in_a_box.types.DagCorpus`: the parse
reduced to data that survives a process boundary -- Dag metadata, `import_errors`, per-file
parse timings, intercepted runtime lookups -- with the top-level mappings exposed as read-only
`MappingProxyType` views, so nothing can mutate one test's corpus out from under another's.
(Each record's `serialized` field is a plain `dict` shared by reference -- treat it as
read-only by convention, the way `dag_bag`'s `DagBag` is.) Extracted from `smoke.py` once
`dag_corpus` gave the builder a second consumer
([ADR 0004](../adr/0004-extract-dag-corpus-from-smoke.md)).

### One parse per run, not per worker

Under `pytest-xdist`, bundled items stay independently schedulable across workers unless the
co-location below forces them onto one. The first item to need the corpus takes an exclusive `fcntl.flock` on the isolated run root, parses the
Dag folder once, and publishes a versioned JSON artifact (`.airflow-dag-corpus.json`) beside
the lock; every other worker *blocks on that lock* and then decodes the artifact rather than
reparsing. A run therefore pays at most one corpus parse no matter how many workers or bundled
items it has -- a cold worker pays a short wait, not a second parse. The `smoke` marker itself
has no scheduling effect, so user-authored smoke tests remain fully parallel.

Decoding the artifact still retains a full `DagCorpus` in that worker's `session.stash` for the
rest of the run, so *which* workers decode it matters even though none of them reparse: an
uncolocated bundled catalog spread across several workers under `--dist loadgroup` retained one
full decoded corpus per worker it landed on, multiplying peak memory on a large corpus (issue
#327). Co-location below exists to bound that to one worker, not just to save the parse.

Within one process, a `dag_bag` already parsed is reused instead of parsed again (the catalog
is always collected last, so this is the common case). While the catalog is enabled,
`airflow_dag_parse_timeout` also governs `dag_bag`'s own parse, so a Dag file that exceeds it
lands in `dag_bag.import_errors` instead of `dag_bag.dags`.

### Co-location under `--dist loadgroup`

Same-process reuse needs the corpus builder and a consumer on the same worker, which
load-balanced scheduling does not guarantee. Under `--dist loadgroup` the plugin forces it with
an `xdist_group` marker, in three shapes, tried in order:

- **`dag_corpus` consumers**: *every* surviving consumer joins the catalog's group. They are
  expected to be few, cheap, read-only checks, so sharing a worker costs nothing and leaving
  one behind would make it pay the flock-wait and decode the artifact itself
- **`dag_bag` consumers**: exactly *one* is chosen as an anchor. Grouping all of them would
  trade one avoided parse for serializing a whole suite's execution onto one worker
- **Fallback**: when neither an eligible `dag_corpus` nor `dag_bag` consumer exists -- a
  smoke-only run (`-m smoke`) is the common case -- the catalog groups with *itself* under a
  plugin-owned `SMOKE_CATALOG_FALLBACK_XDIST_GROUP`, so all of it still lands on one worker
  even with nothing to anchor onto. For a smoke-only `-n auto` run, this is the whole run: it
  now executes serially on that one worker rather than spread across every `-n` worker,
  trading wall-clock time for the bounded memory above

When more than one kind exists, the catalog joins the `dag_corpus` group over the `dag_bag` one,
and either over the fallback. An item that already carries its own explicit `xdist_group` is
never chosen or overwritten.

Consumers are found through pytest's full fixture *closure*, not a test's own signature: a
project fixture that itself declares `dag_bag` (`def subdir_bag(dag_bag): ...`) anchors the
catalog exactly like a direct consumer. A fixture reaching the value through
`request.getfixturevalue("dag_corpus")` is invisible -- pytest's static closure never includes
a dynamic lookup -- so it cannot anchor anything; prefer a normal fixture parameter when that
matters. `-k` deselection is not predicted the way `-m` is, so a consumer dropped only by `-k`
may still be chosen.

`--dist loadgroup` is *not* the default anywhere: `--dist` defaults to `no`, and a plain
`-n auto` promotes it to `load`, under which `xdist_group` is inert and co-location never
happens. Whenever the plugin wants to co-locate and cannot, it says so with a
`SmokeColocationWarning` naming the reason and the extra full Dag parse it costs -- a run
distributing without `loadgroup`, or a `loadgroup` run in which every candidate consumer is
ineligible. One warning per run, not one per worker:

```console
pytest -W error::pytest_airflow_in_a_box.smoke.SmokeColocationWarning
pytest -W ignore::pytest_airflow_in_a_box.smoke.SmokeColocationWarning
```

Promote it on a suite that treats parallel-efficiency regressions as bugs; silence it
otherwise. A smoke-only run with no `dag_bag`/`dag_corpus` consumer at all warns nothing: there
was never a live `DagBag` or requested corpus to reuse, so nothing was missed. It still falls
back to grouping with itself (above) rather than staying ungrouped.

### Fanning the parse out across subprocess workers

For a large corpus (thousands of files), the builder's own single-process parse can become the
dominant cost. Fan-out shards it across subprocesses instead:

```ini
[pytest]
airflow_dag_bag_fanout = true
```

or `--airflow-dag-bag-fanout` for one run. Default off. The builder walks the Dag folder once
from its true configured root -- so `.airflowignore` resolution is identical to the serial path
-- then shards the discovered files across `airflow_dag_bag_fanout_workers` (default `0`,
auto-detected from the CPU count) subprocesses, each importing only its own files, and merges
their results. Each worker reproduces the parse environment, not only `.airflowignore`: it
inherits the parent's `sys.path` and working directory, so a Dag file importing a sibling
package resolves identically either way.

Limits, all of which fall back rather than fail:

- Below `airflow_dag_bag_fanout_min_files` (default `200`) files, fan-out is skipped even when
  enabled. Subprocess spawn and a fresh Airflow import per worker cost real time a small corpus
  never recoups
- `airflow_dag_bag_fanout_timeout` (default `600` seconds) bounds the whole fanned-out parse.
  Any failure -- a shard that could not be spawned, timed out, crashed, or wrote an undecodable
  result -- logs a warning and falls back to the single-process parse
- Fan-out only ever *serializes* when the effective `airflow_serialization_sample_size` is `0`
  (every Dag). A seed-keyed sample needs the whole corpus's `dag_id` set, which no single shard
  has, so a nonzero sample size falls back to the serial path entirely. Within that, fan-out
  still skips the Dag serializer whenever no still-collected smoke item needs a serialized Dag,
  the same short-circuit the serial path applies

Fan-out does **not** extend to `dag_bag`. Its public contract is the real Airflow `DagBag`
class, and there is no format that hands one of those back across a process boundary. A
`dag_bag`-only test on a large corpus still pays one full parse per xdist worker that runs it;
co-location is the only mitigation. A check phrased over portable metadata does not have to
give fan-out up -- use `dag_corpus`.

## Writing your own corpus check

The catalog is not the only consumer of the parsed corpus. `dag_corpus` is a public,
session-scoped [fixture](../reference/fixtures.md) handing back the same portable `DagCorpus`
the catalog builds, so a repository-defined check reuses the shared parse, the co-location,
and fan-out for free:

```python
def test_every_dag_has_an_owner_tag(dag_corpus):
    for dag_id, dag in dag_corpus.dags.items():
        assert dag.tags, f"{dag_id} has no tags"
```

Requesting `dag_corpus` at all makes the builder serialize every Dag, regardless of
`airflow_smoke_disable` or a configured `airflow_serialization_sample_size` -- there is no
cheap way to know in advance which record fields a test body will read.

Prefer `dag_corpus` for a check phrased entirely over static Dag metadata -- tags, task shape,
scheduling policy, serialized structure. Reach for `dag_bag` only once a test needs a live
Airflow object: executing a task, inspecting a real `DAG` or operator instance, or anything
that must call back into Airflow's own APIs rather than read data off the parse.

To narrow a check to one subdirectory, `dagcorpus.dags_under` filters the corpus: a plain
function taking the corpus, the configured Dag folder, and a subdirectory, returning a
read-only `dag_id` -> `CorpusDag` mapping restricted to Dags whose file resolves under that
subdirectory, nested ones included. It only resolves paths -- never opening or listing
directories -- so a subdirectory that does not exist matches nothing rather than raising, and
relative or absolute input, trailing slashes, and `.`/`..` all normalize to the same result:

```python
from pytest_airflow_in_a_box.dagcorpus import dags_under


def test_team_a_dags_are_tagged(dag_corpus, airflow_dags_folder):
    for dag_id, dag in dags_under(dag_corpus, airflow_dags_folder, "team_a").items():
        assert "team-a" in dag.tags, f"{dag_id} is missing its team-a tag"
```
