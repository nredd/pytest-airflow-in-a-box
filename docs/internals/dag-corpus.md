# Corpus parsing, parallelism, and `dag_corpus`

Parsing a Dag folder is the expensive thing this plugin does, and several consumers want the
same parse: the bundled [smoke catalog](../guide/smoke-tests.md), the `dag_bag` fixture, and
your own whole-corpus checks. This page is how one parse gets shared, and what to reach for
when it does not.

You arrive here from a `SmokeColocationWarning`, from a suite whose corpus parse dominates the
run, or from writing a repository-defined corpus check.

## `DagCorpus`, the portable value

`pytest_airflow_in_a_box.types.DagCorpus` is the parse reduced to data that survives a process
boundary -- no live Airflow objects:

- `dags` -- `dag_id` -> `CorpusDag` (`tags`, `tasks`, `fileloc`, `can_be_scheduled`, `catchup`,
  and, when serialized, `serialized`)
- `import_errors` -- file path -> traceback
- `dagbag_stats` -- per-file parse timings
- `runtime_lookups` -- Variable/Connection lookups intercepted during the parse

`dags` and `import_errors` are read-only `MappingProxyType` views: assigning into them raises
`TypeError`. That matters because one `DagCorpus` is built per worker process and shared by
every consuming test in it, so nothing can mutate one test's corpus out from under another's.
The guarantee stops at the top level -- each record's `serialized` field is a plain `dict`,
shared by reference across every consumer in the process including the catalog's own checks.
Treat it as read-only by convention, the way `dag_bag`'s `DagBag` is.

Extracted from `smoke.py` once `dag_corpus` gave the builder a second, independently-motivated
consumer ([ADR 0004](../adr/0004-extract-dag-corpus-from-smoke.md)).

## One parse per run, not per worker

Under `pytest-xdist`, bundled items stay independently schedulable across workers unless the
co-location below forces them onto one. The first item to need the corpus takes an exclusive
`fcntl.flock` on the isolated run root, parses the Dag folder once, and publishes a versioned
JSON artifact (`.airflow-dag-corpus.json`) beside the lock; every other worker *blocks on that
lock* and then decodes the artifact rather than reparsing. A run therefore pays at most one
corpus parse no matter how many workers or bundled items it has -- a cold worker pays a short
wait, not a second parse. The `smoke` marker itself has no scheduling effect, so user-authored
smoke tests remain fully parallel.

Decoding the artifact still retains a full `DagCorpus` in that worker's `session.stash` for the
rest of the run, so *which* workers decode it matters even though none of them reparse: an
uncolocated bundled catalog spread across several workers under `--dist loadgroup` retained one
full decoded corpus per worker it landed on, multiplying peak memory on a large corpus (issue
#327). Co-location below exists to bound that to one worker, not just to save the parse.

Within one process, a `dag_bag` already parsed is reused instead of parsed again (the catalog
is always collected last, so this is the common case). While the catalog is enabled,
`airflow_dag_parse_timeout` also governs `dag_bag`'s own parse, so a Dag file that exceeds it
lands in `dag_bag.import_errors` instead of `dag_bag.dags`.

## Co-location under `--dist loadgroup`

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
  even with nothing to anchor onto

When more than one kind exists, the catalog joins the `dag_corpus` group over the `dag_bag` one,
and either over the fallback. An item that already carries its own explicit `xdist_group` is
never chosen or overwritten.

Consumers are found through pytest's full fixture *closure*, not a test's own signature: a
project fixture that itself declares `dag_bag` (`def subdir_bag(dag_bag): ...`) anchors the
catalog exactly like a direct consumer. A fixture reaching the value through
`request.getfixturevalue("dag_corpus")` is invisible -- pytest's static closure never includes
a dynamic lookup -- so it cannot anchor anything, and if it is the only thing in the run that
would have forced serialization or fan-out eligibility, it silently does not get either. Prefer
a normal fixture parameter over `getfixturevalue` when that matters. `-k` deselection is not
predicted the way `-m` is, so a consumer dropped only by `-k` may still be chosen.

### `SmokeColocationWarning`

`--dist loadgroup` is *not* the default anywhere: `--dist` defaults to `no`, and a plain
`-n auto` promotes it to `load`, under which `xdist_group` is inert and co-location never
happens. Whenever the plugin wants to co-locate and cannot, it says so with a
`SmokeColocationWarning` naming the reason and the extra full Dag parse it costs -- a run
distributing without `loadgroup`, or a `loadgroup` run in which every candidate consumer is
ineligible (each already carries its own `xdist_group`, or each is about to be deselected by
`-m`). One warning per run, not one per worker.

```console
pytest -W error::pytest_airflow_in_a_box.smoke.SmokeColocationWarning
pytest -W ignore::pytest_airflow_in_a_box.smoke.SmokeColocationWarning
```

Promote it on a suite that treats parallel-efficiency regressions as bugs; silence it
otherwise. A smoke-only run with no `dag_bag`/`dag_corpus` consumer at all warns nothing: there
was never a live `DagBag` or requested corpus to reuse, so nothing was missed. It still falls
back to grouping with itself (above) rather than staying ungrouped.

## Fanning the parse out across subprocess workers

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
their results.

Each worker reproduces the parse environment, not only `.airflowignore`: it inherits the
parent's `sys.path` (any `pythonpath` ini entries, rootdir/conftest insertions) and runs with
the same working directory, so a Dag file importing a sibling package -- a common `dags/` plus
shared-package layout -- resolves identically either way.

Limits, all of which fall back rather than fail:

- Below `airflow_dag_bag_fanout_min_files` (default `200`) files, fan-out is skipped even when
  enabled. Subprocess spawn and a fresh Airflow import per worker cost real time a small corpus
  never recoups
- `airflow_dag_bag_fanout_timeout` (default `600` seconds) bounds the whole fanned-out parse.
  Any failure -- a shard that could not be spawned, timed out, crashed, or wrote an undecodable
  result -- logs a warning and falls back to the single-process parse
- Fan-out activates independently of serialization, but only ever *serializes* when the
  effective `airflow_serialization_sample_size` is `0` (every Dag). A seed-keyed sample needs
  the whole corpus's `dag_id` set, which no single shard has, so a nonzero sample size falls
  back to the serial path entirely. Within that, fan-out still skips the Dag serializer
  whenever no still-collected smoke item needs a serialized Dag, the same short-circuit the
  serial path applies

Fan-out does **not** extend to `dag_bag`. Its public contract is the real Airflow `DagBag`
class, and there is no format that hands one of those back across a process boundary. A
`dag_bag`-only test on a large corpus still pays one full parse per xdist worker that runs it;
the co-location above is the only mitigation. A check phrased over portable metadata does not
have to give fan-out up -- use `dag_corpus`.

## The `dag_corpus` fixture

Session-scoped, returning the same `DagCorpus` the catalog builds, so a repository-defined
whole-corpus check reuses the shared parse, the co-location, and fan-out for free:

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

### Narrowing to one subdirectory

`dag_corpus` covers the whole configured Dag folder. `dagcorpus.dags_under` filters it: a plain
function taking the corpus, the configured Dag folder, and a subdirectory, returning a
read-only `dag_id` -> `CorpusDag` mapping restricted to Dags whose file resolves under that
subdirectory, nested ones included. It only resolves paths -- reading path metadata, never
opening or listing directories -- so a subdirectory that does not exist matches nothing rather
than raising, and relative or absolute input, trailing slashes, and `.`/`..` all normalize to
the same result:

```python
from pytest_airflow_in_a_box.dagcorpus import dags_under


def test_team_a_dags_are_tagged(dag_corpus, airflow_dags_folder):
    for dag_id, dag in dags_under(dag_corpus, airflow_dags_folder, "team_a").items():
        assert "team-a" in dag.tags, f"{dag_id} is missing its team-a tag"
```
