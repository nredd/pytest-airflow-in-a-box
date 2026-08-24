# Extract the Dag corpus builder from smoke.py into dagcorpus.py, fixing a serialization gap

ADR 0001 named `_build_smoke_corpus` and its collaborators (the corpus data model, the
fan-out-eligible builder, the cross-worker JSON cache) as "out of this change's scope" for
future reconsideration -- they were still private to `smoke.py`, with the bundled
`--airflow-smoke` catalog as their only consumer. Issue #277 asked for a public `dag_corpus`
fixture, backed by the same portable, fan-out-eligible parse the catalog already builds, so a
repository-defined whole-corpus check (e.g. "every Dag must carry an `owner` tag") does not
have to choose between reimplementing that machinery or accepting a full serial `dag_bag`
parse per xdist worker. That is the second real, independently-motivated consumer this
repo's stated precedent (`db.py`, `components.py`, ADR 0001 itself) treats as the trigger for
extraction, rather than leaving a growing pile of "shared" logic owned by one module's
private namespace.

A minimal-diff alternative was considered: leave `smoke.py` owning everything and add a
ten-line fixture shim calling its private `_smoke_corpus`. It was rejected for two reasons.
First, naming: issue #277 frames `dag_corpus` consumers as "repository-defined... not part of
the smoke catalog," but a shim fixture would still hand back a type literally named
`SmokeCorpus`, imported from `pytest_airflow_in_a_box.smoke` -- misleading for a check that
has nothing to do with `--airflow-smoke`. Second, and decisive on its own:
`_smoke_serialization_needed` decided whether the corpus builder called the Dag serializer by
reading `airflow_smoke_disable`/`airflow_dag_snapshot_dir` ini values, without ever checking
whether `--airflow-smoke` itself was enabled. A project with those ini values set but smoke
off (plausible leftover config, or smoke used with those two checks disabled) would make
`_smoke_serialization_needed` return `False` unconditionally -- so a bare `dag_corpus`-only
run would silently build a corpus with every `.serialized` field `None`, a smoke-catalog ini
knob quietly deciding a public fixture's data shape. A shim fixture calling `_smoke_corpus`
directly would inherit that bug outright, and fixing it correctly requires exactly the kind
of generalization extraction does anyway.

We moved the corpus data model (`SmokeTask`/`SmokeDagFileStat`/`SmokeDag`/`SmokeCorpus`) and
the building/caching pipeline (`_build_smoke_corpus`, the payload codecs, the flock-guarded
shared-artifact cache, `_smoke_corpus` itself) out of `smoke.py` into a new `dagcorpus.py`,
renamed to the generalized `CorpusTask`/`CorpusDagFileStat`/`CorpusDag`/`DagCorpus` family and
`_build_dag_corpus`/`_dag_corpus_payload`/`_dag_corpus_from_payload`/`_shared_dag_corpus`, with
the caching entry point promoted to a public `get_dag_corpus` (dropping the leading
underscore, since it now has two real callers in two modules). `smoke.py` keeps everything
actually specific to the bundled catalog -- `_smoke_enabled`, `_smoke_in_scope`,
`_disabled_smoke_items`, `_smoke_serialization_needed`, `_smoke_item_timeout`,
`SMOKE_CATALOG`, `SmokeCheck`, `SmokeContext`, `SmokeCollector` -- and `SmokeContext.corpus`
becomes a one-line delegation to `get_dag_corpus`.

Alongside the move, we fixed the serialization-need gap directly rather than carrying it
forward under a new name. `dagcorpus.py` now owns `_corpus_serialization_needed`: a run with
at least one collected `dag_corpus` consumer (tracked via
`DAG_CORPUS_WANTS_SERIALIZATION_KEY`, set by `mark_dag_corpus_requested` from `plugin.py`'s
collection hook) always serializes every Dag, since there is no cheap way to know a test
body's field usage in advance. Absent that, the predicate checks `_smoke_enabled` and
`_smoke_in_scope` before ever delegating to `_smoke_serialization_needed` -- so the smoke
catalog's own ini knobs decide serialization only when the catalog is actually the one asking.
A run with neither a `dag_corpus` consumer nor an active catalog defaults to serializing
everything, the same safe default a `dag_bag`-equivalent parse would produce. Both call sites
that used to read `_smoke_serialization_needed` directly (the fan-out `serialize=` argument in
`_build_dag_corpus`, and `_select_serialization_sample`'s internal gate) now go through this
predicate instead.

xdist co-location generalizes the same way, deliberately asymmetrically to the existing
`dag_bag` precedent. `_colocate_smoke_catalog_with_dag_bag` anchors on exactly one `dag_bag`
consumer, to avoid serializing a suite's potentially large set of real-execution tests onto a
single worker just to save one parse. The new `_colocate_dag_corpus_consumers` groups *every*
surviving `dag_corpus` consumer instead: those consumers are expected to be few, cheap,
read-only metadata checks, so colocating all of them costs nothing extra, while leaving any of
them behind would mean it independently pays the `_shared_dag_corpus` flock-wait and
JSON-decode cost the whole mechanism exists to avoid. When a run has both kinds of consumer,
the corpus pass runs first and, if it grouped anything, hands the `dag_bag` pass an empty
smoke-item list so the two groups never both claim the catalog -- `get_dag_corpus`'s
cross-worker flock election still guarantees exactly one global build regardless of which
group wins, so this precedence is a scheduling choice, not a correctness one.

Consequences: `SecretsLookup` (`_compat/introspection.py`, previously unexported anywhere) is
now re-exported from `dagcorpus.py.__all__`, since it is reachable through the public
`DagCorpus.runtime_lookups` field. `dagcorpus.py` needs one deferred import back into
`smoke.py` (for `_smoke_enabled`/`_smoke_in_scope`/`_smoke_serialization_needed`, inside
`_corpus_serialization_needed`) to avoid a load-time cycle, mirroring the existing
`fixtures/dagbag.py::_cached_dag_bag` idiom; `fixtures/dagcorpus.py`'s own fixture needs the
same treatment for `get_dag_corpus` itself, since `dagcorpus.py` importing `fixtures.dagbag`
at module level runs this package's `fixtures/__init__.py`, which now also imports
`fixtures/dagcorpus.py` -- a cycle that only exists because the new fixture module both
depends on and is depended on by the same package init. `.airflow-smoke-corpus.json`/
`.airflow-smoke-corpus.lock` are renamed to `.airflow-dag-corpus.json`/
`.airflow-dag-corpus.lock` -- safe, since these are ephemeral per-run scratch files, not a
stable external contract. Unlike `dag_bag`'s fixture, `dag_corpus` never calls
`ensure_database` unconditionally: the builder already initializes the database
conditionally, only when parse-time secrets resolution is active, and forcing it eagerly here
would make a `dag_corpus`-only run pay the one-time migration cost even when nothing needs it.
`DagCorpus.dags`/`.import_errors` are `MappingProxyType` views, not plain `dict`s -- issue
#277 explicitly asked for "a documented immutable public model," and `frozen=True` alone only
blocks reassigning an attribute, not mutating a mutable object it points to; a consumer
mutating a shared, cross-worker-cached corpus would corrupt it for every other consumer in the
same worker process. `CorpusDag.tags`/`.tasks` already used `frozenset`/`tuple` for the same
reason; `dags`/`import_errors` were the two fields that hadn't caught up.

Separately profiled and rejected in the same pass: swapping stdlib `json` for `orjson` in the
fan-out payload codecs (`parallel_dagbag.py`'s `encode_shard`/`merge_shard_payloads`,
`dagcorpus.py`'s payload functions). Synthetic-corpus profiling found JSON encode/decode under
5% of total fan-out wall-clock at every scale tested -- subprocess spawn and Airflow import
plus DAG-file parsing dominate by a wide margin -- so the dependency add was not worth
carrying.
