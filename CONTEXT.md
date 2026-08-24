# pytest-airflow-in-a-box

A pytest plugin that tests Apache Airflow 3 DAGs without a live deployment: an isolated
`AIRFLOW_HOME`, a disposable metadata database, and typed fixtures for Dags, DagRuns, task
instances, and a live REST API.

## Language

**Smoke check**:
One bundled, opt-in policy assertion over a parsed Dag corpus (e.g. "no Dag enables
`catchup`", "every `dag_id` matches a configured pattern"). Declared as a `SmokeCheck` entry
in `SMOKE_CATALOG` (`smoke.py`); collected as a "smoke item" -- the term still used in
`airflow_smoke_disable`'s help text and error messages, since that option names collected
items, not catalog entries.

**Smoke catalog**:
The full set of bundled smoke checks (`SMOKE_CATALOG`), collected as a synthetic `smoke`
node under `SmokeCollector` when `--airflow-smoke`/`airflow_smoke` is enabled.

**SmokeCheck**:
The frozen dataclass describing one catalog entry: its collected name, an `enable(config)`
predicate resolving its payload (or `None` when disabled), its extra pytest marks, and the
`run(context, payload)` function that executes it.

**SmokeContext**:
The lazily-computed bundle (`session`, `config`, plus cached `corpus`/`dag_folder`/
`serialized_cache`/`disabled` properties) a check's `run` reads from. Constructing one
directly, with a property pre-seeded, is the test seam for a check -- no monkeypatching of
`smoke` internals needed.

**DagCorpus**:
The portable, fan-out-eligible representation of one parsed Dag folder (`dagcorpus.py`):
`dags` (a `dag_id`-keyed map of `CorpusDag` records -- tags, tasks, `fileloc`,
`can_be_scheduled`, `catchup`, and, when serialized, `serialized`), `import_errors`,
`dagbag_stats`, and `runtime_lookups`. `dags`/`import_errors` are read-only
`MappingProxyType` views, not plain `dict`s -- `frozen=True` alone only blocks reassigning
an attribute, not mutating a mutable object it points to, and this is a value shared by
every consumer in one worker process. Built once per worker process and shared with local
`pytest-xdist` workers through a flock-guarded JSON artifact (`get_dag_corpus`); the same
instance backs both the public `dag_corpus` fixture and the bundled smoke catalog's
`SmokeContext.corpus`, so whichever one triggers the build, every consumer in that worker
process shares the exact same parse. Extracted from `smoke.py` (see ADR 0003) once
`dag_corpus` gave the corpus builder a second, independently-motivated consumer beyond the
bundled catalog.

**dag_corpus**:
The public, session-scoped fixture wrapping `get_dag_corpus`. For repository-defined checks
phrased entirely over static Dag metadata -- not part of the bundled smoke catalog, and
read-only: unlike `dag_bag`, it never hands back live Airflow objects, so it cannot cross
back into execution. Migration boundary: prefer `dag_corpus` for a metadata-only check, and
reach for `dag_bag` only once a test needs a live Airflow object (executing a task,
inspecting a real `DAG`/operator instance).
