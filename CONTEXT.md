# pytest-airflow-in-a-box

A pytest plugin that tests Apache Airflow 3 DAGs without a live deployment: an isolated
`AIRFLOW_HOME`, a disposable metadata database, and typed fixtures for Dags, DagRuns, task
instances, and a live REST API.

## Language

**Smoke check**:
One bundled, opt-in policy assertion over a parsed Dag corpus (e.g. "no Dag enables
`catchup`", "every `dag_id` matches a configured pattern"). Declared as a `SmokeCheck` entry
in `SMOKE_CATALOG` (`smoke.py`), not a hand-written test function.
_Avoid_: Smoke item (the pre-catalog name for a check's dedicated `pytest.Item` subclass)

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
