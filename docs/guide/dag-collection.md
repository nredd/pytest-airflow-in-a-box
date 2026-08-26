# One pytest item per Dag file

Every `*.py` file in your Dag folder becomes its own named, selectable pytest item that fails
if the file does not import or defines no Dags. One broken file is one red row you can `-k`,
shard across `pytest-xdist`, and read as a per-file line in JUnit XML -- instead of one
aggregated assert that tells you the corpus is broken and nothing else.

```console
pytest --collect-dag-folder=dags/
```

or persistently via the `airflow_collect_dags_folder` ini option. Off unless configured.

## What the ten-line `DagBag` conftest test misses

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

## Overlap with `--airflow-smoke`

Both parse the same corpus, and the import-failure message is byte-identical
(`collection.py:341` vs `smoke.py:1027`). Enabling both parses the Dag folder **twice**.

Rule: `--collect-dag-folder` when you want per-file granularity in the report;
[`--airflow-smoke`](smoke-tests.md) when you want corpus-wide policy -- duplicate `dag_id`s, no
top-level Variable access, `catchup=True` anywhere (opt-in) -- which no per-file item can
phrase. Pick one unless you genuinely want both shapes of report.

## The two Dag folder options

They are different options and this is the canonical statement of the difference:

| Option / ini | Feeds |
| --- | --- |
| `--dag-folder` / `airflow_dags_folder` | The `dag_bag` fixture (and through it [`run_dag`](task-execution.md#testing-a-dag-defined-elsewhere)), the [smoke catalog](smoke-tests.md), and `dag_corpus` |
| `--collect-dag-folder` / `airflow_collect_dags_folder` | Dag-file collection, this page |

They are usually the same directory. Set both if you want both features, and cover both under
`--cov` if you measure [Dag coverage](dag-coverage.md).

## What gets collected

- Files directly below the configured folder or any subdirectory of it, with a `.py` suffix
- Files whose name starts with `_` are skipped
- Each collects a `dag-import` item, auto-marked `db_test`
- Files also matching pytest's `python_files` patterns (`test_*.py`), or passed directly on the
  command line, are collected by pytest's default Python collector too; those duplicates are
  pruned after collection

## Pinned param cases

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
