# Smoke Tests

One-Dag tests cannot detect failures that belong to the *set*: duplicate IDs, import-time I/O,
or an unbounded mapped task. The plugin offers three corpus-level tools, each answering a
different question.

| Tool | Question | Enable |
| --- | --- | --- |
| Smoke catalog | Does the corpus follow our policies? | `--airflow-smoke` |
| Dag-file collection | Which individual file fails to import? | `--collect-dag-folder=dags/` |
| `pytest-cov` | Which Dag lines did the suite execute? | `--cov=dags` |

All plugin-owned checks share a portable `DagCorpus`; the implementation and exact catalog are
in [Smoke catalog and corpus mechanics](../reference/smoke.md).

## Turn on the catalog

```console
pytest --airflow-smoke --dag-folder=dags/
```

Or set `airflow_smoke = true`. The default catalog checks imports, serialization, scheduling,
pool references, top-level Variable/Connection access, and top-level I/O. Optional policies
cover catchup, unbounded expansion, IDs, tags, owners, and serialization snapshots.

These checks remain inside the boundary from [Whose fail is it anyway?](testing-scope.md): the
subject is your schedule, constructor arguments, and import behavior—not whether stock Airflow
works. Disable any policy your repository does not share.

Every item carries the `smoke` marker. Explicit file or node-ID selection drops the catalog
unless the `-m` expression itself names `smoke`; ordinary directory or bare runs retain it.
See the [selection reference](../reference/smoke.md#selection-and-configuration) for exact
precedence and disabling.

## One item per Dag file

```console
pytest --collect-dag-folder=dags/
```

Every `*.py` file below the folder becomes a named `dag-import` item. A file that imports but
declares no Dags fails too. That produces one selectable `-k` target and one JUnit row per file
instead of collapsing fifty broken files into one `DagBag.import_errors` assertion.

Use collection when per-file reporting matters; use the catalog for corpus-wide policy. Using
both parses the folder twice, so do it only when you need both report shapes.

### Pinned param cases

`PYTEST_DAG_CASES` may pin named parameter dictionaries inside a Dag file. The collector reads
the literal by AST and creates one `dag-params[name]` item per case, validating names and values
against every Dag in that file. It is intentionally test metadata in production code; skip it
when your repository forbids that tradeoff.

### The two Dag folder options

Two options sound alike because they feed different consumers:

| Option | Feeds |
| --- | --- |
| `--dag-folder` / `airflow_dags_folder` | `dag_bag`, `run_dag`, the smoke catalog, and `dag_corpus` |
| `--collect-dag-folder` / `airflow_collect_dags_folder` | Per-file Dag collection |

## Dag coverage

```console
pytest --cov=dags --cov=src --cov-report=term-missing
```

No plugin machinery is needed. Airflow changes imported module names, but coverage attributes
executed lines by their real filenames. In-process task runners measure task bodies too.

The exception is `run_dag(..., executor=...)`: its worker subprocess is outside the plugin's
coverage setup. Cover the body through an in-process rung and keep the executor test for what it
uniquely proves—re-import and Task Execution API transport.

The classic false green is `--cov=src` alone: Dag files never enter the report, so 100% measured
nothing under `dags/`. `pytest --airflow-doctor` detects a configured Dag folder outside every
coverage source and prints the fix.

## Writing your own corpus check

`dag_corpus` exposes the same read-only, process-portable metadata used by the catalog:

```python
def test_every_dag_has_an_owner_tag(dag_corpus):
    for dag_id, dag in dag_corpus.dags.items():
        assert dag.tags, f"{dag_id} has no tags"
```

Use it for tags, task shape, schedules, or serialized structure. Use `dag_bag` only when the
test needs live Airflow objects. `dags_under(dag_corpus, airflow_dags_folder, "team_a")` narrows
the mapping to one subtree without reparsing.

Requesting `dag_corpus` serializes every Dag because the builder cannot predict which fields a
test will inspect. Large repositories can sample catalog serialization or fan the parse across
subprocesses; see [performance and parallelism](../reference/smoke.md#performance-and-parallelism).
