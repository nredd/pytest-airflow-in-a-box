# Smoke Tests

One-Dag tests cannot detect failures that belong to the *set*: duplicate IDs, import-time I/O,
or an unbounded mapped task. Choose the corpus-wide tool by the result you need:

| Tool | Question | Enable |
| --- | --- | --- |
| Smoke catalog | Does the corpus follow our policies? | `--airflow-smoke` |
| Dag-file collection | Which individual file fails to import? | `--collect-dag-folder=dags/` |
| `pytest-cov` | Which Dag lines did the suite execute? | `--cov=dags` |

The catalog shares one portable `DagCorpus` across its checks. See the
[catalog reference](../reference/smoke.md) for every check, option, and scaling control.

## Turn on the catalog

```console
pytest --airflow-smoke --dag-folder=dags/
```

For a repository default, set `airflow_smoke = true` and `airflow_dags_folder = dags/`.
The catalog checks imports and duplicate IDs, serialization, scheduling, pools, and top-level
secrets or I/O. Opt-in policies cover catchup, unbounded expansion, IDs, tags, owners, and
serialization snapshots. Disable policies your repository does not share.

Each enabled check is a separate `::smoke::<check-name>` test and JUnit row. The default
catalog initializes the metadata database lazily for its integrity and pool checks; it never
starts the REST API.

Every item carries the `smoke` marker. Bare and directory runs include the catalog; file and
node-ID runs exclude it unless `-m` selects `smoke`. The
[selection reference](../reference/smoke.md#selection-and-configuration) covers filtering and
disabling.

## One item per Dag file

```console
pytest --collect-dag-folder=dags/
```

Every non-underscore `*.py` file below the folder becomes a `dag-import` item. Import errors and
files that declare no Dags fail separately, giving each file a selectable target and JUnit row.
These items initialize the metadata database lazily; they do not start the API.

Use collection when per-file reporting matters; use the catalog for corpus-wide policy. Using
both parses the folder twice, so do it only when you need both report shapes.

### Pinned param cases

`PYTEST_DAG_CASES` pins named parameter dictionaries inside a Dag file. The collector reads the
literal without importing the module, then validates each `dag-params[name]` case against every
Dag in that file. Use it only when production Dag files may contain test metadata.

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

Coverage needs no plugin machinery. It attributes executed lines to their real filenames, and
in-process runners measure task bodies too.

The exception is `run_dag(..., executor=...)`: its worker subprocess is outside the plugin's
coverage setup. Cover the body through an in-process rung and keep the executor test for what it
uniquely proves—re-import and Task Execution API transport.

The classic false green is `--cov=src` alone: 100% then measures nothing under `dags/`.
`pytest --airflow-doctor` detects a configured Dag folder outside every coverage source.

## Writing your own corpus check

`dag_corpus` exposes the same read-only, process-portable metadata used by the catalog:

```python
def test_every_dag_has_an_owner_tag(dag_corpus):
    for dag_id, dag in dag_corpus.dags.items():
        assert dag.tags, f"{dag_id} has no tags"
```

Use it for repository-specific rules over tags, task shape, schedules, or serialized structure;
use `dag_bag` when the test needs live Airflow objects. To filter one subtree without reparsing,
import `dags_under` from `pytest_airflow_in_a_box.dagcorpus`.

Requesting `dag_corpus` serializes every Dag because the builder cannot predict which fields the
test will inspect. Under xdist, prefer `-n auto --dist loadgroup` so corpus consumers share one
worker. Large repositories can sample catalog serialization or fan out parsing; see
[performance and parallelism](../reference/smoke.md#performance-and-parallelism).
