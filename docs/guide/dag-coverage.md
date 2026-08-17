# Dag coverage

Coverage over your own Dag files needs no plugin support -- one `pytest-cov` flag naming the
Dag folder is the whole answer:

```console
pytest --cov=dags --cov-report=term-missing
```

`pytest-cov` is not a dependency of this plugin; add it to your project's dev dependencies
(or, in CI provisioned by the bundled GitHub Action, list it in the file passed to the
Action's `requirements-file` input so it lands in the same venv).

## Why this works

Every parse path in the plugin -- `full_dag_bag`, the smoke catalog's corpus builder, and
Dag-file collection items -- funnels through one plain in-process `DagBag` constructor.
Airflow's importer loads each Dag file with a `SourceFileLoader` whose *module name* is
mangled (`unusual_prefix_<hash>_<stem>`), but whose code objects carry the real on-disk
file path, and `exec_module` runs inside the pytest process. `coverage.py` attributes lines
by code-object filename, not module name, so Dag files land in the report under their real
paths. Task callables execute in-process too (both the DB-backed runners and the DB-free
`run_task`), so operator and TaskFlow bodies are measured, not just top-level Dag
definitions. There is no dag-processor subprocess and no task-SDK bundle parse to lose data
to, which is why none of `coverage.py`'s subprocess settings (`concurrency`,
`sigterm`, `patch = subprocess`) are needed.

## Footguns

- `--cov=src` (or `--cov=<your package>`) *alone* silently excludes the Dag folder.
  `pytest-cov` reports only files below its configured sources, so Dag files vanish from
  the report with no warning and the total looks fine. Pass one `--cov` per measured
  folder: `--cov=dags --cov=src`
- There are TWO Dag folder options: `--dag-folder` / `airflow_dags_folder` feeds
  `full_dag_bag` (and, through it, [`run_dag`](task-execution.md#testing-a-dag-defined-elsewhere))
  and the [smoke catalog](smoke-tests.md), while `--collect-dag-folder` /
  `airflow_collect_dags_folder` feeds [Dag-file collection](dag-collection.md). They are
  usually the same directory, but coverage sees only what `--cov` names -- cover every
  folder your tests actually parse
- When neither `--dag-folder` nor `airflow_dags_folder` is set, `full_dag_bag` falls back
  to the empty scratch `dags/` directory below the disposable bootstrap run root. Never
  feed that path to `--cov`: it is temporary, per-run, and deleted at session end

`pytest --airflow-doctor` prints a "Dag coverage" section covering exactly these: the
resolved Dag and collection folders, whether `pytest-cov` is installed and active, and
whether the Dag folder sits inside a configured `--cov` source (with a copy-pasteable fix
when it does not). See [Diagnostics](../reference/diagnostics.md).

## CI

For merging data recorded in different checkouts or containers (matrix legs, an `xml`
upload), set path-independent recording in your `pyproject.toml`:

```toml
[tool.coverage.run]
relative_files = true
```

Under `pytest-xdist`, `pytest-cov` combines per-worker data files automatically, so totals
are unaffected -- the transient `.coverage.<host>.<pid>.*` files are one per worker, not a
problem to fix. The smoke catalog electing a single worker to parse the corpus (the others
reuse its published serialized artifact) does not distort totals either: parsing is where
Dag-file lines execute, and the electing worker records them.
