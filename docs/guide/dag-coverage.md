# Proving your Dag files are actually executed

A green suite over a 300-file `dags/` folder tells you nothing about how many of those files a
test ever touched. Coverage does, and it needs no plugin support -- one `pytest-cov` flag
naming the Dag folder is the whole answer:

```console
pytest --cov=dags --cov-report=term-missing
```

`pytest-cov` is not a dependency of this plugin; add it to your project's dev dependencies (or,
in CI provisioned by the bundled [GitHub Action](ci/github-action.md), list it in the file
passed to the Action's `requirements-file` input so it lands in the same venv).

## Why this works

Every parse path in the plugin -- `dag_bag`, the [smoke catalog's](smoke-tests.md) corpus
builder, and [Dag-file collection](dag-collection.md) items -- funnels through one plain
in-process `DagBag` constructor. Airflow's importer loads each Dag file with a
`SourceFileLoader` whose *module name* is mangled (`unusual_prefix_<hash>_<stem>`), but whose
code objects carry the real on-disk file path, and `exec_module` runs inside the pytest
process. `coverage.py` attributes lines by code-object filename, not module name, so Dag files
land in the report under their real paths.

Task callables execute in-process too -- both the DB-backed runners and the DB-free
`run_task` -- so operator and TaskFlow bodies are measured, not just top-level Dag definitions.
No in-process path spawns a dag-processor subprocess or a task-SDK bundle parse, so none of
`coverage.py`'s subprocess settings (`concurrency`, `sigterm`, `patch = subprocess`) are needed
for any of it.

## The one path that is not in-process

[`run_dag(dag, executor=...)`](task-execution.md#executor-driven-runs) is the exception. An
executor-driven run re-imports the Dag *file* from a bundle in a fresh supervised worker
subprocess and executes the task body there, reporting back to the live Task Execution API.
Lines that only ever execute inside that worker are not recorded by the pytest-cov session
measuring the parent process, so a task body reached *only* through an `executor=` run reads as
uncovered.

Nothing in this plugin wires up coverage's subprocess support to fix that, and it is not tested
here. Cover the body from an in-process rung -- `run_task`, or `run_dag` without `executor=` --
and keep the `executor=` test for what it uniquely proves: that the body survives re-import in
a subprocess and that your executor round-trips through a real API.

The Dag file's top-level lines stay covered regardless, since the parent process parsed it to
get the Dag in the first place.

## Footguns

- `--cov=src` (or `--cov=<your package>`) *alone* silently excludes the Dag folder.
  `pytest-cov` reports only files below its configured sources, so Dag files vanish from the
  report with no warning and the total looks fine. Pass one `--cov` per measured folder:
  `--cov=dags --cov=src`
- There are TWO Dag folder options, and they can point at different directories. Coverage sees
  only what `--cov` names, so cover every folder your tests actually parse. See
  [the two Dag folder options](dag-collection.md#the-two-dag-folder-options)
- When neither `--dag-folder` nor `airflow_dags_folder` is set, `dag_bag` falls back to the
  empty scratch `dags/` directory below the disposable bootstrap run root. Never feed that path
  to `--cov`: it is temporary, per-run, and deleted at session end

`pytest --airflow-doctor` prints a "Dag coverage" section covering exactly these: the resolved
Dag and collection folders, whether `pytest-cov` is installed and active, and whether the Dag
folder sits inside a configured `--cov` source, with a copy-pasteable fix when it does not. See
[Diagnosing a run](../reference/diagnostics.md).

## CI

For merging data recorded in different checkouts or containers (matrix legs, an `xml` upload),
set path-independent recording in your `pyproject.toml`:

```toml
[tool.coverage.run]
relative_files = true
```

Under `pytest-xdist`, `pytest-cov` combines per-worker data files automatically, so totals are
unaffected -- the transient `.coverage.<host>.<pid>.*` files are one per worker, not a problem
to fix. The smoke catalog electing a single worker to parse the corpus (the others
[decode its published artifact](../internals/dag-corpus.md#one-parse-per-run-not-per-worker))
does not distort totals either: parsing is where Dag-file lines execute, and the electing
worker records them.
