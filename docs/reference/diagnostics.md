# Diagnosing a run

Your Dag coverage report says 100%. It measured nothing.

`pytest --cov=src` over a repo whose Dags live in `dags/` produces a green report that never
opened a Dag file, and neither pytest nor `pytest-cov` will say so -- the files simply are not
in the report. `--airflow-doctor` is the only thing that checks:

```console
$ pytest --airflow-doctor --dag-folder=dags --cov=src
...
## Dag coverage
- Dag folder: `/tmp/repo/dags`
- Collection folder: not configured (`--collect-dag-folder` / `airflow_collect_dags_folder`)
- NOT COVERED: the Dag folder sits outside every configured `--cov` source, so Dag files are silently missing from the report. Add it: `pytest --cov=/tmp/repo/dags --cov-report=term-missing`
```

That is a setup check, not a bug report. Run it once when you wire the plugin in, and again
whenever a run behaves differently from the last one.

## Running it

```console
pytest --airflow-doctor
```

It short-circuits the session: no collection, no workers, no tests. Bootstrap has already run
by then, so the report describes a real bootstrapped environment.

## A real report

Real output of `uv run pytest --airflow-doctor` in this repo, with the capability list cut
down (see the note under the block); nothing else is edited:

```markdown
# pytest-airflow-in-a-box diagnostics

## Storage
- Reason: `caller-temp`
- Network filesystem: `False`

## AIRFLOW_HOME and database
- `AIRFLOW_HOME`: `/Volumes/tome/tmp/pytest-airflow-in-a-box-_n2aa63x`
- Backend tier: `sqlite`
- Database URL scheme: `sqlite`

## Airflow config overrides
- No `airflow_config` overrides are declared

## Versions and capabilities
- `pytest-airflow-in-a-box`: `0.11.1`
- `pytest`: `9.1.1`
- Python: `3.13.15`
- Apache Airflow: `3.3.0`
- Capability `release`: `3.3.0`
- Capability `family`: `apache-airflow-core`
- Capability `dag_bag_location`: `airflow.dag_processing.dagbag`
- Capability `task_instance_runner`: `airflow.sdk.definitions.dag._run_task`
- Capability `has_task_sdk`: `True`
- Capability `uses_structlog`: `True`
- Capability `has_dag_versioning`: `True`
- Capability `dagrun_interface`: `logical_date`
- Capability `api_surface`: `airflow api-server`
- Capability `secrets_resolution`: `airflow.sdk.execution_time.task_runner.SUPERVISOR_COMMS`
- Capability `executor_contract`: `3.3`
- Capability `certification`: `certified`

## Executor
- `core.executor`: `LocalExecutor`

## Migration-strict
- `--airflow-migration-strict`: disabled

## Worker environment drift
- `airflow_worker_env_drift`: `error`

## Dag coverage
- Dag folder: `/Volumes/tome/tmp/pytest-airflow-in-a-box-_n2aa63x/dags` (bootstrap scratch fallback -- neither `--dag-folder` nor `airflow_dags_folder` is configured)
- Collection folder: not configured (`--collect-dag-folder` / `airflow_collect_dags_folder`)
- `pytest-cov`: installed but inactive -- no `--cov` was given, so Dag files are not measured. Pass `--cov=<dag folder> --cov-report=term-missing`

## API server
- Not started: this diagnostic run did not request the `api_server_url` fixture. The API server is a lazy, per-process, session-scoped subprocess with no state before a test requests it.
```

The `Versions and capabilities` section is elided above: the real report prints *every*
resolved capability field, around thirty lines.

## What each section answers

- **Storage** -- which rung of the storage ladder the run landed on and why, plus whether the
  chosen directory is on a network filesystem. See [where the run
  lives](../guide/airflow-home.md)
- **`AIRFLOW_HOME` and database** -- the resolved root, the backend tier, and the database URL
  *scheme*. The full URL is withheld on purpose: a provisioned Postgres URL carries live
  credentials. See [the disposable metadata database](../guide/database.md)
- **Airflow config overrides** -- every override declared through the [`airflow_config` ini
  option](../guide/configuration.md). Those travel on the environment and never appear in the
  generated `airflow.cfg`, so this readback is the only place they are visible -- `airflow
  info` included. A value whose option name reads like a credential (`password`, `secret`,
  `key`, `token`, `conn`, `url`) is replaced by its length; the name is always shown, because
  "which options are set" is the question
- **Versions and capabilities** -- plugin, pytest, Python, and Airflow versions, plus the full
  resolved capability contract. An uncertified Airflow release adds a `DEGRADED:` bullet
  saying the capabilities were resolved by live probing rather than byte-verified
  certification, and naming the remedy. See [what `_compat/`
  absorbs](../internals/compat-layer.md)
- **Executor** -- the resolved `core.executor`. On the Airflow 2.x family with SQLite it adds
  an `INCOMPATIBLE:` bullet when the configured executor is multi-threaded, because Airflow
  2.x's `ready_to_reschedule` sensor dependency rejects that combination with no indication of
  the cause
- **Migration-strict** -- whether [`--airflow-migration-strict`](../guide/migration/strict.md)
  is on, flagged as a no-op off the 2.x family
- **Worker environment drift** -- the configured `airflow_worker_env_drift` policy for a
  worker or isolated child that inherits a drifted Airflow environment
- **Dag coverage** -- the resolved Dag and collection folders, whether `pytest-cov` is
  installed and active, and the containment check above. See [proving your Dag files are
  actually executed](../guide/dag-coverage.md)
- **API server** -- always "not started". A diagnostic run never requests the fixture

## Coverage verdicts

The Dag coverage section is the one that fails you, so its verdicts are worth knowing:

- `pytest-cov`: not installed, or installed but inactive, or disabled by `--no-cov`. No
  measurement is happening, for three different reasons
- `NOT MEASURABLE` -- no Dag folder is configured, so the fallback is a disposable bootstrap
  scratch directory. Never feed that to `--cov`. Set `--dag-folder` or `airflow_dags_folder`
  first
- `NOT COVERED` -- the Dag folder sits outside every `--cov` source. The false green
- `Dag folder covered by --cov=...` -- the numbers mean what you think they mean

## Where the report's `AIRFLOW_HOME` goes

The `AIRFLOW_HOME` printed here belongs to the diagnostic run that just bootstrapped it, not
to a previous test run, and it is removed on the way out unless you pass
`--airflow-home-retention=all`. For the root of a session that actually ran tests, read the
session header line the plugin adds (suppressed by `-q` and `--no-header`) -- see [where the run lives](../guide/airflow-home.md).
