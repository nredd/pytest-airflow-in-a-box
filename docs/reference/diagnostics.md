# Airflow Doctor

Use `pytest --airflow-doctor` to inspect the environment this plugin actually bootstrapped. It
is a one-shot preflight for storage, Airflow compatibility, executor configuration, migration
settings, and Dag coverage—not a test run or a replacement for the smoke catalog.

The most valuable check catches false-green coverage. A repository can report 100% for `src/`
while never measuring its `dags/` folder; neither pytest nor `pytest-cov` treats those absent
files as a failure. Doctor compares the resolved Dag folder with every active `--cov` source.

## Running it

```console
pytest --airflow-doctor --dag-folder=dags --cov=src
```

Doctor runs after early bootstrap, then exits before `pytest_configure`, test collection, or
xdist worker startup. It never runs tests, migrates the metadata database, or starts the REST
API. The report describes this invocation's newly created `AIRFLOW_HOME`, not a previous run.

On a valid invocation, the command exits `0` after printing the report. Labels such as
`INCOMPATIBLE`, `DEGRADED`, `NOT MEASURABLE`, and `NOT COVERED` tell you what to fix; they do
not turn Doctor into a CI gate. Configuration parsing or bootstrap can still fail normally;
retention validation happens after rendering and can make the invocation nonzero even though
the report was printed.

The flag has no ini equivalent. See [CLI and INI options](ini-options.md#diagnostics-and-reports)
for the canonical option catalog and
[`doctor.py`](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/src/pytest_airflow_in_a_box/doctor.py)
for the report implementation.

## A real report

This abbreviated example retains every section. A real report prints every resolved capability
field under `Versions and capabilities`.

```markdown
# pytest-airflow-in-a-box diagnostics

## Storage
- Reason: `caller-temp`
- Network filesystem: `False`

## AIRFLOW_HOME and database
- `AIRFLOW_HOME`: `/tmp/pytest-airflow-in-a-box-abc123`
- Backend tier: `sqlite`
- Database URL scheme: `sqlite`

## Airflow config overrides
- No `airflow_config` overrides are declared

## Versions and capabilities
- `pytest-airflow-in-a-box`: `0.12.0`
- `pytest`: `9.1.1`
- Python: `3.12.13`
- Apache Airflow: `3.3.0`
- Capability `certification`: `certified`

## Executor
- `core.executor`: `LocalExecutor`

## Migration-strict
- `--airflow-migration-strict`: disabled

## Worker environment drift
- `airflow_worker_env_drift`: `error`

## Dag coverage
- Dag folder: `/tmp/repo/dags`
- Collection folder: not configured (`--collect-dag-folder` / `airflow_collect_dags_folder`)
- NOT COVERED: the Dag folder sits outside every configured `--cov` source, so Dag files are silently missing from the report. Add it: `pytest --cov=/tmp/repo/dags --cov-report=term-missing`

## API server
- Not started: this diagnostic run did not request the `api_server_url` fixture.
```

## What each section answers

| Section | Read it for | Follow-up |
| --- | --- | --- |
| **Storage** | The selected [storage-ladder](../internals/test-environments.md#the-isolated-airflow_home) rung and whether its filesystem is network-backed. `True` is expected only when an explicit network location was allowed or the writable fallback was unavoidable. | Pin a local base with `--airflow-home` when SQLite safety or retained logs matter. |
| **`AIRFLOW_HOME` and database** | The exact disposable run root, selected `sqlite` or `postgres` tier, and database URL scheme. Credentials are never printed. | Check [database backend requirements](../internals/test-environments.md#the-disposable-metadata-database) when the tier is unexpected. |
| **Airflow config overrides** | Every early `airflow_config` declaration, sorted by section and key. Credential-shaped keys containing `password`, `secret`, `key`, `token`, `conn`, or `url` show only the value length. | Compare the declarations with the [configuration grammar and ownership rules](ini-options.md#airflow_config). Runtime `airflow_config()` contexts and `airflow_configure` batches do not exist in this pre-test invocation. |
| **Versions and capabilities** | Installed plugin, pytest, Python, and Airflow versions, followed by every resolved compatibility field. `unprobed on this family` means the capability does not apply to that Airflow family. | Interpret the fields through [Compatibility and certification](../internals/compat-layer.md#how-a-probe-works). |
| **Executor** | The effective `core.executor`, including consumer overrides. For a comma-separated executor list, compatibility is judged from the first entry. | Use [session configuration](../guide/custom-components-wiring.md#session-configuration) to change the executor or select [Postgres](../internals/test-environments.md#the-disposable-metadata-database). |
| **Migration-strict** | Whether Airflow 2-to-3 warning promotion is disabled, enabled on Airflow 2, or enabled where it is a no-op. | See [migration-strict mode](../guide/migration.md#migration-strict-mode). |
| **Worker environment drift** | Whether an xdist worker or isolated child rejects inherited bootstrap-variable drift (`error`) or reinstalls the controller's values and warns (`repair`). | Keep `error` unless a foreign conftest or plugin cannot be changed; `repair` does not prevent a later mutation. See [environment ownership](../internals/test-environments.md#pytest-xdist-and-environment-ownership). |
| **Dag coverage** | The resolved Dag folder, the independent per-file collection folder, pytest-cov activation, and whether an active source contains the Dag folder. | Use the verdict table below and the [Dag coverage guide](../guide/smoke-tests.md#dag-coverage). |
| **API server** | Always `Not started`: Doctor exits before a fixture can activate the lazy server. | Use the [REST API fixtures](../guide/rest-api.md) in a real test when server behavior is the subject. |

### Compatibility and executor verdicts

- `INCOMPATIBLE` under **Versions and capabilities** means the installed Airflow family is
  below its structural floor or a required symbol/probe failed. The remaining report still
  renders; install a supported Airflow release or upgrade the plugin.
- `DEGRADED` means the release is structurally supported but has no byte-verified capability
  row. Live probes succeeded, while unknown component-registry caches use generic
  snapshot/restore. Upgrade once that release is certified, or pin a certified release.
- `INCOMPATIBLE` under **Executor** is specific to Airflow 2 with SQLite and a primary executor
  other than `SequentialExecutor` or `DebugExecutor`. Restore a single-threaded executor,
  select Postgres, or—only for an executor known to be single-threaded—set Airflow's exact
  `_AIRFLOW__SKIP_DATABASE_EXECUTOR_COMPATIBILITY_CHECK=1` escape hatch. Executor resolution
  failures are reported as `could not resolve` without hiding the other sections.

## Coverage verdicts

Doctor evaluates these outcomes in order:

| Verdict | Meaning | Action |
| --- | --- | --- |
| `pytest-cov: not installed` | The `--cov` option does not exist. | Install `pytest-cov`, then select the Dag folder as a source. |
| `pytest-cov: installed but inactive` | No `--cov` source activated coverage. | Pass `--cov=<dag folder> --cov-report=term-missing`. |
| `pytest-cov: disabled by --no-cov` | `--no-cov` disabled measurement despite configured sources. | Remove `--no-cov`. |
| `NOT MEASURABLE` | The resolved folder is the disposable bootstrap `dags/` fallback because no repository Dag folder was configured. | Set `--dag-folder` or `airflow_dags_folder`, then cover that path. Never cover the fallback. |
| `NOT COVERED` | Coverage is active, but every source sits outside the resolved Dag folder. | Add the absolute path printed in the copy-pasteable command. |
| `Dag folder covered by --cov=...` | A bare `--cov` or one configured source contains the Dag folder. | Coverage can measure Dag files; this does not prove that tests executed every line. |

The collection-folder line is informational because per-file collection and coverage are
separate mechanisms. A missing or invalid configured collection directory is rendered as
`MISCONFIGURED` while the coverage diagnosis continues.

## Where the report's `AIRFLOW_HOME` goes

The default `failed` retention policy removes Doctor's run directory because Doctor exits
successfully. Pass `--airflow-home-retention=all` to keep it; `none` always discards it. A
Postgres container stops under every policy.

For a session that actually ran tests, read the session header instead. It names that session's
root and is suppressed by `-q` or `--no-header`. See
[the isolated `AIRFLOW_HOME`](../internals/test-environments.md#the-isolated-airflow_home) for
retention, header, and storage-ladder details.
