# Test Environments

Before Airflow imports, the plugin creates an isolated `AIRFLOW_HOME`, configures a disposable
metadata database, and installs the run's configuration. Most users only need to choose where
the run lives, whether it uses SQLite or Postgres, and which configuration scope fits the test.
See [Fixtures](../reference/fixtures.md) and
[CLI and INI options](../reference/ini-options.md) for the complete interfaces.

## The isolated `AIRFLOW_HOME`

The plugin never uses your `~/airflow`. It creates a unique `pytest-airflow-in-a-box-*`
directory before importing consumer conftests or Airflow, then points `AIRFLOW_HOME` at it.
That directory contains the generated configuration, Dags, plugins, logs, authentication
files, and -- on SQLite -- the metadata database. The session header reports its location:

```console
pytest-airflow-in-a-box: AIRFLOW_HOME=/dev/shm/pytest-airflow-in-a-box-8f2a1c (storage: shared-memory, db: sqlite)
```

Under xdist, workers share the controller's run directory and only the controller prints the
header. The `storage:` value identifies the selected base:

| Rung | Base | Selected when |
| --- | --- | --- |
| `explicit` | `--airflow-home=PATH` or `airflow_home` | You supplied a base |
| `caller-temp` | `--basetemp`'s parent, otherwise `$TMPDIR` | The caller selected a local temporary base |
| `shared-memory` | `/dev/shm` | Linux tmpfs has at least 512 MiB free |
| `system-temp` | `tempfile.gettempdir()` | The ordinary local fallback |
| `writable-fallback` | A writable network or unknown filesystem | No verified local base is writable; emits a warning |

On most Linux hosts, `/dev/shm` wins, so retained logs live in RAM and disappear at reboot.
Use `--airflow-home=PATH` for durable artifacts. The plugin removes only the unique child it
creates, never the explicit base itself. It rejects an explicit network or unknown filesystem
unless `--allow-network-airflow-home` acknowledges SQLite's locking risk.

Retention defaults to `failed`: keep the run directory after any non-passing exit, including a
crash. Choose `all` or `none` with `--airflow-home-retention`, and set the number of retained
roots with `--airflow-home-retention-count` (default `3`). A retained directory is always
reported. Postgres containers stop regardless of retention.

One pytest edge remains: a run that fails only because of `--max-warnings` receives that status
after session-finish hooks run, so `failed` retention may remove its directory. Use
`--airflow-home-retention=all` while investigating. The `airflow_home` and
`airflow_dags_folder` fixtures expose the resolved paths; `--airflow-doctor` reports the path,
storage rung, and backend for its own diagnostic run.

## The disposable metadata database

Database setup is lazy: tests that do not request a database-backed fixture never migrate one.
The default backend is one SQLite file for the run, tuned with WAL journaling,
`synchronous = OFF`, and an in-memory temporary store. It is fast, but serializes writers and
cannot reproduce row locks or concurrent-write behavior.

Use Postgres when your code issues metadata SQL or when SQLite's locking model hides or creates
a failure:

```console
pytest --airflow-db-backend=postgres
```

This provisions one testcontainers Postgres instance shared by all xdist workers. It requires
Docker and the [`postgres` extra](../reference/dependencies.md#extras); missing either is a
usage error, never a silent fallback to SQLite.

Within either backend, the `session` fixture rolls back uncommitted work and `dag_maker`
removes rows it owns. Fixtures that seed committed state clean up their own rows. For broader
serial-only cleanup, `clear_db(tables={...})` accepts `TableGroup` members and clears dependent
groups in foreign-key-safe order. Do not call `clear_db` while xdist workers share the database.

## Overriding configuration

All three configuration channels write `AIRFLOW__SECTION__KEY` environment variables; they do
not modify the generated `airflow.cfg`. Environment values take precedence for both Airflow
configuration parsers.

Choose the channel by required lifetime:

- Use the **`airflow_config` ini option** for session constants and anything that must exist
  before consumer conftests or the first Dag parse. Bootstrap-owned settings such as the
  database URL and Dags folder are rejected in favor of their dedicated plugin options.
- Use the **`airflow_config()` context manager or decorator** for one test. It validates the
  complete batch before assignment and restores every name exactly. A `None` value removes the
  variable so Airflow falls back to its normal source; `refresh_settings=True` also recomputes
  cached `airflow.settings` globals.
- Use the session-scoped **`airflow_configure` fixture** for values produced by fixtures, such
  as a temporary path, port, or credential. Its batches unwind in reverse order at teardown.
  It cannot change a Dag already parsed by an earlier session fixture.

```python
from pytest_airflow_in_a_box.config import airflow_config


def test_with_overrides():
    with airflow_config({("core", "unit_test_mode"): "False"}, env={"MY_FLAG": "1"}):
        ...


@airflow_config({("core", "dagbag_import_timeout"): "120"})
def test_decorated(): ...
```

Do not wrap the first request for `api_client` or `api_server_url` in a temporary override: the
session-scoped server keeps the environment it inherited at startup. Retained configuration
files do not contain these overrides; `--airflow-doctor` reports them with credential-shaped
values redacted. See [CLI and INI options](../reference/ini-options.md#core) for grammar,
bootstrap-owned settings, and defaults.

## Seeding Variables and Connections

Prefer Airflow's environment secrets backend when the metastore is not the subject:

```python
def test_hook(monkeypatch):
    monkeypatch.setenv("AIRFLOW_CONN_DB", '{"conn_type": "postgres", "host": "example.com"}')
```

`AIRFLOW_VAR_*` and `AIRFLOW_CONN_*` take precedence over metastore rows. Use
`airflow_variables` or `airflow_connections` when the test must exercise the metastore path;
they commit encrypted rows and remove the rows they created at teardown. A fixture refuses to
seed a name already provided by the environment.

Seeded names are database-global under xdist. Give concurrent tests distinct keys and
connection IDs, or group collisions with `@pytest.mark.xdist_group` and run with
`--dist loadgroup`. See [Fixtures](../reference/fixtures.md#database-and-seeding) for accepted
field shapes and [parse-time resolution](#parse-time-secret-resolution) for Dag-file lookups.

## Captured logs

On Airflow 3, task logs use structlog and do not reach pytest's `caplog`; an empty `caplog` can
therefore make a negative assertion pass vacuously. Use `cap_structlog` for task events. It
survives Airflow reconfiguration while allowing normal output to continue. On Airflow 2, use
`caplog`. See [Fixtures](../reference/fixtures.md#rest-api-and-logging) for the capture views.

## Cluster policies and `airflow_local_settings.py`

The plugin generates `AIRFLOW_HOME/config/airflow_local_settings.py` for its database setup.
Because pytest puts the project root ahead of that directory on `sys.path`, a repository-level
file with the same module name would silently win. The plugin detects that collision and stops
with both paths in the error.

Compose your module into the generated file through a dotted import path:

```ini
[pytest]
airflow_local_settings = myproject.cluster_policies
```

The plugin copies the module's `__all__`, or all non-dunder names when `__all__` is absent, and
keeps its own value on a name collision. Do not edit the generated file; every run recreates
it. For one-test policies, use `airflow_components.policy(...)` instead. See
[Registration and packaging](../guide/custom-components-wiring.md#runtime-component-registration).

## pytest-xdist and environment ownership

Bootstrap installs its `AIRFLOW__*` values before consumer conftests load and refuses to start
if Airflow was already imported. Each xdist worker or isolated child receives the controller's
bootstrap state and verifies every owned variable. Drift usually means a conftest or another
plugin changed Airflow configuration at import time; the default policy raises an error before
the worker continues with a mismatched Dags folder, database, or secret.

If that write cannot be removed, opt into repair:

```ini
[pytest]
airflow_worker_env_drift = repair
```

`repair` restores the controller's values and emits `WorkerEnvironmentDriftWarning`, but cannot
prevent a later mutation. `--airflow-doctor` reports the active policy. At teardown, ini
overrides unwind before the plugin restores the pre-run environment.

## Parse-time secret resolution

On Airflow 3, Variable and Connection lookups normally use a Task SDK supervisor, which does
not exist while a Dag file is being parsed. The plugin supplies environment and metastore
resolution during every parse it owns.

Prefer `AIRFLOW_VAR_*` and `AIRFLOW_CONN_*` for simple parse-time values. Metastore values must
be seeded at session scope before `dag_bag`, collection, or smoke parsing begins. For a lookup
in a test body, seed with `airflow_variables` or `airflow_connections`, then request
`airflow_parse_secrets`.

Pass `--airflow-parse-secrets=off` when unmodified Airflow behavior is the subject. The option
is inert on Airflow 2, where lookups already read the metastore directly.
