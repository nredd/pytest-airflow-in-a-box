# Test Environments

Every session runs inside an environment the plugin builds and throws away: an isolated
`AIRFLOW_HOME`, a disposable metadata database, configuration applied as environment
variables, and a generated `airflow_local_settings.py`. This page covers the mechanisms.
The per-fixture and per-option detail lives in [Fixtures](../reference/fixtures.md) and
[INI options](../reference/ini-options.md).

## The isolated `AIRFLOW_HOME`

Your own `~/airflow` is never touched. The plugin points `AIRFLOW_HOME` at a fresh
throwaway directory before any consumer `conftest.py` is imported and before Airflow is
imported at all, so no test run migrates your dev metadata database, seeds your Variables,
or reads a stale `airflow.cfg`. The directory holds `airflow.cfg`, `dags/`, `logs/`,
`plugins/`, `config/airflow_local_settings.py`, the SimpleAuthManager password file, and --
on the default backend -- the SQLite metadata database. The session header names it,
along with the storage rung and backend:

```console
pytest-airflow-in-a-box: AIRFLOW_HOME=/dev/shm/pytest-airflow-in-a-box-8f2a1c (storage: shared-memory, db: sqlite)
```

Under `xdist` only the controller prints; workers inherit the controller's directory.

The base directory comes off a five-rung ladder, and the header's `storage:` field names
the rung that won:

| Rung | Base | When |
| --- | --- | --- |
| `explicit` | `--airflow-home=PATH` or the `airflow_home` ini option | Always, when supplied |
| `caller-temp` | `--basetemp`'s parent, else `$TMPDIR` | The caller already picked a temp base |
| `shared-memory` | `/dev/shm` | Linux, tmpfs, and at least 512 MiB free |
| `system-temp` | `tempfile.gettempdir()` | The ordinary fallback |
| `writable-fallback` | A writable network or unknown-filesystem base | Nothing local was writable; warns loudly |

`shared-memory` is the surprising one: on most Linux hosts it wins, which makes runs fast
and puts your logs in RAM -- gone at reboot. Pass `--airflow-home=PATH` to pin the run on
durable storage. The explicit base is never removed; only the unique
`pytest-airflow-in-a-box-*` child inside it is. A network or otherwise unclassifiable
explicit base is rejected unless you pass `--allow-network-airflow-home`, because SQLite on
a network filesystem corrupts under lock contention rather than failing cleanly.

Retention mirrors pytest's `tmp_path_retention_policy`: `--airflow-home-retention` /
`airflow_home_retention_policy` takes `all`, `failed` (default -- any non-passing exit
counts, crashes included), or `none`, bounded to the `--airflow-home-retention-count` /
`airflow_home_retention_count` most recent roots (default `3`). A kept directory is
announced at the end of the run, never silently. Retention never leaks a database server --
the Postgres container stops on every policy. One known gap: pytest raises the exit status
to `MAX_WARNINGS_ERROR` *after* every `pytest_sessionfinish` hook, so a run failing only on
`--max-warnings` reads as clean and the default `failed` policy removes its directory --
pass `--airflow-home-retention=all` to inspect one. See
[INI options](../reference/ini-options.md) for the full flag/option matrix, and
[Fixtures](../reference/fixtures.md) for the `airflow_home` / `airflow_dags_folder`
fixtures that hand a test these paths as `pathlib.Path`.

[`--airflow-doctor`](../reference/diagnostics.md) prints the resolved root, storage rung,
and backend without collecting anything -- for its own diagnostic run directory, not a
previous session's.

## The disposable metadata database

Nothing a test writes survives it. The metadata database is created inside this run's
`AIRFLOW_HOME`, migrated lazily the first time a test needs it, and thrown away with the
run directory. Two mechanisms keep tests from leaking into each other inside the run: the
`session` fixture rolls back on teardown, and `dag_maker` deletes the rows it created at
teardown, on every test.

The default backend is a per-run SQLite file tuned for disposable test data (WAL journal,
`synchronous = OFF`, memory temp store). SQLite serializes writers, so it cannot reproduce
real deployment concurrency (row-level locking, `SELECT ... FOR UPDATE`, multiple
concurrent writers). When your own code issues SQL against the metadata database, or a
suite deadlocks, opt into the Postgres backend:

```console
pytest --airflow-db-backend=postgres
```

or persistently via the `airflow_db_backend` ini option. It provisions one
[testcontainers](https://testcontainers.com/) container per session; every `xdist` worker
shares that one database, mirroring production topology. It **requires a running Docker
daemon** and the `postgres` extra, and fails loudly with a `pytest.UsageError` when either
is missing -- a misconfigured Postgres run can never pass as a SQLite run. The two backends
are not behaviorally equivalent, and that divergence is the point.

For resetting state beyond what `dag_maker` owns, `clear_db` /
`pytest_airflow_in_a_box.db.TableGroup` truncate registry-defined table groups in a single
foreign-key-safe order -- serial contexts only, since every xdist worker shares the
database. See [Fixtures](../reference/fixtures.md) for `session`, `dag_maker`, and the
`clear_db` contract, including the borrowed-session commit/rollback semantics.

## Overriding configuration

Every override is an environment variable (`AIRFLOW__SECTION__KEY`) and only an
environment variable -- nothing lands in the generated `airflow.cfg`. The environment
outranks every file on each `conf.get()` on both Airflow families, and every configuration
parser in the process reads it, including the second parser Airflow 3.2 added at
`airflow.sdk.configuration.conf`.

Three channels, by scope:

- The `airflow_config` **ini option** is applied from `pytest_load_initial_conftests`,
  before any consumer conftest is imported -- the only channel early enough for options that
  must precede the first Dag parse (`core.dagbag_import_timeout`,
  `core.dag_ignore_file_syntax`). Options the plugin's own bootstrap owns
  (`database.sql_alchemy_conn`, `core.dags_folder`, ...) are rejected with a message
  naming the supported knob rather than silently fought; the denied set is derived from
  bootstrap's own name list so it cannot go stale. One option is rejected only
  *conditionally*: `core.dagbag_import_timeout` is fine on an ordinary run but an error
  with the smoke catalog enabled, because the catalog pins that variable from
  `airflow_dag_parse_timeout`. Grammar and the full denylist:
  [INI options](../reference/ini-options.md)
- `airflow_config()` is also a **context manager and decorator** for one test:

    ```python
    from pytest_airflow_in_a_box.config import airflow_config


    def test_with_overrides():
        with airflow_config({("core", "unit_test_mode"): "False"}, env={"MY_FLAG": "1"}):
            ...


    @airflow_config({("core", "dagbag_import_timeout"): "120"})
    def test_decorated(): ...
    ```

    A thin wrapper over `monkeypatch.setenv` buying exact restore, `(section, key)` pairs,
    and validation of both mappings before anything is assigned. A `None` value makes a
    name absent so Airflow falls back to its default. `refresh_settings=True` recomputes
    the `airflow.settings` globals (`DAGS_FOLDER`, `PLUGINS_FOLDER`, ...) that a plain
    environment assignment cannot reach. Never wrap the first use of
    `api_client`/`api_server_url` -- the server subprocess inherits the environment live at
    startup and the override outlives the context inside it

- The `airflow_configure` **fixture** applies session-scoped batches from your own session
  fixture, for values a config file cannot hold (a `tmp_path_factory` path, a minted
  credential). Batches unwind last-in-first-out at session teardown. It cannot beat a Dag
  parse a test outside its conftest scope already won -- use the ini option for anything
  that must precede the first parse unconditionally

A retained `AIRFLOW_HOME` inspected with `airflow config list` outside the pytest process
will not show these overrides; [`--airflow-doctor`](../reference/diagnostics.md) echoes
them back, redacting credential-shaped values. Full argument reference:
[Fixtures](../reference/fixtures.md); display defaults and warning-filter defaults the
plugin applies with zero ini: [INI options](../reference/ini-options.md).

## Seeding Variables and Connections

For most tests the environment backend wins and is less machinery:

```python
def test_hook(monkeypatch):
    monkeypatch.setenv("AIRFLOW_CONN_DB", '{"conn_type": "postgres", "host": "example.com"}')
```

Airflow's default secrets search path is the environment backend and *then* the metastore,
so `AIRFLOW_VAR_*` / `AIRFLOW_CONN_*` outranks anything the fixtures commit. Reach for
`airflow_variables` / `airflow_connections` when the *metastore* backend is the subject:
they commit encrypted rows exactly as a deployment would read them, delete every inserted
row on teardown, and refuse to seed an identifier whose environment name is already set.

**Seeded names are database-global, not test-local.** Every `xdist` worker shares one
metadata database and a `conn_id` cannot be renamed per worker, so two tests seeding the
same name concurrently collide. Give each test unique identifiers, or group colliding
tests with `@pytest.mark.xdist_group`. Teardown is safe under distinct names: `variable.id` /
`connection.id` are plain `Integer` keys SQLite reuses after deletes, so cleanup matches the
`key`/`conn_id` as well as the id -- a reused primary key misses instead of deleting another
worker's row. Field shapes and error behavior:
[Fixtures](../reference/fixtures.md). Parse-time `Variable.get()` resolution:
[Parse-time secret resolution](parse-time-secrets.md).

## Captured logs

On Airflow 3, `caplog` sees no task logs -- it comes back **empty**, silently, because
Airflow 3 emits through structlog and never hands records to a stdlib logger, so
log-absence assertions pass vacuously. The `cap_structlog` fixture captures the structlog
events instead, surviving Airflow's own mid-test `structlog.configure` calls and passing
events through so normal output still renders. Membership, `.text`, and `.entries` views:
[Fixtures](../reference/fixtures.md).

## Cluster policies and `airflow_local_settings.py`

Airflow supports exactly one `airflow_local_settings` module process-wide, and the plugin
generates one at `AIRFLOW_HOME/config/airflow_local_settings.py` to install the SQLite
engine tuning. A foreign `airflow_local_settings.py` at your repo root would win the import
silently -- pytest inserts your project root at the *front* of `sys.path` while Airflow
appends `AIRFLOW_HOME/config` at the end -- so a collision guard in `pytest_configure`
aborts the session with a `pytest.UsageError` naming both paths instead.

The fix is the `airflow_local_settings` ini option, taking a **dotted module path** (a
file path is rejected on shape alone):

```ini
[pytest]
airflow_local_settings = myproject.cluster_policies
```

Your module's public names (its `__all__`, else every non-dunder attribute) are composed
into the generated file rather than replacing it, and the plugin's own names win a tie --
so the engine tuning survives regardless of what your module exports. You never edit
`config/airflow_local_settings.py`; it is regenerated deterministically every run. For a
policy scoped to one test, `airflow_components.policy(task_policy=...)` registers it
directly without touching the settings module. Failure modes and option grammar:
[INI options](../reference/ini-options.md); per-test registration:
[Custom components](../guide/custom-components-wiring.md#runtime-component-registration).
