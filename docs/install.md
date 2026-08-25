# Installing the plugin

```console
uv add --dev "pytest-airflow-in-a-box[airflow3]"
```

With pip:

```console
pip install "pytest-airflow-in-a-box[airflow3]"
```

That is the whole install. The `pytest11` entry point registers the plugin automatically --
consumer projects never add a `pytest_plugins` declaration.

Then confirm your environment actually resolves:

```console
pytest --airflow-doctor
```

It prints one report -- Airflow release, capability probes, storage tier, database URL scheme,
Dag folder, coverage containment -- and exits without running tests. If your Airflow pin is
outside the supported range it says `INCOMPATIBLE` there rather than failing somewhere deeper.
See [Diagnosing a run](reference/diagnostics.md) for the full report, and
[Supported Airflow and Python versions](compatibility.md) for what the pin needs to be.

## Extras

The plugin does *not* depend on Airflow itself. The 2.x monolith and the 3.x core both install
under the name `apache-airflow`, so a hard pin in the base dependencies would corrupt a 2.x
environment.

| Extra | Pulls in | When |
| --- | --- | --- |
| `airflow3` | `apache-airflow>=3.1,<4`, `apache-airflow-providers-sqlite>=4.1,<5` | Default. You want Airflow 3 |
| `airflow2` | `apache-airflow>=2.7,<3`, CPython < 3.13 only | Only while migrating -- see [compatibility](compatibility.md) |
| `postgres` | `asyncpg`, `psycopg2-binary`, `testcontainers` | You want `--airflow-db-backend=postgres`. See [the disposable metadata database](guide/database.md) |
| `xdist` | `pytest-xdist>=3.8` | You want `-n auto` |

Install it bare -- no extra -- if your project already pins Airflow itself, for example
through Airflow's published constraints files. That is the normal case for a repo that
deploys to MWAA, Composer, or Astro.

## What comes with it regardless

`pytest-timeout` is a runtime dependency, not an optional one. It backs Airflow's per-file Dag
parse watchdog with a corpus-scaled deadline on every bundled smoke item, so whichever worker
builds the shared corpus cannot wedge the session outside the per-file parser boundary.

`pytest-xdist` is *not* required. Nothing in the plugin imports it; controller bootstrap state
and worker-scoped artifacts are coordinated only when xdist happens to be running. The `xdist`
extra exists so you can opt in without hunting the version floor.

## The plugin is inert until a test needs Airflow

Session startup only prepares a disposable run directory and `AIRFLOW__*` environment
variables. Airflow is imported and the metadata database migrated *lazily*, on the first test
carrying a `db_test` or `api_test` marker or using a database-backed fixture. A
`pytest -k unrelated` run in a shared venv never pays the import or migration cost.

The consequence you have to know: a test that touches the metadata database on its own -- its
own `create_session()` call, say -- with no plugin fixture must carry `db_test`, or
initialization never fires. See [Markers](reference/markers.md).

To turn the plugin off entirely for one run:

```console
pytest -p no:pytest_airflow_in_a_box
```

## Not on Windows

Apache Airflow has no native Windows support, so neither does this. Use WSL2 or the repo's
devcontainer. Platform-independent package checks passing on Windows does not imply Airflow
works there.
