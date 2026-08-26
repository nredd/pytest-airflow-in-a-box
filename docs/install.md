# Install

With `uv`:
```console
uv add --dev "pytest-airflow-in-a-box[airflow3]"
```

With `pip`:

```console
pip install "pytest-airflow-in-a-box[airflow3]"
```

The `pytest11` entry point registers the plugin automatically --
consumer projects never add a `pytest_plugins` declaration.

Verify the install:

```console
pytest --airflow-doctor
```

See [Airflow Doctor](reference/diagnostics.md).

## Extras

The plugin does *not* depend on Airflow itself. The 2.x monolith and the 3.x core both install
under the name `apache-airflow`, so a hard pin in the base dependencies would corrupt a 2.x
environment.

| Extra | Pulls in | When |
| --- | --- | --- |
| `airflow3` | `apache-airflow>=3.1,<4`, `apache-airflow-providers-sqlite>=4.1,<5` | Default. You want Airflow 3 |
| `airflow2` | `apache-airflow>=2.7,<3`, CPython < 3.13 only | Only while migrating -- see [compatibility](internals/certification.md#airflow-2x-is-a-migration-bridge-not-a-second-home) |
| `postgres` | `asyncpg`, `psycopg2-binary`, `testcontainers` | You want `--airflow-db-backend=postgres`. See [the disposable metadata database](internals/test-environments.md#the-disposable-metadata-database) |
| `xdist` | `pytest-xdist>=3.8` | You want `-n auto` |

Install it bare -- no extra -- if your project already pins Airflow itself, for example
through Airflow's published constraints files. That is the normal case for a repo that
deploys to MWAA, Composer, or Astro.

## Plugin dependencies

`pytest-timeout` is a runtime dependency. It backs Airflow's per-file Dag
parse watchdog with a corpus-scaled deadline on every bundled smoke item, so whichever worker
builds the shared corpus cannot wedge the session outside the per-file parser boundary.

`pytest-xdist` is opt-in only but **STRONGLY** recommended. Nothing in the plugin imports it; controller bootstrap state and worker-scoped artifacts are coordinated only when xdist happens to be running. Add it with the `xdist` extra.

## Disabling the plugin

To turn the plugin off entirely for one run:

```console
pytest -p no:pytest_airflow_in_a_box
```


Session startup only prepares a disposable run directory and `AIRFLOW__*` environment
variables. Airflow is imported and the metadata database migrated *lazily*, on the first test
that needs it. A `pytest -k unrelated` run in a shared venv never pays the import or
migration cost. What triggers that lazy init, and the `db_test` gotcha, is on
[Markers](reference/markers.md).

## Add it to your `.github/workflows`

`nredd/pytest-airflow-in-a-box/action@v0` is the CI-native install path: a composite action
that provisions a constraints-pinned `uv` environment for the Airflow/Python pair you name,
then stops -- it never runs `pytest`, you always write the invocation. Inputs, outputs, and
matrix usage are on [The GitHub Action](guide/ci/github-action.md).


## Platform

Linux or macOS only. See [Supported versions](internals/certification.md#what-the-pin-needs-to-be).
