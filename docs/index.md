# pytest-airflow-in-a-box

`pytest-airflow-in-a-box` is a pytest plugin for testing Apache Airflow DAGs without a live
Airflow deployment. It targets Airflow 3 and provides the package and plugin foundation for a
small, typed testing surface.

The package auto-registers with pytest, creates an isolated metadata database, and provides typed
fixtures for persisted Dags, DagRuns, task instances, sessions, and Dag bags.

## Requirements

- CPython 3.10 through 3.14
- pytest 8 or newer
- Apache Airflow 3.1 or newer, below 4
- Linux or macOS for Airflow-backed tests

Apache Airflow does not support native Windows installations. Windows development should use WSL2
or the included devcontainer; platform-independent package checks alone do not imply full Windows
Airflow support.

The released compatibility matrix is exercised against Airflow 3.1.0, 3.1.1, 3.1.2, 3.1.3,
3.1.5, 3.1.6, 3.1.7, 3.1.8, 3.2.0, 3.2.1, 3.2.2, and 3.3.0 across CPython 3.10 through 3.14
using Airflow's published constraints files.

## Installation

```console
uv add --dev "pytest-airflow-in-a-box[airflow3]"
```

The plugin does not depend on Airflow directly -- the `airflow3` extra pins
`apache-airflow>=3.1,<4`, and projects that pin Airflow themselves (for example through
Airflow's published constraints files) can install the plugin bare.

The `pytest11` entry point loads the plugin automatically. Consumer projects do not need to add a
`pytest_plugins` declaration.

The bundled pytest plugins are intentional runtime dependencies. `pytest-xdist` is part of the
supported execution model: controller bootstrap state and worker-scoped artifacts are coordinated
for parallel runs. `pytest-timeout` backs up Airflow's per-file Dag parse watchdog with a
corpus-scaled deadline on every bundled smoke item, so whichever worker produces the shared corpus
cannot wedge the test session outside the per-file parser boundary.

The plugin is inert on runs without Airflow-facing tests: session startup only prepares a
disposable run directory and `AIRFLOW__*` environment variables. Airflow itself is imported and
the metadata database migrated lazily, on the first test that carries a `db_test`/`api_test`
marker or uses a database-backed plugin fixture. A `pytest -k unrelated` run in a shared venv
never pays the Airflow import or migration cost. Tests that touch the metadata database directly
(their own `create_session` calls, for example) without a plugin fixture must carry `db_test` to
trigger initialization.

To disable the plugin entirely for a run:

```console
pytest -p no:pytest_airflow_in_a_box
```

## Where to go next

- [Task execution](guide/task-execution.md) and the `dag_maker` fixture
- [Database backends](guide/database.md) -- SQLite by default, Postgres on request
- [Markers](reference/markers.md) for a quick index of everything the plugin registers
- [Development](development.md) to build the plugin itself

## License

Apache License 2.0. See
[`LICENSE`](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/LICENSE),
[`NOTICE`](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/NOTICE), and
[`PROVENANCE.md`](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/PROVENANCE.md).
