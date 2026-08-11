# pytest-airflow-in-a-box

[![CI](https://github.com/nredd/pytest-airflow-in-a-box/actions/workflows/ci.yml/badge.svg)](https://github.com/nredd/pytest-airflow-in-a-box/actions/workflows/ci.yml)
[![coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/nredd/pytest-airflow-in-a-box/badges/coverage.json)](https://github.com/nredd/pytest-airflow-in-a-box/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pytest-airflow-in-a-box?logo=pypi&logoColor=white)](https://pypi.org/project/pytest-airflow-in-a-box/)
[![Python versions](https://img.shields.io/pypi/pyversions/pytest-airflow-in-a-box?logo=python&logoColor=white)](https://pypi.org/project/pytest-airflow-in-a-box/)
[![License](https://img.shields.io/pypi/l/pytest-airflow-in-a-box?cacheSeconds=3600)](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/LICENSE)

`pytest-airflow-in-a-box` is a pytest plugin for testing Apache Airflow DAGs without a live
Airflow deployment. It targets Airflow 3 and provides the package and plugin foundation for a
small, typed testing surface.

The package auto-registers with pytest, creates an isolated metadata database, and provides typed
fixtures for persisted Dags, DagRuns, task instances, sessions, and Dag bags.

## Contents

- [Quickstart](#quickstart)
- [Why not...](#why-not)
- [Requirements](#requirements)
- [Installation](#installation)
- [Documentation](#documentation)
- [Development](#development)
- [License](#license)

## Quickstart

```console
uv add --dev "pytest-airflow-in-a-box[airflow3]"
pip install "pytest-airflow-in-a-box[airflow3]"
```

```python
from airflow.sdk import task
from airflow.utils.state import TaskInstanceState


def test_answer(dag_maker):
    with dag_maker():

        @task
        def answer():
            return 42

        answer()

    assert dag_maker.run_ti("answer").state == TaskInstanceState.SUCCESS
```

```console
pytest
```

The `pytest11` entry point registers the plugin automatically -- no `pytest_plugins`
declaration needed. See the [documentation site](https://nredd.github.io/pytest-airflow-in-a-box/)
for the full `dag_maker`/`run_ti` surface, sessions, DB-free task execution, deferrable operators,
the REST API fixture, and bundled smoke checks.

## Why not...

- **`dag.test()`** -- Airflow's own built-in helper runs one Dag end to end, but it is not a
  pytest plugin: no fixtures, no isolated metadata database, no `xdist` parallelism, no REST
  API testing
- **upstream `tests_common`** -- the harness Airflow's own core test suite runs on; it targets
  testing Airflow itself, not published as a package for testing DAG-author code
- **Flowminder `pytest-airflow`** -- an inverse concept (runs pytest suites under Airflow,
  rather than testing DAGs under pytest) and unmaintained
- **`airflow-pytest-plugin`** -- generates JUnit-XML dashboards from DAG runs; not aimed at
  isolated, fixture-driven unit testing

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
pip install "pytest-airflow-in-a-box[airflow3]"
```

The plugin does not depend on Airflow directly: the Airflow 2.x monolith and the 3.x core both
install the `airflow` package, so a hard plugin pin would corrupt whichever family you did not
choose. The `airflow3` extra pins `apache-airflow>=3.1,<4` (the meta-package resolves a coherent
core + task-sdk pair). Projects that already pin Airflow themselves -- for example through
Airflow's published constraints files -- can install the plugin bare:

```console
pip install pytest-airflow-in-a-box
```

An `airflow2` extra (`apache-airflow>=2.9,<3`) exists ahead of the planned Airflow 2.x
compatibility tier ([#25](https://github.com/nredd/pytest-airflow-in-a-box/issues/25)); on this
release an Airflow 2.x environment fails session startup with an actionable error.

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

## Documentation

Task execution, deferrable operators, DB-free execution, Variable/Connection seeding, structlog
capture, Dag collection, configuration overrides, smoke tests, database backends and cleanup, the
live REST API, markers, and diagnostics are all covered on the
[documentation site](https://nredd.github.io/pytest-airflow-in-a-box/).

## Development

```console
uv sync
uv run prek install
make all
```

Run the GitHub Actions workflow locally on Linux with [act](https://nektosact.com/):

```console
act pull_request
```

`act` cannot reproduce native macOS or Windows behavior. See
[`CONTRIBUTING.md`](CONTRIBUTING.md) for the full contribution workflow and the
[issue tracker](https://github.com/nredd/pytest-airflow-in-a-box/issues) for open work.

## License

Apache License 2.0. See `LICENSE`, `NOTICE`, and `PROVENANCE.md`.
