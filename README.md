# pytest-airflow-in-a-box

[![CI](https://github.com/nredd/pytest-airflow-in-a-box/actions/workflows/ci.yml/badge.svg)](https://github.com/nredd/pytest-airflow-in-a-box/actions/workflows/ci.yml)
[![coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/nredd/pytest-airflow-in-a-box/badges/coverage.json)](https://github.com/nredd/pytest-airflow-in-a-box/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pytest-airflow-in-a-box?logo=pypi&logoColor=white&cacheSeconds=300)](https://pypi.org/project/pytest-airflow-in-a-box/)
[![Python versions](https://img.shields.io/pypi/pyversions/pytest-airflow-in-a-box?logo=python&logoColor=white&cacheSeconds=300)](https://pypi.org/project/pytest-airflow-in-a-box/)
[![License](https://img.shields.io/pypi/l/pytest-airflow-in-a-box?cacheSeconds=3600)](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/LICENSE)
[![Docs](https://img.shields.io/badge/docs-mkdocs-blue?logo=materialformkdocs&logoColor=white)](https://nredd.github.io/pytest-airflow-in-a-box/)
[![Airflow](https://img.shields.io/badge/airflow-3.1--3.3%20%7C%202.7--2.11-017CEE?logo=apacheairflow&logoColor=white)](#requirements)

`pytest-airflow-in-a-box` is a pytest plugin for testing Apache Airflow DAGs without a live
Airflow deployment. It provides the package + plugin foundation for a small, typed testing
surface. It primarily targets Airflow 3 but maintains a certified Airflow 2.7-2.11
compatibility tier.

It is for testing the Airflow code you wrote -- your Dags, plus the custom operators, hooks,
sensors, decorators, and connection types they lean on -- rather than Airflow's own machinery.
See [What to test](https://nredd.github.io/pytest-airflow-in-a-box/guide/testing-scope/) for
where that line falls.

The package auto-registers with pytest, creates an isolated metadata database, and provides typed
fixtures for persisted Dags, DagRuns, task instances, sessions, and Dag bags.

## Contents

- [Quickstart](#quickstart)
- [Fixtures](#fixtures)
- [Markers](#markers)
- [Why not...](#why-not)
- [Requirements](#requirements)
- [Installation](#installation)
- [GitHub Action](#github-action)
- [Migration diff orchestrator](#migration-diff-orchestrator)
- [Documentation](#documentation)
- [Development](#development)
- [License](#license)
- [Manifesto](#manifesto)

## Quickstart

```console
uv add --dev "pytest-airflow-in-a-box[airflow3]"
pip install "pytest-airflow-in-a-box[airflow3]"
```

Point the plugin at your repo's `dags/` folder and run a real Dag end to end:

```python
def test_my_dag(dag_bag, run_dag):
    dag = dag_bag.dags["my_dag_id"]

    result = run_dag(dag)

    assert result.success
    assert result.order == ["extract", "load"]
```

```console
pytest --dag-folder=dags
```

### Testing an adhoc Dag

For a Dag authored directly in the test, rather than loaded from your `dags/` folder,
`dag_maker` builds and persists one for you:

```python
from airflow.sdk import task


def test_dag(dag_maker):
    with dag_maker():

        @task
        def produce():
            return 21

        @task
        def consume(value):
            return value * 2

        consume(produce())

    result = dag_maker.run()

    assert result.success
    assert result.xcoms == {"produce": 21, "consume": 42}
    assert result.order == ["produce", "consume"]
```

```console
pytest
```

`run_dag()` and `dag_maker.run()` both return the same inert `DagRunResult` snapshot:
`states`, `xcoms`, `errors`, `order`, and per-task access via `result["task_id"]`. Single
tasks run with `dag_maker.run_ti("produce")`, and `pytest_airflow_in_a_box.matchers`
supports one-expression bulk assertions like
`assert result == {"produce": succeeded(21), "consume": succeeded(42)}`.

### Testing a standalone task

A `@task` needs no Dag at all -- and no database. `run_task` executes the decorated
function's operator in process via a synthetic per-test Dag (Airflow 3.x only):

```python
from airflow.sdk import task


@task
def add(x: int, y: int) -> int:
    return x + y


def test_add(run_task):
    result = run_task(add(1, 2).operator)

    assert result.xcoms["return_value"] == 3
```

The same pattern renders templates without executing (`render_task`) and hand-drives a bare
`execute()` (`task_context`) -- see
[DB-free task execution](https://nredd.github.io/pytest-airflow-in-a-box/guide/db-free-execution/).

The `pytest11` entry point registers the plugin automatically -- no `pytest_plugins`
declaration needed. See the [documentation site](https://nredd.github.io/pytest-airflow-in-a-box/)
for the full `dag_maker`/`run_dag`/`run`/`run_ti` surface, sessions, DB-free task execution,
deferrable operators, the REST API fixture, and bundled smoke checks.

## Fixtures

The full reference, with scopes, database behavior, and Airflow-2.x availability, lives at
[Fixtures](https://nredd.github.io/pytest-airflow-in-a-box/reference/fixtures/):

| Fixture | One-liner |
| ------- | --------- |
| `dag_bag` | All Dags parsed from the configured Dag directory, once per worker process |
| `dag_maker` | Build and persist a Dag authored in the test; `run()` / `run_ti()` execute it; upstream `tests_common` harness kwargs plus scheduler-side `serialized_dag`/`dag_model`/`timetable`/`sync_dagbag_to_db()` handles |
| `create_task_instance` | One call: a `TaskInstance` with its Dag and DagRun rows, upstream-parity |
| `create_dummy_dag` | One call: a single-`EmptyOperator` Dag plus a scheduled DagRun, upstream-parity |
| `run_dag` | Run an externally-authored Dag, e.g. one pulled from `dag_bag` |
| `run_task` | DB-free in-process runner for a single operator or standalone `@task` |
| `render_task` | DB-free rendering of an operator's `template_fields`, no `execute()` |
| `task_context` | DB-free Task SDK template context for hand-driven `execute()` calls |
| `session` | Airflow metadata DB session, rolled back on teardown |
| `airflow_variables` | Seed Airflow Variables for one test, deleted on teardown |
| `airflow_connections` | Seed Airflow Connections for one test, deleted on teardown |
| `airflow_parse_secrets` | Resolve top-level Variable/Connection lookups in Dag files |
| `airflow_configure` | Apply `airflow_config` overrides until session teardown |
| `airflow_components` | Register custom plugins, listeners, policies, executors, timetables |
| `airflow_home` | This run's isolated `AIRFLOW_HOME` as a `Path` |
| `airflow_dags_folder` | The Dag directory `dag_bag` parses, as a `Path` |
| `api_server_url` | Base URL of the isolated live Airflow API server |
| `api_client` | Authenticated client bound to the isolated API server |
| `api_base_url` | The live server URL, published to Airflow config for `api_test` tests |
| `cap_structlog` | Capture structlog events emitted during the test |
| `testing_dag_bundle` | Register the shared `testing` Dag bundle row upstream tests write against |

## Markers

The full reference, with argument grammar and gating behavior, lives at
[Markers](https://nredd.github.io/pytest-airflow-in-a-box/reference/markers/):

| Marker | One-liner |
| ------ | --------- |
| `db_test` | Requires the isolated Airflow metadata database |
| `api_test` | Starts the isolated REST API server, publishes its base URL |
| `postgres` | Requires a provisioned Postgres metadata database |
| `compat` | End-user tests exercised across the version matrix |
| `need_serialized_dag([enabled])` | Accepted for upstream compatibility; every Dag serializes at persistence |
| `environment(name)` | Run only when the named environment's sentinel path exists |
| `requires_airflow2` / `requires_airflow3` | Run only on the named Airflow family, auto-skipped elsewhere |
| `smoke` | Bundled zero-boilerplate check, opt in with `airflow_smoke` |
| `airflow_isolated(...)` | Run in a one-shot child pytest process with entry-point/env overrides |

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
- **a dagbag import test plus calling `task.function` directly** -- covers "the Dag parses"
  and "the callable works" and nothing in between: task relations, cross-Dag asset triggering,
  DagRun-to-DagRun relations, and retry behavior. See the
  [cookbook](https://nredd.github.io/pytest-airflow-in-a-box/guide/cookbook/#what-a-dagbag-callable-test-misses)
  for what it misses

## Requirements

- pytest 8 or newer
- Linux or macOS for Airflow-backed tests

Apache Airflow does not support native Windows installations. Windows development should use WSL2
or the included devcontainer; platform-independent package checks alone do not imply full Windows
Airflow support.

The released compatibility matrix is exercised in CI against every combination below, using
Airflow's published constraints files:

| Tier                   | Airflow versions                                                                | Python           | OS                                       | Metadata DB                    |
| ----------------------- | -------------------------------------------------------------------------------- | ----------------- | ------------------------------------------ | -------------------------------- |
| 3.x (primary)           | 3.1.0, 3.1.1, 3.1.2, 3.1.3, 3.1.5, 3.1.6, 3.1.7, 3.1.8, 3.2.0, 3.2.1, 3.2.2, 3.3.0, 3.3.1 | 3.10 - 3.14        | Linux (glibc, musl, arm64), macOS          | SQLite (WAL), Postgres (testcontainers) |
| 2.x (certified, [#25](https://github.com/nredd/pytest-airflow-in-a-box/issues/25)) | 2.7.3, 2.8.4, 2.9.3, 2.10.5, 2.11.2                                               | 3.10 - 3.12 on 2.9+, 3.10 - 3.11 on 2.7/2.8 (Airflow 2.x never supported 3.13+) | Linux                                     | SQLite (WAL)                     |

On the 2.x family, `run_task`, `render_task`, `task_context`, `cap_structlog`, the REST
API fixtures, and `run_dag(executor=...)` fail with actionable errors naming the 2.x
alternative; the `requires_airflow2`/`requires_airflow3`
markers auto-skip
on the other family so one suite runs green on both sides of a migration. The 2.x tier is
exercised through the end-user consumer contract (`tests/enduser`, marked `compat`) rather than
the full internal suite.

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

The `airflow2` extra (`apache-airflow>=2.7,<3`, carrying an explicit `python_version < '3.13'`
marker because Airflow 2.x never supported 3.13 -- on newer interpreters the extra resolves to
nothing and the plugin's runtime check names the fix) installs the certified Airflow 2.x
compatibility tier ([#25](https://github.com/nredd/pytest-airflow-in-a-box/issues/25)):
`dag_maker` (including whole-DagRun execution through `dag_maker.run()`), `run_ti`,
`dag_bag`, `run_dag`, `clear_db`, seeding, the configuration surface
(`airflow_config`, `airflow_configure`, `airflow_home`, `airflow_dags_folder`),
and the bundled smoke checks run against 2.7.3, 2.8.4,
2.9.3, 2.10.5, and 2.11.2. The marker is the family-wide cap; 2.7.3 and 2.8.4 cap lower still,
at 3.11, and the plugin's runtime check names the offending release. Requesting both Airflow
extras together fails at resolution for pip and uv alike, since the `apache-airflow` version
ranges are disjoint.

The `pytest11` entry point loads the plugin automatically. Consumer projects do not need to add a
`pytest_plugins` declaration.

`pytest-timeout` is an intentional runtime dependency: it backs up Airflow's per-file Dag parse
watchdog with a corpus-scaled deadline on every bundled smoke item, so whichever worker produces
the shared corpus cannot wedge the test session outside the per-file parser boundary.

`pytest-xdist` is not required to use the plugin -- controller bootstrap state and worker-scoped
artifacts are coordinated whenever xdist happens to be running, but nothing in the plugin imports
it directly. Install the `xdist` extra to opt into parallel runs:

```console
pip install "pytest-airflow-in-a-box[xdist]"
```

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

## GitHub Action

A composite GitHub Action wraps the constraints-pinned `uv` + Airflow setup this repo's own
compat matrix uses, for a Dag repo's own CI. It provisions the environment and stops -- you
always write the invocation, so the example below shows several distinct features of the
plugin rather than baking one blessed command into the action.

```yaml
- uses: actions/checkout@v5
- uses: nredd/pytest-airflow-in-a-box/action@v0
  id: airflow-env
  with:
    airflow-version: "3.3.0"
    python-version: "3.12"

# Plain unit tests
- run: ${{ steps.airflow-env.outputs.python-path }} -m pytest

# Bundled smoke checks (Dag import + parse-time diagnostics)
- run: ${{ steps.airflow-env.outputs.python-path }} -m pytest --airflow-smoke

# Migration outcome diff: record a baseline, then compare on a later run
- run: ${{ steps.airflow-env.outputs.python-path }} -m pytest --airflow-record=baseline.json
- run: ${{ steps.airflow-env.outputs.python-path }} -m pytest --airflow-baseline=baseline.json

# The `airflow-migration-diff` console script, via the venv-path output
- run: ${{ steps.airflow-env.outputs.venv-path }}/bin/airflow-migration-diff --project-dir .
```

Drop it straight into a `strategy.matrix` loop -- it's a single step with scalar inputs:

```yaml
jobs:
  test:
    strategy:
      matrix:
        airflow-version: ["3.2.2", "3.3.0", "3.3.1"]
        python-version: ["3.11", "3.12", "3.13"]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: nredd/pytest-airflow-in-a-box/action@v0
        id: airflow-env
        with:
          airflow-version: ${{ matrix.airflow-version }}
          python-version: ${{ matrix.python-version }}
      - run: ${{ steps.airflow-env.outputs.python-path }} -m pytest
```

| Input               | Required | Default    | Description                                                              |
| -------------------- | -------- | ---------- | ---------------------------------------------------------------------------- |
| `airflow-version`    | yes      | --          | Exact Apache Airflow version to install.                                     |
| `python-version`     | yes      | --          | Python version to provision.                                                 |
| `extra`              | no       | `airflow3` | Plugin extra to install: `airflow3` or `airflow2`.                          |
| `plugin-version`     | no       | latest     | Exact `pytest-airflow-in-a-box` version to install.                          |
| `uv-version`         | no       | `0.12.2`   | `uv` version to install.                                                      |
| `working-directory`  | no       | `.`        | Directory to run in.                                                         |
| `requirements-file`  | no       | (none)     | Extra requirements file to install into the same environment.                |
| `report-dir`         | no       | (none)     | Directory for `pytest.log`/`pytest.xml`, appended to `PYTEST_ADDOPTS`.        |

Outputs: `python-path` (the provisioned venv's `python`), `venv-path` (the venv directory,
for console scripts), and `report-dir` (the absolute report directory, for an upload step).
The examples above pin `@v0`: `release.yml` moves a `v<major>` tag (`v0` while pre-1.0, `v1`
once `1.0.0` ships) to the latest published release on that major line, so `@v0` tracks the
newest 0.x. Pin a full release tag (e.g. `@v0.7.0`) for an exact, non-moving reference.

## Migration diff orchestrator

`airflow-migration-diff` is a console script that `uv`-provisions a disposable Airflow 2.x
environment and a disposable Airflow 3.x environment, records outcomes on each, and prints the
categorized migration diff -- one command that tells a migrating team exactly what breaks:

```console
airflow-migration-diff --project-dir . -- -k "not slow"
```

Exit code `0` means no regressions, `1` means at least one was found, and `2` means the
orchestrator itself failed (missing `uv`, a provisioning failure, and the like). See the
[documentation site](https://nredd.github.io/pytest-airflow-in-a-box/guide/migration-orchestrator/)
for the full option reference.

## Documentation

What to test (and what not to), task execution, deferrable operators, DB-free execution,
Variable/Connection seeding, structlog capture, Dag collection, configuration overrides, smoke
tests, report artifacts, database backends and cleanup, the live REST API, the migration outcome
diff, markers, and diagnostics are all covered on the
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

## Manifesto

In 2024, I learned that my team was abandoning Jenkins for our nightly regressions. A
righteous tear rolled down my cheek when I heard the replacement was Airflow: a
Python-native workflow platform. As a lover of all things slick and hyper-engineered, I was
overjoyed to rewrite all those DISGUSTING unversioned shell scripts into a beautiful
library of documented, statically-analyzed, and unit-tested code. Fast forward a few
months--I have some crazy 500+ task DAG templates underway (for convoluted semiconductor
design methodologies) that were IMPOSSIBLE to fully verify outside of a live Airflow
instance. I yearned for a far-off land where I could develop alone in my teched-out Python
cave, talk to absolutely no one, and ship complete Methodologies without a whisper in the
night. This plugin is the closest thing we have 🫡
