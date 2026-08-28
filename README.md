# pytest-airflow-in-a-box

[![CI](https://github.com/nredd/pytest-airflow-in-a-box/actions/workflows/ci.yml/badge.svg)](https://github.com/nredd/pytest-airflow-in-a-box/actions/workflows/ci.yml)
[![coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/nredd/pytest-airflow-in-a-box/badges/coverage.json)](https://github.com/nredd/pytest-airflow-in-a-box/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pytest-airflow-in-a-box?logo=pypi&logoColor=white&cacheSeconds=300)](https://pypi.org/project/pytest-airflow-in-a-box/)
[![Python versions](https://img.shields.io/pypi/pyversions/pytest-airflow-in-a-box?logo=python&logoColor=white&cacheSeconds=300)](https://pypi.org/project/pytest-airflow-in-a-box/)
[![License](https://img.shields.io/pypi/l/pytest-airflow-in-a-box?cacheSeconds=3600)](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/LICENSE)
[![Docs](https://img.shields.io/badge/docs-mkdocs-blue?logo=materialformkdocs&logoColor=white)](https://nredd.github.io/pytest-airflow-in-a-box/)
[![Airflow](https://img.shields.io/badge/airflow-3.1--3.3%20%7C%202.7--2.11-017CEE?logo=apacheairflow&logoColor=white)](https://nredd.github.io/pytest-airflow-in-a-box/#supported-versions)

Your Dag files import. Your task callables pass. Production still breaks.

> Well, you never really __verified__ the `DAG`

Trigger rules, branch skips, rendered templates, connection resolution, and operator
serialization remain untested until deployment. This plugin runs those seams in `pytest`, in
your repo's own CI, with no scheduler, webserver, or `~/airflow`.

For a team owning a `dags/` repo on Airflow 3 that writes its own operators, hooks,
sensors, and connection types -- deployed by someone else (MWAA, Composer, Astro,
self-hosted). If your repo is 100% stock operators, `dag.test()` plus a `DagBag` import
test is enough; the full list is on the
[documentation site](https://nredd.github.io/pytest-airflow-in-a-box/).

Already have a `DagBag` import test and a pile of `task.function(...)` calls? Here is
[exactly where they stop](https://nredd.github.io/pytest-airflow-in-a-box/guide/testing-scope/#the-failures-worth-catching),
and [why not `dag.test()`, `DebugExecutor`, or your own `conftest.py`](https://nredd.github.io/pytest-airflow-in-a-box/guide/testing-scope/#why-not-dagtest).

## Quickstart

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

`run_dag` proves your *real file*, under its real `dag_id`, actually finishes in the states
you expect. `dag_bag` parses the folder once per worker process, and `result.order` records
execution order rather than graph topology. In-test Dags (`dag_maker`), single operators
without a database (`run_task`), and matchers are in the
[Quickstart](https://nredd.github.io/pytest-airflow-in-a-box/quickstart/).

## Installation

```console
uv add --dev "pytest-airflow-in-a-box[airflow3]"
```

The plugin does not depend on Airflow directly: the Airflow 2.x monolith and the 3.x core both
install under the name `apache-airflow`, so a hard plugin pin would corrupt whichever family you did not
choose. The `airflow3` extra pins `apache-airflow>=3.1,<4`, matching the dependency restriction
in `pyproject.toml`. Projects that already pin Airflow themselves -- for example through
Airflow's published constraints files -- install the plugin bare. Every extra and common
combination is listed under
[Dependencies and extras](https://nredd.github.io/pytest-airflow-in-a-box/reference/dependencies/).

In CI, `nredd/pytest-airflow-in-a-box/action@v0` provisions a constraints-pinned environment --
see [The GitHub Action](https://nredd.github.io/pytest-airflow-in-a-box/guide/ci/github-action/).

## Requirements

- CPython 3.10 through 3.14, pytest 8 or newer
- All major Apache Airflow versions
- Linux or macOS. Airflow has no native Windows support -- use WSL2 or the devcontainer

Which Airflow and Python combinations are actually exercised in CI, and what the 2.x tier does
and does not cover, are stated once in
[Compatibility and certification](https://nredd.github.io/pytest-airflow-in-a-box/internals/compat-layer/#supported-and-certified).
After installing, `pytest --airflow-doctor` tells you whether your own pin works.

## What ships

Typed fixtures, grouped by the job:

| Job | Reach for |
| --- | --- |
| Run one operator with no database | `run_task`, `render_task`, `task_context` |
| Run a real `DagRun` and assert on it | `dag_maker`, `run_dag`, `dag_bag` |
| Give the run its environment | `airflow_home`, `airflow_configure`, `airflow_variables`, `airflow_connections` |
| Register a custom component for one test | `airflow_components` |
| Assert on what a task logged | `cap_structlog` |
| Check every Dag at once | `dag_corpus` |
| Talk to a live Airflow API | `api_client`, `api_base_url` |

Every fixture, its return type, and its scope:
[Fixtures](https://nredd.github.io/pytest-airflow-in-a-box/reference/fixtures/). Markers are
listed in [Markers](https://nredd.github.io/pytest-airflow-in-a-box/reference/markers/).

Also in the box: corpus smoke checks (`--airflow-smoke`), a disposable metadata database, an
isolated `AIRFLOW_HOME`, report artifacts that survive `-n auto`, a
[GitHub Action](https://nredd.github.io/pytest-airflow-in-a-box/guide/ci/github-action/), and an
Airflow 2-to-3 migration toolkit fronted by the `airflow-migration-diff` console script.

## Documentation

The [documentation site](https://nredd.github.io/pytest-airflow-in-a-box/) follows the reader,
one deep link per stage:

- [Whose fail is it anyway?](https://nredd.github.io/pytest-airflow-in-a-box/guide/testing-scope/) -- what earns a test
- [The fidelity ladder](https://nredd.github.io/pytest-airflow-in-a-box/guide/ladder/) -- which rung to stand on, and what each one costs
- [Smoke Tests](https://nredd.github.io/pytest-airflow-in-a-box/guide/smoke-tests/) -- properties of the whole corpus
- [Checking components](https://nredd.github.io/pytest-airflow-in-a-box/guide/custom-components/) -- validate Airflow extension points before registration
- [Airflow 2->3 Migration](https://nredd.github.io/pytest-airflow-in-a-box/guide/migration/) -- arrive migrating, leave with a suite
- [Under the hood](https://nredd.github.io/pytest-airflow-in-a-box/internals/compat-layer/) -- what `_compat/` absorbs, and why

Contributing, the local gate, and running CI with `act`:
[`CONTRIBUTING.md`](CONTRIBUTING.md) and
[Developing this plugin](https://nredd.github.io/pytest-airflow-in-a-box/development/).

## License

Apache License 2.0. See `LICENSE`, `NOTICE`, and `PROVENANCE.md`.
