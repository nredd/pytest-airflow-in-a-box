# pytest-airflow-in-a-box

[![CI](https://github.com/nredd/pytest-airflow-in-a-box/actions/workflows/ci.yml/badge.svg)](https://github.com/nredd/pytest-airflow-in-a-box/actions/workflows/ci.yml)
[![coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/nredd/pytest-airflow-in-a-box/badges/coverage.json)](https://github.com/nredd/pytest-airflow-in-a-box/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pytest-airflow-in-a-box?logo=pypi&logoColor=white&cacheSeconds=300)](https://pypi.org/project/pytest-airflow-in-a-box/)
[![Python versions](https://img.shields.io/pypi/pyversions/pytest-airflow-in-a-box?logo=python&logoColor=white&cacheSeconds=300)](https://pypi.org/project/pytest-airflow-in-a-box/)
[![License](https://img.shields.io/pypi/l/pytest-airflow-in-a-box?cacheSeconds=3600)](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/LICENSE)
[![Docs](https://img.shields.io/badge/docs-mkdocs-blue?logo=materialformkdocs&logoColor=white)](https://nredd.github.io/pytest-airflow-in-a-box/)
[![Airflow](https://img.shields.io/badge/airflow-3.1--3.3%20%7C%202.7--2.11-017CEE?logo=apacheairflow&logoColor=white)](https://nredd.github.io/pytest-airflow-in-a-box/compatibility/)

Your Dag files import cleanly and your callables pass. This proves the seams between them
work -- in your own CI, with no scheduler, no webserver, and no `~/airflow`.

For a team owning a `dags/` repo on Airflow 3 that writes its own operators, hooks, sensors,
and connection types. Not for testing Airflow's own machinery, which Airflow already tests.

Already have a dagbag import test and a pile of `task.function(...)` calls? Here is
[exactly where they stop](https://nredd.github.io/pytest-airflow-in-a-box/why/dagbag-callable-gap/),
and [why not `dag.test()`, `DebugExecutor`, or your own `conftest.py`](https://nredd.github.io/pytest-airflow-in-a-box/why/why-not/).

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

`run_dag` proves your *real file*, under its real `dag_id`, actually settles the way you think.
`result.order` is the executed order, not graph topology.

## Installation

```console
uv add --dev "pytest-airflow-in-a-box[airflow3]"
```

The plugin does not depend on Airflow directly: the Airflow 2.x monolith and the 3.x core both
install the `airflow` package, so a hard plugin pin would corrupt whichever family you did not
choose. The `airflow3` extra pins `apache-airflow>=3.1,<4`. Projects that already pin Airflow
themselves -- for example through Airflow's published constraints files -- install the plugin
bare. Full detail:
[Installing the plugin](https://nredd.github.io/pytest-airflow-in-a-box/install/).

## Requirements

- CPython 3.10 through 3.14, pytest 8 or newer
- Apache Airflow 3.1+ below 4, or 2.7+ below 3 on the certified 2.x tier
- Linux or macOS. Airflow has no native Windows support -- use WSL2 or the devcontainer

Which Airflow and Python combinations are actually exercised in CI, and what the 2.x tier does
and does not cover, are stated once in
[Supported Airflow and Python versions](https://nredd.github.io/pytest-airflow-in-a-box/compatibility/).
After installing, `pytest --airflow-doctor` tells you whether your own pin works.

## What ships

Typed fixtures, grouped by the job:

| Job | Reach for |
| --- | --- |
| Run one operator with no database | `run_task`, `render_task`, `task_context` |
| Run a real DagRun and assert on it | `dag_maker`, `run_dag`, `dag_bag` |
| Give the run its environment | `airflow_home`, `airflow_configure`, `airflow_variables`, `airflow_connections` |
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

- [Why test your Dag code](https://nredd.github.io/pytest-airflow-in-a-box/why/) -- the failures that need a DagRun to exist
- [Deciding which failures are yours](https://nredd.github.io/pytest-airflow-in-a-box/guide/testing-scope/) -- what earns a test
- [The fidelity ladder](https://nredd.github.io/pytest-airflow-in-a-box/guide/ladder/) -- which rung to stand on, and what each one costs
- [Smoke checks over every Dag](https://nredd.github.io/pytest-airflow-in-a-box/guide/smoke-tests/) -- properties of the whole corpus
- [Migrating from Airflow 2 to 3](https://nredd.github.io/pytest-airflow-in-a-box/guide/migration/) -- arrive migrating, leave with a suite
- [Under the hood](https://nredd.github.io/pytest-airflow-in-a-box/internals/compat-layer/) -- what `_compat/` absorbs, and why

Contributing, the local gate, and running CI with `act`:
[`CONTRIBUTING.md`](CONTRIBUTING.md) and
[Working on the plugin itself](https://nredd.github.io/pytest-airflow-in-a-box/development/).

## License

Apache License 2.0. See `LICENSE`, `NOTICE`, and `PROVENANCE.md`.
