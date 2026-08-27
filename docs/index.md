# pytest-airflow-in-a-box

Your Dag files import. Your task callables pass. Production still breaks. Why?

Well, you never really __verified__ the `DAG`. That leaves trigger rules, branch skips,
rendered templates, connection resolution, and operator serialization untested until
deployment.


!!! abstract ""
    `pytest-airflow-in-a-box` runs those seams in `pytest`—no scheduler, webserver, or Airflow
    instance required.

## Who is this for?

Use this plugin when you:

- Own an Airflow Dag repo and need failures to surface before production deployment.
- Write custom operators, hooks, sensors, TaskFlow decorators, or connection types.
- Ship timetables, listeners, executors, policies, providers, or other Airflow extensions.
- Test code that calls Airflow's REST API.
- Are migrating a test suite from Airflow 2 to 3.

!!! tip ""
    If your repo is 100% stock operators, `dag.test()` plus a `DagBag` import test is
    enough. Use this plugin when the interesting Airflow behavior is yours.

Already have a `DagBag` import test and `dag.test()`? See
[what they miss](guide/testing-scope.md#the-failures-worth-catching) and
[why not `dag.test()`, `DebugExecutor`, or your own `conftest.py`](guide/testing-scope.md#why-not-dagtest).

## Start here

- [Install and Quickstart](quickstart.md)
- [The fidelity ladder](guide/ladder.md) -- how much to test
- [Deciding which failures are yours](guide/testing-scope.md) -- where the line falls
- [Fixtures](reference/fixtures.md) and [diagnosing a run](reference/diagnostics.md)
- Still on Airflow 2? [Start here](guide/migration.md)

## Supported versions

Supported: all major Apache Airflow versions, CPython 3.10–3.14, pytest 8 or newer, and Linux
or macOS. See [Compatibility and certification](internals/compat-layer.md#supported-and-certified)
for the exact Airflow/Python combinations exercised in CI, or use
[`pytest --airflow-doctor`](reference/diagnostics.md) to verify the environment you installed.


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

## License

Apache License 2.0. See
[`LICENSE`](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/LICENSE),
[`NOTICE`](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/NOTICE), and
[`PROVENANCE.md`](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/PROVENANCE.md).
