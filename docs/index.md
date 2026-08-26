# pytest-airflow-in-a-box

Your `DagBag` has no import errors and your `DagRun`s are green. Why are things still breaking???

Trigger rules, branch skips, rendered templates, `conn_id` resolution, operator serialization -- these failures are `DagRun`-shaped, and production is the
first place a `DagRun` exists. This plugin moves them (and so much more!) into `pytest`. Run it wherever you please--no instance required!

Who is this for:
- You want to write `DAG`s that don't break
- You write your own `BaseOperator` subclasses, hooks, sensors, `@task` decorators, and
  connection types
- You ship custom components -- timetables, listeners, executors -- or code that talks to the REST API
- You need help migrating from Airflow 2 to 3

!!! tip ""
    If your repo is 100% stock operators, `dag.test()` plus a `DagBag` import test is
    enough. This is for the repos where the interesting code is yours.

Already have a `DagBag` import test and `dag.test()`? See
[what they miss](why/index.md) and
[why not `dag.test()`, `DebugExecutor`, or your own `conftest.py`](why/index.md#why-not).

## Start here

- [Quickstart](quickstart.md)
- [Installing the plugin](install.md)
- [The fidelity ladder](guide/ladder.md) -- how much to test
- [Deciding which failures are yours](guide/testing-scope.md) -- where the line falls
- [Fixtures](reference/fixtures.md) and [diagnosing a run](reference/diagnostics.md)
- Still on Airflow 2? [Start here](guide/migration/index.md)

## Supported versions

Run `pytest --airflow-doctor` ([Airflow Doctor](reference/diagnostics.md)) and read the
answer for the environment you actually have. The map behind that answer -- the pin,
certified releases, CI legs, and the 2.x tier -- is [Certification](internals/certification.md).


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
