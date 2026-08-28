# pytest-airflow-in-a-box

Your Dag files import. Your task callables pass. Production still breaks.

!!! question ""
    Did you __verify__ the `DAG`?

Import and callable tests do not exercise trigger rules, branch skips, rendered templates,
connection resolution, or operator serialization. `pytest-airflow-in-a-box` tests those seams
in `pytest`, before deployment—no scheduler, webserver, or live Airflow environment required.

## Who is this for?

Use this plugin when your team owns Airflow behavior that must work before deployment:

- Run and inspect Dags from your repository.
- Exercise custom operators, hooks, sensors, TaskFlow decorators, or connection types.
- Validate timetables, listeners, executors, policies, providers, and other extensions.
- Test code against a live Airflow REST API.
- Keep one test suite working while migrating from Airflow 2 to 3.

!!! tip ""
    If your repo is 100% stock operators, `dag.test()` plus a `DagBag` import test is
    enough. Use this plugin when the interesting Airflow behavior is yours.

## Start here

- [Install the plugin and run your first Dag](quickstart.md).
- [Decide which failures belong in your suite](guide/testing-scope.md).
- [Choose the least expensive runner that proves your claim](guide/ladder.md).
- [Look up fixtures](reference/fixtures.md) or [diagnose an environment](reference/diagnostics.md).
- [Migrate a suite from Airflow 2 to 3](guide/migration.md).

## Supported versions

The plugin supports all major Apache Airflow versions on CPython 3.10–3.14 with pytest 8 or
newer, on Linux and macOS. See
[Compatibility and certification](internals/compat-layer.md#supported-and-certified) for the
exact combinations exercised in CI, or run
[`pytest --airflow-doctor`](reference/diagnostics.md) to verify your environment.


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
