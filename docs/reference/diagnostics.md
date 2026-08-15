# Diagnostics

`--airflow-doctor` prints a one-shot, copy-pasteable report and exits without collecting or
running tests -- useful for bug reports and "why is it slow/failing here" triage:

```console
pytest --airflow-doctor
```

The report covers the storage ladder decision and its reason, the resolved `AIRFLOW_HOME`,
database URL scheme, and backend tier, plugin/pytest/Python/Airflow versions plus the resolved
capability table, the resolved `core.executor` (flagging a 2.x SQLite run whose configuration
overrides the plugin's `SequentialExecutor` default with a multi-threaded executor, which
Airflow's `ready_to_reschedule` dependency rejects), whether
[`--airflow-migration-strict`](../guide/migration-strict.md) is enabled (flagging it as a no-op
off the Airflow 2.x family), [Dag coverage](../guide/dag-coverage.md) readiness (the resolved
Dag and collection folders, whether `pytest-cov` is installed and active, and whether the Dag
folder sits inside a configured `--cov` source, with a copy-pasteable fix when it does not),
and API server state. The API server section always reads "not
started": the `api_server_url` fixture is a lazy, per-process, session-scoped subprocess with no
state before a test requests it or an `api_test`-marked test runs, and a standalone
`--airflow-doctor` invocation never does either.

`--airflow-doctor` short-circuits the session, so the `AIRFLOW_HOME` it reports belongs to the
diagnostic run it just bootstrapped, not to a previous test run, and that directory is removed
again on the way out unless you pass `--airflow-home-retention=all`. For the root of a session
that actually ran tests, read the header line every run prints, and see
[the isolated `AIRFLOW_HOME`](../guide/airflow-home.md) for how the storage ladder picks it and
how `--airflow-home-retention` keeps it around afterward.
