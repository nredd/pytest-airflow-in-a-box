# Diagnostics

`--airflow-doctor` prints a one-shot, copy-pasteable report and exits without collecting or
running tests -- useful for bug reports and "why is it slow/failing here" triage:

```console
pytest --airflow-doctor
```

The report covers the storage ladder decision and its reason, the resolved `AIRFLOW_HOME`,
database URL scheme, and backend tier, the resolved `core.executor` (flagging the 2.x SQLite +
non-single-threaded-executor conflict caused by Airflow's `unit_test_mode` overlay),
plugin/pytest/Python/Airflow versions plus the resolved capability table, and API server state.
The API server section always reads "not started": the `api_server_url` fixture is a lazy,
per-process, session-scoped subprocess with no state before a test requests it or an
`api_test`-marked test runs, and a standalone `--airflow-doctor` invocation never does either.
