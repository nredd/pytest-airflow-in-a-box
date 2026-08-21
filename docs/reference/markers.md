# Markers

- `airflow_isolated(entry_points=..., environment=..., name=..., timeout=...)`: run in a
  one-shot child pytest process with a synthetic entry-point distribution on
  `PYTHONPATH` and `AIRFLOW__*` overrides applied before the first Airflow import --
  see [Isolated entry-point tests](../guide/isolated-tests.md)
- `db_test`: requires the isolated metadata database (triggers its lazy initialization)
- `api_test`: starts the isolated REST API server lazily and publishes its URL as
  `AIRFLOW__API__BASE_URL` for the test's duration (triggers lazy database initialization)
- `postgres`: requires a provisioned Postgres metadata database (the `postgres` extra plus Docker)
- `compat`: end-user tests exercised across the version matrix
- `need_serialized_dag([enabled])`: request serialized Dag behavior from `dag_maker`
- `environment(name)`: run only when the named environment's sentinel path exists, configured via
  the `airflow_environments` ini line list (`lab = /opt/lab/sentinel`)
- `requires_airflow2` / `requires_airflow3`: run only on the named Airflow family,
  auto-skipped elsewhere -- the bridge that lets one suite run green on both sides of a
  2 -> 3 migration
- `smoke`: a bundled zero-boilerplate check, opt in with `airflow_smoke`
