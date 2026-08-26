# Seeding Variables and Connections

Reach for `airflow_variables` / `airflow_connections` when the *metastore* backend is the
subject: your hook resolves a `conn_id` through Airflow's secrets search path, and you want the
row that a live deployment would read, encrypted and committed, in the metadata database.

For everything else, the environment backend is less machinery and it wins anyway:

```python
def test_hook(monkeypatch):
    monkeypatch.setenv("AIRFLOW_CONN_DB", '{"conn_type": "postgres", "host": "example.com"}')
```

That is not a workaround. Airflow's default secrets search path is the environment backend and
*then* the metastore backend, so `AIRFLOW_VAR_*` / `AIRFLOW_CONN_*` -- however it got set,
including through [`airflow_config(env=...)`](configuration.md) -- outranks anything these
fixtures commit. Both fixtures refuse to seed an identifier whose `AIRFLOW_VAR_*` /
`AIRFLOW_CONN_*` name is already set, rather than leave you a silently shadowed row to debug.

## Seeding the metastore

```python
def test_hook(airflow_connections, airflow_variables, dag_maker):
    airflow_variables({"answer": "42"})
    airflow_connections(
        {"db": {"conn_type": "postgres", "host": "example.com", "password": "s3cret"}}
    )

    with dag_maker():
        MyOperator(task_id="read", conn_id="db")

    assert dag_maker.run_ti("read").state == TaskInstanceState.SUCCESS
```

Rows are committed, so hooks, operators, and Airflow's metastore secrets backend resolve them
exactly as they would in a deployment, and every row the fixture inserted is deleted on
teardown -- including after a failing test.

Connection fields are the same flat shape [`run_task(connections=...)`](db-free-execution.md)
takes, so `conn_type` defaults to `generic` and `extra` is a JSON object string, not a dict. A
`uri` is not accepted -- pass the fields. Repeated calls accumulate rather than replace, and
neither fixture overwrites a row it did not insert, so an existing key, `conn_id`, or one of
Airflow's default connections fails loudly instead of being clobbered.

**Seeded names are database-global, not test-local.** Every `xdist` worker shares one metadata
database and the `conn_id` in the test's operator cannot be renamed per worker, so two tests
seeding the same name concurrently collide. Give each test unique identifiers, or group
colliding tests onto one worker with `@pytest.mark.xdist_group`.

Teardown, on the other hand, is safe under distinct names. `variable.id` and `connection.id`
are plain `Integer` primary keys with no `sqlite_autoincrement`, so SQLite reuses the value
once the highest row is deleted -- deleting seeded rows by id alone would take another
worker's live seed. Cleanup matches the `key`/`conn_id` as well as the id, so a reused
primary key misses instead of deleting a stranger's row.

## Lookups that run at Dag parse time

A `Variable.get()` at Dag *top level* runs while the file is imported, not while a task
executes, and on Airflow 3 that lookup has no supervisor to answer it. `AIRFLOW_VAR_*` /
`AIRFLOW_CONN_*` still resolve there; metastore rows only resolve because the plugin shims the
lookup, and the seeding then has to happen before anything parses. See
[Parse-time secret resolution](../internals/parse-time-secrets.md).
