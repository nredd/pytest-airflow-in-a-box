# Seeding Variables and Connections

`airflow_variables` and `airflow_connections` are the metastore counterparts to `run_task`'s
`variables=`/`connections=` keywords. Rows are committed, so hooks, operators, and Airflow's
metastore secrets backend resolve them exactly as they would in a deployment, and every row the
fixture inserted is deleted on teardown -- including after a failing test:

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

Connection fields are the same flat shape `run_task(connections=...)` takes, so `conn_type`
defaults to `generic` and `extra` is a JSON object string, not a dict. A `uri` is not accepted --
pass the fields. Repeated calls accumulate rather than replace, and neither fixture overwrites a
row it did not insert, so an existing key, `conn_id`, or one of Airflow's default connections
fails loudly instead of being clobbered.

**Environment variables outrank these rows.** Airflow's default secrets search path is the
environment backend and *then* the metastore backend, so `AIRFLOW_VAR_ANSWER` or `AIRFLOW_CONN_DB`
-- however it got set, including through
[`airflow_config(env=...)`](configuration.md) -- wins over anything seeded here. Rather than
leave a silently shadowed row to debug, both fixtures refuse to seed an identifier whose
`AIRFLOW_VAR_*`/`AIRFLOW_CONN_*` name is already set. Seed through the environment when you want
the environment backend exercised, and through these fixtures when you want the metastore one.

**Seeded names are database-global, not test-local.** Every `xdist` worker shares one metadata
database and the `conn_id` in the test's operator cannot be renamed per worker, so two tests
seeding the same name concurrently collide. Give each test unique identifiers, or group colliding
tests onto one worker with `@pytest.mark.xdist_group`.
