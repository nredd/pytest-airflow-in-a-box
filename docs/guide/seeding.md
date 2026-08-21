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

## Top-level lookups, at Dag parse time

A `Variable.get()` or `BaseHook.get_connection()` written at Dag *top level* -- outside any task
-- runs while the file is being imported, not while a task executes. On Airflow 3 that lookup
goes through the Task SDK, which expects a supervisor process to answer it, and there is none
during a test parse. Depending on the release you get
`ImportError: cannot import name 'SUPERVISOR_COMMS'` (3.1) or a silent miss that surfaces as
`AirflowNotFoundException` (3.2+). Upstream has tracked this since 2025 in
[apache/airflow#51816](https://github.com/apache/airflow/issues/51816) and
[#48554](https://github.com/apache/airflow/issues/48554), and
[PR #61630](https://github.com/apache/airflow/pull/61630) defers the root-cause fix.

The plugin answers those lookups itself, from the same metastore rows `airflow_variables` and
`airflow_connections` commit. Every parse the plugin runs is covered: `dag_bag`, the
`--collect-dag-folder` import items, and the smoke catalog's corpus build. Nothing reads the
database until a Dag actually issues a lookup, so a Dag folder that never touches Variables or
Connections costs nothing.

**Seed at session scope, before anything parses.** `dag_bag` parses once per worker and
stashes the result for the whole session, and collected Dag items are plain `pytest.Item`s with
no fixture support at all. Both are settled before any function-scoped fixture gets a turn, so
parse-time values belong in `pytest_sessionstart`:

```python
# conftest.py
def pytest_sessionstart(session):
    from airflow.models.variable import Variable
    from airflow.utils.session import create_session

    from pytest_airflow_in_a_box._compat import ensure_database
    from pytest_airflow_in_a_box.bootstrap import get_bootstrap_state

    ensure_database(get_bootstrap_state(session.config).root)
    with create_session() as db_session:
        db_session.add(Variable(key="region", val="us-east-1"))
```

`AIRFLOW_VAR_*`/`AIRFLOW_CONN_*` work here too -- the environment backend still outranks the
metastore, exactly as it does at task time.

Seeding from `airflow_variables` and then asking for the bag with
`request.getfixturevalue("dag_bag")` does work, but only for whichever test materializes
the bag first. Any earlier test or smoke item that touches `dag_bag` unseeded pins a bag
full of import errors for the rest of the session, so that form goes flaky the moment file
order, `-k`, or `-p randomly` changes. Prefer the session-scoped seed above.

For a lookup that runs in the test body or inside a `with dag_maker(...)` block rather than
during a file parse, request the `airflow_parse_secrets` fixture:

```python
def test_body_lookup(airflow_variables, airflow_parse_secrets):
    airflow_variables({"region": "us-east-1"})

    assert Variable.get("region") == "us-east-1"
```

`--airflow-parse-secrets=off` (or `airflow_parse_secrets = off` in the ini file) turns all of
this off and leaves Airflow's own resolution in place, which is what you want when you are
testing the un-shimmed behavior itself. Airflow 2.x reads the metastore directly at parse time
and needs none of this, so the option is a no-op there.
