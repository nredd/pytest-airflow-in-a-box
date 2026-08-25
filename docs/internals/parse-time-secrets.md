# Parse-time secret resolution

You are here because a Dag file that reads a Variable or a Connection at *top level* fails to
import under test, with one of two symptoms:

- `ImportError: cannot import name 'SUPERVISOR_COMMS'` -- Airflow 3.1
- `AirflowNotFoundException`, or your `default=` silently coming back -- Airflow 3.2 and newer

One root cause. Airflow 3 routes every Variable and Connection lookup through the Task SDK,
which expects a supervisor process to answer on
`airflow.sdk.execution_time.task_runner.SUPERVISOR_COMMS`. A Dag parse under test has no
supervisor. On 3.1 the fall-through tail imports `SUPERVISOR_COMMS` directly and raises; on
3.2+ `ensure_secrets_backend_loaded` picks a fallback chain that omits the metastore backend, so
the lookup misses quietly. Upstream tracks this in
[apache/airflow#51816](https://github.com/apache/airflow/issues/51816) and
[#48554](https://github.com/apache/airflow/issues/48554);
[PR #61630](https://github.com/apache/airflow/pull/61630) defers the root-cause fix to a
lazy-init `InProcessExecutionAPI`.

## What the plugin does

`_compat/parse_time.py` assigns a `SUPERVISOR_COMMS` of its own for the duration of a parse and
answers lookups from the metadata database -- the same rows
[`airflow_variables` / `airflow_connections`](../guide/seeding.md) commit, read back through
the ORM so Fernet decryption runs. Assigning the attribute satisfies 3.1's direct import *and*
selects 3.2+'s client chain, so one shim covers every certified 3.x release.

Every parse site the plugin owns installs it: the `dag_bag` fixture,
[`--collect-dag-folder`](../guide/dag-collection.md) import and params items, and the
[smoke](../guide/smoke-tests.md) corpus build. Nothing touches the database until a Dag issues
a lookup, so a Dag folder that never reads a Variable or a Connection pays nothing.

There is no substitute for this at the fixture level. `--collect-dag-folder` items are plain
`pytest.Item`s (`DagImportItem`, `DagParamsCaseItem`) with no fixture support at all, so no
consumer fixture can be in scope when they parse.

A miss is not silent either: an unresolved lookup logs at `debug` with the exact call to make,
e.g. ``Seed it before the Dag is parsed via `airflow_variables({'region': ...})`, from a
session-scoped fixture when the Dag folder is parsed once per session``. It is logged rather
than raised because `Variable.get(key, default=...)` is a normal optional-config pattern, and
because Airflow's worker backend discards the error response detail before the Dag sees it.

## Seed at session scope, before anything parses

`dag_bag` is session-scoped and stashes its result for the whole session; collected Dag items
have no fixtures. Both are settled before any function-scoped fixture gets a turn, so
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

Those are private module paths, and they are the reason this recipe lives in `internals/`.

`AIRFLOW_VAR_*` / `AIRFLOW_CONN_*` work here too, and outrank the metastore exactly as they do
at task time. Prefer them when the value is a plain string and you do not need the metastore
backend exercised.

Seeding from `airflow_variables` and then asking for the bag with
`request.getfixturevalue("dag_bag")` does work, but only for whichever test materializes the
bag first. Any earlier test or smoke item that touches `dag_bag` unseeded pins a bag full of
import errors for the rest of the session, so that form goes flaky the moment file order, `-k`,
or `-p randomly` changes.

## Lookups outside a parse

For a lookup that runs in the test body or inside a `with dag_maker(...)` block rather than
during a file parse, request `airflow_parse_secrets`. Seed first, request second -- the shim
reads the database per lookup instead of snapshotting it:

```python
def test_body_lookup(airflow_variables, airflow_parse_secrets):
    airflow_variables({"region": "us-east-1"})

    assert Variable.get("region") == "us-east-1"
```

## Turning it off

`--airflow-parse-secrets=off`, or `airflow_parse_secrets = off` in the ini file, removes the
shim and leaves Airflow's own resolution in place. That is what you want when the un-shimmed
behavior is itself the subject. Airflow 2.x reads the metastore directly at parse time, so the
option is a no-op there.
