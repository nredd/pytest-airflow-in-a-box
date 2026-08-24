# Database

## Backends

The metadata database defaults to a tuned, WAL-mode SQLite file created per run -- fast and
correct for single-writer test workloads, and the right default. SQLite serializes writers, so it
cannot reproduce the concurrency semantics a real deployment runs on (row-level locking,
`SELECT ... FOR UPDATE`, multiple concurrent writers). Opt into a disposable Postgres backend when
you need that fidelity:

```console
uv add --dev "pytest-airflow-in-a-box[airflow3,postgres]"
pytest --airflow-db-backend=postgres
```

or persistently via the `airflow_db_backend` ini option (`sqlite` or `postgres`). The Postgres
backend provisions one container per session with [testcontainers](https://testcontainers.com/)
and hands Airflow the resulting SQLAlchemy URL; every worker in an `xdist` run shares that one
database, mirroring the production topology of one metadata database behind many workers. It
**requires a running Docker daemon** and the `postgres` extra. When either is missing the plugin
**fails loudly with a usage error** rather than silently skipping, so a misconfigured Postgres run
can never be mistaken for a passing SQLite run.

SQLite-with-WAL and Postgres are not behaviorally equivalent; a suite green on one is not
guaranteed green on the other. That divergence is the point -- run the Postgres backend to catch
dialect- and concurrency-specific behavior before it ships.

Plugin contributors can install the optional dependencies with `make install-postgres` (or
`uv sync --extra postgres`).

## Sessions

The `session` fixture yields a metadata `Session` and rolls back on teardown, so staged
writes leak nothing between tests. Passing it to `dag_maker(session=...)` routes that
context's metadata writes through it -- but persistence *commits* the session, so anything
staged on it at that point commits with it, and the everything-rolls-back guarantee narrows
to state staged after the last `dag_maker` commit. Rows `dag_maker` itself created are
removed at fixture teardown either way -- and on the 3.x family so is every other DagRun
carrying the Dag's `dag_id` (`dag.test()`'s manual run, the Backfill machinery's runs),
whose task instances would otherwise block the `dag_version` delete. See
[Upstream harness keywords](task-execution.md#upstream-harness-keywords).

At teardown, `dag_maker` *rolls back* a borrowed session before cleaning up its rows on a
fresh one: an uncommitted flush left on the borrowed session would otherwise hold SQLite's
single write lock across cleanup and fail it deterministically with `database is locked`
(issue [#263](https://github.com/nredd/pytest-airflow-in-a-box/issues/263)). Anything
flushed-but-uncommitted on the borrowed session at teardown is therefore discarded -- the
same fate the `session` fixture's own rollback would deal it a moment later. Uncommitted flushes on *other* sessions are the consumer's responsibility:
SQLite allows one writer per database, so commit or roll back sibling sessions before
`dag_maker` persists or cleans up, or use the Postgres backend.

## Cleanup

`clear_db` is a registry-driven whole-database reset for serial setup and teardown contexts:

```python
from pytest_airflow_in_a_box.db import TableGroup, clear_db

clear_db()  # every group
clear_db(tables={TableGroup.VARIABLES})  # one group
```

Requesting a group also clears the groups whose rows reference it (`RUNS` clears task instances
and XCom rows, and `BACKFILL` -- the one group referenced BY `RUNS` rather than the reverse --
clears `RUNS` first), and clearing `CONNECTIONS` recreates Airflow's default connections.
