# The disposable metadata database

Nothing a test writes survives it, and nothing it writes reaches a database you care about.
The metadata database is created fresh inside this run's
[isolated `AIRFLOW_HOME`](airflow-home.md), migrated lazily the first time a test actually
needs it, and thrown away with the run directory. Two mechanisms keep tests from leaking
into each other inside that run:

- The `session` fixture rolls back on teardown, so anything staged through it disappears
- `dag_maker` deletes the rows it created at teardown, on every test, without you asking

The default backend is a per-run SQLite file tuned for disposable test data (WAL journal,
`synchronous = OFF`, memory temp store). That is the right default and most repos never
change it.

## Sessions

The `session` fixture yields a metadata `Session` and rolls back on teardown, so staged
writes leak nothing between tests. Passing it to `dag_maker(session=...)` routes that
context's metadata writes through it -- but persistence *commits* the session, so anything
staged on it at that point commits with it, and the everything-rolls-back guarantee narrows
to state staged after the last `dag_maker` commit. Rows `dag_maker` itself created are
removed at fixture teardown either way -- and on the 3.x family so is every other DagRun
carrying the Dag's `dag_id` (`dag.test()`'s manual run, the Backfill machinery's runs),
whose task instances would otherwise block the `dag_version` delete. See
[Upstream harness keywords](../internals/tests-common-parity.md#upstream-harness-keywords).

At teardown, `dag_maker` *rolls back* a borrowed session before cleaning up its rows on a
fresh one: an uncommitted flush left on the borrowed session would otherwise hold SQLite's
single write lock across cleanup and fail it deterministically with `database is locked`
(issue [#263](https://github.com/nredd/pytest-airflow-in-a-box/issues/263)). Anything
flushed-but-uncommitted on the borrowed session at teardown is therefore discarded -- the
same fate the `session` fixture's own rollback would deal it a moment later. Uncommitted
flushes on *other* sessions are the consumer's responsibility: SQLite allows one writer per
database, so commit or roll back sibling sessions before `dag_maker` persists or cleans up,
or use the Postgres backend.

## Resetting the whole database

`dag_maker` already cleans up after itself. `clear_db` is for the rest -- Variables,
Connections, Logs, leftover DagRuns some other code path created -- when a serial setup or
teardown needs the database back at a known state:

```python
from pytest_airflow_in_a_box.db import TableGroup, clear_db

clear_db()  # every group
clear_db(tables={TableGroup.VARIABLES})  # one group
```

!!! warning "Serial contexts only"

    `clear_db` truncates tables every xdist worker shares. Under `-n auto`, which is what
    CI runs, a `clear_db()` in one worker deletes rows another worker is mid-test on. Use
    it in a serial run, or in a fixture you have gated to serial, and let `dag_maker` own
    cleanup during parallel runs.

The value is not "it truncates tables" -- it is the registry behind it. The registry lists
every clearable group once, in a single global foreign-key-safe delete order, so requesting
a group also clears the groups whose rows reference it. `RUNS` pulls in task instances and
XCom rows; `TRIGGERS` pulls in the task instances that reference triggers. `BACKFILL` is the
one inversion: its rows are referenced *by* `RUNS` (`dag_run.backfill_id`, no `ondelete`
action), so clearing `BACKFILL` clears `RUNS` first, the reverse of every other implication.
Clearing `CONNECTIONS` recreates Airflow's default connections.

Two deliberate non-implications, because they are user configuration rather than run state:
clearing `DAGS` without `ASSETS` leaves asset definitions and their Dag reference rows
alone, and clearing `RUNS` without `BACKFILL` leaves completed backfill definitions alone.

A hand-written delete list is the alternative, and it rots. The table set moves under you
across Airflow minors -- asset partition tables arrived in 3.2, the `asset_trigger`
association left after 3.1, and 2.x spells the whole asset family `dataset*`. The registry
carries a family-parallel 2.x variant and marks version-optional tables as skippable, so the
same `TableGroup` names work across every certified release. Your list would just start
raising `AttributeError` on upgrade.

## Backends

SQLite serializes writers, so it cannot reproduce the concurrency semantics a real
deployment runs on (row-level locking, `SELECT ... FOR UPDATE`, multiple concurrent
writers). That mostly does not matter: [deciding which failures are
yours](testing-scope.md) rules metadata-DB mechanics out of scope, so dialect fidelity pays
off for a narrow slice -- your own code issuing its own SQL against the metadata database,
or a suite you have seen deadlock. Opt in when you are in that slice:

```console
uv add --dev "pytest-airflow-in-a-box[airflow3,postgres]"
pytest --airflow-db-backend=postgres
```

or persistently via the `airflow_db_backend` ini option (`sqlite` or `postgres`). The
Postgres backend provisions one container per session with
[testcontainers](https://testcontainers.com/) and hands Airflow the resulting SQLAlchemy
URL; every worker in an `xdist` run shares that one database, mirroring the production
topology of one metadata database behind many workers.

It **requires a running Docker daemon** and the `postgres` extra. When either is missing the
plugin **fails loudly with a `pytest.UsageError`** rather than silently skipping, so a
misconfigured Postgres run can never be mistaken for a passing SQLite run.

SQLite-with-WAL and Postgres are not behaviorally equivalent; a suite green on one is not
guaranteed green on the other. That divergence is the point -- run the Postgres backend to
catch dialect- and concurrency-specific behavior before it ships.
