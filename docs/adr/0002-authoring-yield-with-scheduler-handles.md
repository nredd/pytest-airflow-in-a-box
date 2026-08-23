# dag_maker yields the authoring Dag; scheduler-side state is opt-in handles

Issue #238 mapped where `dag_maker`'s contract diverges from upstream `tests_common`,
using the #227 experiment's per-version drift scans (Airflow 2.9.3 through 3.4.0.dev).
After the cheap wins landed -- harness-kwarg routing (#250) and the one-call
`create_task_instance`/`create_dummy_dag` factories (#237) -- the remaining question was
structural: which side of Airflow 3's sdk/scheduler split does the context hand back? The
drift data showed the scheduler-side failure classes rotate with every Airflow minor
(3.1's `'DAG' object has no attribute 'create_dagrun'` class vanishes at 3.2, replaced by
`sync_dagbag_to_db` and `infer_manual_data_interval` classes), so chasing individual
methods would never converge; only the side of the split is a stable decision.

We decided the context keeps yielding the mutable authoring Dag -- `airflow.sdk.DAG` on
3.x, `airflow.models.dag.DAG` on the certified 2.x family. This plugin tests Dags as
users author them; it is not a `tests_common` clone, and handing back scheduler-side
objects would both break the authoring workflow (`with dag_maker() as dag:` defining
operators) and couple the yield to an API surface that churns per minor. Scheduler-side
state is instead exposed through narrow opt-in handles on the factory: `serialized_dag`
(now always populated after persistence -- the `serialized=` flag and
`need_serialized_dag` marker were pure exposure gates over a row that always existed, and
are retained as compat no-ops), `dag_model` (the live `DagModel` ORM row, typed as the
structural `DagModelRow` protocol rather than the ORM class), and `sync_dagbag_to_db()`
(upstream's mutate-then-resync shape, re-running the persistence sequence without
`persist_dag`'s delete-on-failure cleanup).

Run-id conventions also stay divergent on purpose: fixture-created runs keep
`manual__pytest-airflow-in-a-box-<dag_id>-<hash>` instead of upstream's `test` /
logical-date-derived ids. The hashed id is the xdist collision mitigation for
same-`dag_id` contention; upstream's fixed `test` id would reintroduce exactly the
cross-worker row collisions the naming scheme exists to prevent. The drift scans put this
class at a flat ~50 upstream-test failures at every certified version -- a price we accept.

Consequences: `serialized=` / `need_serialized_dag` no longer change behavior (a suite
can no longer assert "not serialized" via `serialized_dag is None` after exit); the
sdk-object failure classes in upstream suites (timetables missing
`infer_manual_data_interval`, `DAG.clear`, `_time_restriction`) and the run-id assertion
class remain out of scope by design; and upstream-parity work on `dag_maker` is bounded
to harness kwargs and factory handles -- never the yielded object, never run-id
semantics. On 3.x each `sync_dagbag_to_db()` may record a new DagVersion; DagRuns created
before a resync keep the version they were created with.
