# Fixtures

Every public fixture the plugin registers, in one place. "DB" means requesting the fixture
triggers lazy metadata-database initialization; fixtures marked "DB" are the members of
`fixtures.DATABASE_FIXTURE_NAMES`. DB-free fixtures never import Airflow's ORM or run a
migration. "3.x only" fixtures fail on the Airflow 2.x tier with an actionable error naming
the 2.x alternative.

## Running Dags and tasks

| Fixture | Scope | DB | Airflow | Returns |
| ------- | ----- | -- | ------- | ------- |
| `dag_bag` | session | yes | 2.x + 3.x | The `DagBag` parsed once per worker process from the configured Dag directory ([Task execution](../guide/dagrun-execution.md#testing-a-dag-defined-elsewhere)) |
| `dag_corpus` | session | conditional | 2.x + 3.x | The portable, fan-out-eligible `DagCorpus` parsed once per worker process from the configured Dag directory -- read-only metadata, not live Airflow objects; shared with the bundled `--airflow-smoke` catalog's own corpus-wide checks, and requesting it makes the builder serialize every Dag ([Writing your own corpus check](../guide/smoke-tests.md#writing-your-own-corpus-check)) |
| `dag_maker` | function | yes | 2.x + 3.x | A factory building and persisting a Dag authored in the test, with `run()` / `run_ti()` execution; accepts upstream `tests_common`'s `session`/`bundle_name`/`bundle_version` harness keywords and exposes the scheduler-side `serialized_dag`/`dag_model`/`timetable`/`sync_dagbag_to_db()` handles ([Scheduler-side handles](../internals/tests-common-parity.md#scheduler-side-handles)) |
| `create_task_instance` | function | yes | 2.x + 3.x | An upstream-parity one-call factory: a `TaskInstance` with its Dag and `DagRun` rows, composed over `dag_maker`. Returns the plain ORM instance -- no `ti.run()` wrapper ([Upstream one-call factories](../internals/tests-common-parity.md#upstream-one-call-factories)) |
| `create_dummy_dag` | function | yes | 2.x + 3.x | An upstream-parity one-call factory: a persisted single-`EmptyOperator` Dag plus, by default, a scheduled `DagRun` ([Upstream one-call factories](../internals/tests-common-parity.md#upstream-one-call-factories)) |
| `run_dag` | function | yes | 2.x + 3.x | A runner for externally-authored Dags, e.g. ones pulled from `dag_bag`. `executor=` drives the run through a real executor instead of in-process, 3.x only ([Task execution](../guide/executor-driven-runs.md)) |
| `run_task` | function | no | 3.x only | A DB-free in-process Task SDK runner for a single operator or standalone `@task` ([DB-free task execution](../guide/db-free-execution.md)) |
| `render_task` | function | no | 3.x only | A DB-free in-process renderer for an operator's `template_fields`, without calling `execute()` ([DB-free task execution](../guide/db-free-execution.md)) |
| `task_context` | function | no | 3.x only | A DB-free in-process Task SDK template-context factory for hand-driven `execute()` calls ([DB-free task execution](../guide/db-free-execution.md)) |

## Database and seeding

| Fixture | Scope | DB | Airflow | Returns |
| ------- | ----- | -- | ------- | ------- |
| `session` | function | yes | 2.x + 3.x | An Airflow metadata `Session`, rolled back on teardown ([Database](../internals/test-environments.md#the-disposable-metadata-database)) |
| `airflow_variables` | function | yes | 2.x + 3.x | A seeder persisting Airflow Variables for one test, deleted on teardown ([Seeding](../internals/test-environments.md#seeding-variables-and-connections)) |
| `airflow_connections` | function | yes | 2.x + 3.x | A seeder persisting Airflow Connections for one test, deleted on teardown ([Seeding](../internals/test-environments.md#seeding-variables-and-connections)) |
| `airflow_parse_secrets` | function | yes | 2.x + 3.x | Nothing -- requesting it resolves top-level `Variable.get` / `Connection.get` lookups in Dag files for the whole test ([Seeding](../internals/test-environments.md#seeding-variables-and-connections)) |
| `testing_dag_bundle` | function | yes | 3.x only | Nothing -- requesting it registers the shared `testing` Dag bundle row upstream core tests bulk-write metadata against. Idempotent, never deleted at teardown -- a conditional delete would race other xdist workers and the per-run database is disposable ([Upstream one-call factories](../internals/tests-common-parity.md#upstream-one-call-factories)) |

## Configuration and paths

| Fixture | Scope | DB | Airflow | Returns |
| ------- | ----- | -- | ------- | ------- |
| `airflow_configure` | session | no | 2.x + 3.x | A callable applying `airflow_config` overrides until session teardown ([Airflow configuration](../internals/test-environments.md#overriding-configuration)) |
| `airflow_components` | function | no | 3.x only | A registry for custom plugins, listeners, policies, secrets backends, executors, and timetables ([Custom components](../guide/custom-components.md)) |
| `airflow_home` | session | no | 2.x + 3.x | This run's isolated `AIRFLOW_HOME` as a `pathlib.Path` ([Where the run lives](../internals/test-environments.md#the-isolated-airflow_home)) |
| `airflow_dags_folder` | session | no | 2.x + 3.x | The Dag directory `dag_bag` parses, as a `pathlib.Path` ([The two Dag folder options](../guide/smoke-tests.md#the-two-dag-folder-options)) |

`airflow_home` and `airflow_dags_folder` deliberately share their names with ini options --
fixtures and ini options live in separate pytest registries. Note the `airflow_home` ini
option names the *base* directory to provision under; the fixture returns the disposable
per-run root created below it.

## REST API and logging

| Fixture | Scope | DB | Airflow | Returns |
| ------- | ----- | -- | ------- | ------- |
| `api_server_url` | session | yes | 3.x only | The base URL of one isolated Airflow API server started for this process's session ([Live REST API](../guide/rest-api.md)) |
| `api_client` | session | yes | 3.x only | An authenticated client bound to the isolated API server ([Live REST API](../guide/rest-api.md)) |
| `api_base_url` | function (autouse) | no | 3.x only when active | The live server URL, published through Airflow configuration for tests marked `api_test`; inert (`None`) everywhere else, so its 2.x failure only reaches `api_test`-marked tests ([Live REST API](../guide/rest-api.md)) |
| `cap_structlog` | function | no | 3.x only | A capture recording structlog events emitted during the test ([Structlog capture](../internals/test-environments.md#captured-logs)) |

Return types are the typed contracts in `pytest_airflow_in_a_box.types` (`DagMaker`,
`CreateTaskInstance`, `CreateDummyDag`, `RunDag`, `RunTask`, `RenderTask`, `TaskContext`,
`AirflowVariables`, `AirflowConnections`, `AirflowConfigure`, `ComponentRegistry`, `DagCorpus`), so
fixture-parameter annotations autocomplete and type-check in consumer suites.
