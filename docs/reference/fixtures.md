# Fixtures

The `pytest11` entry point registers every fixture below. Scope is pytest scope; under xdist,
each worker has its own session fixtures. Cost describes activation:

- **DB** initializes the disposable metadata database for tests containing the fixture.
- **API** starts one server per pytest process and always includes **DB**.
- **Conditional DB** applies only to `dag_corpus`: it needs the database when parse-time
  secrets use the metastore.

Fixtures marked **3.x only** fail on Airflow 2 with an actionable alternative.

## Running Dags and tasks

| Fixture | Scope / Airflow | Cost | Use and return |
| --- | --- | --- | --- |
| `dag_bag` | session · 2.x + 3.x | DB | Live `DagBag` parsed once per worker. Select repository Dags from it for [`run_dag`](../guide/ladder.md#testing-a-dag-defined-elsewhere). |
| `dag_corpus` | session · 2.x + 3.x | Conditional DB | Read-only, process-portable `DagCorpus` for repository-wide checks. Requesting it serializes every Dag; see [custom corpus checks](../guide/smoke-tests.md#writing-your-own-corpus-check). |
| `dag_maker` | function · 2.x + 3.x | DB | `DagMaker` authors, persists, and runs test-defined Dags. See its [scheduler handles and upstream keywords](../internals/tests-common-parity.md#scheduler-side-handles). |
| `create_task_instance` | function · 2.x + 3.x | DB | `CreateTaskInstance` returns one ORM `TaskInstance` with its Dag and `DagRun`; it adds no `ti.run()` wrapper. See [one-call factories](../internals/tests-common-parity.md#upstream-one-call-factories). |
| `create_dummy_dag` | function · 2.x + 3.x | DB | `CreateDummyDag`, an upstream-compatible factory returning `(dag, empty_operator)`. It creates a scheduled `DagRun` by default; pass `with_dagrun_type=None` to omit it. |
| `run_dag` | function · 2.x + 3.x | DB; API with `executor=` | `RunDag` executes an externally authored Dag and returns `DagRunResult`. [Executor mode](../guide/ladder.md#executor-driven-runs) is 3.x only. |
| `run_task` | function · 3.x only | none | `RunTask` executes one operator through the in-process Task SDK and returns `TaskRunResult`; no ORM rows or migration. |
| `render_task` | function · 3.x only | none | `RenderTask` returns a prepared copy with `template_fields` rendered and never calls `execute()`. |
| `task_context` | function · 3.x only | none | `TaskContext` opens a seeded context for hand-driving the prepared task. The [DB-free rung](../guide/ladder.md#one-operator-no-database) compares these three fixtures. |

## Database and seeding

| Fixture | Scope / Airflow | Cost | Use and return |
| --- | --- | --- | --- |
| `session` | function · 2.x + 3.x | DB | Airflow metadata `Session`; teardown rolls back uncommitted work and closes it. Explicit commits persist within the disposable database. |
| `airflow_variables` | function · 2.x + 3.x | DB | `AirflowVariables` callable that commits Variable rows and deletes its rows at teardown. |
| `airflow_connections` | function · 2.x + 3.x | DB | `AirflowConnections` callable that commits Connection rows and deletes its rows at teardown. |
| `airflow_parse_secrets` | function · 2.x + 3.x | DB | Activation fixture returning `None`. It enables Variable and Connection resolution outside plugin-owned Dag parses for the test; it is inert on Airflow 2 and with parse-time resolution disabled. |
| `testing_dag_bundle` | function · 3.x only | DB | Returns `None`; creates the shared `testing` Dag-bundle row for upstream-style metadata tests and leaves it in the disposable database. |

Prefer environment-backed secrets unless testing the metastore. Seeded identifiers are
database-global under xdist; precedence and collision rules live under
[Variables and Connections](../internals/test-environments.md#seeding-variables-and-connections).

## Configuration and paths

| Fixture | Scope / Airflow | Cost | Use and return |
| --- | --- | --- | --- |
| `airflow_configure` | session · 2.x + 3.x | none | `AirflowConfigure` applies runtime overrides until teardown. Use ini for values that must precede every Dag parse; see [configuration scopes](../internals/test-environments.md#overriding-configuration). |
| `airflow_components` | function · 3.x only | none | `ComponentRegistry` provides reversible test registration. It does not prove [production discovery](../guide/custom-components-wiring.md#runtime-component-registration). |
| `airflow_home` | session · 2.x + 3.x | none | `pathlib.Path` for this run's disposable `AIRFLOW_HOME`. The same-named ini option selects its parent directory, not this exact path. |
| `airflow_dags_folder` | session · 2.x + 3.x | none | `pathlib.Path` that `dag_bag`, `run_dag`, `dag_corpus`, and smoke checks parse. It is distinct from the per-file collection folder; see [the two Dag folder options](../guide/smoke-tests.md#the-two-dag-folder-options). |

## REST API and logging

| Fixture | Scope / Airflow | Cost | Use and return |
| --- | --- | --- | --- |
| `api_server_url` | session · 3.x only | DB + API | Base URL string for the live, loopback-only Airflow API server. Requesting it also publishes the URL for that test. |
| `api_client` | session · 3.x only | DB + API | Authenticated `AirflowApiClient` bound to the same server; methods return `ApiResponse(status, body)`. |
| `api_base_url` | function, autouse · active path 3.x only | none when inert; DB + API when active | Publishes the URL for tests using `api_client`, `api_server_url`, or `api_test`; otherwise returns `None`. Requesting it alone does not activate the server. |
| `cap_structlog` | function · 3.x only | none | `StructlogCapture` containing Airflow structlog events emitted during the test. On Airflow 2, use pytest's `caplog`. |

See the [REST API guide](../guide/rest-api.md) for server behavior and
[captured logs](../internals/test-environments.md#captured-logs) for `caplog` boundaries.

## Typing and activation

Callable and structural contracts such as `DagMaker`, `RunDag`, `RunTask`, `TaskContext`, and
`ComponentRegistry` are exported from `pytest_airflow_in_a_box.types`. Other rows return the
concrete standard-library, SQLAlchemy, Airflow, or API-client type named above.

Fixtures activate resources through pytest's fixture closure; markers cover tests that reach
the same resources through their own code. See [Markers](markers.md) for `db_test` and
`api_test`, and [CLI and INI options](ini-options.md) for run-wide configuration.
