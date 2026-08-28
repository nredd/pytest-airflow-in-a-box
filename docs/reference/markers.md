# Markers

The plugin registers ten markers. Three gate execution, two activate resources, one creates an
isolated child process, three are selection labels, and one is an upstream-compatible no-op.
Use `pytest --markers` for the short descriptions; this page records their costs and behavior.

| Marker | Apply it when | Effect |
| --- | --- | --- |
| `requires_airflow2` | The test is valid only on Airflow 2 | Import-free family gate; skips on Airflow 3 or without Airflow. |
| `requires_airflow3` | The test is valid only on Airflow 3 | Import-free family gate; skips on Airflow 2 or without Airflow. |
| `environment(name)` | The test needs an external environment represented by a sentinel path | Skips when the configured sentinel is absent; malformed or unknown names are errors. |
| `db_test` | Your code reaches the metadata database without requesting a database fixture | Triggers lazy database initialization. |
| `api_test` | Your code discovers the REST API through Airflow configuration instead of an API fixture | Triggers the database and API server, then publishes `api.base_url` for that test. Airflow 3 only. |
| `airflow_isolated(...)` | Airflow must discover synthetic entry points or read different `AIRFLOW__*` values before import | Runs the test in a one-shot child pytest process. |
| `postgres` | The test belongs in a Postgres-selected run | Label only; it neither selects Postgres nor checks Docker. |
| `compat` | The test exercises the public consumer contract across certified runtimes | Label only; used by this repository's compatibility matrix. |
| `smoke` | The item is one of the plugin's generated corpus checks | Label added automatically; the smoke option controls collection. |
| `need_serialized_dag([enabled])` | An upstream-derived test carries this compatibility marker | Validated no-op; every persisted Dag already serializes. |

## Gate order

Before a marked test runs, the plugin applies gates in this order:

1. `requires_airflow2` / `requires_airflow3`
2. `environment(name)`
3. the `airflow_isolated` refusal under xdist
4. `db_test` / `api_test` database initialization

Family and environment gates therefore skip before an Airflow import or database migration.
At the end of collection, the plugin makes the same prediction for database-using items; a
serial run initializes no database when every such item will be gated out. Xdist workers
initialize on their first surviving database test instead.

## Family gates: `requires_airflow2`, `requires_airflow3`

```python
import pytest


@pytest.mark.requires_airflow3
def test_asset_scheduling(dag_maker) -> None: ...


@pytest.mark.requires_airflow2
def test_legacy_behavior() -> None: ...
```

Use both markers without arguments. Classification inspects installed distribution metadata
without importing Airflow: `apache-airflow-core` identifies Airflow 3; otherwise the
`apache-airflow` major version identifies Airflow 2. An Airflow-free environment skips both
markers. This also lets the [migration outcome diff](../guide/migration.md#diffing-outcomes-across-the-upgrade)
classify these skips as `gated` rather than regressions.

Use these markers for a dual-family suite. Do not hand-roll a version check against only the
`apache-airflow` distribution: a core-only Airflow 3 installation may not provide it.

## Environment gate: `environment(name)`

Declare sentinel paths in `airflow_environments`, then name exactly one:

```ini
[pytest]
airflow_environments =
    lab = /opt/lab/sentinel
    warehouse = fixtures/warehouse/.ready
```

```python
@pytest.mark.environment("lab")
def test_reads_the_lab_share() -> None: ...
```

Relative paths resolve from pytest's root path. A missing sentinel skips the test; the
sentinel may be a file, directory, or mount point because only existence is checked. A
duplicate configuration name, unknown marker name, keyword argument, non-string name, or
anything other than one positional name is a `pytest.UsageError`. See the
[`airflow_environments` option](ini-options.md#core) for its exact grammar.

## Database gates: `db_test`, `api_test`

`db_test` asks for the disposable metadata database without adding a fixture to the test.
Most tests do not need it explicitly: fixtures such as `dag_maker`, `run_dag`, `dag_bag`, and
`session` already trigger database initialization through their fixture closure. Use the
marker when your own helper reaches the ORM directly. See the
[database lifecycle](../internals/test-environments.md#the-disposable-metadata-database).

`api_test` includes the database cost and starts one REST API server per pytest process. For
the marked test, the autouse `api_base_url` fixture publishes the server URL as
`AIRFLOW__API__BASE_URL` and restores the environment afterward. Requesting `api_client` or
`api_server_url` activates the same path without the marker. The API path is Airflow 3 only;
the [REST API guide](../guide/rest-api.md) covers authentication, scopes, and xdist ports.

## Isolation: `airflow_isolated`

Use isolation when entry-point discovery or import-time Airflow configuration is the subject:

```python
@pytest.mark.airflow_isolated(
    entry_points={"airflow.plugins": "my_plugin = my_pkg.plugin:MyPlugin"},
    environment={"AIRFLOW__CORE__PARALLELISM": "1"},
    name="my-dist",
    timeout=120,
)
def test_plugin_is_discovered() -> None: ...
```

Every argument is a keyword. Supply `entry_points`, `environment`, or both; a bare marker is
an error.

| Keyword | Contract |
| --- | --- |
| `entry_points` | Mapping of group names to one string or a list/tuple of `name = module:attr` strings. The child sees a synthetic `0.0.0` distribution on `PYTHONPATH`. |
| `environment` | Mapping of `AIRFLOW__*` names to string values. Bootstrap-owned names are rejected; use the corresponding plugin option instead. This keyword is unrelated to the `environment(name)` gate. |
| `name` | Optional valid distribution name; normalized like a Python package name. A stable payload-derived name is used otherwise. |
| `timeout` | Positive child-process timeout in seconds; default `300`. |

After deselection, tests in the same module with identical normalized payloads share one child
pytest invocation. Outcomes are replayed into the outer session. Malformed payloads fail the
session before tests begin.

`airflow_isolated` is refused on an xdist worker because nested children would race on shared
batch scratch paths. Run these tests serially. Family and environment gates still run first,
so a gated isolated test skips instead of raising the xdist refusal. See
[isolated entry-point discovery](../guide/custom-components-wiring.md#isolated-entry-point-discovery).

## Selection labels

- `postgres` is only a `-m` label. To actually use Postgres, install the
  [`postgres` extra](dependencies.md#extras) and pass `--airflow-db-backend=postgres`; Docker
  must be available. CI can then select the labeled subset with `-m postgres` or include it in
  a larger expression.
- `compat` labels this repository's consumer-contract tests for the
  [certified runtime matrix](../internals/compat-layer.md#what-ci-actually-exercises). It has no
  runtime behavior and is rarely useful in a downstream suite.
- `smoke` is added to generated catalog items. Enable their collection with
  `--airflow-smoke` or `airflow_smoke = true`, then use normal `-m smoke` selection. See
  [Smoke Tests](../guide/smoke-tests.md) and the [catalog mechanics](smoke.md).

## Upstream compatibility: `need_serialized_dag([enabled])`

This marker accepts zero arguments or one positional boolean. Keyword, multiple, or non-boolean
arguments are usage errors. It deliberately changes nothing: every Dag persisted by
`dag_maker` already serializes, so `dag_maker.serialized_dag` is always available. Keep it
while porting upstream tests; omit it in new tests. See
[`tests_common` parity](../internals/tests-common-parity.md#scheduler-side-handles).
