# REST API

Use the live API fixtures when your code must discover and call an Airflow endpoint. The plugin
starts a real Airflow 3 `api-server` against the test metadata database; sessions with neither
API nor executor-driven tests start nothing.

## Test endpoint discovery

Request `api_server_url` when the assertion needs the endpoint itself. The fixture starts the
server and publishes its URL as `AIRFLOW__API__BASE_URL` for that test, so code using
`conf.get("api", "base_url")` finds the live server:

```python
def test_hook_targets_the_running_server(api_server_url: str) -> None:
    assert MyAirflowApiHook().base_url == api_server_url
```

Requesting `api_client` activates the same behavior. Use the `api_test` marker instead when the
code under test calls the configured endpoint but the test itself requests neither fixture:

```python
import pytest


@pytest.mark.api_test
def test_hook_reaches_airflow() -> None:
    response = MyAirflowApiHook().get("/api/v2/version")

    assert response.status == 200
```

The URL override lasts for one test and is then restored. Do not wrap the first fixture request
in a temporary [`airflow_config`](../internals/test-environments.md#overriding-configuration)
context: the session-scoped server inherits its environment when it starts.

## Assert through the API

`api_client` carries a `SimpleAuthManager` bearer token and exposes `get`, `post`, `patch`, and
`delete`. Use it to inspect state your code or fixtures produced, rather than retesting stock
endpoints:

```python
def test_seeded_connection_decrypts_server_side(airflow_connections, api_client) -> None:
    airflow_connections(
        {"my_conn": {"conn_type": "http", "host": "127.0.0.1", "password": "s3cret"}}
    )

    response = api_client.get("/api/v2/connections/my_conn")

    assert response.status == 200
    assert response.body["host"] == "127.0.0.1"
```

The server is a separate process, but it shares the run's metadata database and Fernet key.
Persist Dags with `dag_maker` before requesting their API representation; context exit writes
the serialized scheduler state the server reads.

Each client call returns `ApiResponse(status, body)`. JSON is decoded, an empty response becomes
`None`, and a non-JSON response remains text. HTTP error statuses are returned for assertion;
an unreachable server raises `ApiServerError`. Paths must start with `/`, and `get` accepts a
`params` mapping for query strings.

## Server boundaries

- `api_server_url` and `api_client` are session-scoped. `api_base_url`, the autouse fixture that
  publishes the URL, is function-scoped and inert outside activated tests.
- Each pytest process gets one loopback-only server on an ephemeral port. Under xdist, every
  worker gets a different server and port while all workers share the metadata database. A lost
  port-bind race is retried automatically.
- The server exposes the `core` and `execution` apps. The latter is also the Task Execution API
  used by [executor-driven runs](ladder.md#executor-driven-runs).
- Airflow 2.x has no `airflow api-server`; requesting these fixtures fails with an actionable
  error. A FAB `airflow webserver` tier remains demand-driven
  ([#25](https://github.com/nredd/pytest-airflow-in-a-box/issues/25)).
