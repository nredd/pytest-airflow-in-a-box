# REST API

The job here is *your* code that resolves an Airflow endpoint -- a hook, an operator, a
callback that posts back to the API server. Airflow ships no default for `api.base_url`, so in
a test session `conf.get("api", "base_url")` raises `AirflowConfigException`, and the usual
cure -- a `fallback=` or a hard-coded `http://localhost:8080` -- points your code at a server
nobody is running. Mocked out, that test passes forever.

Mark the test `api_test` and the plugin starts a real `airflow api-server` and publishes its
URL as `AIRFLOW__API__BASE_URL` for the test's duration, so your resolution path resolves to
something that answers:

```python
import pytest


@pytest.mark.api_test
def test_hook_targets_the_running_server(api_server_url: str) -> None:
    assert MyAirflowApiHook().base_url == api_server_url
```

Activation is the `api_test` marker, or a fixture closure containing `api_client` or
`api_server_url` -- either one publishes the URL. Every other test starts nothing.

`api_client` is a typed client bound to that server, authenticated through `SimpleAuthManager`.
Use it to assert on state your code produced, not on stock endpoints -- for example that a
Connection you seeded in the pytest process decrypts in the server's *separate* process, which
is the run's pinned `AIRFLOW__CORE__FERNET_KEY` doing its job:

```python
import pytest


@pytest.mark.api_test
def test_seeded_connection_decrypts_server_side(airflow_connections, api_client) -> None:
    airflow_connections(
        {"my_conn": {"conn_type": "http", "host": "127.0.0.1", "password": "s3cret"}}
    )

    response = api_client.get("/api/v2/connections/my_conn")

    assert response.status == 200
    assert response.body["host"] == "127.0.0.1"
```

The contract:

- One server per test *process*, started lazily on first activation and reused for the
  session. Under xdist each worker owns its own server and they share the metadata database
- Loopback only, on an ephemeral port. A worker that loses the bind race retries with a fresh
  port rather than borrowing another worker's server
- `AirflowApiClient.get`/`post`/`patch`/`delete` return an `ApiResponse` with `status` and a
  decoded `body`. HTTP error statuses are returned, not raised

The server runs `--apps core,execution`, so the same process serves the Task Execution API that
[executor-driven runs](ladder.md#executor-driven-runs) point supervised workers at.

Airflow 2.x has no `airflow api-server`, so on the 2.x family these fixtures fail with an
actionable error. A FAB `airflow webserver` tier is demand-driven
([#25](https://github.com/nredd/pytest-airflow-in-a-box/issues/25)).
