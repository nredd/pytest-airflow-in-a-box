# Live REST API

`api_client` lazily starts one isolated `airflow api-server` per test process on a loopback
ephemeral port and returns a typed client authenticated through SimpleAuthManager:

```python
import pytest


@pytest.mark.api_test
def test_api(api_client, dag_maker):
    with dag_maker(dag_id="visible"):
        ...

    response = api_client.get("/api/v2/dags/visible")

    assert response.status == 200
    assert response.body["dag_id"] == "visible"
```

The `api_test` marker alone also starts the server, and every activated test -- marked or
requesting `api_client`/`api_server_url` -- gets the selected URL published as
`AIRFLOW__API__BASE_URL` for its duration, so application code can discover the endpoint
through active Airflow configuration:

```python
import pytest
from airflow.configuration import conf


@pytest.mark.api_test
def test_application_client():
    base_url = conf.get("api", "base_url")
    assert base_url.startswith("http://127.0.0.1:")
```
