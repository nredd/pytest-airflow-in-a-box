# Live REST API

`api_client` lazily starts one isolated `airflow api-server` per test process on a loopback
ephemeral port and returns a typed client authenticated through SimpleAuthManager. Airflow
2.x has no `airflow api-server`, so on the 2.x family the REST API fixtures fail with an
actionable error (a FAB `airflow webserver` tier is demand-driven, tracked in
[#25](https://github.com/nredd/pytest-airflow-in-a-box/issues/25)):

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

The server runs with `--apps core,execution`, so it serves the Task Execution API at
`/execution` alongside the public `/api/v2`. That second app is what supervised task workers
report to, and it is what makes
[executor-driven runs](task-execution.md#executor-driven-runs) possible -- upstream's
`dag.test(use_executor=True)` queues real workloads but has no way to stand a server up
inside a test process ([apache/airflow#59074](https://github.com/apache/airflow/issues/59074)).

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
