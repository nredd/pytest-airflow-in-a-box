"""Exercise end-user workflows against the isolated REST API."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest
from airflow.providers.standard.operators.empty import EmptyOperator

from piab.fixtures.api import AirflowApiClient
from piab.types import AirflowConnections, DagMaker

pytestmark = [pytest.mark.compat, pytest.mark.api_test, pytest.mark.db_test]


def test_dag_tasks_and_paginated_runs_are_visible(
    api_client: AirflowApiClient, dag_maker: DagMaker
) -> None:
    """Read authored structure and paginate persisted runs."""

    with dag_maker(dag_id="compat_api_catalog"):
        first = EmptyOperator(task_id="first")
        second = EmptyOperator(task_id="second")
        first >> second
    start_date = dag_maker.start_date
    assert start_date is not None
    dag_maker.create_dagrun(run_id="compat_api_run_1")
    # Distinct logical date on purpose: runs share `(dag_id, logical_date)`
    # uniqueness, and the deterministic default (docs/adr/0003) is the Dag's
    # `start_date` for every manual run.
    dag_maker.create_dagrun(
        run_id="compat_api_run_2",
        logical_date=start_date + timedelta(days=1),
    )

    dag = api_client.get("/api/v2/dags/compat_api_catalog")
    tasks = api_client.get("/api/v2/dags/compat_api_catalog/tasks")
    runs = api_client.get(
        "/api/v2/dags/compat_api_catalog/dagRuns",
        params={"limit": "1", "offset": "0", "order_by": "run_id"},
    )

    assert dag.status == tasks.status == runs.status == 200
    assert dag.body["dag_id"] == "compat_api_catalog"
    assert {task["task_id"] for task in tasks.body["tasks"]} == {"first", "second"}
    assert runs.body["total_entries"] == 2
    assert len(runs.body["dag_runs"]) == 1


def test_dagrun_and_task_instance_states_can_be_mutated(
    api_client: AirflowApiClient, dag_maker: DagMaker
) -> None:
    """Patch one DagRun and one task instance through stable endpoints."""

    with dag_maker(dag_id="compat_api_states"):
        EmptyOperator(task_id="task")
    start_date = dag_maker.start_date
    assert start_date is not None
    dag_maker.create_dagrun(run_id="compat_api_state_run")
    # Distinct logical date on purpose -- see `test_dag_tasks_and_paginated_runs_are_visible`.
    dag_maker.create_dagrun(
        run_id="compat_api_ti_run",
        logical_date=start_date + timedelta(days=1),
    )

    run = api_client.patch(
        "/api/v2/dags/compat_api_states/dagRuns/compat_api_state_run",
        json_body={"state": "success"},
    )
    ti = api_client.patch(
        "/api/v2/dags/compat_api_states/dagRuns/compat_api_ti_run/taskInstances/task",
        json_body={"new_state": "success"},
    )
    page = api_client.get(
        "/api/v2/dags/compat_api_states/dagRuns/compat_api_ti_run/taskInstances",
        params={"limit": "1", "offset": "0"},
    )

    assert run.status == ti.status == page.status == 200
    assert run.body["state"] == "success"
    assert ti.body["task_instances"][0]["state"] == "success"
    assert page.body["total_entries"] == 1


def test_variable_crud(api_client: AirflowApiClient) -> None:
    """Create, read, update, and delete a synthetic Variable."""

    path = "/api/v2/variables/compat_api_variable"
    created = api_client.post(
        "/api/v2/variables",
        json_body={"key": "compat_api_variable", "value": "first", "description": "compat"},
    )
    try:
        read = api_client.get(path)
        updated = api_client.patch(
            path,
            json_body={
                "key": "compat_api_variable",
                "value": "second",
                "description": "compat",
            },
        )
    finally:
        deleted = api_client.delete(path)

    assert created.status == 201
    assert read.status == updated.status == 200
    assert read.body["value"] == "first"
    assert updated.body["value"] == "second"
    assert deleted.status == 204
    assert api_client.get(path).status == 404


def test_connection_crud(api_client: AirflowApiClient) -> None:
    """Round-trip a loopback-only synthetic Connection."""

    path = "/api/v2/connections/compat_api_connection"
    body: dict[str, Any] = {
        "connection_id": "compat_api_connection",
        "conn_type": "sqlite",
        "description": "synthetic loopback",
        "host": "127.0.0.1",
        "schema": ":memory:",
        "extra": "{}",
    }
    created = api_client.post("/api/v2/connections", json_body=body)
    try:
        read = api_client.get(path)
        updated = api_client.patch(path, json_body={**body, "description": "updated"})
    finally:
        deleted = api_client.delete(path)

    assert created.status == 201
    assert read.status == updated.status == 200
    assert read.body["host"] == "127.0.0.1"
    assert updated.body["description"] == "updated"
    assert deleted.status == 204
    assert api_client.get(path).status == 404


def test_api_server_decrypts_a_seeded_connection(
    airflow_connections: AirflowConnections, api_client: AirflowApiClient
) -> None:
    """Decrypt encrypted metadata written by the pytest process in the API server subprocess.

    Airflow's `unit_test_mode` generates a fresh random Fernet key per process, so this proves
    the run's pinned `AIRFLOW__CORE__FERNET_KEY` reaches the `api_client` server subprocess too.
    """

    airflow_connections(
        {
            "compat_fernet_conn": {
                "conn_type": "http",
                "host": "127.0.0.1",
                "password": "s3cret",
                "extra": '{"region": "local"}',
            }
        }
    )

    response = api_client.get("/api/v2/connections/compat_fernet_conn")

    assert response.status == 200
    assert response.body["host"] == "127.0.0.1"
    assert response.body["extra"] == '{"region": "local"}'
