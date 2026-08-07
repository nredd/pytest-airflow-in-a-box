"""Test the live API server fixture and typed client.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html
"""

from __future__ import annotations

import pytest

from pytest_airflow_in_a_box.fixtures.api import (
    AirflowApiClient,
    ApiResponse,
    ApiServerError,
    _decode_body,
)
from pytest_airflow_in_a_box.types import DagMaker

pytestmark = [pytest.mark.api_test, pytest.mark.db_test]


def test_client_rejects_invalid_inputs() -> None:
    """Validate base URL and path shapes before any network traffic."""

    with pytest.raises(ValueError, match="`base_url` must be an HTTP URL"):
        AirflowApiClient("ftp://example.com")
    client = AirflowApiClient("http://127.0.0.1:1")
    with pytest.raises(ValueError, match="`path` must be absolute"):
        client.get("api/v2/version")


def test_client_raises_when_server_is_unreachable() -> None:
    """Wrap connection failures in `ApiServerError`."""

    client = AirflowApiClient("http://127.0.0.1:9")

    with pytest.raises(ApiServerError, match="Could not reach API server"):
        client.get("/api/v2/version")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (b"", None),
        (b'{"key": 1}', {"key": 1}),
        (b"plain text", "plain text"),
    ],
)
def test_decode_body_handles_json_text_and_empty(raw: bytes, expected: object) -> None:
    """Decode JSON bodies, pass through text, and map empty to ``None``."""

    assert _decode_body(raw) == expected


def test_dag_listing_requires_authentication(api_server_url: str) -> None:
    """Reject unauthenticated requests against a protected endpoint."""

    anonymous = AirflowApiClient(api_server_url)

    response = anonymous.get("/api/v2/dags")

    assert response.status == 401


def test_authenticated_client_reads_version(api_client: AirflowApiClient) -> None:
    """Fetch the running Airflow version through the authenticated client."""

    response = api_client.get("/api/v2/version")

    assert isinstance(response, ApiResponse)
    assert response.status == 200
    assert isinstance(response.body, dict)
    assert response.body["version"]


def test_persisted_dag_is_visible_through_the_api(
    api_client: AirflowApiClient,
    dag_maker: DagMaker,
) -> None:
    """Read a dag_maker-persisted Dag back through the live REST API."""

    # Deferred so the Dag context builds the operator at test run time.
    from airflow.providers.standard.operators.empty import EmptyOperator

    with dag_maker(dag_id="api_visible_dag"):
        EmptyOperator(task_id="noop")

    response = api_client.get("/api/v2/dags/api_visible_dag")

    assert response.status == 200
    assert isinstance(response.body, dict)
    assert response.body["dag_id"] == "api_visible_dag"
