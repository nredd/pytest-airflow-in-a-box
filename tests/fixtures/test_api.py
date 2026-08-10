"""Test the live API server fixture and typed client.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html
"""

from __future__ import annotations

import email.message
import os
import subprocess
import urllib.error
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box.fixtures import api
from pytest_airflow_in_a_box.fixtures.api import (
    AirflowApiClient,
    ApiResponse,
    ApiServerError,
    _decode_body,
)
from pytest_airflow_in_a_box.types import DagMaker

pytestmark = [pytest.mark.db_test]


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


@pytest.mark.api_test
def test_marker_alone_starts_and_publishes_the_server() -> None:
    """Discover the marker-activated server through active Airflow configuration."""

    # Deferred so Airflow configuration loads inside the bootstrapped session.
    from airflow.configuration import conf

    base_url = conf.get("api", "base_url")

    assert base_url == os.environ["AIRFLOW__API__BASE_URL"]
    assert base_url.startswith("http://127.0.0.1:")
    assert api._server_responds(base_url)


def test_explicit_fixture_publishes_the_url(api_server_url: str) -> None:
    """Publish the URL for an unmarked test that requests the server fixture."""

    assert os.environ["AIRFLOW__API__BASE_URL"] == api_server_url


def test_dag_listing_requires_authentication(api_server_url: str) -> None:
    """Reject unauthenticated requests against a protected endpoint."""

    anonymous = AirflowApiClient(api_server_url)

    response = anonymous.get("/api/v2/dags")

    assert response.status == 401


@pytest.mark.api_test
def test_authenticated_client_reads_version(api_client: AirflowApiClient) -> None:
    """Fetch the running Airflow version through the authenticated client."""

    response = api_client.get("/api/v2/version")

    assert isinstance(response, ApiResponse)
    assert response.status == 200
    assert isinstance(response.body, dict)
    assert response.body["version"]


@pytest.mark.api_test
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


def _fake_state(tmp_path: Path) -> Any:
    """Create a state double exposing only the log directory.

    Parameters:
        tmp_path: pathlib.Path containing the fake run root.

    Returns:
        types.SimpleNamespace shaped like the bootstrap state surface used.
    """

    logs = tmp_path / "logs"
    logs.mkdir(exist_ok=True)
    return SimpleNamespace(logs_folder=logs)


class _FakeProcess:
    """Scriptable stand-in for the API server subprocess."""

    def __init__(
        self,
        *,
        returncode: int | None = None,
        wait_timeouts: int = 0,
    ) -> None:
        """Configure poll and wait behavior.

        Parameters:
            returncode: int | None reported by ``poll``.
            wait_timeouts: int number of ``wait`` calls that time out first.
        """

        self.returncode = returncode
        self.wait_timeouts = wait_timeouts
        self.calls: list[str] = []

    def poll(self) -> int | None:
        """Report the configured exit code.

        Returns:
            int | None containing the configured exit code.
        """

        self.calls.append("poll")
        return self.returncode

    def terminate(self) -> None:
        """Record the terminate request."""

        self.calls.append("terminate")

    def kill(self) -> None:
        """Record the kill request."""

        self.calls.append("kill")

    def wait(self, timeout: float | None = None) -> int:
        """Time out the configured number of times, then return.

        Parameters:
            timeout: float | None forwarded by the caller.

        Returns:
            int containing a benign exit code.

        Raises:
            subprocess.TimeoutExpired: While configured timeouts remain.
        """

        self.calls.append("wait")
        if self.wait_timeouts > 0:
            self.wait_timeouts -= 1
            raise subprocess.TimeoutExpired(cmd="airflow", timeout=timeout or 0)
        return 0


def test_request_encodes_query_parameters(monkeypatch: pytest.MonkeyPatch) -> None:
    """Append encoded query parameters to the request URL."""

    seen: list[str] = []

    def fake_urlopen(request: Any, timeout: float | None = None) -> Any:
        del timeout
        seen.append(request.full_url)
        raise urllib.error.HTTPError(
            request.full_url, 418, "teapot", email.message.Message(), None
        )

    monkeypatch.setattr(api.urllib.request, "urlopen", fake_urlopen)
    client = AirflowApiClient("http://127.0.0.1:1")

    response = client.get("/api/v2/dags", params={"limit": "1", "tags": "a b"})

    assert response.status == 418
    assert seen == ["http://127.0.0.1:1/api/v2/dags?limit=1&tags=a+b"]


def test_patch_and_delete_wrappers_forward_method(monkeypatch: pytest.MonkeyPatch) -> None:
    """Send the wrapper's HTTP method through `request`."""

    methods: list[str] = []

    def fake_request(self: Any, method: str, path: str, **kwargs: Any) -> str:
        del self, path, kwargs
        methods.append(method)
        return "sentinel"

    monkeypatch.setattr(AirflowApiClient, "request", fake_request)
    client = AirflowApiClient("http://127.0.0.1:1")

    assert client.patch("/x", json_body={}) == "sentinel"
    assert client.delete("/x") == "sentinel"
    assert methods == ["PATCH", "DELETE"]


def test_fetch_access_token_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reject refused requests and token-free response bodies."""

    responses = iter(
        [
            ApiResponse(401, {"detail": "nope"}),
            ApiResponse(201, {"access_token": ""}),
        ]
    )

    def fake_post(self: Any, path: str, *, json_body: Any = None) -> ApiResponse:
        del self, path, json_body
        return next(responses)

    monkeypatch.setattr(AirflowApiClient, "post", fake_post)

    with pytest.raises(ApiServerError, match="Could not obtain an API token"):
        api.fetch_access_token("http://127.0.0.1:1")
    with pytest.raises(ApiServerError, match="no `access_token`"):
        api.fetch_access_token("http://127.0.0.1:1")


def test_server_responds_treats_http_errors_as_alive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Count any HTTP response, including rejections, as responsive."""

    def raise_http_error(request: Any, timeout: float | None = None) -> Any:
        del timeout
        raise urllib.error.HTTPError(
            request.full_url, 401, "denied", email.message.Message(), None
        )

    monkeypatch.setattr(api.urllib.request, "urlopen", raise_http_error)

    assert api._server_responds("http://127.0.0.1:1") is True


def test_log_tail_reports_unreadable_logs(tmp_path: Path) -> None:
    """Explain a missing server log instead of raising."""

    assert "no server log available" in api._log_tail(tmp_path / "absent.log")


def test_launch_fails_when_the_server_exits_early(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Surface an early server exit with the log tail."""

    process = _FakeProcess(returncode=3)
    monkeypatch.setattr(api.subprocess, "Popen", lambda *_a, **_k: process)
    monkeypatch.setattr(api, "_server_responds", lambda _url: False)

    with pytest.raises(ApiServerError, match="exited with code 3"):
        next(api._launch_api_server(_fake_state(tmp_path)))

    assert "terminate" in process.calls


def test_launch_fails_when_startup_times_out(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Surface a startup deadline overrun with the log tail."""

    process = _FakeProcess(returncode=None)
    monkeypatch.setattr(api.subprocess, "Popen", lambda *_a, **_k: process)
    monkeypatch.setattr(api, "_server_responds", lambda _url: False)
    monkeypatch.setattr(api, "API_SERVER_STARTUP_TIMEOUT_SECONDS", -1.0)

    with pytest.raises(ApiServerError, match="did not respond within"):
        next(api._launch_api_server(_fake_state(tmp_path)))


def test_launch_kills_a_server_that_ignores_terminate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Escalate to kill when graceful shutdown times out."""

    process = _FakeProcess(wait_timeouts=1)
    monkeypatch.setattr(api.subprocess, "Popen", lambda *_a, **_k: process)
    monkeypatch.setattr(api, "_server_responds", lambda _url: True)

    launcher = api._launch_api_server(_fake_state(tmp_path))
    base_url = next(launcher)
    assert base_url.startswith("http://127.0.0.1:")
    with pytest.raises(StopIteration):
        next(launcher)

    assert process.calls[-3:] == ["terminate", "wait", "kill"] or process.calls[-4:] == [
        "terminate",
        "wait",
        "kill",
        "wait",
    ]
