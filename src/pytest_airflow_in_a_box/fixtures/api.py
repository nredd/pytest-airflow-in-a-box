"""Live Airflow REST API server fixture and a small typed client.

The server is a lazy, session-scoped, per-process concern: it starts only when
the first ``api_test`` actually requests it, binds a loopback ephemeral port,
and serves Airflow's ``core`` API app against the shared isolated metadata
database. Under xdist each worker owns its own server process; they share the
database, so no cross-worker coordination or port election is needed. This
deliberately replaces the plan's controller-owned single server: it is
strictly simpler and costs nothing on sessions without API tests.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html
    https://airflow.apache.org/docs/apache-airflow/stable/security/api.html
"""

from __future__ import annotations

import json
import logging
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import pytest

from pytest_airflow_in_a_box.bootstrap import get_bootstrap_state

LOGGER = logging.getLogger(__name__)

API_SERVER_STARTUP_TIMEOUT_SECONDS = 120.0
API_SERVER_SHUTDOWN_TIMEOUT_SECONDS = 15.0
API_SERVER_POLL_INTERVAL_SECONDS = 0.5
DEFAULT_USERNAME = "admin"
DEFAULT_PASSWORD = "admin"
LOG_TAIL_CHARACTERS = 4000


class ApiServerError(RuntimeError):
    """Report a live API server that failed to start or respond."""


@dataclass(frozen=True)
class ApiResponse:
    """One decoded REST API response.

    Parameters:
        status: int containing the HTTP status code.
        body: Any containing the decoded JSON body, or ``None`` when empty.
    """

    status: int
    body: Any


class AirflowApiClient:
    """Minimal typed client bound to one live Airflow API server.

    Parameters:
        base_url: str containing the server's loopback base URL.
        token: str | None containing a JWT bearer token, or ``None`` for
            unauthenticated requests.

    Raises:
        ValueError: The base URL is not an HTTP URL.
    """

    def __init__(self, base_url: str, *, token: str | None = None) -> None:
        if not base_url.startswith("http"):
            raise ValueError(f"`base_url` must be an HTTP URL: '{base_url}'")
        self.base_url = base_url.rstrip("/")
        self.token = token

    def request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
    ) -> ApiResponse:
        """Send one JSON request and decode the JSON response.

        Parameters:
            method: str containing the HTTP method.
            path: str containing the absolute API path.
            json_body: dict[str, Any] | None containing the JSON payload.
            params: dict[str, str] | None containing query parameters.

        Returns:
            ApiResponse containing status and decoded body; HTTP error statuses
            are returned, not raised.

        Raises:
            ValueError: The path does not start with ``/``.
            ApiServerError: The server cannot be reached at all.
        """

        if not path.startswith("/"):
            raise ValueError(f"`path` must be absolute: '{path}'")
        url = self.base_url + path
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"
        headers = {"Accept": "application/json"}
        data: bytes | None = None
        if json_body is not None:
            data = json.dumps(json_body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if self.token is not None:
            headers["Authorization"] = f"Bearer {self.token}"
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request) as response:
                return ApiResponse(int(response.status), _decode_body(response.read()))
        except urllib.error.HTTPError as error:
            return ApiResponse(int(error.code), _decode_body(error.read()))
        except urllib.error.URLError as error:
            raise ApiServerError(f"Could not reach API server at '{url}': {error}") from error

    def get(self, path: str, *, params: dict[str, str] | None = None) -> ApiResponse:
        """Send one GET request.

        Parameters:
            path: str containing the absolute API path.
            params: dict[str, str] | None containing query parameters.

        Returns:
            ApiResponse containing status and decoded body.
        """

        return self.request("GET", path, params=params)

    def post(self, path: str, *, json_body: dict[str, Any] | None = None) -> ApiResponse:
        """Send one POST request.

        Parameters:
            path: str containing the absolute API path.
            json_body: dict[str, Any] | None containing the JSON payload.

        Returns:
            ApiResponse containing status and decoded body.
        """

        return self.request("POST", path, json_body=json_body)

    def patch(self, path: str, *, json_body: dict[str, Any] | None = None) -> ApiResponse:
        """Send one PATCH request.

        Parameters:
            path: str containing the absolute API path.
            json_body: dict[str, Any] | None containing the JSON payload.

        Returns:
            ApiResponse containing status and decoded body.
        """

        return self.request("PATCH", path, json_body=json_body)

    def delete(self, path: str) -> ApiResponse:
        """Send one DELETE request.

        Parameters:
            path: str containing the absolute API path.

        Returns:
            ApiResponse containing status and decoded body.
        """

        return self.request("DELETE", path)


def _decode_body(raw: bytes) -> Any:
    """Decode one response body as JSON when present.

    Parameters:
        raw: bytes containing the response payload.

    Returns:
        Any containing decoded JSON, the raw text when not JSON, or ``None``
        when empty.
    """

    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw.decode("utf-8", "replace")


def fetch_access_token(
    base_url: str,
    *,
    username: str = DEFAULT_USERNAME,
    password: str = DEFAULT_PASSWORD,
) -> str:
    """Fetch one SimpleAuthManager JWT for the isolated server.

    Parameters:
        base_url: str containing the server's loopback base URL.
        username: str containing the SimpleAuthManager username.
        password: str containing the SimpleAuthManager password.

    Returns:
        str containing the bearer token.

    Raises:
        ApiServerError: The token endpoint refused the request.
    """

    client = AirflowApiClient(base_url)
    response = client.post(
        "/auth/token",
        json_body={"username": username, "password": password},
    )
    if response.status not in (200, 201) or not isinstance(response.body, dict):
        raise ApiServerError(
            f"Could not obtain an API token from '{base_url}/auth/token': "
            f"status {response.status}, body {response.body!r}"
        )
    token = response.body.get("access_token")
    if not isinstance(token, str) or not token:
        raise ApiServerError(f"Token endpoint returned no `access_token`: {response.body!r}")
    return token


def _free_port() -> int:
    """Reserve one currently free loopback port.

    Returns:
        int containing the port number.
    """

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _server_responds(base_url: str) -> bool:
    """Check whether the server answers HTTP at all.

    Parameters:
        base_url: str containing the server's loopback base URL.

    Returns:
        bool indicating any HTTP response, including auth rejections.
    """

    request = urllib.request.Request(f"{base_url}/api/v2/version", method="GET")
    try:
        with urllib.request.urlopen(request, timeout=5):
            return True
    except urllib.error.HTTPError:
        return True
    except (urllib.error.URLError, OSError):
        return False


def _log_tail(log_path: Any) -> str:
    """Read the tail of the server log for failure diagnostics.

    Parameters:
        log_path: Any containing the pathlib.Path of the server log.

    Returns:
        str containing the final log characters, or an explanatory note.
    """

    try:
        return log_path.read_text(encoding="utf-8", errors="replace")[-LOG_TAIL_CHARACTERS:]
    except OSError as error:
        return f"<no server log available: {error}>"


@pytest.fixture(scope="session")
def api_server_url(pytestconfig: pytest.Config) -> Iterator[str]:
    """Start one isolated Airflow API server for this process's session.

    Yields:
        str containing the server's loopback base URL.

    Raises:
        ApiServerError: The server exited early or never became responsive.
    """

    state = get_bootstrap_state(pytestconfig)
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    log_path = state.logs_folder / f"api-server-{port}.log"
    command = [
        sys.executable,
        "-m",
        "airflow",
        "api-server",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--apps",
        "core",
        "--workers",
        "1",
    ]
    LOGGER.info(f"Starting isolated Airflow API server on '{base_url}'")
    with log_path.open("wb") as log_stream:
        process = subprocess.Popen(command, stdout=log_stream, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + API_SERVER_STARTUP_TIMEOUT_SECONDS
            while not _server_responds(base_url):
                if process.poll() is not None:
                    raise ApiServerError(
                        f"Airflow API server exited with code {process.returncode} before "
                        f"responding; log tail:\n{_log_tail(log_path)}"
                    )
                if time.monotonic() > deadline:
                    raise ApiServerError(
                        f"Airflow API server did not respond within "
                        f"{API_SERVER_STARTUP_TIMEOUT_SECONDS:.0f}s; log tail:\n"
                        f"{_log_tail(log_path)}"
                    )
                time.sleep(API_SERVER_POLL_INTERVAL_SECONDS)
            yield base_url
        finally:
            process.terminate()
            try:
                process.wait(timeout=API_SERVER_SHUTDOWN_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


@pytest.fixture(scope="session")
def api_client(api_server_url: str) -> AirflowApiClient:
    """Return an authenticated client bound to the isolated API server.

    Parameters:
        api_server_url: str containing the live server's base URL.

    Returns:
        AirflowApiClient carrying a SimpleAuthManager bearer token.
    """

    return AirflowApiClient(api_server_url, token=fetch_access_token(api_server_url))


__all__ = (
    "AirflowApiClient",
    "ApiResponse",
    "ApiServerError",
    "api_client",
    "api_server_url",
    "fetch_access_token",
)
