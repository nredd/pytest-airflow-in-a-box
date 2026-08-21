"""Live Airflow REST API server fixture and a small typed client.

The server is a lazy, session-scoped, per-process concern: it starts when the
first ``api_test``-marked test runs or a test requests ``api_client`` or
``api_server_url``, binds a loopback ephemeral port, and serves Airflow's
``core`` and ``execution`` API apps against the shared isolated metadata
database -- the latter is the Task Execution API that executor-driven runs
point supervised task workers at. For each such
test the selected URL is published as ``AIRFLOW__API__BASE_URL`` so
application code can discover the endpoint through active Airflow
configuration. Under xdist each worker owns its own server process; they share
the database. Port selection stays uncoordinated across workers, but a worker
that loses the resulting bind race retries with a freshly probed port (see
``API_SERVER_BIND_RETRIES``) rather than silently sharing another worker's
server. This deliberately replaces the plan's controller-owned single server:
it is strictly simpler and costs nothing on sessions without API tests.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html
    https://airflow.apache.org/docs/apache-airflow/stable/security/api.html
"""

from __future__ import annotations

import json
import logging
import os
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

from pytest_airflow_in_a_box._compat import ensure_database
from pytest_airflow_in_a_box._compat.capabilities import require_v3
from pytest_airflow_in_a_box.bootstrap import BootstrapState, get_bootstrap_state
from pytest_airflow_in_a_box.config import airflow_config

LOGGER = logging.getLogger(__name__)

API_SERVER_STARTUP_TIMEOUT_SECONDS = 120.0
API_SERVER_SHUTDOWN_TIMEOUT_SECONDS = 15.0
API_SERVER_POLL_INTERVAL_SECONDS = 0.5
API_SERVER_BIND_RETRIES = 5
DEFAULT_USERNAME = "admin"
DEFAULT_PASSWORD = "admin"
LOG_TAIL_CHARACTERS = 4000
_BIND_CONFLICT_MARKERS = ("address already in use", "errno 98", "errno 48")


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


def _lost_port_bind_race(log_tail: str) -> bool:
    """Recognize a server exit caused by another process already owning the port.

    Parameters:
        log_tail: str containing the tail of the server's redirected log.

    Returns:
        bool indicating the log names an address-in-use bind failure.
    """

    lowered = log_tail.lower()
    return any(marker in lowered for marker in _BIND_CONFLICT_MARKERS)


def _launch_api_server(state: BootstrapState) -> Iterator[str]:
    """Start, hand over, and finally stop one isolated Airflow API server.

    ``_free_port`` only advises a port; nothing holds it reserved between the
    probe and the subprocess actually binding it, so two xdist workers can be
    handed the same port. When that happens the subprocess exits immediately
    with an address-in-use error, which this function retries against a
    freshly probed port, up to ``API_SERVER_BIND_RETRIES`` attempts.

    Parameters:
        state: BootstrapState providing the run's log directory.

    Yields:
        str containing the server's loopback base URL.

    Raises:
        ApiServerError: The server exited early or never became responsive.
    """

    attempt = 1
    while True:
        port = _free_port()
        base_url = f"http://127.0.0.1:{port}"
        # `state.logs_folder` is shared across every xdist worker (bootstrap.py
        # propagates one path via workerinput), so racing workers that are handed
        # the same advisory port must not also collide on the log filename.
        log_path = state.logs_folder / f"api-server-{port}-{os.getpid()}.log"
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
            # `core` alone leaves `/execution` unmounted, and the Task Execution API is
            # what a real executor's supervised workers talk to (`app.py` mounts the
            # sub-app only for `all` or `execution`). Both apps share one `create_dag_bag`
            # call inside the same process, so serving both costs no extra worker.
            "core,execution",
            "--workers",
            "1",
        ]
        LOGGER.info(f"Starting isolated Airflow API server on '{base_url}' (attempt {attempt})")
        with log_path.open("wb") as log_stream:
            process = subprocess.Popen(command, stdout=log_stream, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + API_SERVER_STARTUP_TIMEOUT_SECONDS
                lost_bind_race = False
                while True:
                    # Check our own process before trusting a response on this
                    # port: two workers can share one advisory port, and a dead
                    # process must never be mistaken for the worker that's
                    # actually listening, however briefly, before it exits.
                    exit_code = process.poll()
                    if exit_code is not None:
                        tail = _log_tail(log_path)
                        if _lost_port_bind_race(tail) and attempt < API_SERVER_BIND_RETRIES:
                            LOGGER.warning(
                                f"Port {port} lost a bind race to another worker on attempt "
                                f"{attempt}; retrying with a freshly probed port"
                            )
                            lost_bind_race = True
                            attempt += 1
                            break
                        raise ApiServerError(
                            f"Airflow API server exited with code {exit_code} before "
                            f"responding; log tail:\n{tail}"
                        )
                    if _server_responds(base_url):
                        break
                    if time.monotonic() > deadline:
                        raise ApiServerError(
                            f"Airflow API server did not respond within "
                            f"{API_SERVER_STARTUP_TIMEOUT_SECONDS:.0f}s; log tail:\n"
                            f"{_log_tail(log_path)}"
                        )
                    time.sleep(API_SERVER_POLL_INTERVAL_SECONDS)
                if lost_bind_race:
                    continue
                yield base_url
                return
            finally:
                process.terminate()
                try:
                    process.wait(timeout=API_SERVER_SHUTDOWN_TIMEOUT_SECONDS)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


@pytest.fixture(scope="session")
def api_server_url(pytestconfig: pytest.Config) -> Iterator[str]:
    """Start one isolated Airflow API server for this process's session.

    Yields:
        str containing the server's loopback base URL.

    Raises:
        ApiServerError: The server exited early or never became responsive.
    """

    require_v3(
        "api_server_url",
        "2.x has no `airflow api-server`; a FAB `airflow webserver` tier is "
        "demand-driven Phase 4.",
    )
    state = get_bootstrap_state(pytestconfig)
    ensure_database(state.root)
    yield from _launch_api_server(state)


@pytest.fixture(scope="session")
def api_client(api_server_url: str) -> AirflowApiClient:
    """Return an authenticated client bound to the isolated API server.

    Parameters:
        api_server_url: str containing the live server's base URL.

    Returns:
        AirflowApiClient carrying a SimpleAuthManager bearer token.
    """

    return AirflowApiClient(api_server_url, token=fetch_access_token(api_server_url))


@pytest.fixture(autouse=True)
def api_base_url(request: pytest.FixtureRequest) -> Iterator[str | None]:
    """Publish the live server's URL through Airflow configuration for one test.

    Activates for tests carrying the ``api_test`` marker and tests whose fixture
    closure contains ``api_client`` or ``api_server_url``: the session-scoped
    server starts lazily and its URL is exported as ``AIRFLOW__API__BASE_URL``,
    so application code discovers the endpoint through
    ``conf.get("api", "base_url")``. The environment is restored exactly after
    each test. Every other test sees ``None`` and starts nothing. Being autouse,
    this fixture appears in every closure, so requesting it explicitly does not
    activate the server by itself.

    Parameters:
        request: pytest.FixtureRequest exposing markers and the fixture closure.

    Yields:
        str | None containing the published base URL, or ``None`` when inactive.
    """

    active = request.node.get_closest_marker("api_test") is not None or not {
        "api_client",
        "api_server_url",
    }.isdisjoint(request.fixturenames)
    if not active:
        yield None
        return
    # The server subprocess launches before the override, so it never inherits it.
    base_url = request.getfixturevalue("api_server_url")
    with airflow_config({("api", "base_url"): base_url}):
        yield base_url


__all__ = (
    "AirflowApiClient",
    "ApiResponse",
    "ApiServerError",
    "api_base_url",
    "api_client",
    "api_server_url",
    "fetch_access_token",
)
