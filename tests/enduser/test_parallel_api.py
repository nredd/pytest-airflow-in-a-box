"""Exercise per-worker lazy Airflow API servers under pytest-xdist.

The API server is per-worker and lazy by design (PLAN.md): each worker starts
its own ``airflow api-server`` on a loopback ephemeral port against the one
shared metadata database. Two nested files under ``--dist=loadfile`` force
exactly two servers, the most expensive scenario in this suite.

References:
    https://pytest-xdist.readthedocs.io/en/stable/distribution.html
    https://github.com/nredd/pytest-airflow-in-a-box/issues/45
"""

from __future__ import annotations

import json
import platform
import sys

import pytest

pytestmark = [pytest.mark.compat, pytest.mark.api_test, pytest.mark.db_test]

# Every nested run re-bootstraps Airflow and starts two API servers; macOS and
# musl CI hosts need roughly double the glibc-Linux budget.
NESTED_RUN_TIMEOUT_SECONDS = (
    300 if sys.platform == "linux" and platform.libc_ver()[0] == "glibc" else 600
)

_API_SUITE = """
    import json
    import os
    from pathlib import Path

    import pytest

    RECORD_DIR = Path({record_dir!r})

    pytestmark = pytest.mark.api_test


    def test_version_endpoint_responds(api_client):
        response = api_client.get("/api/v2/version")
        assert response.status == 200
        worker = os.environ["PYTEST_XDIST_WORKER"]
        record = {{"worker": worker, "api_server_url": api_client.base_url}}
        (RECORD_DIR / worker).write_text(json.dumps(record), encoding="utf-8")
"""


@pytest.mark.timeout(NESTED_RUN_TIMEOUT_SECONDS)
def test_each_worker_starts_its_own_api_server(pytester: pytest.Pytester) -> None:
    """Serve two workers from two lazy API servers on distinct loopback ports."""

    record_dir = pytester.path / "records"
    record_dir.mkdir()
    source = _API_SUITE.format(record_dir=str(record_dir))
    pytester.makepyfile(test_api_first=source, test_api_second=source)

    result = pytester.runpytest_subprocess("-q", "-n", "2", "--dist=loadfile")

    result.assert_outcomes(passed=2)
    records = [
        json.loads(path.read_text(encoding="utf-8")) for path in sorted(record_dir.iterdir())
    ]
    assert len(records) == 2
    assert len({record["worker"] for record in records}) == 2
    assert len({record["api_server_url"] for record in records}) == 2
