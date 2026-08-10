"""Test marker-driven API server activation and base-URL publication.

The nested suites stub `api_server_url`/`api_client` so no real server starts;
a sentinel file records whether the stub was ever instantiated.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/57
"""

from __future__ import annotations

import pytest

_STUB_CONFTEST = """
    from pathlib import Path
    from types import SimpleNamespace

    import pytest

    SENTINEL = Path({sentinel!r})


    @pytest.fixture(scope="session")
    def api_server_url():
        SENTINEL.write_text("started", encoding="utf-8")
        return "http://127.0.0.1:9"


    @pytest.fixture(scope="session")
    def api_client(api_server_url):
        return SimpleNamespace(base_url=api_server_url)
"""


def test_marker_and_fixture_requests_publish_the_url(pytester: pytest.Pytester) -> None:
    """Publish `AIRFLOW__API__BASE_URL` for marked and fixture-requesting tests."""

    sentinel = pytester.path / "server-started"
    pytester.makeconftest(_STUB_CONFTEST.format(sentinel=str(sentinel)))
    pytester.makepyfile(
        """
        import os

        import pytest


        @pytest.mark.api_test
        def test_marker_alone_publishes():
            assert os.environ["AIRFLOW__API__BASE_URL"] == "http://127.0.0.1:9"


        def test_explicit_server_url_publishes(api_server_url):
            assert os.environ["AIRFLOW__API__BASE_URL"] == api_server_url


        def test_explicit_client_publishes(api_client):
            assert os.environ["AIRFLOW__API__BASE_URL"] == api_client.base_url


        def test_publication_is_restored_after_marked_tests():
            assert "AIRFLOW__API__BASE_URL" not in os.environ
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=4)
    assert sentinel.exists()


def test_unmarked_suite_never_starts_the_server(pytester: pytest.Pytester) -> None:
    """Start nothing and publish nothing for a suite without marker or fixtures."""

    sentinel = pytester.path / "server-started"
    pytester.makeconftest(_STUB_CONFTEST.format(sentinel=str(sentinel)))
    pytester.makepyfile(
        """
        import os


        def test_no_publication_without_marker_or_fixture():
            assert "AIRFLOW__API__BASE_URL" not in os.environ
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)
    assert not sentinel.exists()
