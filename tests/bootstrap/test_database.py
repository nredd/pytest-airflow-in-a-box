"""Test controller-owned Airflow metadata database initialization ordering."""

from __future__ import annotations

import pytest

from pytest_airflow_in_a_box import plugin
from pytest_airflow_in_a_box.plugin import get_bootstrap_state


@pytest.fixture(autouse=True)
def _isolate_nested_xdist_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Prevent an outer worker identity from leaking into nested pytest processes."""

    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    monkeypatch.delenv("PYTEST_AIRFLOW_IN_A_BOX_BOOTSTRAP_STATE", raising=False)


def test_serial_collection_observes_initialized_database(pytester: pytest.Pytester) -> None:
    """Finish metadata initialization before serial consumer module collection."""

    pytester.makepyfile(
        """
        import os
        import sqlite3
        from pathlib import Path

        database_path = Path(os.environ["AIRFLOW_HOME"]) / "airflow.db"
        assert database_path.is_file()
        with sqlite3.connect(database_path) as connection:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
            revision = connection.execute(
                "SELECT version_num FROM alembic_version"
            ).fetchone()
        assert "dag" in tables
        assert revision is not None and revision[0]

        def test_database_ready_during_collection():
            assert True
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_xdist_workers_inherit_one_initialized_database(pytester: pytest.Pytester) -> None:
    """Initialize once before two real workers import their consumer conftest."""

    marker_path = pytester.path / "initdb-processes"
    worker_dir = pytester.path / "workers"
    worker_dir.mkdir()
    pytester.makeconftest(
        f"""
        import os
        import sqlite3
        from pathlib import Path

        from airflow.utils import db as airflow_db

        marker_path = Path({str(marker_path)!r})
        original_initdb = airflow_db.initdb

        def recording_initdb():
            original_initdb()
            with marker_path.open("a", encoding="ascii") as marker_file:
                marker_file.write(f"{{os.getpid()}}\\n")

        airflow_db.initdb = recording_initdb

        worker = os.environ.get("PYTEST_XDIST_WORKER")
        if worker is not None:
            database_path = Path(os.environ["AIRFLOW_HOME"]) / "airflow.db"
            assert database_path.is_file()
            with sqlite3.connect(database_path) as connection:
                tables = {{
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }}
                revision = connection.execute(
                    "SELECT version_num FROM alembic_version"
                ).fetchone()
            assert "dag" in tables
            assert revision is not None and revision[0]
            Path({str(worker_dir)!r}, worker).write_text("ready", encoding="ascii")
        """
    )
    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.parametrize("case", range(8))
        def test_database_ready(case):
            assert case >= 0
        """
    )

    result = pytester.runpytest_subprocess("-q", "-n2")

    result.assert_outcomes(passed=8)
    assert len(marker_path.read_text(encoding="ascii").splitlines()) == 1
    assert {path.name for path in worker_dir.iterdir()} == {"gw0", "gw1"}


def test_non_owner_worker_skips_initialization(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Skip the initialization seam when current PID differs from the owner."""

    state = get_bootstrap_state(request.config)

    def unexpected_initialization() -> None:
        """Fail if the worker branch reaches database initialization."""

        raise AssertionError("Worker attempted database initialization")

    monkeypatch.setattr(plugin.os, "getpid", lambda: state.owner_pid + 1)
    monkeypatch.setattr(plugin, "initialize_database", unexpected_initialization)

    plugin.pytest_sessionstart(request.session)
