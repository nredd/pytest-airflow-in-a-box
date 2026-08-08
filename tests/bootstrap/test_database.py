"""Test lazy Airflow metadata database initialization ordering."""

from __future__ import annotations

import pytest


def test_plugin_is_inert_without_airflow_tests(pytester: pytest.Pytester) -> None:
    """Never import Airflow or create the metadata database on a non-Airflow run."""

    pytester.makepyfile(
        """
        import os
        import sys
        from pathlib import Path

        from pytest_airflow_in_a_box._compat.database import (
            DATABASE_READY_SENTINEL_NAME,
        )

        def test_plain_suite_stays_airflow_free():
            assert "airflow" not in sys.modules
            root = Path(os.environ["AIRFLOW_HOME"])
            assert not (root / "airflow.db").exists()
            assert not (root / DATABASE_READY_SENTINEL_NAME).exists()
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_serial_database_initializes_lazily_before_first_db_test(
    pytester: pytest.Pytester,
) -> None:
    """Leave the database absent through collection and migrate it for the first test."""

    pytester.makepyfile(
        """
        import os
        import sqlite3
        from pathlib import Path

        import pytest

        database_path = Path(os.environ["AIRFLOW_HOME"]) / "airflow.db"
        assert not database_path.exists()

        @pytest.mark.db_test
        def test_database_ready_when_marked_test_runs():
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
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_xdist_workers_race_to_one_initialization(pytester: pytest.Pytester) -> None:
    """Initialize exactly once while racing workers all observe a migrated database."""

    marker_path = pytester.path / "initdb-processes"
    worker_dir = pytester.path / "workers"
    worker_dir.mkdir()
    pytester.makeconftest(
        f"""
        import os
        from pathlib import Path

        from airflow.utils import db as airflow_db

        marker_path = Path({str(marker_path)!r})
        original_initdb = airflow_db.initdb

        def recording_initdb():
            original_initdb()
            with marker_path.open("a", encoding="ascii") as marker_file:
                marker_file.write(f"{{os.getpid()}}\\n")

        airflow_db.initdb = recording_initdb
        """
    )
    pytester.makepyfile(
        f"""
        import os
        import sqlite3
        from pathlib import Path

        import pytest

        worker_dir = Path({str(worker_dir)!r})

        @pytest.mark.db_test
        @pytest.mark.parametrize("case", range(8))
        def test_database_ready(case):
            database_path = Path(os.environ["AIRFLOW_HOME"]) / "airflow.db"
            assert database_path.is_file()
            with sqlite3.connect(database_path) as connection:
                revision = connection.execute(
                    "SELECT version_num FROM alembic_version"
                ).fetchone()
            assert revision is not None and revision[0]
            worker = os.environ["PYTEST_XDIST_WORKER"]
            (worker_dir / worker).write_text("ready", encoding="ascii")
        """
    )

    result = pytester.runpytest_subprocess("-q", "-n2")

    result.assert_outcomes(passed=8)
    assert len(marker_path.read_text(encoding="ascii").splitlines()) == 1
    assert {path.name for path in worker_dir.iterdir()} == {"gw0", "gw1"}


def test_fixture_closure_triggers_initialization(pytester: pytest.Pytester) -> None:
    """Initialize for an unmarked test whose user fixture wraps a plugin fixture."""

    pytester.makeconftest(
        """
        import pytest

        @pytest.fixture
        def wrapped_session(session):
            return session
        """
    )
    pytester.makepyfile(
        """
        from sqlalchemy import text

        def test_wrapped_session_reaches_migrated_database(wrapped_session):
            revision = wrapped_session.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar()
            assert revision
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_dynamic_fixture_request_triggers_initialization(pytester: pytest.Pytester) -> None:
    """Initialize inside the fixture when the setup hook cannot see a static closure."""

    pytester.makepyfile(
        """
        import os
        from pathlib import Path

        from sqlalchemy import text

        def test_dynamic_session_request(request):
            database_path = Path(os.environ["AIRFLOW_HOME"]) / "airflow.db"
            assert not database_path.exists()
            session = request.getfixturevalue("session")
            revision = session.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar()
            assert revision
            assert database_path.is_file()
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_environment_gate_skips_before_initialization(pytester: pytest.Pytester) -> None:
    """Skip an environment-gated database test without paying initialization."""

    marker_path = pytester.path / "initdb-processes"
    pytester.makeconftest(
        f"""
        import os
        from pathlib import Path

        from airflow.utils import db as airflow_db

        marker_path = Path({str(marker_path)!r})
        original_initdb = airflow_db.initdb

        def recording_initdb():
            original_initdb()
            with marker_path.open("a", encoding="ascii") as marker_file:
                marker_file.write(f"{{os.getpid()}}\\n")

        airflow_db.initdb = recording_initdb
        """
    )
    pytester.makeini(
        """
        [pytest]
        airflow_environments =
            staging = missing-environment-sentinel
        """
    )
    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.db_test
        @pytest.mark.environment("staging")
        def test_gated_database_test():
            raise AssertionError("The environment gate must skip this test")
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(skipped=1)
    assert not marker_path.exists()


def test_unknown_environment_still_initializes_and_fails_at_setup(
    pytester: pytest.Pytester,
) -> None:
    """Initialize despite an unknown environment marker and fail the item at setup."""

    marker_path = pytester.path / "initdb-processes"
    pytester.makeconftest(
        f"""
        import os
        from pathlib import Path

        from airflow.utils import db as airflow_db

        marker_path = Path({str(marker_path)!r})
        original_initdb = airflow_db.initdb

        def recording_initdb():
            original_initdb()
            with marker_path.open("a", encoding="ascii") as marker_file:
                marker_file.write(f"{{os.getpid()}}\\n")

        airflow_db.initdb = recording_initdb
        """
    )
    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.db_test
        @pytest.mark.environment("unknown")
        def test_unknown_environment():
            raise AssertionError("The unknown environment must fail this test")
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(errors=1)
    result.stdout.fnmatch_lines(["*Unknown test environment `unknown`*"])
    assert len(marker_path.read_text(encoding="ascii").splitlines()) == 1


def test_blocked_plugin_leaves_pytest_untouched(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep pytest pristine when the plugin is blocked with `-p no:`."""

    monkeypatch.delenv("AIRFLOW_HOME", raising=False)
    pytester.makepyfile(
        """
        import os
        import sys

        def test_plugin_is_absent():
            assert "pytest_airflow_in_a_box.plugin" not in sys.modules
            assert "PYTEST_AIRFLOW_IN_A_BOX_BOOTSTRAP_STATE" not in os.environ
            assert "AIRFLOW_HOME" not in os.environ
        """
    )

    result = pytester.runpytest_subprocess("-p", "no:pytest_airflow_in_a_box", "-q")

    result.assert_outcomes(passed=1)
