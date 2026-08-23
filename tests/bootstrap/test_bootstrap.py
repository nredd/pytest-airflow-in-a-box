"""Test early pytest and Airflow bootstrap lifecycle behavior."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pytest_airflow_in_a_box.airflow_cfg import sqlite_url
from pytest_airflow_in_a_box.bootstrap import STATE_ENVIRONMENT_VARIABLE
from pytest_airflow_in_a_box.storage import locate_storage


def test_serial_bootstrap_state_and_cleanup(pytester: pytest.Pytester) -> None:
    """Create complete serial state and remove its owned tree at unconfigure."""

    record_path = pytester.path / "serial-state.json"
    pytester.makepyfile(
        f"""
        import json
        import os
        import sqlite3
        from pathlib import Path

        import pytest

        from pytest_airflow_in_a_box.plugin import get_bootstrap_state

        @pytest.mark.db_test
        def test_state(pytestconfig):
            state = get_bootstrap_state(pytestconfig)
            assert state.root == Path(os.environ["AIRFLOW_HOME"])
            assert state.config_path == Path(os.environ["AIRFLOW_CONFIG"])
            assert state.dags_folder.is_dir()
            assert state.logs_folder.is_dir()
            assert state.plugins_folder.is_dir()
            assert list(state.plugins_folder.iterdir()) == []
            assert state.password_file.is_file()
            assert state.config_path.is_file()
            assert (state.root / "config" / "airflow_local_settings.py").is_file()
            assert state.database_path.is_file()
            with sqlite3.connect(state.database_path) as connection:
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
            Path({str(record_path)!r}).write_text(
                json.dumps(state.to_payload()), encoding="utf-8"
            )
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)
    payload = json.loads(record_path.read_text(encoding="utf-8"))
    assert not Path(payload["root"]).exists()


def test_airflow_plugins_folder_ini_symlinks_source_entries(pytester: pytest.Pytester) -> None:
    """Symlink a configured plugins source directory's entries into the run's `plugins/`."""

    source = pytester.path / "shared-plugins"
    source.mkdir()
    (source / "my_plugin.py").write_text("VALUE = 1\n", encoding="utf-8")
    pytester.makeini(f"[pytest]\nairflow_plugins_folder = {source}\n")
    pytester.makepyfile(
        """
        from pytest_airflow_in_a_box.plugin import get_bootstrap_state

        def test_plugins_are_linked(pytestconfig):
            state = get_bootstrap_state(pytestconfig)
            linked = state.plugins_folder / "my_plugin.py"
            assert linked.is_symlink()
            assert linked.read_text(encoding="utf-8") == "VALUE = 1\\n"
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_airflow_plugins_folder_ini_resolves_relative_to_the_rootdir(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Anchor a relative `airflow_plugins_folder` to the rootdir, not the invocation CWD.

    Regression test: without rootpath-anchoring, invoking pytest from a subdirectory of
    the project made a relative, genuinely-existing source directory look missing (a
    CWD-relative `is_dir()` check raised a usage error), while invoking from the rootdir
    itself created dangling symlinks instead (a CWD-relative source, but `symlink_to`
    resolves a relative target against the *link's own* directory, not the CWD) --
    silently dropping every plugin either way.

    Parameters:
        pytester: pytest.Pytester running the assertion in a real bootstrapped subprocess.
        monkeypatch: pytest.MonkeyPatch changing the invocation directory.
    """

    source = pytester.path / "shared-plugins"
    source.mkdir()
    (source / "my_plugin.py").write_text("VALUE = 1\n", encoding="utf-8")
    pytester.makeini("[pytest]\nairflow_plugins_folder = shared-plugins\n")
    pytester.makepyfile(
        """
        from pytest_airflow_in_a_box.plugin import get_bootstrap_state

        def test_plugins_are_linked(pytestconfig):
            state = get_bootstrap_state(pytestconfig)
            linked = state.plugins_folder / "my_plugin.py"
            assert linked.is_symlink()
            assert linked.read_text(encoding="utf-8") == "VALUE = 1\\n"
        """
    )
    subdir = pytester.path / "subdir"
    subdir.mkdir()
    monkeypatch.chdir(subdir)

    result = pytester.runpytest_subprocess("-q", str(pytester.path))

    result.assert_outcomes(passed=1)


def test_airflow_plugins_folder_ini_rejects_a_missing_source(pytester: pytest.Pytester) -> None:
    """Give an actionable error when the configured plugins source does not exist."""

    missing = pytester.path / "does-not-exist"
    pytester.makeini(f"[pytest]\nairflow_plugins_folder = {missing}\n")
    pytester.makepyfile("def test_never_runs():\n    assert False\n")

    result = pytester.runpytest_subprocess("-q")

    assert result.ret != 0
    result.stderr.fnmatch_lines(["*airflow_plugins_folder*must name a directory*"])


def test_airflow_environment_precedes_conftest_import(pytester: pytest.Pytester) -> None:
    """Install usable Airflow configuration before importing a consumer conftest."""

    marker_path = pytester.path / "conftest-imported"
    pytester.makeconftest(
        f"""
        import os
        from pathlib import Path

        assert Path(os.environ["AIRFLOW_HOME"]).is_dir()
        assert Path(os.environ["AIRFLOW_CONFIG"]).is_file()
        assert os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] == "True"
        import airflow
        Path({str(marker_path)!r}).write_text(airflow.__version__, encoding="utf-8")
        """
    )
    pytester.makepyfile("def test_ready():\n    assert True\n")

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)
    assert marker_path.read_text(encoding="utf-8")


def test_airflow_metadata_engine_receives_sqlite_pragmas(pytester: pytest.Pytester) -> None:
    """Load the generated override through Airflow and tune its actual metadata engine."""

    pytester.makepyfile(
        """
        from airflow import settings
        from pytest_airflow_in_a_box.storage import calculate_profile

        def test_metadata_engine():
            profile = calculate_profile()
            with settings.engine.connect() as connection:
                values = {
                    name: connection.exec_driver_sql(f"PRAGMA {name}").scalar_one()
                    for name in (
                        "journal_mode", "synchronous", "temp_store", "mmap_size",
                        "cache_size", "busy_timeout", "page_size", "locking_mode",
                    )
                }
            assert values == {
                "journal_mode": "wal",
                "synchronous": 0,
                "temp_store": 2,
                "mmap_size": profile.mmap_size,
                "cache_size": profile.cache_size,
                "busy_timeout": 30000,
                "page_size": 8192,
                "locking_mode": "normal",
            }
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)


def test_explicit_local_base_gets_unique_owned_child(pytester: pytest.Pytester) -> None:
    """Preserve an explicit user base while cleaning its unique run child."""

    explicit_base = pytester.path / "explicit-base"
    explicit_base.mkdir()
    record_path = pytester.path / "explicit-root"
    pytester.makepyfile(
        f"""
        from pathlib import Path
        from pytest_airflow_in_a_box.plugin import get_bootstrap_state

        def test_explicit(pytestconfig):
            state = get_bootstrap_state(pytestconfig)
            assert state.root.parent == Path({str(explicit_base)!r})
            assert state.storage_reason == "explicit"
            Path({str(record_path)!r}).write_text(str(state.root), encoding="utf-8")
        """
    )

    result = pytester.runpytest_subprocess("-q", "--airflow-home", str(explicit_base))

    result.assert_outcomes(passed=1)
    assert explicit_base.is_dir()
    assert not Path(record_path.read_text(encoding="utf-8")).exists()


def test_explicit_network_base_requires_opt_in(tmp_path: Path) -> None:
    """Reject a simulated network explicit base through the storage integration seam."""

    mounts = f"device {tmp_path} nfs4 rw 0 0"

    with pytest.raises(ValueError, match="allow_network=True"):
        locate_storage(tmp_path, mounts_text=mounts, platform_name="linux")


def test_malformed_inherited_state_fails_loudly(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject malformed state before a simulated local worker loads conftests."""

    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw0")
    monkeypatch.setenv(STATE_ENVIRONMENT_VARIABLE, "not-json")
    pytester.makepyfile("def test_never_runs():\n    assert False\n")

    result = pytester.runpytest_subprocess("-q")

    assert result.ret != 0
    result.stderr.fnmatch_lines(["*Invalid inherited Airflow bootstrap state*"])


def test_stale_inherited_state_fails_loudly(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject inherited state whose controller artifacts no longer exist."""

    missing = pytester.path / "missing-root"
    payload = {
        "version": 5,
        "owner_pid": 1,
        "root": str(missing),
        "dags_folder": str(missing / "dags"),
        "logs_folder": str(missing / "logs"),
        "database_path": str(missing / "airflow.db"),
        "password_file": str(missing / "passwords.json"),
        "config_path": str(missing / "airflow.cfg"),
        "plugins_folder": str(missing / "plugins"),
        "jwt_secret": "secret",
        "fernet_key": "fernet",
        "storage_reason": "system-temp",
        "network_storage": False,
        "sql_alchemy_conn": sqlite_url(missing / "airflow.db"),
        "db_backend": "sqlite",
        "family": "apache-airflow-core",
        "executor": "",
        "xcom_backend": "",
        "secrets_backend": "",
        "secrets_backend_kwargs": "",
    }
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw0")
    monkeypatch.setenv(STATE_ENVIRONMENT_VARIABLE, json.dumps(payload))
    pytester.makepyfile("def test_never_runs():\n    assert False\n")

    result = pytester.runpytest_subprocess("-q")

    assert result.ret != 0
    result.stderr.fnmatch_lines(["*Bootstrap state is stale*"])


def test_inherited_state_requires_derived_local_settings(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject inherited state missing its derivable local-settings artifact."""

    root = pytester.path / "inherited-root"
    (root / "dags").mkdir(parents=True)
    (root / "logs").mkdir()
    (root / "plugins").mkdir()
    (root / "passwords.json").write_text("{}", encoding="utf-8")
    (root / "airflow.cfg").write_text("", encoding="utf-8")
    payload = {
        "version": 5,
        "owner_pid": 1,
        "root": str(root),
        "dags_folder": str(root / "dags"),
        "logs_folder": str(root / "logs"),
        "database_path": str(root / "airflow.db"),
        "password_file": str(root / "passwords.json"),
        "config_path": str(root / "airflow.cfg"),
        "plugins_folder": str(root / "plugins"),
        "jwt_secret": "secret",
        "fernet_key": "fernet",
        "storage_reason": "system-temp",
        "network_storage": False,
        "sql_alchemy_conn": sqlite_url(root / "airflow.db"),
        "db_backend": "sqlite",
        "family": "apache-airflow-core",
        "executor": "",
        "xcom_backend": "",
        "secrets_backend": "",
        "secrets_backend_kwargs": "",
    }
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw0")
    monkeypatch.setenv(STATE_ENVIRONMENT_VARIABLE, json.dumps(payload))
    pytester.makepyfile("def test_never_runs():\n    assert False\n")

    result = pytester.runpytest_subprocess("-q")

    assert result.ret != 0
    result.stderr.fnmatch_lines(["*Bootstrap state is stale*"])


def test_preimported_airflow_is_rejected(pytester: pytest.Pytester) -> None:
    """Give an actionable error when another startup plugin imports Airflow first."""

    pytester.makepyfile(
        early_import="""
        import sys
        import types
        sys.modules["airflow"] = types.ModuleType("airflow")
        """,
        test_sample="def test_never_runs():\n    assert False\n",
    )

    result = pytester.runpytest_subprocess("-q", "-p", "early_import")

    assert result.ret != 0
    result.stderr.fnmatch_lines(["*Airflow was imported before pytest-airflow-in-a-box*"])


def test_two_xdist_workers_share_controller_state(pytester: pytest.Pytester) -> None:
    """Make local xdist workers use one controller-created root and secret."""

    record_dir = pytester.path / "worker-records"
    record_dir.mkdir()
    pytester.makepyfile(
        f"""
        import json
        import os
        import time
        from pathlib import Path

        import pytest
        from pytest_airflow_in_a_box.plugin import get_bootstrap_state

        @pytest.mark.parametrize("case", range(8))
        def test_shared(pytestconfig, case):
            del case
            state = get_bootstrap_state(pytestconfig)
            worker = os.environ["PYTEST_XDIST_WORKER"]
            Path({str(record_dir)!r}, worker).write_text(
                json.dumps({{"root": str(state.root), "secret": state.jwt_secret,
                            "owner_pid": state.owner_pid}}),
                encoding="utf-8",
            )
            time.sleep(0.05)
        """
    )

    result = pytester.runpytest_subprocess("-q", "-n2")

    result.assert_outcomes(passed=8)
    records = [json.loads(path.read_text(encoding="utf-8")) for path in record_dir.iterdir()]
    assert len(records) == 2
    assert len({record["root"] for record in records}) == 1
    assert len({record["secret"] for record in records}) == 1
    assert len({record["owner_pid"] for record in records}) == 1
    assert not Path(records[0]["root"]).exists()


_FOREIGN_MUTATOR_CONFTEST = """
import os

# A conftest-imported plugin that rewrites `AIRFLOW__*` at module scope, the shape
# `tests_common/pytest_plugin.py` has upstream. This runs after the plugin exported
# its bootstrap state, on the controller and again in every worker process.
os.environ["AIRFLOW__CORE__PLUGINS_FOLDER"] = os.path.join(os.getcwd(), "foreign-plugins")
"""


def test_foreign_env_mutation_aborts_a_worker_with_a_diagnosis(
    pytester: pytest.Pytester,
) -> None:
    """Explain the likely cause when a worker inherits a mutated Airflow environment.

    The reproduction from the issue: the controller survives because the mutation lands
    after its own bootstrap, but every worker inherits the result and trips the drift
    check before it collects anything.

    Parameters:
        pytester: pytest.Pytester driving a real nested session.

    References:
        https://github.com/nredd/pytest-airflow-in-a-box/issues/241
    """

    pytester.makeconftest(_FOREIGN_MUTATOR_CONFTEST)
    pytester.makepyfile("def test_needs_a_worker():\n    assert True\n")

    result = pytester.runpytest_subprocess("-q", "-p", "xdist", "-n", "2")

    assert result.ret != 0
    output = "\n".join(result.outlines + result.errlines)
    assert "disagrees with state: `AIRFLOW__CORE__PLUGINS_FOLDER`" in output
    assert "another pytest plugin or conftest likely mutated `AIRFLOW__*`" in output
    assert "airflow_worker_env_drift = repair" in output


def test_repair_policy_lets_a_worker_coexist_with_a_foreign_mutator(
    pytester: pytest.Pytester,
) -> None:
    """Keep the run alive under `repair`, and say so where the user will see it.

    Parameters:
        pytester: pytest.Pytester driving a real nested session.

    References:
        https://github.com/nredd/pytest-airflow-in-a-box/issues/241
    """

    pytester.makefile(".ini", pytest="[pytest]\nairflow_worker_env_drift = repair\n")
    pytester.makeconftest(_FOREIGN_MUTATOR_CONFTEST)
    pytester.makepyfile("def test_needs_a_worker():\n    assert True\n")

    result = pytester.runpytest_subprocess("-q", "-p", "xdist", "-n", "2")

    result.assert_outcomes(passed=1, warnings=2)
    output = "\n".join(result.outlines + result.errlines)
    assert "Re-installed 1 inherited Airflow variable(s)" in output
    assert "`AIRFLOW__CORE__PLUGINS_FOLDER`" in output
