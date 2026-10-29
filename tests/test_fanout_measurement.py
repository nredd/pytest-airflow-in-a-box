"""Preserve parent-process measurement when portable corpus fan-out is requested.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/370
"""

from __future__ import annotations

import json
import sys
from types import ModuleType, SimpleNamespace

import pytest

from pytest_airflow_in_a_box import parallel_dagbag


@pytest.mark.parametrize("active", [False, True])
def test_trace_callback_detection(active: bool, monkeypatch: pytest.MonkeyPatch) -> None:
    """Recognize a testmon-style trace callback without requiring coverage to be imported.

    Parameters:
        active: bool indicating whether a trace callback is installed.
        monkeypatch: pytest.MonkeyPatch replacing the interpreter probe.
    """

    monkeypatch.setattr(sys, "gettrace", lambda: object() if active else None)
    monkeypatch.delitem(sys.modules, "coverage", raising=False)

    assert parallel_dagbag._measurement_active() is active


@pytest.mark.parametrize("active", [False, True])
def test_monitoring_coverage_detection(active: bool, monkeypatch: pytest.MonkeyPatch) -> None:
    """Recognize coverage even when its monitoring backend installs no trace callback.

    Parameters:
        active: bool indicating whether coverage has a current instance.
        monkeypatch: pytest.MonkeyPatch replacing optional coverage and interpreter probes.
    """

    coverage = ModuleType("coverage")
    monkeypatch.setattr(
        coverage,
        "Coverage",
        SimpleNamespace(current=lambda: object() if active else None),
        raising=False,
    )
    monkeypatch.setitem(sys.modules, "coverage", coverage)
    monkeypatch.setattr(sys, "gettrace", lambda: None)

    assert parallel_dagbag._measurement_active() is active


@pytest.mark.parametrize("smoke", [False, True], ids=["dag-corpus", "smoke-catalog"])
def test_fanout_requested_dags_are_measured(
    pytester: pytest.Pytester, smoke: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Measure real Dag imports under pytest-cov despite an explicit fan-out request.

    Parameters:
        pytester: pytest.Pytester creating an isolated consumer project.
        smoke: bool selecting the smoke catalog instead of a fixture consumer.
        monkeypatch: pytest.MonkeyPatch isolating nested coverage data from the outer gate.
    """

    # pytest-cov erases its data files before starting. Never let a nested run inherit
    # the development gate's COVERAGE_FILE and erase earlier subprocess measurements.
    monkeypatch.setenv("COVERAGE_FILE", str(pytester.path / ".coverage"))
    dags = pytester.path / "dags"
    dags.mkdir()
    for index in range(2):
        (dags / f"dag_{index}.py").write_text(
            "from airflow.sdk import DAG\n"
            f"dag = DAG(dag_id='measured_{index}', schedule=None, catchup=False)\n",
            encoding="utf-8",
        )
    pytester.makeini(
        "[pytest]\nairflow_dags_folder = dags\nairflow_dag_bag_fanout = true\n"
        "airflow_dag_bag_fanout_min_files = 1\nairflow_dag_bag_fanout_workers = 2\n"
        "airflow_parse_secrets = off\n"
    )
    if not smoke:
        pytester.makepyfile(
            "def test_imports(dag_corpus):\n    assert not dag_corpus.import_errors\n"
        )
    cov_config = pytester.path / ".coveragerc"
    cov_config.write_text("[run]\nsource = dags\n", encoding="utf-8")
    result = pytester.runpytest_subprocess(
        "-q",
        "--cov=dags",
        "--log-cli-level=WARNING",
        f"--cov-config={cov_config}",
        "--cov-report=json:dag-coverage.json",
        *(["--airflow-smoke", "-m", "smoke"] if smoke else []),
        timeout=120,
    )

    assert result.ret == 0
    report = json.loads((pytester.path / "dag-coverage.json").read_text(encoding="utf-8"))
    for index in range(2):
        measured = report["files"][f"dags/dag_{index}.py"]
        assert measured["executed_lines"] == [1, 2]
        assert measured["missing_lines"] == []
    assert "Dag bag fan-out disabled while tracing is active" in result.stdout.str()
