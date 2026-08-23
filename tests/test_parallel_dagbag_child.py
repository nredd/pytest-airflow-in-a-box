"""Test the Dag bag fan-out child script's own error handling directly.

Its success path is exercised end to end (as a real subprocess) by
`tests/test_dag_bag_fanout.py`; these tests cover the two failure branches that never
reach that far: missing environment configuration, and an exception raised while
building the shard's payload.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pytest_airflow_in_a_box import parallel_dagbag_child
from pytest_airflow_in_a_box.parallel_dagbag import (
    SHARD_OUTPUT_PATH_ENVIRONMENT_VARIABLE,
    SHARD_SPEC_PATH_ENVIRONMENT_VARIABLE,
)


def test_main_fails_without_required_environment_variables(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exit with failure and name the missing variables on stderr.

    Written directly to `sys.stderr`, not logged: `_log_tail` in `parallel_dagbag.py`
    reads exactly this process's redirected stdout/stderr as its one diagnostic channel
    on failure, and this runs before any Airflow import could configure logging.
    """

    monkeypatch.delenv(SHARD_SPEC_PATH_ENVIRONMENT_VARIABLE, raising=False)
    monkeypatch.delenv(SHARD_OUTPUT_PATH_ENVIRONMENT_VARIABLE, raising=False)

    assert parallel_dagbag_child.main() == parallel_dagbag_child._EXIT_FAILURE
    assert SHARD_SPEC_PATH_ENVIRONMENT_VARIABLE in capsys.readouterr().err


def test_main_fails_when_shard_processing_raises(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exit with failure and write the exception to stderr when building the payload raises."""

    output_path = tmp_path / "output.json"
    spec_path = tmp_path / "spec.json"
    spec_path.write_text("not valid json", encoding="utf-8")
    monkeypatch.setenv(SHARD_SPEC_PATH_ENVIRONMENT_VARIABLE, str(spec_path))
    monkeypatch.setenv(SHARD_OUTPUT_PATH_ENVIRONMENT_VARIABLE, str(output_path))

    assert parallel_dagbag_child.main() == parallel_dagbag_child._EXIT_FAILURE
    assert not output_path.exists()
    assert "Dag bag fan-out shard failed" in capsys.readouterr().err
