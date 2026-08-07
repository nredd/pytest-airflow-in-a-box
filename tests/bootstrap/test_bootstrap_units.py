"""Unit tests for bootstrap validation and error paths."""

from __future__ import annotations

import json
import os
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box import bootstrap
from pytest_airflow_in_a_box.bootstrap import (
    STATE_ENVIRONMENT_VARIABLE,
    STATE_KEY,
    WORKER_INPUT_KEY,
    BootstrapState,
    configure_node,
    get_bootstrap_state,
    validate_configure,
)


def _artifact_state(root: Path, *, owner_pid: int = 1234) -> BootstrapState:
    """Create a state whose artifacts genuinely exist below one root.

    Parameters:
        root: pathlib.Path receiving the run artifact layout.
        owner_pid: int identifying the pretend controller process.

    Returns:
        BootstrapState whose file-existence validation passes.
    """

    (root / "dags").mkdir(parents=True)
    (root / "logs").mkdir()
    (root / "config").mkdir()
    (root / "simple_auth_manager_passwords.json").write_text("{}\n", encoding="utf-8")
    (root / "airflow.cfg").write_text("", encoding="utf-8")
    (root / "config" / "airflow_local_settings.py").write_text("", encoding="utf-8")
    return BootstrapState(
        version=bootstrap.STATE_VERSION,
        owner_pid=owner_pid,
        root=root,
        dags_folder=root / "dags",
        logs_folder=root / "logs",
        database_path=root / "airflow.db",
        password_file=root / "simple_auth_manager_passwords.json",
        config_path=root / "airflow.cfg",
        jwt_secret="secret",
        storage_reason="explicit",
        network_storage=False,
    )


@pytest.mark.parametrize(
    ("payload", "match"),
    [
        ({"key": 7}, "`key` must be a non-empty string"),
        ({"key": ""}, "`key` must be a non-empty string"),
        ({}, "`key` must be a non-empty string"),
    ],
)
def test_require_string_rejects_bad_values(payload: dict[str, object], match: str) -> None:
    """Reject absent, empty, and non-string fields."""

    with pytest.raises(ValueError, match=match):
        bootstrap._require_string(payload, "key")


@pytest.mark.parametrize("payload", [{"key": "7"}, {"key": True}, {}])
def test_require_int_rejects_bad_values(payload: dict[str, object]) -> None:
    """Reject absent, boolean, and non-integer fields."""

    with pytest.raises(ValueError, match="`key` must be an integer"):
        bootstrap._require_int(payload, "key")


@pytest.mark.parametrize("payload", [{"key": 1}, {"key": "true"}, {}])
def test_require_bool_rejects_bad_values(payload: dict[str, object]) -> None:
    """Reject absent and non-boolean fields."""

    with pytest.raises(ValueError, match="`key` must be a boolean"):
        bootstrap._require_bool(payload, "key")


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (lambda _payload: "not a dict", "must be a JSON object with string keys"),
        (lambda payload: {**payload, 7: "x"}, "must be a JSON object with string keys"),
        (
            lambda payload: {k: v for k, v in payload.items() if k != "root"},
            "missing or unexpected",
        ),
        (lambda payload: {**payload, "extra": 1}, "missing or unexpected"),
        (lambda payload: {**payload, "version": 99}, "Unsupported bootstrap state version"),
        (
            lambda payload: {**payload, "dags_folder": "relative/dags"},
            "paths must be absolute",
        ),
        (
            lambda payload: {**payload, "dags_folder": str(Path(payload["root"]).parent / "dags")},
            "direct children of the run root",
        ),
    ],
)
def test_state_from_payload_rejects_malformed_payloads(
    tmp_path: Path,
    mutate: Any,
    match: str,
) -> None:
    """Reject structurally invalid payload shapes with named reasons."""

    payload = _artifact_state(tmp_path / "run").to_payload()

    with pytest.raises(ValueError, match=match):
        bootstrap._state_from_payload(mutate(dict(payload)), validate_files=False)


def test_state_from_environment_requires_the_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse to run a worker that inherited no bootstrap state."""

    monkeypatch.delenv(STATE_ENVIRONMENT_VARIABLE, raising=False)

    with pytest.raises(pytest.UsageError, match="did not inherit bootstrap state"):
        bootstrap._state_from_environment()


def test_argument_value_requires_a_following_value() -> None:
    """Reject a trailing option with no value argument."""

    with pytest.raises(pytest.UsageError, match="`--airflow-home` requires a value"):
        bootstrap._argument_value(["--airflow-home"], "--airflow-home")


def test_allow_network_rejects_non_boolean_ini() -> None:
    """Reject a malformed `allow_network_airflow_home` ini value."""

    config: Any = SimpleNamespace(getini=lambda _name: "yes")

    with pytest.raises(pytest.UsageError, match="must be a boolean"):
        bootstrap._allow_network(config, [])


def test_owner_state_wraps_storage_selection_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Wrap storage-selection failures in one actionable usage error."""

    def explode(**_kwargs: Any) -> Any:
        raise ValueError("no storage for you")

    monkeypatch.setattr(bootstrap, "locate_storage", explode)
    ini_values = {"airflow_home": "", "allow_network_airflow_home": False}
    config: Any = SimpleNamespace(
        getini=lambda name: ini_values[name], add_cleanup=lambda _callback: None
    )

    with pytest.raises(pytest.UsageError, match="Could not create isolated Airflow storage"):
        bootstrap._owner_state(config, [])


def test_owner_state_cleans_up_after_provisioning_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Remove the partial run directory and restore environment on failure."""

    base = tmp_path / "base"
    base.mkdir()
    cleanups: list[Any] = []

    def fail_write(*_args: Any, **_kwargs: Any) -> None:
        raise ValueError("config write failed")

    monkeypatch.setattr(bootstrap, "write_airflow_config", fail_write)
    monkeypatch.setenv("AIRFLOW_HOME", "pre-existing-value")
    config: Any = SimpleNamespace(
        getini=lambda name: {"airflow_home": str(base), "allow_network_airflow_home": False}[name],
        add_cleanup=cleanups.append,
    )

    with pytest.raises(pytest.UsageError, match="Could not provision isolated Airflow storage"):
        bootstrap._owner_state(config, [])

    assert list(base.iterdir()) == []
    assert os.environ["AIRFLOW_HOME"] == "pre-existing-value"
    assert len(cleanups) == 1
    cleanups[0]()


def test_worker_environment_mismatch_fails_loudly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Name every mismatched variable when the worker environment disagrees."""

    state = _artifact_state(tmp_path / "run")
    monkeypatch.setenv(bootstrap.XDIST_WORKER_ENVIRONMENT_VARIABLE, "gw0")
    monkeypatch.setenv(
        STATE_ENVIRONMENT_VARIABLE,
        json.dumps(state.to_payload(), sort_keys=True, separators=(",", ":")),
    )
    for name, value in bootstrap._environment(state).items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("AIRFLOW_HOME", "/somewhere/else")
    # The early-import guard fires first in-process; the worker path under
    # test begins after it.
    monkeypatch.delitem(sys.modules, "airflow", raising=False)

    config: Any = SimpleNamespace()

    with pytest.raises(pytest.UsageError, match="disagrees with state: `AIRFLOW_HOME`"):
        bootstrap.load_initial_state(config, [])


def test_get_bootstrap_state_requires_stashed_state() -> None:
    """Report unavailable bootstrap state as a usage error."""

    config: Any = SimpleNamespace(stash=pytest.Stash())

    with pytest.raises(pytest.UsageError, match="bootstrap state is unavailable"):
        get_bootstrap_state(config)


def test_configure_node_rejects_remote_gateways(tmp_path: Path) -> None:
    """Refuse to send state to a non-popen execnet gateway."""

    state = _artifact_state(tmp_path / "run")
    stash = pytest.Stash()
    stash[STATE_KEY] = state
    node: Any = SimpleNamespace(
        gateway=SimpleNamespace(spec=SimpleNamespace(popen=None)),
        config=SimpleNamespace(stash=stash),
        workerinput={},
    )

    with pytest.raises(pytest.UsageError, match="Remote xdist gateways are unsupported"):
        configure_node(node)


def test_validate_configure_rejects_malformed_workerinput(tmp_path: Path) -> None:
    """Wrap workerinput payload validation failures with context."""

    state = _artifact_state(tmp_path / "run")
    stash = pytest.Stash()
    stash[STATE_KEY] = state
    config: Any = SimpleNamespace(stash=stash, workerinput={WORKER_INPUT_KEY: {"nope": 1}})

    with pytest.raises(pytest.UsageError, match="Invalid controller bootstrap state"):
        validate_configure(config)


def test_validate_configure_rejects_state_disagreement(tmp_path: Path) -> None:
    """Reject workerinput state that differs from the inherited state."""

    state = _artifact_state(tmp_path / "run")
    other = _artifact_state(tmp_path / "other-run", owner_pid=4321)
    stash = pytest.Stash()
    stash[STATE_KEY] = state
    config: Any = SimpleNamespace(
        stash=stash,
        workerinput={WORKER_INPUT_KEY: other.to_payload()},
    )

    with pytest.raises(pytest.UsageError, match="differs from inherited state"):
        validate_configure(config)


def test_allow_network_honors_the_command_line_flag() -> None:
    """Accept the explicit command-line opt-in without reading ini state."""

    config: Any = SimpleNamespace()

    assert bootstrap._allow_network(config, ["--allow-network-airflow-home"]) is True


def test_state_from_payload_skips_file_validation_when_asked(tmp_path: Path) -> None:
    """Rebuild state from a payload without touching the filesystem."""

    state = _artifact_state(tmp_path / "run")
    shutil.rmtree(tmp_path / "run")

    rebuilt = bootstrap._state_from_payload(state.to_payload(), validate_files=False)

    assert rebuilt == state
