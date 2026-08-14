"""Test `--airflow-record` outcome accumulation, derivation, and artifact writing."""

from __future__ import annotations

import json
from importlib import metadata
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box import record
from pytest_airflow_in_a_box._compat.capabilities import AirflowFamily
from pytest_airflow_in_a_box.artifact import UNKNOWN, Outcome, OutcomeEntry


class _FakeReport:
    """Minimal `pytest.TestReport` double for accumulator unit tests.

    `wasxfail` is set as a real instance attribute only when requested, so
    `hasattr(report, "wasxfail")` reflects presence exactly as it does on a real
    xfail-processed report.
    """

    def __init__(
        self,
        *,
        nodeid: str = "tests/x.py::test_a",
        when: str = "setup",
        outcome: str = "passed",
        duration: float = 1.0,
        user_properties: list[tuple[str, object]] | None = None,
        wasxfail: str | None = None,
    ) -> None:
        self.nodeid = nodeid
        self.when = when
        self.outcome = outcome
        self.duration = duration
        self.user_properties = user_properties if user_properties is not None else []
        if wasxfail is not None:
            self.wasxfail = wasxfail

    @property
    def failed(self) -> bool:
        """Report failure the way `pytest.TestReport.failed` would."""

        return self.outcome == "failed"

    @property
    def skipped(self) -> bool:
        """Report skip the way `pytest.TestReport.skipped` would."""

        return self.outcome == "skipped"


def _report(
    *,
    nodeid: str = "tests/x.py::test_a",
    when: str = "setup",
    outcome: str = "passed",
    duration: float = 1.0,
    user_properties: list[tuple[str, object]] | None = None,
    wasxfail: str | None = None,
) -> Any:
    """Build a `_FakeReport` typed as `Any`, matching every `pytest.TestReport` call site.

    `ty` checks `record` module functions against the real `pytest.TestReport` class
    nominally, so a duck-typed double must cross that boundary as `Any`, the same
    escape hatch `_fake_config` uses for `pytest.Config`.

    Parameters:
        nodeid: str containing the fake report's nodeid.
        when: str containing the fake phase (`setup`/`call`/`teardown`).
        outcome: str containing the fake `report.outcome`.
        duration: float containing the fake phase duration.
        user_properties: list[tuple[str, object]] | None containing stashed properties.
        wasxfail: str | None setting `report.wasxfail` only when not `None`.

    Returns:
        Any containing a `_FakeReport` instance.
    """

    return _FakeReport(
        nodeid=nodeid,
        when=when,
        outcome=outcome,
        duration=duration,
        user_properties=user_properties,
        wasxfail=wasxfail,
    )


def _fake_config(
    *,
    airflow_record: str | None = None,
    airflow_baseline: str | None = None,
    airflow_baseline_select: str | None = None,
    airflow_baseline_xfail: str | None = None,
    workerinput: dict[str, object] | None = None,
) -> Any:
    """Create a minimal configuration double for `record` module unit tests.

    Parameters:
        airflow_record: str | None containing the `--airflow-record` option value.
        airflow_baseline: str | None containing the `--airflow-baseline` option value.
        airflow_baseline_select: str | None containing the select-mode option value.
        airflow_baseline_xfail: str | None containing the baseline-xfail option value.
        workerinput: dict[str, object] | None marking the double as an xdist worker.

    Returns:
        types.SimpleNamespace shaped like the configuration surface under test.
    """

    config = SimpleNamespace(
        option=SimpleNamespace(
            airflow_record=airflow_record,
            airflow_baseline=airflow_baseline,
            airflow_baseline_select=airflow_baseline_select,
            airflow_baseline_xfail=airflow_baseline_xfail,
        ),
        stash=pytest.Stash(),
    )
    if workerinput is not None:
        config.workerinput = workerinput
    return config


@pytest.fixture(autouse=True)
def _isolate_active_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """Snapshot and restore the module-global active config around each test.

    `record._ACTIVE_CONFIG` is process-global and the real plugin session running this
    test suite has already set it; tests that call `configure`/`unconfigure` or read
    `_ACTIVE_CONFIG` must not leak a fake config into the rest of the suite.
    """

    monkeypatch.setattr(record, "_ACTIVE_CONFIG", record._ACTIVE_CONFIG)


@pytest.mark.parametrize(
    ("record_value", "baseline_value", "select_value", "xfail_value", "expected"),
    [
        (None, None, None, None, False),
        ("out.json", None, None, None, True),
        (None, "base.json", None, None, True),
        (None, None, "passing", None, True),
        (None, None, None, "prior.json", True),
    ],
)
def test_is_recording_active(
    record_value: str | None,
    baseline_value: str | None,
    select_value: str | None,
    xfail_value: str | None,
    expected: bool,
) -> None:
    """Report activity whenever any migration-diff option is supplied."""

    config = _fake_config(
        airflow_record=record_value,
        airflow_baseline=baseline_value,
        airflow_baseline_select=select_value,
        airflow_baseline_xfail=xfail_value,
    )

    assert record._is_recording_active(config) is expected


def test_configure_installs_stash_and_remembers_config() -> None:
    """Install the accumulator stash and set the module-level active config."""

    config = _fake_config(airflow_record="out.json")

    record.configure(config)

    assert config.stash[record._ACTIVE_KEY] is True
    assert config.stash[record._ACCUMULATOR_KEY] == {}
    assert config.stash[record._OUTCOMES_KEY] == {}
    assert record._ACTIVE_CONFIG is config


def test_unconfigure_forgets_only_the_matching_config() -> None:
    """Clear the active config only when it matches the one being unconfigured."""

    first = _fake_config()
    second = _fake_config()
    record.configure(first)
    record.configure(second)

    record.unconfigure(first)  # stale: `second` is now active, must survive

    assert record._ACTIVE_CONFIG is second

    record.unconfigure(second)

    assert record._ACTIVE_CONFIG is None


def test_stash_gated_property_noop_when_inactive() -> None:
    """Leave `user_properties` untouched when recording is inactive."""

    config = _fake_config()
    config.stash[record._ACTIVE_KEY] = False
    item: Any = SimpleNamespace(config=config, get_closest_marker=lambda _name: None)
    report = _report()

    record.stash_gated_property(item, report)

    assert report.user_properties == []


def test_stash_gated_property_appends_when_active(monkeypatch: pytest.MonkeyPatch) -> None:
    """Append the family-gate flag when recording is active."""

    config = _fake_config()
    config.stash[record._ACTIVE_KEY] = True
    item: Any = SimpleNamespace(config=config, get_closest_marker=lambda _name: None)
    report = _report()
    monkeypatch.setattr(record, "would_family_gate", lambda _item: True)

    record.stash_gated_property(item, report)

    assert report.user_properties == [(record.GATED_PROPERTY_NAME, True)]


def test_read_gated_finds_stashed_property() -> None:
    """Read the stashed flag back out of `user_properties`."""

    report = _report(user_properties=[("other", "x"), (record.GATED_PROPERTY_NAME, True)])

    assert record._read_gated(report) is True


def test_read_gated_defaults_false_when_absent() -> None:
    """Default to `False` when the property was never stashed."""

    report = _report(user_properties=[("other", "x")])

    assert record._read_gated(report) is False


def test_accumulate_setup_call_teardown_phases() -> None:
    """Fold setup, call, and teardown reports into one accumulator."""

    accumulators: dict[str, record._NodeAccumulator] = {}
    setup = _report(when="setup", outcome="passed", duration=0.1)
    call = _report(
        when="call",
        outcome="passed",
        duration=0.2,
        user_properties=[(record.GATED_PROPERTY_NAME, True)],
    )
    teardown = _report(when="teardown", outcome="passed", duration=0.3)

    record._accumulate(accumulators, setup)
    record._accumulate(accumulators, call)
    record._accumulate(accumulators, teardown)

    node = accumulators[call.nodeid]
    assert node.gated is True
    assert node.duration == pytest.approx(0.6)
    assert node.setup_failed is False
    assert node.call_outcome == "passed"
    assert node.call_wasxfail is False
    assert node.teardown_failed is False


def test_accumulate_ignores_unrecognized_phase() -> None:
    """Update duration/gated but no phase field for an unrecognized `when` value."""

    accumulators: dict[str, record._NodeAccumulator] = {}
    report = _report(when="collect", outcome="passed", duration=0.5)

    record._accumulate(accumulators, report)

    node = accumulators[report.nodeid]
    assert node.duration == pytest.approx(0.5)
    assert node.setup_failed is False
    assert node.call_outcome is None
    assert node.teardown_failed is False


def test_accumulate_records_call_wasxfail() -> None:
    """Detect an xfail-marked call phase via attribute presence."""

    accumulators: dict[str, record._NodeAccumulator] = {}
    report = _report(when="call", outcome="skipped", wasxfail="known issue")

    record._accumulate(accumulators, report)

    assert accumulators[report.nodeid].call_wasxfail is True


@pytest.mark.parametrize(
    (
        "teardown_failed",
        "setup_failed",
        "call_outcome",
        "call_wasxfail",
        "expected_outcome",
        "expected_phase",
    ),
    [
        (True, False, None, False, Outcome.ERROR.value, "teardown"),
        (True, True, None, False, Outcome.ERROR.value, "teardown"),
        (False, True, None, False, Outcome.ERROR.value, "setup"),
        (False, False, None, False, Outcome.SKIPPED.value, None),
        (False, False, "passed", False, Outcome.PASSED.value, None),
        (False, False, "passed", True, Outcome.XPASSED.value, None),
        (False, False, "failed", False, Outcome.FAILED.value, None),
        (False, False, "failed", True, Outcome.FAILED.value, None),
        (False, False, "skipped", False, Outcome.SKIPPED.value, None),
        (False, False, "skipped", True, Outcome.XFAILED.value, None),
    ],
)
def test_finalize_derives_every_outcome(
    teardown_failed: bool,
    setup_failed: bool,
    call_outcome: str | None,
    call_wasxfail: bool,
    expected_outcome: str,
    expected_phase: str | None,
) -> None:
    """Derive every outcome combination the accumulator can produce."""

    node = record._NodeAccumulator(
        gated=True,
        duration=2.5,
        setup_failed=setup_failed,
        call_outcome=call_outcome,
        call_wasxfail=call_wasxfail,
        teardown_failed=teardown_failed,
    )

    entry = record._finalize(node)

    assert entry == OutcomeEntry(
        outcome=expected_outcome, phase=expected_phase, gated=True, duration=2.5
    )


def test_handle_logreport_noop_on_xdist_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    """Skip accumulation entirely on an xdist worker."""

    monkeypatch.setenv(record.XDIST_WORKER_ENVIRONMENT_VARIABLE, "gw0")
    monkeypatch.setattr(record, "_ACTIVE_CONFIG", _fake_config(airflow_record="x.json"))

    record.handle_logreport(_report(when="teardown"))  # must not raise


def test_handle_logreport_noop_without_active_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """Skip accumulation when no config has been configured yet."""

    monkeypatch.delenv(record.XDIST_WORKER_ENVIRONMENT_VARIABLE, raising=False)
    monkeypatch.setattr(record, "_ACTIVE_CONFIG", None)

    record.handle_logreport(_report(when="teardown"))  # must not raise


def test_handle_logreport_noop_when_inactive(monkeypatch: pytest.MonkeyPatch) -> None:
    """Skip accumulation when the active config opted out of recording."""

    monkeypatch.delenv(record.XDIST_WORKER_ENVIRONMENT_VARIABLE, raising=False)
    config = _fake_config()
    config.stash[record._ACTIVE_KEY] = False
    monkeypatch.setattr(record, "_ACTIVE_CONFIG", config)

    record.handle_logreport(_report(when="teardown"))

    assert record._OUTCOMES_KEY not in config.stash


def test_handle_logreport_accumulates_without_finalizing_before_teardown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Accumulate a non-teardown report without writing an outcome yet."""

    monkeypatch.delenv(record.XDIST_WORKER_ENVIRONMENT_VARIABLE, raising=False)
    config = _fake_config(airflow_record="x.json")
    record.configure(config)
    monkeypatch.setattr(record, "_ACTIVE_CONFIG", config)

    record.handle_logreport(_report(when="setup", outcome="passed"))

    assert config.stash[record._OUTCOMES_KEY] == {}
    assert "tests/x.py::test_a" in config.stash[record._ACCUMULATOR_KEY]


def test_handle_logreport_finalizes_at_teardown(monkeypatch: pytest.MonkeyPatch) -> None:
    """Finalize and store the outcome, then drop the working accumulator."""

    monkeypatch.delenv(record.XDIST_WORKER_ENVIRONMENT_VARIABLE, raising=False)
    config = _fake_config(airflow_record="x.json")
    record.configure(config)
    monkeypatch.setattr(record, "_ACTIVE_CONFIG", config)

    record.handle_logreport(_report(when="setup", outcome="passed", duration=0.1))
    record.handle_logreport(_report(when="call", outcome="passed", duration=0.2))
    record.handle_logreport(_report(when="teardown", outcome="passed", duration=0.1))

    outcomes = config.stash[record._OUTCOMES_KEY]
    assert outcomes["tests/x.py::test_a"]["outcome"] == Outcome.PASSED.value
    assert outcomes["tests/x.py::test_a"]["duration"] == pytest.approx(0.4)
    assert "tests/x.py::test_a" not in config.stash[record._ACCUMULATOR_KEY]


def test_live_outcomes_returns_a_snapshot() -> None:
    """Return a copy so later mutation does not alias the stash."""

    config = _fake_config()
    record.configure(config)
    entry = OutcomeEntry(outcome=Outcome.PASSED.value, phase=None, gated=False, duration=1.0)
    config.stash[record._OUTCOMES_KEY]["tests/x.py::test_a"] = entry

    outcomes = record.live_outcomes(config)
    outcomes["tests/x.py::test_a"] = OutcomeEntry(
        outcome=Outcome.FAILED.value, phase=None, gated=False, duration=0.0
    )

    assert (
        config.stash[record._OUTCOMES_KEY]["tests/x.py::test_a"]["outcome"] == Outcome.PASSED.value
    )


def test_resolve_airflow_version_and_family_no_family(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fall back to `UNKNOWN` for both fields when no family is installed."""

    monkeypatch.setattr(record, "installed_family", lambda: None)

    assert record._resolve_airflow_version_and_family() == (UNKNOWN, UNKNOWN)


def test_resolve_airflow_version_and_family_found(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve the installed distribution version for the classified family."""

    monkeypatch.setattr(record, "installed_family", lambda: AirflowFamily.V3)
    monkeypatch.setattr(metadata, "version", lambda _name: "3.3.0")

    assert record._resolve_airflow_version_and_family() == ("3.3.0", AirflowFamily.V3.value)


def test_resolve_airflow_version_and_family_unreadable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fall back to `UNKNOWN` for the version when metadata is unreadable."""

    def _raise(_name: str) -> str:
        raise metadata.PackageNotFoundError

    monkeypatch.setattr(record, "installed_family", lambda: AirflowFamily.V2)
    monkeypatch.setattr(metadata, "version", _raise)

    assert record._resolve_airflow_version_and_family() == (UNKNOWN, AirflowFamily.V2.value)


def test_build_artifact_reports_recorder_versions(monkeypatch: pytest.MonkeyPatch) -> None:
    """Populate every field of a freshly built artifact."""

    monkeypatch.setattr(
        record, "_resolve_airflow_version_and_family", lambda: ("2.11.2", "apache-airflow")
    )
    outcomes = {
        "tests/x.py::test_a": OutcomeEntry(outcome="passed", phase=None, gated=False, duration=1.0)
    }

    artifact = record.build_artifact(outcomes, complete=True)

    assert artifact["schema_version"] == 1
    assert artifact["airflow_version"] == "2.11.2"
    assert artifact["airflow_family"] == "apache-airflow"
    assert artifact["complete"] is True
    assert artifact["outcomes"] == outcomes


@pytest.mark.parametrize(
    ("exitstatus", "expected_complete"),
    [
        (pytest.ExitCode.OK, True),
        (pytest.ExitCode.TESTS_FAILED, True),
        (pytest.ExitCode.INTERRUPTED, False),
        (pytest.ExitCode.INTERNAL_ERROR, False),
    ],
)
def test_build_live_artifact_complete_flag(
    exitstatus: pytest.ExitCode, expected_complete: bool
) -> None:
    """Derive `complete` from the raw session exit status."""

    config = _fake_config()
    record.configure(config)

    artifact = record.build_live_artifact(config, exitstatus)

    assert artifact["complete"] is expected_complete


def test_write_recorded_artifact_noop_on_xdist_worker(tmp_path: Path) -> None:
    """Skip writing on an xdist worker."""

    destination = tmp_path / "out.json"
    config = _fake_config(airflow_record=str(destination), workerinput={})
    session: Any = SimpleNamespace(config=config)

    record.write_recorded_artifact(session, pytest.ExitCode.OK)

    assert not destination.exists()


def test_write_recorded_artifact_noop_without_destination() -> None:
    """Skip writing when `--airflow-record` was not supplied."""

    config = _fake_config()
    record.configure(config)
    session: Any = SimpleNamespace(config=config)

    record.write_recorded_artifact(session, pytest.ExitCode.OK)  # must not raise


def test_write_recorded_artifact_writes_the_live_artifact(tmp_path: Path) -> None:
    """Write the accumulated live outcomes to the requested destination."""

    destination = tmp_path / "nested" / "out.json"
    config = _fake_config(airflow_record=str(destination))
    record.configure(config)
    config.stash[record._OUTCOMES_KEY]["tests/x.py::test_a"] = OutcomeEntry(
        outcome="passed", phase=None, gated=False, duration=1.0
    )
    session: Any = SimpleNamespace(config=config)

    record.write_recorded_artifact(session, pytest.ExitCode.OK)

    payload = json.loads(destination.read_text(encoding="utf-8"))
    assert payload["complete"] is True
    assert payload["outcomes"]["tests/x.py::test_a"]["outcome"] == "passed"
