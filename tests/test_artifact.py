"""Test the migration outcome diff artifact schema, load, and write helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pytest_airflow_in_a_box import artifact
from pytest_airflow_in_a_box.artifact import (
    ARTIFACT_SCHEMA_VERSION,
    Artifact,
    Outcome,
    OutcomeEntry,
    load_artifact,
    write_artifact,
)


def _outcome_entry(
    *,
    outcome: str = "passed",
    phase: str | None = None,
    gated: bool = False,
    duration: float = 1.5,
) -> OutcomeEntry:
    """Build one outcome entry for artifact fixtures.

    Parameters:
        outcome: str containing one `Outcome` value.
        phase: str | None naming `setup`/`teardown` for an `error` outcome.
        gated: bool recording whether the family gate would apply.
        duration: float containing the summed phase duration.

    Returns:
        OutcomeEntry containing the assembled entry.
    """

    return OutcomeEntry(outcome=outcome, phase=phase, gated=gated, duration=duration)


def _artifact(
    *, outcomes: dict[str, OutcomeEntry] | None = None, complete: bool = True
) -> Artifact:
    """Build one complete artifact fixture.

    Parameters:
        outcomes: dict[str, OutcomeEntry] | None containing the recorded outcomes,
            defaulting to one passed nodeid.
        complete: bool recording whether the session finished cleanly.

    Returns:
        Artifact containing the assembled payload.
    """

    return Artifact(
        schema_version=ARTIFACT_SCHEMA_VERSION,
        plugin_version="0.4.0",
        airflow_version="2.11.2",
        airflow_family="apache-airflow",
        python_version="3.12.0",
        pytest_version="8.4.0",
        created_at="2026-08-13T00:00:00+00:00",
        complete=complete,
        outcomes=outcomes
        if outcomes is not None
        else {"tests/test_x.py::test_a": _outcome_entry()},
    )


def test_outcome_values() -> None:
    """Cover the closed set of outcome values used throughout the diff."""

    assert {member.value for member in Outcome} == {
        "passed",
        "failed",
        "error",
        "skipped",
        "xfailed",
        "xpassed",
    }


def test_write_then_load_round_trips(tmp_path: Path) -> None:
    """Round-trip a complete artifact through `write_artifact`/`load_artifact`."""

    path = tmp_path / "nested" / "artifact.json"
    original = _artifact()

    write_artifact(path, original)
    loaded = load_artifact(path)

    assert loaded == original


def test_write_artifact_creates_parent_directories(tmp_path: Path) -> None:
    """Create missing parent directories before writing."""

    path = tmp_path / "a" / "b" / "c" / "artifact.json"

    write_artifact(path, _artifact())

    assert path.is_file()


def test_write_artifact_wraps_os_error(tmp_path: Path) -> None:
    """Render a write failure as a `pytest.UsageError`."""

    blocking_file = tmp_path / "blocked"
    blocking_file.write_text("not a directory", encoding="utf-8")
    path = blocking_file / "artifact.json"

    with pytest.raises(pytest.UsageError, match="Could not write artifact"):
        write_artifact(path, _artifact())


def test_load_artifact_wraps_unreadable_file(tmp_path: Path) -> None:
    """Render a missing file as a `pytest.UsageError`."""

    with pytest.raises(pytest.UsageError, match="Could not read artifact"):
        load_artifact(tmp_path / "missing.json")


def test_load_artifact_rejects_invalid_json(tmp_path: Path) -> None:
    """Reject a file that is not valid JSON."""

    path = tmp_path / "artifact.json"
    path.write_text("{not json", encoding="utf-8")

    with pytest.raises(pytest.UsageError, match="is not valid JSON"):
        load_artifact(path)


def test_load_artifact_rejects_non_object_json(tmp_path: Path) -> None:
    """Reject top-level JSON that is not an object."""

    path = tmp_path / "artifact.json"
    path.write_text("[1, 2, 3]", encoding="utf-8")

    with pytest.raises(pytest.UsageError, match="must be a JSON object"):
        load_artifact(path)


def test_load_artifact_rejects_schema_version_mismatch(tmp_path: Path) -> None:
    """Reject an artifact written by an incompatible `schema_version`."""

    path = tmp_path / "artifact.json"
    payload = dict(_artifact())
    payload["schema_version"] = 2
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(pytest.UsageError, match="schema_version"):
        load_artifact(path)


@pytest.mark.parametrize(
    ("key", "value", "match"),
    [
        ("schema_version", "1", "must be an integer"),
        ("schema_version", True, "must be an integer"),
        ("plugin_version", "", "must be a non-empty string"),
        ("plugin_version", 7, "must be a non-empty string"),
        ("complete", "true", "must be a boolean"),
        ("outcomes", "nope", "field `outcomes` must be a JSON object"),
    ],
)
def test_load_artifact_rejects_malformed_top_level_fields(
    tmp_path: Path, key: str, value: object, match: str
) -> None:
    """Reject a missing or mistyped top-level field."""

    path = tmp_path / "artifact.json"
    payload = dict(_artifact())
    payload[key] = value
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(pytest.UsageError, match=match):
        load_artifact(path)


def test_load_artifact_rejects_missing_top_level_field(tmp_path: Path) -> None:
    """Reject an artifact missing a required top-level field entirely."""

    path = tmp_path / "artifact.json"
    payload = dict(_artifact())
    del payload["created_at"]
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(pytest.UsageError, match="field `created_at`"):
        load_artifact(path)


def test_load_artifact_rejects_non_object_outcome_entry(tmp_path: Path) -> None:
    """Reject an outcome entry that is not itself a JSON object."""

    path = tmp_path / "artifact.json"
    payload: dict[str, object] = dict(_artifact())
    payload["outcomes"] = {"tests/x.py::test_a": None}
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(pytest.UsageError, match="must be a JSON object"):
        load_artifact(path)


def test_load_artifact_rejects_empty_nodeid(tmp_path: Path) -> None:
    """Reject an empty-string nodeid key."""

    path = tmp_path / "artifact.json"
    payload = dict(_artifact(outcomes={"": _outcome_entry()}))
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(pytest.UsageError, match="non-empty strings"):
        load_artifact(path)


@pytest.mark.parametrize(
    ("entry", "match"),
    [
        ({"outcome": 7, "phase": None, "gated": False, "duration": 1.0}, "must be a string"),
        (
            {"outcome": "bogus", "phase": None, "gated": False, "duration": 1.0},
            "not a recognized outcome",
        ),
        ({"phase": None, "gated": False, "duration": 1.0}, "must be a string"),
        (
            {"outcome": "passed", "gated": False, "duration": 1.0},
            "missing field `phase`",
        ),
        (
            {"outcome": "passed", "phase": "bogus", "gated": False, "duration": 1.0},
            "must be null, `setup`, or `teardown`",
        ),
        (
            {"outcome": "error", "phase": None, "gated": False, "duration": 1.0},
            "must be set if and only if",
        ),
        (
            {"outcome": "passed", "phase": "setup", "gated": False, "duration": 1.0},
            "must be set if and only if",
        ),
        (
            {"outcome": "passed", "phase": None, "gated": "yes", "duration": 1.0},
            "field `gated` must be a boolean",
        ),
        (
            {"outcome": "passed", "phase": None, "duration": 1.0},
            "field `gated` must be a boolean",
        ),
        (
            {"outcome": "passed", "phase": None, "gated": False, "duration": "slow"},
            "field `duration` must be a number",
        ),
        (
            {"outcome": "passed", "phase": None, "gated": False, "duration": True},
            "field `duration` must be a number",
        ),
        (
            {"outcome": "passed", "phase": None, "gated": False},
            "field `duration` must be a number",
        ),
    ],
)
def test_load_artifact_rejects_malformed_outcome_entry(
    tmp_path: Path, entry: dict[str, object], match: str
) -> None:
    """Reject every malformed shape of one outcome entry."""

    path = tmp_path / "artifact.json"
    payload = dict(_artifact())
    payload["outcomes"] = {"tests/x.py::test_a": entry}
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(pytest.UsageError, match=match):
        load_artifact(path)


def test_load_artifact_accepts_error_outcome_with_setup_phase(tmp_path: Path) -> None:
    """Accept a well-formed `error` outcome carrying its failing phase."""

    path = tmp_path / "artifact.json"
    payload = dict(
        _artifact(
            outcomes={
                "tests/x.py::test_a": _outcome_entry(outcome="error", phase="setup"),
                "tests/x.py::test_b": _outcome_entry(outcome="error", phase="teardown"),
            }
        )
    )
    path.write_text(json.dumps(payload), encoding="utf-8")

    loaded = load_artifact(path)

    assert loaded["outcomes"]["tests/x.py::test_a"]["phase"] == "setup"
    assert loaded["outcomes"]["tests/x.py::test_b"]["phase"] == "teardown"


def test_load_artifact_accepts_integer_duration(tmp_path: Path) -> None:
    """Accept a JSON integer duration, coercing it to `float`."""

    path = tmp_path / "artifact.json"
    payload = dict(_artifact(outcomes={"tests/x.py::test_a": _outcome_entry(duration=2)}))
    path.write_text(json.dumps(payload), encoding="utf-8")

    loaded = load_artifact(path)

    assert loaded["outcomes"]["tests/x.py::test_a"]["duration"] == 2.0
    assert isinstance(loaded["outcomes"]["tests/x.py::test_a"]["duration"], float)


def test_artifact_module_exports_schema_surface() -> None:
    """Expose the schema surface a sibling migration plugin is expected to import."""

    assert artifact.ARTIFACT_SCHEMA_VERSION == 1
    assert artifact.UNKNOWN == "unknown"
