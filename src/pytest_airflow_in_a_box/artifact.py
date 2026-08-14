"""Migration outcome diff artifact schema, versioned from 1.

This is the public schema module `--airflow-record`/`--airflow-baseline` (issue #42)
writes and reads, and the module a sibling migration-orchestrator plugin (issue #44) is
expected to import directly: the `Outcome` enum, the `OutcomeEntry`/`Artifact` typed
shapes, and `write_artifact`/`load_artifact`. Nothing here imports Airflow or resolves
Airflow version/family metadata -- that resolution is a recording concern that lives in
`record.py`, keeping this module a pure schema plus JSON I/O boundary.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.UsageError
    https://docs.python.org/3/library/json.html
    https://docs.python.org/3/library/typing.html#typing.TypedDict
"""

from __future__ import annotations

import json
import logging
from enum import Enum
from pathlib import Path
from typing import TypedDict

import pytest

LOGGER = logging.getLogger(__name__)

ARTIFACT_SCHEMA_VERSION = 1
# Shared fallback for an unresolvable `airflow_version`/`airflow_family` field, used by
# both `record.py` (writing) and `baseline.py` (the same-family comparison warning).
UNKNOWN = "unknown"
_PHASE_VALUES = ("setup", "teardown")


class Outcome(str, Enum):
    """Closed set of derived per-test outcomes recorded in a migration artifact."""

    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"
    SKIPPED = "skipped"
    XFAILED = "xfailed"
    XPASSED = "xpassed"


class OutcomeEntry(TypedDict):
    """One nodeid's recorded outcome.

    Parameters:
        outcome: str containing one `Outcome` value.
        phase: str | None naming `setup`/`teardown` when `outcome` is `error` (the
            phase whose exception produced it), else `None`. A call-phase exception is
            always recorded as `failed`, never `error`.
        gated: bool recording whether a `requires_airflow2`/`requires_airflow3` family
            marker would gate (skip) this item in the recording environment, computed
            by semantic recomputation of the marker condition rather than by parsing
            the skip reason.
        duration: float containing the sum, in seconds, of every phase report's
            duration that actually ran for this nodeid.
    """

    outcome: str
    phase: str | None
    gated: bool
    duration: float


class Artifact(TypedDict):
    """Migration outcome diff artifact, schema version 1.

    Parameters:
        schema_version: int identifying the serialized artifact schema; always
            `ARTIFACT_SCHEMA_VERSION` for a value this module writes or accepts.
        plugin_version: str containing the recording `pytest-airflow-in-a-box` version.
        airflow_version: str containing the recording environment's installed Airflow
            distribution version, or `"unknown"` when it could not be resolved.
        airflow_family: str containing the recording environment's raw `AirflowFamily`
            value, or `"unknown"` when it could not be resolved.
        python_version: str containing the recording environment's Python version.
        pytest_version: str containing the recording environment's pytest version.
        created_at: str containing a UTC ISO 8601 timestamp.
        complete: bool recording whether the recording session finished without an
            internal error or a `KeyboardInterrupt`; `False` for a session that hit
            `pytest.ExitCode.INTERRUPTED` or `pytest.ExitCode.INTERNAL_ERROR`. A hard
            process kill (SIGKILL, OOM) writes no artifact at all -- there is no
            crash-safe partial artifact, by design.
        outcomes: dict[str, OutcomeEntry] mapping nodeid to its recorded outcome.
            Nodeids match exactly on comparison; there is no fuzzy pairing.
    """

    schema_version: int
    plugin_version: str
    airflow_version: str
    airflow_family: str
    python_version: str
    pytest_version: str
    created_at: str
    complete: bool
    outcomes: dict[str, OutcomeEntry]


def _require_string_field(payload: dict[str, object], key: str, path: Path) -> str:
    """Read one required non-empty string field from a decoded artifact payload.

    Parameters:
        payload: dict[str, object] containing the decoded top-level artifact object.
        key: str naming the required field.
        path: pathlib.Path containing the artifact file, named in the error message.

    Returns:
        str containing the field value.

    Raises:
        pytest.UsageError: The field is absent or is not a non-empty string.
    """

    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise pytest.UsageError(f"Artifact `{path}` field `{key}` must be a non-empty string")
    return value


def _require_bool_field(payload: dict[str, object], key: str, path: Path) -> bool:
    """Read one required boolean field from a decoded artifact payload.

    Parameters:
        payload: dict[str, object] containing the decoded top-level artifact object.
        key: str naming the required field.
        path: pathlib.Path containing the artifact file, named in the error message.

    Returns:
        bool containing the field value.

    Raises:
        pytest.UsageError: The field is absent or is not a boolean.
    """

    value = payload.get(key)
    if not isinstance(value, bool):
        raise pytest.UsageError(f"Artifact `{path}` field `{key}` must be a boolean")
    return value


def _require_int_field(payload: dict[str, object], key: str, path: Path) -> int:
    """Read one required non-boolean integer field from a decoded artifact payload.

    Parameters:
        payload: dict[str, object] containing the decoded top-level artifact object.
        key: str naming the required field.
        path: pathlib.Path containing the artifact file, named in the error message.

    Returns:
        int containing the field value.

    Raises:
        pytest.UsageError: The field is absent or is not an integer.
    """

    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise pytest.UsageError(f"Artifact `{path}` field `{key}` must be an integer")
    return value


def _require_outcome_entry(value: object, nodeid: str, path: Path) -> OutcomeEntry:
    """Validate and construct one outcome entry from decoded JSON data.

    Parameters:
        value: object containing the candidate decoded outcome entry.
        nodeid: str naming the outcome entry's nodeid, for error messages.
        path: pathlib.Path containing the artifact file, named in the error message.

    Returns:
        OutcomeEntry containing the validated entry.

    Raises:
        pytest.UsageError: The entry is malformed or internally inconsistent.
    """

    if not isinstance(value, dict):
        raise pytest.UsageError(
            f"Artifact `{path}` outcome entry `{nodeid}` must be a JSON object"
        )
    entry: dict[str, object] = value

    outcome_value = entry.get("outcome")
    if not isinstance(outcome_value, str):
        raise pytest.UsageError(
            f"Artifact `{path}` outcome entry `{nodeid}` field `outcome` must be a string"
        )
    try:
        outcome = Outcome(outcome_value)
    except ValueError as error:
        raise pytest.UsageError(
            f"Artifact `{path}` outcome entry `{nodeid}` field `outcome` is not a "
            f"recognized outcome: '{outcome_value}'"
        ) from error

    if "phase" not in entry:
        raise pytest.UsageError(
            f"Artifact `{path}` outcome entry `{nodeid}` is missing field `phase`"
        )
    phase = entry["phase"]
    if phase is not None and phase not in _PHASE_VALUES:
        raise pytest.UsageError(
            f"Artifact `{path}` outcome entry `{nodeid}` field `phase` must be null, "
            f"`setup`, or `teardown`: '{phase}'"
        )
    if (outcome is Outcome.ERROR) != (phase is not None):
        raise pytest.UsageError(
            f"Artifact `{path}` outcome entry `{nodeid}` field `phase` must be set if "
            f"and only if `outcome` is `error`"
        )
    phase_value: str | None = str(phase) if phase is not None else None

    gated = entry.get("gated")
    if not isinstance(gated, bool):
        raise pytest.UsageError(
            f"Artifact `{path}` outcome entry `{nodeid}` field `gated` must be a boolean"
        )

    duration = entry.get("duration")
    if isinstance(duration, bool) or not isinstance(duration, (int, float)):
        raise pytest.UsageError(
            f"Artifact `{path}` outcome entry `{nodeid}` field `duration` must be a number"
        )

    return OutcomeEntry(
        outcome=outcome.value, phase=phase_value, gated=gated, duration=float(duration)
    )


def load_artifact(path: Path) -> Artifact:
    """Load and structurally validate one migration artifact.

    Parameters:
        path: pathlib.Path containing the artifact JSON file.

    Returns:
        Artifact containing validated schema-version-1 data.

    Raises:
        pytest.UsageError: The file is unreadable, not valid JSON, not a JSON object,
            missing or malformed a required field (named in the message, along with
            `path`), or was written by an incompatible `schema_version`.
    """

    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as error:
        raise pytest.UsageError(f"Could not read artifact `{path}`: {error}") from error
    try:
        decoded: object = json.loads(raw)
    except json.JSONDecodeError as error:
        raise pytest.UsageError(f"Artifact `{path}` is not valid JSON: {error}") from error
    if not isinstance(decoded, dict):
        raise pytest.UsageError(f"Artifact `{path}` must be a JSON object")
    payload: dict[str, object] = decoded

    schema_version = _require_int_field(payload, "schema_version", path)
    if schema_version != ARTIFACT_SCHEMA_VERSION:
        raise pytest.UsageError(
            f"Artifact `{path}` field `schema_version` is `{schema_version}`, which this "
            f"plugin does not support; it reads `schema_version` `{ARTIFACT_SCHEMA_VERSION}`"
        )

    outcomes_value = payload.get("outcomes")
    if not isinstance(outcomes_value, dict):
        raise pytest.UsageError(f"Artifact `{path}` field `outcomes` must be a JSON object")
    outcomes: dict[str, OutcomeEntry] = {}
    for nodeid, entry in outcomes_value.items():
        # A JSON object's keys are always strings by construction (`json.loads` never
        # produces anything else), so only emptiness needs checking here.
        if not nodeid:
            raise pytest.UsageError(
                f"Artifact `{path}` field `outcomes` keys must be non-empty strings"
            )
        outcomes[str(nodeid)] = _require_outcome_entry(entry, str(nodeid), path)

    return Artifact(
        schema_version=schema_version,
        plugin_version=_require_string_field(payload, "plugin_version", path),
        airflow_version=_require_string_field(payload, "airflow_version", path),
        airflow_family=_require_string_field(payload, "airflow_family", path),
        python_version=_require_string_field(payload, "python_version", path),
        pytest_version=_require_string_field(payload, "pytest_version", path),
        created_at=_require_string_field(payload, "created_at", path),
        complete=_require_bool_field(payload, "complete", path),
        outcomes=outcomes,
    )


def write_artifact(path: Path, artifact: Artifact) -> None:
    """Write one migration artifact as indented, key-sorted JSON.

    Parameters:
        path: pathlib.Path destination for the artifact file. Parent directories are
            created as needed.
        artifact: Artifact containing the payload to serialize.

    Raises:
        pytest.UsageError: The destination cannot be created or written.
    """

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except OSError as error:
        raise pytest.UsageError(f"Could not write artifact `{path}`: {error}") from error


__all__ = (
    "ARTIFACT_SCHEMA_VERSION",
    "UNKNOWN",
    "Artifact",
    "Outcome",
    "OutcomeEntry",
    "load_artifact",
    "write_artifact",
)
