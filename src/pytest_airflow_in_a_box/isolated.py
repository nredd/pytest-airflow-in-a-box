"""Run `airflow_isolated` tests in one-shot child pytest processes.

Anything registered through a setuptools entry point -- `airflow.plugins`,
`apache_airflow_provider`, `airflow.policy` -- cannot be tested in process: discovery
reads installed distribution metadata through ``_get_grouped_entry_points``, which is
``functools.cache``d in two vendored copies, and a per-test XCom backend binds at
module import (``airflow/sdk/execution_time/xcom.py`` evaluates
``resolve_xcom_backend()`` at module scope). Monkeypatching those caches would test
the patch, not the packaging.

The `airflow_isolated` marker instead re-invokes ``sys.executable -m pytest`` for the
marked tests with a synthetic ``dist-info`` prepended to ``PYTHONPATH`` and optional
``AIRFLOW__*`` overrides applied. The child inherits the parent's bootstrap state --
``AIRFLOW_HOME``, metadata database, Fernet key, JWT secret -- through the same
environment channel an xdist worker uses (it is structurally an xdist worker of one),
then writes a JSON report envelope the parent replays as its own item reports. All
marked tests in one module sharing an identical marker payload run in a single child,
so cost scales with distinct isolation environments, not tests.

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/115
    https://docs.python.org/3/library/importlib.metadata.html
    https://packaging.python.org/en/latest/specifications/entry-points/
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_runtest_protocol
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import re
import shlex
import signal
import subprocess
import sys
from collections.abc import Collection
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import pytest

from pytest_airflow_in_a_box import record
from pytest_airflow_in_a_box.bootstrap import (
    ISOLATED_WORKER_ENVIRONMENT_VARIABLE,
    XDIST_WORKER_ENVIRONMENT_VARIABLE,
    BootstrapState,
    _environment,
    get_bootstrap_state,
)
from pytest_airflow_in_a_box.isolated_child import (
    ISOLATED_REPORT_PATH_ENVIRONMENT_VARIABLE,
    REPORT_FORMAT,
    REPORT_VERSION,
)
from pytest_airflow_in_a_box.markers import MarkedNode

LOGGER = logging.getLogger(__name__)

# PEP 508 distribution-name shape, validated before PEP 503 normalization.
_NAME_PATTERN = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$")
_NAME_NORMALIZE_PATTERN = re.compile(r"[-_.]+")
# Entry-point group shape: dotted words, per the entry-points specification. Anything
# looser could corrupt the generated `entry_points.txt` INI section headers.
_GROUP_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

ISOLATED_MARKER_NAME = "airflow_isolated"
DEFAULT_TIMEOUT_SECONDS = 300.0
_PAYLOAD_KEYWORDS = frozenset({"entry_points", "environment", "name", "timeout"})
# `ExitCode.OK` and `ExitCode.TESTS_FAILED` are the two statuses a child that ran its
# collected tests can produce; everything else means the session itself broke.
_NORMAL_EXIT_STATUSES = (0, 1)
_LOG_TAIL_LINES = 50

_BATCHES_KEY = pytest.StashKey[dict[str, "IsolatedBatch"]]()


@dataclass(frozen=True)
class IsolatedPayload:
    """Normalized `airflow_isolated` marker keyword arguments.

    Parameters:
        entry_points: tuple[tuple[str, tuple[str, ...]], ...] mapping sorted entry-point
            group names to sorted `name = module:attr` lines.
        environment: tuple[tuple[str, str], ...] containing sorted `AIRFLOW__*`
            environment overrides.
        name: str | None containing the PEP 503-normalized synthetic distribution name.
        timeout: float bounding the child pytest process runtime in seconds.
    """

    entry_points: tuple[tuple[str, tuple[str, ...]], ...]
    environment: tuple[tuple[str, str], ...]
    name: str | None
    timeout: float

    @property
    def key(self) -> str:
        """Serialize the payload as its canonical batching identity.

        Returns:
            str containing compact key-sorted JSON of every payload field.
        """

        return json.dumps(
            {
                "entry_points": self.entry_points,
                "environment": self.environment,
                "name": self.name,
                "timeout": self.timeout,
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    @property
    def distribution_name(self) -> str:
        """Resolve the synthetic distribution name, deriving a default when unset.

        Returns:
            str containing the explicit `name` or a payload-derived default.
        """

        return self.name or f"pytest-airflow-in-a-box-isolated-{_digest(self.key)}"


@dataclass
class IsolatedBatch:
    """One child invocation covering every identically-marked item in one module.

    Parameters:
        payload: IsolatedPayload shared by every batched item.
        digest: str containing the eight-character batch identity hash.
        nodeids: tuple[str, ...] naming the batched items in session order.
        started: bool recording whether the child invocation already ran.
        failure: str | None describing a whole-batch failure, when one occurred.
        exit_status: int | None containing the child's exit status once it ran.
        reports: dict[str, list[pytest.TestReport]] mapping child nodeids to their
            deserialized phase reports.
    """

    payload: IsolatedPayload
    digest: str
    nodeids: tuple[str, ...]
    started: bool = False
    failure: str | None = None
    exit_status: int | None = None
    reports: dict[str, list[pytest.TestReport]] = field(default_factory=dict)


def _digest(text: str) -> str:
    """Hash one canonical identity string to a short stable identifier.

    Parameters:
        text: str containing the canonical identity input.

    Returns:
        str containing the first eight hex characters of the SHA-256 digest.
    """

    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:8]


def _validated_entry_points(value: object) -> tuple[tuple[str, tuple[str, ...]], ...]:
    """Validate and normalize the `entry_points` marker keyword.

    Parameters:
        value: object containing the raw marker keyword value.

    Returns:
        tuple[tuple[str, tuple[str, ...]], ...] containing sorted groups and lines.

    Raises:
        pytest.UsageError: The value or any group or line inside it is malformed.
    """

    if value is None:
        return ()
    if not isinstance(value, dict):
        raise pytest.UsageError(
            f"Marker `{ISOLATED_MARKER_NAME}` keyword `entry_points` must map group names "
            f"to `name = module:attr` lines: '{value}'"
        )
    groups: list[tuple[str, tuple[str, ...]]] = []
    for group, raw_lines in value.items():
        if not isinstance(group, str) or _GROUP_PATTERN.fullmatch(group) is None:
            raise pytest.UsageError(
                f"Marker `{ISOLATED_MARKER_NAME}` entry-point group names must match "
                f"`{_GROUP_PATTERN.pattern}`: '{group}'"
            )
        lines: tuple[object, ...]
        if isinstance(raw_lines, str):
            lines = (raw_lines,)
        elif isinstance(raw_lines, (list, tuple)):
            lines = tuple(raw_lines)
        else:
            raise pytest.UsageError(
                f"Marker `{ISOLATED_MARKER_NAME}` entry-point group `{group}` must be one "
                f"line or a list of lines: '{raw_lines}'"
            )
        validated: list[str] = []
        for line in lines:
            if not isinstance(line, str):
                raise pytest.UsageError(
                    f"Marker `{ISOLATED_MARKER_NAME}` entry-point line in group `{group}` "
                    f"must be a string: '{line}'"
                )
            name, separator, target = (part.strip() for part in line.partition("="))
            malformed = not separator or not name or not target
            # Internal whitespace covers newline injection into the generated
            # `entry_points.txt`, which would smuggle an unvalidated second line.
            if malformed or any(ch.isspace() for ch in f"{name}{target}"):
                raise pytest.UsageError(
                    f"Marker `{ISOLATED_MARKER_NAME}` entry-point line in group `{group}` "
                    f"must be `name = module:attr` with no internal whitespace: '{line}'"
                )
            validated.append(f"{name} = {target}")
        groups.append((group, tuple(sorted(validated))))
    return tuple(sorted(groups))


def _validated_environment(
    value: object, reserved: Collection[str]
) -> tuple[tuple[str, str], ...]:
    """Validate and normalize the `environment` marker keyword.

    Parameters:
        value: object containing the raw marker keyword value.
        reserved: Collection[str] naming bootstrap-owned environment variables the
            marker must not override (the child's inherited-state validation would
            reject the disagreement after the fact, uselessly).

    Returns:
        tuple[tuple[str, str], ...] containing sorted override pairs.

    Raises:
        pytest.UsageError: The value or any key or entry inside it is malformed, or a
            key collides with a bootstrap-owned variable.
    """

    if value is None:
        return ()
    if not isinstance(value, dict):
        raise pytest.UsageError(
            f"Marker `{ISOLATED_MARKER_NAME}` keyword `environment` must map `AIRFLOW__*` "
            f"variable names to string values: '{value}'"
        )
    overrides: list[tuple[str, str]] = []
    for key, override in value.items():
        if not isinstance(key, str) or not key.startswith("AIRFLOW__"):
            raise pytest.UsageError(
                f"Marker `{ISOLATED_MARKER_NAME}` environment override names must start "
                f"with `AIRFLOW__`: '{key}'"
            )
        if not isinstance(override, str):
            raise pytest.UsageError(
                f"Marker `{ISOLATED_MARKER_NAME}` environment override `{key}` must be a "
                f"string: '{override}'"
            )
        if key in reserved:
            raise pytest.UsageError(
                f"Marker `{ISOLATED_MARKER_NAME}` environment override `{key}` collides "
                "with a bootstrap-owned variable; configure it through the plugin's ini "
                "options instead"
            )
        overrides.append((key, override))
    return tuple(sorted(overrides))


def _validated_name(value: object) -> str | None:
    """Validate and PEP 503-normalize the `name` marker keyword.

    Parameters:
        value: object containing the raw marker keyword value.

    Returns:
        str | None containing the normalized distribution name.

    Raises:
        pytest.UsageError: The value is not a valid distribution name.
    """

    if value is None:
        return None
    if not isinstance(value, str) or _NAME_PATTERN.fullmatch(value) is None:
        raise pytest.UsageError(
            f"Marker `{ISOLATED_MARKER_NAME}` keyword `name` must be a valid distribution "
            f"name: '{value}'"
        )
    return _NAME_NORMALIZE_PATTERN.sub("-", value).lower()


def _validated_timeout(value: object) -> float:
    """Validate the `timeout` marker keyword.

    Parameters:
        value: object containing the raw marker keyword value.

    Returns:
        float containing the validated timeout, or the default when unset.

    Raises:
        pytest.UsageError: The value is not a positive number.
    """

    if value is None:
        return DEFAULT_TIMEOUT_SECONDS
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise pytest.UsageError(
            f"Marker `{ISOLATED_MARKER_NAME}` keyword `timeout` must be a positive number "
            f"of seconds: '{value}'"
        )
    return float(value)


def read_isolated_marker(node: MarkedNode, reserved: Collection[str]) -> IsolatedPayload | None:
    """Read and validate one node's closest `airflow_isolated` marker.

    Parameters:
        node: MarkedNode supporting closest-marker lookup.
        reserved: Collection[str] naming bootstrap-owned environment variables.

    Returns:
        IsolatedPayload | None containing the normalized payload, or ``None`` when the
        marker is absent.

    Raises:
        pytest.UsageError: The marker carries positional or unknown keyword arguments,
            any keyword is malformed, or neither `entry_points` nor `environment` is
            supplied.
    """

    marker = node.get_closest_marker(ISOLATED_MARKER_NAME)
    if marker is None:
        return None
    if marker.args:
        raise pytest.UsageError(f"Marker `{ISOLATED_MARKER_NAME}` accepts keyword arguments only")
    unknown = set(marker.kwargs) - _PAYLOAD_KEYWORDS
    if unknown:
        names = ", ".join(f"`{name}`" for name in sorted(unknown))
        raise pytest.UsageError(
            f"Marker `{ISOLATED_MARKER_NAME}` received unknown keyword arguments: {names}"
        )
    payload = IsolatedPayload(
        entry_points=_validated_entry_points(marker.kwargs.get("entry_points")),
        environment=_validated_environment(marker.kwargs.get("environment"), reserved),
        name=_validated_name(marker.kwargs.get("name")),
        timeout=_validated_timeout(marker.kwargs.get("timeout")),
    )
    if not payload.entry_points and not payload.environment:
        raise pytest.UsageError(
            f"Marker `{ISOLATED_MARKER_NAME}` requires `entry_points` and/or `environment`; "
            "a bare marker would isolate nothing"
        )
    return payload


def build_dist_info(payload: IsolatedPayload, site_dir: Path) -> Path:
    """Write one synthetic distribution's ``dist-info`` below a scratch site directory.

    The site directory is what goes on the child's ``PYTHONPATH``:
    ``importlib.metadata``'s path finder then yields the synthetic distribution to
    Airflow's real entry-point discovery, with no monkeypatching anywhere.

    Parameters:
        payload: IsolatedPayload naming the distribution and its entry points.
        site_dir: pathlib.Path receiving the ``<name>-0.0.0.dist-info`` directory.

    Returns:
        pathlib.Path containing the site directory for ``PYTHONPATH``.

    Raises:
        pytest.UsageError: The scratch directory cannot be created or written.
    """

    name = payload.distribution_name
    dist_dir = site_dir / f"{name.replace('-', '_')}-0.0.0.dist-info"
    sections = [
        "[{group}]\n{body}\n".format(group=group, body="\n".join(lines))
        for group, lines in payload.entry_points
    ]
    try:
        dist_dir.mkdir(parents=True, exist_ok=True)
        (dist_dir / "METADATA").write_text(
            f"Metadata-Version: 2.1\nName: {name}\nVersion: 0.0.0\n", encoding="utf-8"
        )
        (dist_dir / "entry_points.txt").write_text("\n".join(sections), encoding="utf-8")
        (dist_dir / "RECORD").write_text("", encoding="utf-8")
    except OSError as error:
        raise pytest.UsageError(
            f"Could not write synthetic distribution below '{site_dir}': {error}"
        ) from error
    return site_dir


def store_batches(session: pytest.Session) -> None:
    """Validate every `airflow_isolated` marker and stash the session's batch map.

    Called from ``pytest_collection_finish``, after deselection, for two reasons: the
    final item list has every deselection (`-m`, `-k`, `--airflow-baseline-select`)
    already applied, so a deselected marked test never drags its module-mates into a
    child run; and a malformed marker payload aborts here, before any test runs, as a
    single clean usage error -- a ``pytest.UsageError`` escaping the runtest-protocol
    hook mid-run would strand the already-run part of the session instead. Skipped
    inside an isolated child and on xdist workers, where the marker is inert or
    refused and the batch map would go unread.

    Parameters:
        session: pytest.Session whose deselected item list is final.

    Raises:
        pytest.UsageError: Some marked item carries a malformed marker payload.
    """

    if os.environ.get(ISOLATED_WORKER_ENVIRONMENT_VARIABLE) or os.environ.get(
        XDIST_WORKER_ENVIRONMENT_VARIABLE
    ):
        return
    session.config.stash[_BATCHES_KEY] = _compute_batches(session)


def _compute_batches(session: pytest.Session) -> dict[str, IsolatedBatch]:
    """Group every collected `airflow_isolated` item into per-payload batches.

    Parameters:
        session: pytest.Session whose final item list is being batched.

    Returns:
        dict[str, IsolatedBatch] mapping each marked nodeid to its shared batch.

    Raises:
        pytest.UsageError: Some marked item carries a malformed marker payload.
    """

    reserved = frozenset(_environment(get_bootstrap_state(session.config)))
    grouped: dict[tuple[str, str], list[str]] = {}
    payloads: dict[tuple[str, str], IsolatedPayload] = {}
    for candidate in session.items:
        payload = read_isolated_marker(candidate, reserved)
        if payload is None:
            continue
        group_key = (str(candidate.path), payload.key)
        payloads[group_key] = payload
        grouped.setdefault(group_key, []).append(candidate.nodeid)
    batches: dict[str, IsolatedBatch] = {}
    for group_key, nodeids in grouped.items():
        batch = IsolatedBatch(
            payload=payloads[group_key],
            digest=_digest(json.dumps(group_key)),
            nodeids=tuple(nodeids),
        )
        for nodeid in nodeids:
            batches[nodeid] = batch
    return batches


def _log_tail(path: Path) -> str:
    """Read the last captured child output lines for a failure message.

    Parameters:
        path: pathlib.Path containing the child's combined output log.

    Returns:
        str containing up to the last fifty log lines, or a placeholder when the log
        cannot be read.
    """

    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return "<child log unavailable>"
    return "\n".join(lines[-_LOG_TAIL_LINES:])


def _decode_envelope(path: Path) -> dict[str, list[pytest.TestReport]]:
    """Load and strictly validate one child report envelope.

    Parameters:
        path: pathlib.Path containing the child-written JSON envelope.

    Returns:
        dict[str, list[pytest.TestReport]] mapping nodeids to deserialized phase
        reports in emission order.

    Raises:
        ValueError: The envelope is unreadable, not valid JSON, or malformed in any
            field; the message names the offending shape.
    """

    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as error:
        raise ValueError(f"could not read the child report '{path}': {error}") from error
    try:
        decoded: object = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ValueError(f"the child report '{path}' is not valid JSON: {error}") from error
    if not isinstance(decoded, dict):
        raise ValueError(f"the child report '{path}' must be a JSON object")
    envelope: dict[str, object] = decoded
    if envelope.get("format") != REPORT_FORMAT:
        raise ValueError(
            f"the child report '{path}' field `format` must be `{REPORT_FORMAT}`: "
            f"'{envelope.get('format')}'"
        )
    if envelope.get("version") != REPORT_VERSION:
        raise ValueError(
            f"the child report '{path}' field `version` must be `{REPORT_VERSION}`: "
            f"'{envelope.get('version')}'"
        )
    exit_status = envelope.get("exit_status")
    if isinstance(exit_status, bool) or not isinstance(exit_status, int):
        raise ValueError(f"the child report '{path}' field `exit_status` must be an integer")
    entries = envelope.get("reports")
    if not isinstance(entries, list):
        raise ValueError(f"the child report '{path}' field `reports` must be a list")
    reports: dict[str, list[pytest.TestReport]] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError(f"the child report '{path}' entries must be JSON objects")
        nodeid = entry.get("nodeid")
        if not isinstance(nodeid, str) or not nodeid:
            raise ValueError(f"the child report '{path}' entries must carry a non-empty `nodeid`")
        try:
            report = pytest.TestReport._from_json(entry)
        except Exception as error:
            raise ValueError(
                f"the child report '{path}' entry for `{nodeid}` could not be "
                f"deserialized: {error}"
            ) from error
        if isinstance(report.longrepr, list):
            # JSON has no tuple, so a skip's `(path, lineno, message)` longrepr round-
            # trips as a list -- which the terminal reporter's skip folding asserts
            # against. Restore the tuple shape pytest constructed.
            report.longrepr = tuple(report.longrepr)
        reports.setdefault(nodeid, []).append(report)
    return reports


def _synthesized_report(
    item: pytest.Item,
    *,
    outcome: Literal["passed", "failed"],
    longrepr: str | None,
    when: Literal["setup", "call", "teardown"],
) -> pytest.TestReport:
    """Build one phase report attributed to an item the child never reported on.

    Parameters:
        item: pytest.Item the report is attributed to.
        outcome: str naming the reported outcome.
        longrepr: str | None describing the failure, when there is one.
        when: str naming the reported phase.

    Returns:
        pytest.TestReport containing the synthesized phase outcome.
    """

    return pytest.TestReport(
        nodeid=item.nodeid,
        location=item.location,
        keywords=dict.fromkeys(item.keywords, 1),
        outcome=outcome,
        longrepr=longrepr,
        when=when,
        sections=[],
        duration=0.0,
        user_properties=[],
    )


def _synthesized_failure(item: pytest.Item, message: str) -> list[pytest.TestReport]:
    """Build a failed call report plus a passed teardown report for one item.

    The trailing teardown report is load-bearing, not decoration: the
    ``--airflow-record`` accumulator only finalizes a nodeid on a terminal report, so
    a lone ``call`` failure would dangle forever and silently vanish from an artifact
    stamped complete. The pair mirrors the report shape of a genuinely failed run.

    Parameters:
        item: pytest.Item the failure is attributed to.
        message: str describing the child-process failure.

    Returns:
        list[pytest.TestReport] containing the failed call and passed teardown pair.
    """

    return [
        _synthesized_report(item, outcome="failed", longrepr=message, when="call"),
        _synthesized_report(item, outcome="passed", longrepr=None, when="teardown"),
    ]


def _run_batch(batch: IsolatedBatch, item: pytest.Item, state: BootstrapState) -> None:
    """Run one batch's child pytest process and record its outcome on the batch.

    Parameters:
        batch: IsolatedBatch naming the payload and batched nodeids.
        item: pytest.Item leading the batch (the first member in run order).
        state: BootstrapState providing the run root and log directory.
    """

    batch.started = True
    scratch = state.root / "isolated" / batch.digest
    try:
        site_dir = build_dist_info(batch.payload, scratch / "site")
    except pytest.UsageError as error:
        batch.failure = str(error)
        return
    report_path = scratch / "report.json"
    env = os.environ.copy()
    # The child is an isolated worker, never an xdist worker: a leaked outer worker
    # identity would misroute its report artifacts and record accumulation. Ambient
    # `PYTEST_ADDOPTS` is scrubbed for the same reason `-o addopts=` clears the ini
    # value: an inherited `-n`/`--cov`/`-k` must not reshape the child session.
    env.pop(XDIST_WORKER_ENVIRONMENT_VARIABLE, None)
    env.pop("PYTEST_ADDOPTS", None)
    env[ISOLATED_WORKER_ENVIRONMENT_VARIABLE] = f"iso-{batch.digest}"
    env[ISOLATED_REPORT_PATH_ENVIRONMENT_VARIABLE] = str(report_path)
    ambient = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{site_dir}{os.pathsep}{ambient}" if ambient else str(site_dir)
    env |= dict(batch.payload.environment)
    command = [
        sys.executable,
        "-m",
        "pytest",
        "-p",
        "no:xdist",
        "-p",
        "pytest_airflow_in_a_box.isolated_child",
        "-o",
        "addopts=",
        "-q",
        *batch.nodeids,
    ]
    # Shared `state.logs_folder` plus a pid suffix, matching the API-server log naming:
    # concurrent parents (or a retried batch) must not collide on the filename.
    log_path = state.logs_folder / f"isolated-{batch.digest}-{os.getpid()}.log"
    LOGGER.info(
        f"Running isolated batch `{batch.digest}` ({len(batch.nodeids)} tests) as "
        f"distribution `{batch.payload.distribution_name}`"
    )
    with log_path.open("wb") as log_stream:
        # A fresh session makes the child a process-group leader, so a timeout kill
        # reaps grandchildren too (the REST API server fixture spawns one).
        process = subprocess.Popen(
            command,
            stdout=log_stream,
            stderr=subprocess.STDOUT,
            env=env,
            cwd=item.config.rootpath,
            start_new_session=True,
        )
        try:
            process.wait(timeout=batch.payload.timeout)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            batch.failure = (
                f"The isolated child exceeded its {batch.payload.timeout}s timeout and "
                f"was killed; child log: '{log_path}'"
            )
            return
    batch.exit_status = process.returncode
    if process.returncode not in _NORMAL_EXIT_STATUSES:
        batch.failure = (
            f"The isolated child exited with status {process.returncode} before "
            f"completing its run ({shlex.join(command)}); last child output:\n"
            f"{_log_tail(log_path)}"
        )
        return
    try:
        batch.reports = _decode_envelope(report_path)
    except ValueError as error:
        batch.failure = (
            f"The isolated child's report could not be replayed: {error}; last child "
            f"output:\n{_log_tail(log_path)}"
        )
        return
    known = set(batch.nodeids)
    extras = [nodeid for nodeid in batch.reports if nodeid not in known]
    if extras:
        names = ", ".join(f"`{nodeid}`" for nodeid in extras)
        item.warn(
            pytest.PytestWarning(
                f"The isolated child reported tests outside its batch; dropped: {names}"
            )
        )


def _replay_item(item: pytest.Item, batch: IsolatedBatch, nextitem: pytest.Item | None) -> None:
    """Re-emit one batched item's child reports through the parent's hooks.

    Every report goes through ``pytest_runtest_logstart``/``logreport``/``logfinish``,
    so the terminal reporter, junitxml, session exit status, and the migration-diff
    accumulator all observe the child outcome exactly as they would a local one. Every
    replayed and synthesized report gets the migration-diff `gated` user property
    stashed parent-side: the child never receives the CLI-only recording options, so
    its own `pytest_runtest_makereport` wrapper stashed nothing.

    Claiming the protocol also claims its teardown duty: pytest's own protocol would
    end with ``teardown_exact(nextitem)``, unwinding setup-stack entries the next item
    does not need (or everything, at session end). Skipping that leaves a predecessor's
    collectors on the stack, and the next item's setup asserts against exactly that.
    No setup ran for this item itself, so only inherited stack entries unwind; a
    finalizer failure is replayed as this item's teardown error.

    Parameters:
        item: pytest.Item whose protocol slot is being filled.
        batch: IsolatedBatch carrying the child outcome for this item.
        nextitem: pytest.Item | None scheduled after this one, bounding the teardown.
    """

    ihook = item.ihook
    ihook.pytest_runtest_logstart(nodeid=item.nodeid, location=item.location)
    reports = batch.reports.get(item.nodeid, [])
    if batch.failure is not None:
        replayed = _synthesized_failure(item, batch.failure)
    elif not reports:
        replayed = _synthesized_failure(
            item,
            f"The isolated child produced no report for this test (child exit "
            f"status {batch.exit_status})",
        )
    else:
        replayed = reports
    for report in replayed:
        record.stash_gated_property(item, report)
        ihook.pytest_runtest_logreport(report=report)
    try:
        item.session._setupstate.teardown_exact(nextitem)
    except Exception as error:
        failure = _synthesized_report(
            item,
            outcome="failed",
            longrepr=f"A predecessor's teardown failed: {error!r}",
            when="teardown",
        )
        record.stash_gated_property(item, failure)
        ihook.pytest_runtest_logreport(report=failure)
    ihook.pytest_runtest_logfinish(nodeid=item.nodeid, location=item.location)


def apply_xdist_refusal(item: pytest.Item) -> None:
    """Refuse one `airflow_isolated` item running on an xdist worker.

    Nesting is rejected explicitly rather than debugging grandchildren: an isolated
    child spawned from an xdist worker would race sibling workers on batch scratch
    directories and replay reports into a worker whose controller already aggregates
    them. Raised from ``pytest_runtest_setup``, not the protocol hook, on purpose:
    a ``pytest.UsageError`` escaping a worker's ``pytest_runtest_protocol`` hookimpl
    surfaces through execnet as a crashed-node traceback wall, while the setup phase
    renders the same message as an ordinary per-test error (the same routing
    ``plugin._ensure_database_or_usage_error`` documents).

    Parameters:
        item: pytest.Item about to enter its setup phase.

    Raises:
        pytest.UsageError: The item carries the marker on an xdist worker.
    """

    if item.get_closest_marker(ISOLATED_MARKER_NAME) is None:
        return
    if os.environ.get(ISOLATED_WORKER_ENVIRONMENT_VARIABLE):
        # The serially spawned child of an isolated batch, not an xdist worker.
        return
    if os.environ.get(XDIST_WORKER_ENVIRONMENT_VARIABLE):
        raise pytest.UsageError(
            f"Marker `{ISOLATED_MARKER_NAME}` is unsupported under pytest-xdist; run "
            "marked tests without `-n`"
        )


def runtest_protocol(item: pytest.Item, nextitem: pytest.Item | None) -> bool | None:
    """Run one `airflow_isolated` item through its batch's child process.

    Parameters:
        item: pytest.Item about to enter its runtest protocol.
        nextitem: pytest.Item | None scheduled after this one, bounding the claimed
            protocol's setup-stack teardown.

    Returns:
        bool | None containing ``True`` when the item was replayed from a child run,
        or ``None`` to let pytest run an unmarked item (or a marked item inside the
        child itself) in process.
    """

    if item.get_closest_marker(ISOLATED_MARKER_NAME) is None:
        return None
    if os.environ.get(ISOLATED_WORKER_ENVIRONMENT_VARIABLE):
        # We ARE the child: the marker is inert, so the batched tests run in process
        # and a grandchild can never spawn.
        return None
    if os.environ.get(XDIST_WORKER_ENVIRONMENT_VARIABLE):
        # Fall through to pytest's own protocol; `apply_xdist_refusal` raises the
        # explicit refusal from the setup phase, where a worker renders it cleanly.
        return None
    config = item.config
    batch = config.stash[_BATCHES_KEY][item.nodeid]
    if not batch.started:
        _run_batch(batch, item, get_bootstrap_state(config))
    _replay_item(item, batch, nextitem)
    return True


__all__ = (
    "DEFAULT_TIMEOUT_SECONDS",
    "ISOLATED_MARKER_NAME",
    "IsolatedBatch",
    "IsolatedPayload",
    "apply_xdist_refusal",
    "build_dist_info",
    "read_isolated_marker",
    "runtest_protocol",
    "store_batches",
)
