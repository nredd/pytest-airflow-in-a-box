"""Test the `AIRFLOW_HOME` session header and the run-directory retention policy.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.hookspec.pytest_report_header
    https://docs.pytest.org/en/stable/reference/reference.html#confval-tmp_path_retention_policy
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box import _airflow_home
from pytest_airflow_in_a_box._airflow_home import RetentionPolicy
from pytest_airflow_in_a_box.bootstrap import STATE_VERSION, BootstrapState
from pytest_airflow_in_a_box.storage.locate import (
    RUN_DIRECTORY_PREFIX,
    SHARED_MEMORY_PATH,
    StorageReason,
)
from pytest_airflow_in_a_box.storage.provision import DbBackend

HEADER_PATTERN = re.compile(
    r"^pytest-airflow-in-a-box: AIRFLOW_HOME=(.+) \(storage: ", re.MULTILINE
)


def _config(
    *,
    airflow_home_retention: object = None,
    ini_retention: object = "failed",
    airflow_home_retention_count: object = None,
    ini_retention_count: object = "3",
    stash: pytest.Stash | None = None,
) -> Any:
    """Create a minimal configuration double for retention config-reader tests.

    Parameters:
        airflow_home_retention: object containing the parsed
            ``--airflow-home-retention`` option value.
        ini_retention: object containing the ``airflow_home_retention_policy`` ini value.
        airflow_home_retention_count: object containing the parsed
            ``--airflow-home-retention-count`` option value.
        ini_retention_count: object containing the ``airflow_home_retention_count`` ini
            value.
        stash: pytest.Stash | None containing a pre-populated stash; defaults to an
            empty one.

    Returns:
        types.SimpleNamespace shaped like the configuration surface under test, typed
        as ``Any`` for the ``pytest.Config`` call sites.
    """

    option_values: dict[str, object] = {
        "airflow_home_retention": airflow_home_retention,
        "airflow_home_retention_count": airflow_home_retention_count,
    }
    ini_values: dict[str, object] = {
        "airflow_home_retention_policy": ini_retention,
        "airflow_home_retention_count": ini_retention_count,
    }
    return SimpleNamespace(
        getoption=lambda name: option_values[name],
        getini=lambda name: ini_values[name],
        stash=pytest.Stash() if stash is None else stash,
    )


def _state(
    *,
    root: Path = Path("/tmp/pytest-airflow-in-a-box-fake"),
    storage_reason: object = StorageReason.SYSTEM_TEMP,
    db_backend: object = DbBackend.SQLITE,
    owner_pid: int | None = None,
) -> BootstrapState:
    """Build a `BootstrapState` double carrying only the fields under test.

    Parameters:
        root: pathlib.Path containing the run root the header must name.
        storage_reason: object containing the storage-ladder rung that chose the base.
        db_backend: object naming the metadata database backend.
        owner_pid: int | None identifying the owning process; defaults to this process,
            which is what makes the double look like a controller rather than a worker.

    Returns:
        BootstrapState containing the requested fields and inert placeholders elsewhere.
    """

    return BootstrapState(
        version=STATE_VERSION,
        owner_pid=os.getpid() if owner_pid is None else owner_pid,
        root=root,
        dags_folder=root / "dags",
        logs_folder=root / "logs",
        database_path=root / "airflow.db",
        password_file=root / "passwords.json",
        config_path=root / "airflow.cfg",
        plugins_folder=root / "plugins",
        jwt_secret="secret",
        fernet_key="fernet",
        storage_reason=str(storage_reason),
        network_storage=False,
        sql_alchemy_conn="sqlite:////tmp/airflow.db",
        db_backend=str(db_backend),
        family="apache-airflow-core",
        executor="",
        xcom_backend="",
        secrets_backend="",
        secrets_backend_kwargs="",
    )


class _FakeTerminalReporter:
    """Collect rendered lines instead of writing to a real terminal."""

    def __init__(self) -> None:
        self.lines: list[str] = []

    def write_line(self, line: str, **markup: bool) -> None:
        """Record one rendered line, ignoring markup keywords."""

        del markup
        self.lines.append(line)

    def write_sep(self, sep: str, title: str | None = None, **markup: bool) -> None:
        """Record one rendered separator line, ignoring markup keywords."""

        del markup
        self.lines.append(f"{sep} {title}")


def _terminal_reporter() -> Any:
    """Build a `_FakeTerminalReporter` typed as `Any` for `pytest.TerminalReporter` call sites."""

    return _FakeTerminalReporter()


def _header_root(result: pytest.RunResult) -> Path:
    """Read the isolated run root off a subprocess run's session header line.

    Parameters:
        result: pytest.RunResult containing the inner session's captured output.

    Returns:
        pathlib.Path containing the run root the header named.

    Raises:
        AssertionError: The header line is absent from the captured output.
    """

    match = HEADER_PATTERN.search(result.stdout.str())
    assert match is not None, f"no AIRFLOW_HOME header line in output:\n{result.stdout.str()}"
    return Path(match.group(1))


def _run_roots(pytester: pytest.Pytester) -> set[Path]:
    """List the bootstrap run directories a subprocess run could have left behind.

    ``runpytest_subprocess`` passes ``--basetemp``, so the storage ladder resolves to
    the ``caller-temp`` rung below ``pytester.path`` and every inner run's root is a
    direct child of it.

    Parameters:
        pytester: pytest.Pytester owning the temporary directory under test.

    Returns:
        set[pathlib.Path] containing the run directories present right now.
    """

    return set(pytester.path.glob(f"{RUN_DIRECTORY_PREFIX}*"))


# --- resolve_retention_policy -----------------------------------------------------


def test_resolve_retention_policy_prefers_cli_option() -> None:
    """Give the CLI option precedence over the ini value."""

    config = _config(airflow_home_retention="all", ini_retention="none")

    assert _airflow_home.resolve_retention_policy(config) is RetentionPolicy.ALL


def test_resolve_retention_policy_reads_ini_when_cli_absent() -> None:
    """Fall back to the ini value when the CLI option is unset."""

    config = _config(ini_retention="none")

    assert _airflow_home.resolve_retention_policy(config) is RetentionPolicy.NONE


def test_resolve_retention_policy_defaults_to_failed() -> None:
    """Resolve the house default when neither channel overrides it."""

    config = _config()

    assert _airflow_home.resolve_retention_policy(config) is _airflow_home.DEFAULT_RETENTION_POLICY
    assert _airflow_home.DEFAULT_RETENTION_POLICY is RetentionPolicy.FAILED


def test_resolve_retention_policy_caches_resolution() -> None:
    """Resolve the policy once and serve later calls from the stash."""

    reads: list[str] = []
    config = _config(airflow_home_retention="all")
    original_getoption = config.getoption
    config.getoption = lambda name: (reads.append(name), original_getoption(name))[1]

    assert _airflow_home.resolve_retention_policy(config) is RetentionPolicy.ALL
    assert _airflow_home.resolve_retention_policy(config) is RetentionPolicy.ALL
    assert reads == ["airflow_home_retention"]


def test_resolve_retention_policy_rejects_an_unknown_value() -> None:
    """Reject an ini value outside the three supported policies."""

    config = _config(ini_retention="sometimes")

    with pytest.raises(pytest.UsageError, match="must be `all`, `failed`, or `none`: 'sometimes'"):
        _airflow_home.resolve_retention_policy(config)


@pytest.mark.parametrize("ini_retention", ["", None], ids=["empty", "non-string"])
def test_resolve_retention_policy_rejects_a_blank_value(ini_retention: object) -> None:
    """Reject an empty or non-string ini value before enum coercion sees it.

    Parameters:
        ini_retention: object containing the malformed ini value under test.
    """

    config = _config(ini_retention=ini_retention)

    with pytest.raises(pytest.UsageError, match="must be `all`, `failed`, or `none`"):
        _airflow_home.resolve_retention_policy(config)


# --- resolve_retention_count --------------------------------------------------------


def test_resolve_retention_count_prefers_cli_option() -> None:
    """Give the CLI option precedence over the ini value."""

    config = _config(airflow_home_retention_count="5", ini_retention_count="2")

    assert _airflow_home.resolve_retention_count(config) == 5


def test_resolve_retention_count_reads_ini_when_cli_absent() -> None:
    """Fall back to the ini value when the CLI option is unset."""

    config = _config(ini_retention_count="7")

    assert _airflow_home.resolve_retention_count(config) == 7


def test_resolve_retention_count_defaults_to_three() -> None:
    """Resolve the house default when neither channel overrides it."""

    config = _config()

    assert _airflow_home.resolve_retention_count(config) == _airflow_home.DEFAULT_RETENTION_COUNT
    assert _airflow_home.DEFAULT_RETENTION_COUNT == 3


def test_resolve_retention_count_caches_resolution() -> None:
    """Resolve the count once and serve later calls from the stash."""

    reads: list[str] = []
    config = _config(airflow_home_retention_count="5")
    original_getoption = config.getoption
    config.getoption = lambda name: (reads.append(name), original_getoption(name))[1]

    assert _airflow_home.resolve_retention_count(config) == 5
    assert _airflow_home.resolve_retention_count(config) == 5
    assert reads == ["airflow_home_retention_count"]


@pytest.mark.parametrize(
    "ini_retention_count", ["0", "-1", "nope"], ids=["zero", "negative", "non-integer"]
)
def test_resolve_retention_count_rejects_an_invalid_value(ini_retention_count: str) -> None:
    """Reject a non-positive or non-integer ini value.

    Parameters:
        ini_retention_count: str containing the malformed ini value under test.
    """

    config = _config(ini_retention_count=ini_retention_count)

    with pytest.raises(pytest.UsageError, match="must be a positive integer"):
        _airflow_home.resolve_retention_count(config)


@pytest.mark.parametrize("ini_retention_count", ["", None], ids=["empty", "non-string"])
def test_resolve_retention_count_rejects_a_blank_value(ini_retention_count: object) -> None:
    """Reject an empty or non-string ini value before ``int`` coercion sees it.

    Parameters:
        ini_retention_count: object containing the malformed ini value under test.
    """

    config = _config(ini_retention_count=ini_retention_count)

    with pytest.raises(pytest.UsageError, match="must be a positive integer"):
        _airflow_home.resolve_retention_count(config)


def test_retention_count_reads_the_resolved_stash_value() -> None:
    """Read the resolved count back from the stash, without re-parsing anything."""

    stash = pytest.Stash()
    stash[_airflow_home.RETENTION_COUNT_KEY] = 9

    assert _airflow_home.retention_count(_config(stash=stash)) == 9


def test_retention_count_falls_back_to_the_default() -> None:
    """Apply the documented default when the session died before `pytest_configure`."""

    assert _airflow_home.retention_count(_config()) == _airflow_home.DEFAULT_RETENTION_COUNT


# --- session outcome and the retention decision -----------------------------------


@pytest.mark.parametrize(
    ("exitstatus", "expected"),
    [
        (int(pytest.ExitCode.OK), False),
        (int(pytest.ExitCode.NO_TESTS_COLLECTED), False),
        (int(pytest.ExitCode.TESTS_FAILED), True),
        (int(pytest.ExitCode.INTERRUPTED), True),
        (int(pytest.ExitCode.INTERNAL_ERROR), True),
        (int(pytest.ExitCode.USAGE_ERROR), True),
    ],
)
def test_record_session_outcome_treats_every_unclean_status_as_failure(
    exitstatus: int, expected: bool
) -> None:
    """Record every exit status outside the clean set as a failed run.

    `NO_TESTS_COLLECTED` is the deliberate carve-out: nothing ran, so nothing touched
    the isolated run directory and there is nothing in it to inspect.

    Parameters:
        exitstatus: int containing pytest's raw session exit status.
        expected: bool containing the recorded failure flag.
    """

    config = _config()

    _airflow_home.record_session_outcome(config, exitstatus)

    assert _airflow_home.session_failed(config) is expected


def test_session_failed_reads_an_unmarked_invocation_as_no_session() -> None:
    """Read a missing mark as "no session ran", which is not a failure.

    `pytest --help`, `pytest --markers`, an argparse usage error, and an abort during
    `pytest_configure` all bootstrap a run root without ever starting a session. None of
    them is a failed run, and none of them should keep a directory.
    """

    assert _airflow_home.session_failed(_config()) is False


def test_mark_session_started_records_a_pessimistic_failure() -> None:
    """Treat a started session as failed until `pytest_sessionfinish` overwrites it."""

    config = _config()

    _airflow_home.mark_session_started(config)

    assert _airflow_home.session_failed(config) is True


def test_a_started_session_that_never_finishes_stays_failed() -> None:
    """Keep the pessimistic mark when the session dies between start and finish."""

    config = _config()

    _airflow_home.mark_session_started(config)
    _airflow_home.record_session_outcome(config, int(pytest.ExitCode.OK))

    assert _airflow_home.session_failed(config) is False


@pytest.mark.parametrize(
    ("policy", "recorded", "expected"),
    [
        (RetentionPolicy.ALL, None, True),
        (RetentionPolicy.ALL, False, True),
        (RetentionPolicy.ALL, True, True),
        (RetentionPolicy.FAILED, None, False),
        (RetentionPolicy.FAILED, False, False),
        (RetentionPolicy.FAILED, True, True),
        (RetentionPolicy.NONE, None, False),
        (RetentionPolicy.NONE, False, False),
        (RetentionPolicy.NONE, True, False),
    ],
)
def test_retain_airflow_home_covers_the_policy_matrix(
    policy: RetentionPolicy, recorded: bool | None, expected: bool
) -> None:
    """Decide retention across every policy and every recorded outcome.

    Parameters:
        policy: RetentionPolicy resolved for the run.
        recorded: bool | None containing the recorded failure flag, or ``None`` when no
            session ever started.
        expected: bool containing the expected retention decision.
    """

    stash = pytest.Stash()
    stash[_airflow_home.RETENTION_POLICY_KEY] = policy
    if recorded is not None:
        stash[_airflow_home.SESSION_FAILED_KEY] = recorded
    config = _config(stash=stash)

    assert _airflow_home.retain_airflow_home(config) is expected


def test_retain_airflow_home_falls_back_to_the_default_policy() -> None:
    """Apply the documented default when the session died before `pytest_configure`."""

    stash = pytest.Stash()
    stash[_airflow_home.SESSION_FAILED_KEY] = True

    assert _airflow_home.retain_airflow_home(_config(stash=stash)) is True


# --- mark_root_retained and prune_retained_roots -----------------------------------


def _make_run_dir(base: Path, suffix: str, *, retained_mtime: float | None = None) -> Path:
    """Create one fake run directory under a base, optionally marked as retained.

    Parameters:
        base: pathlib.Path containing the storage base to create the directory under.
        suffix: str appended to `RUN_DIRECTORY_PREFIX` to name the directory.
        retained_mtime: float | None setting the retained marker's modification time
            when given; the directory is left unmarked -- an "active" run -- when `None`.

    Returns:
        pathlib.Path containing the newly created run directory.
    """

    root = base / f"{RUN_DIRECTORY_PREFIX}{suffix}"
    root.mkdir()
    if retained_mtime is not None:
        marker = root / _airflow_home.RETAINED_MARKER_NAME
        marker.touch()
        os.utime(marker, (retained_mtime, retained_mtime))
    return root


def test_mark_root_retained_writes_the_marker(tmp_path: Path) -> None:
    """Touch the sentinel file inside the run directory."""

    root = tmp_path / f"{RUN_DIRECTORY_PREFIX}kept"
    root.mkdir()

    _airflow_home.mark_root_retained(root)

    assert (root / _airflow_home.RETAINED_MARKER_NAME).is_file()


def test_prune_retained_roots_keeps_the_most_recent_marked_siblings(tmp_path: Path) -> None:
    """Remove marked siblings past the `keep` most recent, by marker mtime."""

    oldest = _make_run_dir(tmp_path, "1", retained_mtime=1.0)
    middle = _make_run_dir(tmp_path, "2", retained_mtime=2.0)
    newest = _make_run_dir(tmp_path, "3", retained_mtime=3.0)

    _airflow_home.prune_retained_roots(tmp_path, keep=2)

    assert not oldest.exists()
    assert middle.exists()
    assert newest.exists()


def test_prune_retained_roots_never_touches_an_unmarked_directory(tmp_path: Path) -> None:
    """Leave an active run's directory alone regardless of `keep`.

    An unmarked sibling has no way to prove it is finished rather than in-flight -- this
    process's own about-to-be-created root, or another concurrent invocation sharing the
    same base. `keep=0` forces maximum pruning to prove the marker check, not the count,
    is what protects it.
    """

    active = _make_run_dir(tmp_path, "active")
    _make_run_dir(tmp_path, "1", retained_mtime=1.0)

    _airflow_home.prune_retained_roots(tmp_path, keep=0)

    assert active.exists()
    assert _run_roots_matching(tmp_path) == {active}


def test_prune_retained_roots_never_touches_another_users_marked_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Skip a marked sibling owned by a different uid, never pruning or counting it.

    `/dev/shm` and system temp are typically world-writable, so a marker this process
    did not create belongs to another user on the same host -- neither a removal
    candidate (permission denied) nor a directory this run's "other retained roots"
    count should claim credit for.

    Parameters:
        tmp_path: pathlib.Path providing an isolated storage base to scan.
        monkeypatch: pytest.MonkeyPatch simulating a different owning uid.
    """

    other = _make_run_dir(tmp_path, "other-user", retained_mtime=1.0)
    real_uid = (other / _airflow_home.RETAINED_MARKER_NAME).stat().st_uid
    monkeypatch.setattr(os, "getuid", lambda: real_uid + 1)

    assert _airflow_home._retained_siblings(tmp_path) == []

    _airflow_home.prune_retained_roots(tmp_path, keep=0)

    assert other.exists()


def test_prune_retained_roots_logs_a_removal_failure_instead_of_swallowing_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Log a warning when a stale directory cannot be removed, rather than ignoring it.

    A silently swallowed removal failure would let the `keep` bound stop holding with
    nothing anywhere saying so.

    Parameters:
        tmp_path: pathlib.Path providing an isolated storage base to scan.
        monkeypatch: pytest.MonkeyPatch simulating an `OSError` from `shutil.rmtree`.
        caplog: pytest.LogCaptureFixture capturing the warning.
    """

    stale = _make_run_dir(tmp_path, "1", retained_mtime=1.0)
    _make_run_dir(tmp_path, "2", retained_mtime=2.0)

    def _raise(path: Path) -> None:
        del path
        raise OSError("permission denied")

    monkeypatch.setattr(_airflow_home.shutil, "rmtree", _raise)
    caplog.set_level(logging.WARNING, logger=_airflow_home.LOGGER.name)

    _airflow_home.prune_retained_roots(tmp_path, keep=1)

    assert f"Could not prune retained AIRFLOW_HOME '{stale}'" in caplog.text


def test_prune_retained_roots_ignores_directories_outside_the_run_prefix(tmp_path: Path) -> None:
    """Leave a directory that does not match `RUN_DIRECTORY_PREFIX` untouched."""

    unrelated = tmp_path / "not-a-run-directory"
    unrelated.mkdir()

    _airflow_home.prune_retained_roots(tmp_path, keep=0)

    assert unrelated.exists()


def _run_roots_matching(base: Path) -> set[Path]:
    """List every run directory still present under a base.

    Parameters:
        base: pathlib.Path containing the storage base to scan.

    Returns:
        set[pathlib.Path] containing the run directories present right now.
    """

    return set(base.glob(f"{RUN_DIRECTORY_PREFIX}*"))


# --- report_header ----------------------------------------------------------------


@pytest.mark.parametrize("reason", list(StorageReason))
@pytest.mark.parametrize("backend", list(DbBackend))
def test_report_header_names_the_root_its_rung_and_its_backend(
    reason: StorageReason, backend: DbBackend
) -> None:
    """Render one header line for every storage rung and both database backends.

    Parameters:
        reason: StorageReason describing the storage-ladder rung under test.
        backend: DbBackend naming the metadata database backend under test.
    """

    root = Path("/dev/shm/pytest-airflow-in-a-box-8f2a1c")
    state = _state(root=root, storage_reason=reason, db_backend=backend)

    assert _airflow_home.report_header(state) == [
        f"pytest-airflow-in-a-box: AIRFLOW_HOME={root} (storage: {reason}, db: {backend})"
    ]


def test_report_header_is_silent_on_an_xdist_worker() -> None:
    """Stay silent in a process that inherited the root instead of creating it."""

    assert _airflow_home.report_header(_state(owner_pid=1)) == []


# --- terminal_summary -------------------------------------------------------------


def test_terminal_summary_reports_a_retained_root(tmp_path: Path) -> None:
    """Name the surviving directory and the policy that kept it."""

    root = tmp_path / f"{RUN_DIRECTORY_PREFIX}kept"
    stash = pytest.Stash()
    stash[_airflow_home.RETENTION_POLICY_KEY] = RetentionPolicy.ALL
    reporter = _terminal_reporter()

    _airflow_home.terminal_summary(reporter, _config(stash=stash), _state(root=root))

    assert reporter.lines == [
        "= airflow-in-a-box",
        f"Retained AIRFLOW_HOME (retention policy: all; 0 other retained roots kept): {root}",
    ]


def test_terminal_summary_reports_the_number_of_already_retained_roots(tmp_path: Path) -> None:
    """Count pre-existing marked siblings under the same base, not this run's own root."""

    _make_run_dir(tmp_path, "1", retained_mtime=1.0)
    _make_run_dir(tmp_path, "2", retained_mtime=2.0)
    root = tmp_path / f"{RUN_DIRECTORY_PREFIX}new"
    stash = pytest.Stash()
    stash[_airflow_home.RETENTION_POLICY_KEY] = RetentionPolicy.ALL
    reporter = _terminal_reporter()

    _airflow_home.terminal_summary(reporter, _config(stash=stash), _state(root=root))

    assert reporter.lines[-1] == (
        f"Retained AIRFLOW_HOME (retention policy: all; 2 other retained roots kept): {root}"
    )


def test_terminal_summary_warns_about_a_retained_shared_memory_root(tmp_path: Path) -> None:
    """Flag a retained `/dev/shm` root as RAM the run holds until it is removed.

    Only `storage_reason` -- not the root's actual location -- triggers the warning, so
    this stays off the real `/dev/shm` and scans an isolated `tmp_path` base instead.
    """

    stash = pytest.Stash()
    stash[_airflow_home.RETENTION_POLICY_KEY] = RetentionPolicy.ALL
    reporter = _terminal_reporter()
    state = _state(
        root=tmp_path / f"{RUN_DIRECTORY_PREFIX}8f2a1c",
        storage_reason=StorageReason.SHARED_MEMORY,
    )

    _airflow_home.terminal_summary(reporter, _config(stash=stash), state)

    assert f"WARNING: '{SHARED_MEMORY_PATH}' is RAM-backed" in reporter.lines[-1]
    assert "`--airflow-home=PATH`" in reporter.lines[-1]


def test_terminal_summary_is_silent_when_the_root_is_discarded() -> None:
    """Render nothing when cleanup is about to remove the directory."""

    stash = pytest.Stash()
    stash[_airflow_home.RETENTION_POLICY_KEY] = RetentionPolicy.NONE
    reporter = _terminal_reporter()

    _airflow_home.terminal_summary(reporter, _config(stash=stash), _state())

    assert reporter.lines == []


def test_terminal_summary_suppresses_the_stderr_fallback() -> None:
    """Record the announcement so cleanup does not repeat it on `stderr`."""

    stash = pytest.Stash()
    stash[_airflow_home.RETENTION_POLICY_KEY] = RetentionPolicy.ALL

    _airflow_home.terminal_summary(_terminal_reporter(), _config(stash=stash), _state())

    assert stash[_airflow_home.RETENTION_ANNOUNCED_KEY] is True


def test_terminal_summary_is_silent_on_an_xdist_worker() -> None:
    """Render nothing in a process that does not own the run directory."""

    stash = pytest.Stash()
    stash[_airflow_home.RETENTION_POLICY_KEY] = RetentionPolicy.ALL
    reporter = _terminal_reporter()

    _airflow_home.terminal_summary(reporter, _config(stash=stash), _state(owner_pid=1))

    assert reporter.lines == []


# --- announce_retained_root -------------------------------------------------------


def test_announce_retained_root_names_the_directory_on_stderr(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Write the surviving path to `stderr` when no terminal summary reported it.

    Only `storage_reason` -- not the root's actual location -- triggers the RAM warning,
    so this stays off the real `/dev/shm` and scans an isolated `tmp_path` base instead.

    Parameters:
        tmp_path: pathlib.Path providing an isolated storage base to scan.
        capsys: pytest.CaptureFixture capturing the announcement stream.
    """

    root = tmp_path / f"{RUN_DIRECTORY_PREFIX}8f2a1c"
    stash = pytest.Stash()
    stash[_airflow_home.RETENTION_POLICY_KEY] = RetentionPolicy.FAILED
    config = _config(stash=stash)

    _airflow_home.announce_retained_root(config, root, str(StorageReason.SHARED_MEMORY))

    captured = capsys.readouterr()
    assert captured.out == ""
    assert (
        f"pytest-airflow-in-a-box: Retained AIRFLOW_HOME "
        f"(retention policy: failed; 0 other retained roots kept): {root}" in captured.err
    )
    assert f"WARNING: '{SHARED_MEMORY_PATH}' is RAM-backed" in captured.err
    assert stash[_airflow_home.RETENTION_ANNOUNCED_KEY] is True


def test_announce_retained_root_defers_to_an_earlier_announcement(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Stay silent once the terminal summary has already named the directory.

    Parameters:
        capsys: pytest.CaptureFixture capturing the announcement stream.
    """

    stash = pytest.Stash()
    stash[_airflow_home.RETENTION_ANNOUNCED_KEY] = True

    _airflow_home.announce_retained_root(
        _config(stash=stash), Path("/tmp/kept"), str(StorageReason.SYSTEM_TEMP)
    )

    assert capsys.readouterr().err == ""


# --- end-to-end through a real session --------------------------------------------


_PASSING_SUITE = """
    def test_ok():
        assert True
"""

_FAILING_SUITE = """
    def test_bad():
        assert False
"""


@pytest.mark.parametrize(
    ("policy", "failing", "survives"),
    [
        ("all", False, True),
        ("all", True, True),
        ("failed", False, False),
        ("failed", True, True),
        ("none", False, False),
        ("none", True, False),
    ],
)
def test_retention_matrix_end_to_end(
    pytester: pytest.Pytester, policy: str, failing: bool, survives: bool
) -> None:
    """Keep or remove the real run directory exactly as the policy dictates.

    Reads the root off the session header line rather than from inside the inner
    suite, so the header and the retention decision are checked against each other in
    the same run.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
        policy: str containing the `--airflow-home-retention` value under test.
        failing: bool selecting a failing rather than a passing inner suite.
        survives: bool containing whether the run directory must still exist.
    """

    pytester.makepyfile(test_suite=_FAILING_SUITE if failing else _PASSING_SUITE)

    result = pytester.runpytest_subprocess(f"--airflow-home-retention={policy}")

    if failing:
        result.assert_outcomes(failed=1)
    else:
        result.assert_outcomes(passed=1)
    root = _header_root(result)
    assert root.is_dir() is survives
    if survives:
        result.stdout.fnmatch_lines(
            [f"Retained AIRFLOW_HOME (retention policy: {policy}; 0 other retained roots kept):*"]
        )
    else:
        assert "Retained AIRFLOW_HOME" not in result.stdout.str()


def test_default_policy_keeps_a_failing_run_directory(pytester: pytest.Pytester) -> None:
    """Retain a failing run's directory with no option and no ini value at all.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    pytester.makepyfile(test_suite=_FAILING_SUITE)

    result = pytester.runpytest_subprocess()

    result.assert_outcomes(failed=1)
    assert _header_root(result).is_dir()


def test_retention_count_bounds_retained_roots_across_runs(pytester: pytest.Pytester) -> None:
    """Prune retained roots down to the configured count, oldest first.

    Five failing runs in a row against the same `pytester.path` base with
    `--airflow-home-retention-count=3`: only the 3 most recently created run
    directories survive, no matter how many failing runs came before them.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    pytester.makepyfile(test_suite=_FAILING_SUITE)

    roots: list[Path] = []
    for _ in range(5):
        result = pytester.runpytest_subprocess(
            "--airflow-home-retention=failed", "--airflow-home-retention-count=3"
        )
        result.assert_outcomes(failed=1)
        roots.append(_header_root(result))

    assert _run_roots(pytester) == set(roots[-3:])


def test_retention_count_message_matches_what_survives(pytester: pytest.Pytester) -> None:
    """Report the count that will actually be on disk once this run's cleanup finishes.

    `_retained_lines` runs before `bootstrap`'s cleanup closure marks and prunes, so a
    naive pre-prune sibling count would announce roots this same cleanup pass is about to
    delete. With `--airflow-home-retention-count=1` every run's announce line must say
    "0 other retained roots kept", never "1" -- and exactly one directory must survive
    each time.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    pytester.makepyfile(test_suite=_FAILING_SUITE)

    for _ in range(3):
        result = pytester.runpytest_subprocess(
            "--airflow-home-retention=failed", "--airflow-home-retention-count=1"
        )
        result.assert_outcomes(failed=1)
        root = _header_root(result)
        result.stdout.fnmatch_lines(
            ["Retained AIRFLOW_HOME (retention policy: failed; 0 other retained roots kept):*"]
        )
        assert _run_roots(pytester) == {root}


def test_retention_count_default_keeps_three(pytester: pytest.Pytester) -> None:
    """Bound retained roots to the documented default of 3 with no option or ini value.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    pytester.makepyfile(test_suite=_FAILING_SUITE)

    roots: list[Path] = []
    for _ in range(4):
        result = pytester.runpytest_subprocess("--airflow-home-retention=failed")
        result.assert_outcomes(failed=1)
        roots.append(_header_root(result))

    assert _run_roots(pytester) == set(roots[-3:])


def test_unknown_cli_retention_count_is_rejected(pytester: pytest.Pytester) -> None:
    """Reject a non-integer `--airflow-home-retention-count` value.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    pytester.makepyfile(test_suite=_PASSING_SUITE)

    result = pytester.runpytest_subprocess("--airflow-home-retention-count=nope")

    assert result.ret == pytest.ExitCode.USAGE_ERROR
    assert "must be a positive integer" in result.stdout.str() + result.stderr.str()


def test_blank_ini_retention_count_is_rejected(pytester: pytest.Pytester) -> None:
    """Fail loudly at configure time on an empty retention-count ini value.

    Mirrors `test_blank_ini_policy_is_rejected`: the aborted session never reaches
    `pytest_sessionstart`, so nothing marks it as a started run and cleanup discards the
    bootstrap directory rather than accumulating one per failed attempt.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    pytester.makeini("[pytest]\nairflow_home_retention_count =\n")
    pytester.makepyfile(test_suite=_PASSING_SUITE)
    before = _run_roots(pytester)

    result = pytester.runpytest_subprocess()

    assert result.ret == pytest.ExitCode.USAGE_ERROR
    output = result.stdout.str() + result.stderr.str()
    assert "`airflow_home_retention_count` must be a positive integer" in output
    assert "INTERNALERROR" not in output
    assert "Retained AIRFLOW_HOME" not in output
    assert _run_roots(pytester) == before


_CRASHING_SESSIONSTART_CONFTEST = """
    from pathlib import Path

    from pytest_airflow_in_a_box.plugin import get_bootstrap_state

    RECORD = Path({record!r})


    def pytest_sessionstart(session):
        RECORD.write_text(str(get_bootstrap_state(session.config).root), encoding="utf-8")
        raise RuntimeError("simulated startup crash")
"""


def test_a_crash_before_sessionfinish_retains_the_root(pytester: pytest.Pytester) -> None:
    """Keep the run directory when the session dies before `pytest_sessionfinish` runs.

    pytest only dispatches `pytest_sessionfinish` once `pytest_sessionstart` has
    returned (`_pytest.main.wrap_session`'s ``initstate >= 2`` guard), while
    `config._ensure_unconfigure` -- and therefore the bootstrap cleanup closure -- runs
    either way. That is the crash path: no outcome was ever recorded, and the default
    `failed` policy must read the absence as a failure and keep the artifacts.

    The root is recorded from the conftest rather than read off the header: the terminal
    reporter's own `pytest_sessionstart` hookimpl is ``trylast``, so the crash preempts
    the header line this run would otherwise have printed. That is exactly why cleanup
    announces the kept path on `stderr` -- neither terminal channel ran.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    record_path = pytester.path / "crashed-root"
    pytester.makeconftest(_CRASHING_SESSIONSTART_CONFTEST.format(record=str(record_path)))
    pytester.makepyfile(test_suite=_PASSING_SUITE)

    result = pytester.runpytest_subprocess()

    assert result.ret == pytest.ExitCode.INTERNAL_ERROR
    root = Path(record_path.read_text(encoding="utf-8"))
    assert root.is_dir()
    assert "AIRFLOW_HOME=" not in result.stdout.str()
    assert (
        f"Retained AIRFLOW_HOME (retention policy: failed; 0 other retained roots kept): {root}"
        in result.stderr.str()
    )


_RECORDING_PROVISIONER_PLUGIN = """
    from pathlib import Path

    import pytest_airflow_in_a_box.bootstrap as bootstrap
    from pytest_airflow_in_a_box.airflow_cfg import sqlite_url

    MARKER = Path({marker!r})


    class RecordingProvisioner:
        def start(self, *, database_path, database_name):
            del database_name
            return sqlite_url(database_path)

        def stop(self):
            MARKER.write_text("stopped", encoding="utf-8")


    def _select(backend):
        del backend
        return RecordingProvisioner()


    bootstrap.select_provisioner = _select
"""


@pytest.mark.parametrize("policy", [str(value) for value in RetentionPolicy])
def test_provisioner_stops_on_every_retention_policy(
    pytester: pytest.Pytester, policy: str
) -> None:
    """Stop the provisioner on every policy so a retained run leaks no container.

    The plugin module is loaded with ``-p`` rather than from a conftest: bootstrap runs
    during `pytest_load_initial_conftests`, before any consumer conftest is imported, so
    a conftest patch would land too late to be seen.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
        policy: str containing the `--airflow-home-retention` value under test.
    """

    marker = pytester.path / f"stopped-{policy}"
    pytester.makepyfile(
        recording_provisioner=_RECORDING_PROVISIONER_PLUGIN.format(marker=str(marker)),
        test_suite=_FAILING_SUITE,
    )

    result = pytester.runpytest_subprocess(
        "-p", "recording_provisioner", f"--airflow-home-retention={policy}"
    )

    result.assert_outcomes(failed=1)
    assert marker.read_text(encoding="utf-8") == "stopped"


def test_unknown_cli_policy_is_rejected(pytester: pytest.Pytester) -> None:
    """Reject an unsupported `--airflow-home-retention` value at argument-parse time.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    pytester.makepyfile(test_suite=_PASSING_SUITE)

    result = pytester.runpytest_subprocess("--airflow-home-retention=sometimes")

    assert result.ret == pytest.ExitCode.USAGE_ERROR
    assert "sometimes" in result.stdout.str() + result.stderr.str()


def test_blank_ini_policy_is_rejected(pytester: pytest.Pytester) -> None:
    """Fail loudly at configure time on an empty ini value, not from inside cleanup.

    The aborted session never reaches `pytest_sessionstart`, so nothing marks it as a
    started run and cleanup discards the bootstrap directory. Without that distinction a
    developer fixing an ini typo would accumulate one full run root per attempt, in RAM
    on a `/dev/shm` host, with nothing inside worth reading.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    pytester.makeini("[pytest]\nairflow_home_retention_policy =\n")
    pytester.makepyfile(test_suite=_PASSING_SUITE)
    before = _run_roots(pytester)

    result = pytester.runpytest_subprocess()

    assert result.ret == pytest.ExitCode.USAGE_ERROR
    output = result.stdout.str() + result.stderr.str()
    assert "`airflow_home_retention_policy` must be `all`, `failed`, or `none`" in output
    assert "INTERNALERROR" not in output
    assert "Retained AIRFLOW_HOME" not in output
    assert _run_roots(pytester) == before


def test_doctor_honors_an_explicit_retention_request(pytester: pytest.Pytester) -> None:
    """Keep the diagnostic run's own directory when `--airflow-home-retention=all` asks.

    `--airflow-doctor` short-circuits before `pytest_configure`, so the policy is
    resolved from `pytest_cmdline_main` instead. Without that the flag's documented
    "always keep" would be silently ignored on exactly the invocation that just printed
    the path.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
    """

    pytester.makepyfile(test_suite=_PASSING_SUITE)
    before = _run_roots(pytester)

    kept = pytester.runpytest_subprocess("--airflow-doctor", "--airflow-home-retention=all")
    assert kept.ret == 0
    assert len(_run_roots(pytester) - before) == 1

    discarded = pytester.runpytest_subprocess("--airflow-doctor", "--airflow-home-retention=none")
    assert discarded.ret == 0
    assert len(_run_roots(pytester) - before) == 1


@pytest.mark.parametrize("arguments", [("--help",), ("--markers",), ("--nonexistent-flag",)])
def test_an_invocation_that_starts_no_session_keeps_nothing(
    pytester: pytest.Pytester, arguments: tuple[str, ...]
) -> None:
    """Discard the bootstrap directory for invocations that never run a session.

    `--help`, `--markers`, and an argparse usage error all provision a run root during
    `pytest_load_initial_conftests` and none of them starts a session. Reading an absent
    outcome as a failure would retain every one of them, forever.

    Parameters:
        pytester: pytest.Pytester running the generated suite in a subprocess.
        arguments: tuple[str, ...] containing the non-session invocation under test.
    """

    pytester.makepyfile(test_suite=_PASSING_SUITE)
    before = _run_roots(pytester)

    result = pytester.runpytest_subprocess(*arguments)

    assert "Retained AIRFLOW_HOME" not in result.stdout.str() + result.stderr.str()
    assert _run_roots(pytester) == before
