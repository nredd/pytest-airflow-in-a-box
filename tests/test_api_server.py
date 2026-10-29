"""Exercise the API server exec boundary and Linux worker-death regression.

References:
    https://man7.org/linux/man-pages/man2/PR_SET_PDEATHSIG.2const.html
"""

from __future__ import annotations

import contextlib
import errno
import os
import runpy
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from typing import Any

import psutil
import pytest

from pytest_airflow_in_a_box import _api_server


@pytest.mark.parametrize("platform", ["linux", "darwin"])
@pytest.mark.parametrize("parent_pid", [100, 101])
def test_exec_arms_parent_death_before_replacing_process(
    monkeypatch: pytest.MonkeyPatch, platform: str, parent_pid: int
) -> None:
    """Arm Linux SIGKILL before exec and detect a worker lost during startup."""

    calls: list[Any] = []

    class Libc:
        """Record the Linux system call without signalling the test process."""

        def prctl(self, *args: int) -> int:
            """Accept the signal configuration and record its exact arguments."""

            calls.append(args)
            return 0

    monkeypatch.setattr(_api_server.sys, "platform", platform)
    monkeypatch.setattr(_api_server.ctypes, "CDLL", lambda *_a, **_k: Libc())
    monkeypatch.setattr(_api_server.os, "getppid", lambda: 100)
    monkeypatch.setattr(_api_server.os, "getpid", lambda: 200)
    monkeypatch.setattr(_api_server.os, "kill", lambda *args: calls.append(args))
    monkeypatch.setattr(_api_server.os, "execv", lambda *args: calls.append(args))
    command = [sys.executable, "-m", "airflow"]

    _api_server.main(parent_pid, command)

    expected: list[Any] = []
    if platform == "linux":
        expected.append((1, signal.SIGKILL, 0, 0, 0))
        if parent_pid != 100:
            expected.append((200, signal.SIGKILL))
    expected.append((sys.executable, command))
    assert calls == expected


def test_exec_refuses_to_launch_without_parent_death_protection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Propagate prctl failures rather than silently launching an orphanable server."""

    class Libc:
        """Reject the system call as a restricted Linux environment might."""

        def prctl(self, *_args: int) -> int:
            """Return a failing syscall result."""

            return -1

    monkeypatch.setattr(_api_server.sys, "platform", "linux")
    monkeypatch.setattr(_api_server.ctypes, "CDLL", lambda *_a, **_k: Libc())
    monkeypatch.setattr(_api_server.ctypes, "get_errno", lambda: errno.EPERM)

    with pytest.raises(OSError, match="Operation not permitted"):
        _api_server.main(os.getpid(), [sys.executable])


def test_module_entrypoint_passes_parent_and_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Parse the worker PID separately from the untouched server argument vector."""

    calls: list[Any] = []
    monkeypatch.setattr(sys, "argv", ["wrapper", "100", sys.executable, "-m", "airflow"])
    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(os, "execv", lambda *args: calls.append(args))

    runpy.run_path(str(_api_server.__file__), run_name="__main__")

    assert calls == [(sys.executable, [sys.executable, "-m", "airflow"])]


def _wait_for_pid(path: Path, worker: subprocess.Popen[bytes]) -> int:
    """Wait boundedly for the server stand-in to publish its PID.

    Parameters:
        path: Path containing the server readiness file.
        worker: subprocess.Popen[bytes] owning the server.

    Returns:
        int identifying the live server.
    """

    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        assert worker.poll() is None, "Worker exited before starting its API server"
        if path.exists() and (content := path.read_text()):
            return int(content)
        time.sleep(0.05)
    pytest.fail("Server did not publish its PID")


@pytest.mark.skipif(sys.platform != "linux", reason="PR_SET_PDEATHSIG is Linux-specific")
def test_sigkilled_worker_does_not_leave_its_api_server_or_kill_a_sibling(
    tmp_path: Path,
) -> None:
    """SIGKILL a worker using the real launcher; only its server must stop.

    The command boundary substitutes a tiny ``airflow`` executable module so
    this regression measures process ownership, not database/bootstrap latency.
    """

    workers: list[subprocess.Popen[bytes]] = []
    servers: list[int] = []
    script = textwrap.dedent("""\
        import sys
        import time
        from pathlib import Path
        from types import SimpleNamespace
        from pytest_airflow_in_a_box.fixtures import api
        root = Path(sys.argv[1])
        api._server_responds = lambda url: (root / 'ready').exists()
        launcher = api._launch_api_server(SimpleNamespace(logs_folder=root))
        next(launcher)
        time.sleep(60)
    """)
    try:
        for index in range(2):
            root = tmp_path / str(index)
            package = root / "airflow"
            package.mkdir(parents=True)
            (package / "__init__.py").write_text("")
            (package / "__main__.py").write_text(
                "import os, time\nfrom pathlib import Path\n"
                "Path('ready').write_text(str(os.getpid()))\ntime.sleep(60)\n"
            )
            worker = subprocess.Popen([sys.executable, "-c", script, str(root)], cwd=root)
            workers.append(worker)
            servers.append(_wait_for_pid(root / "ready", worker))
        workers[0].kill()
        workers[0].wait(timeout=10)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if (
                not psutil.pid_exists(servers[0])
                or psutil.Process(servers[0]).status() == psutil.STATUS_ZOMBIE
            ):
                break
            time.sleep(0.05)
        else:
            pytest.fail("API server survived its SIGKILLed worker")
        assert workers[1].poll() is None
        assert psutil.Process(servers[1]).status() != psutil.STATUS_ZOMBIE
    finally:
        for worker in workers:
            worker.kill()
            worker.wait(timeout=10)
        for pid in servers:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(pid, signal.SIGKILL)
