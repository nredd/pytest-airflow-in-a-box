"""Exec the API server with a Linux parent-death signal, without ``preexec_fn``.

This interpreter is single-threaded when calling ``prctl``; a pytest worker need
not be. The signal survives exec, and checking the expected parent after arming
it closes the race where the worker dies before ``prctl`` runs. Airflow runs
with one uvicorn worker, in this process rather than a multiprocess supervisor.

References:
    https://man7.org/linux/man-pages/man2/PR_SET_PDEATHSIG.2const.html
    https://docs.python.org/3/library/subprocess.html#subprocess.Popen
"""

from __future__ import annotations

import ctypes
import os
import signal
import sys


def main(parent_pid: int, command: list[str]) -> None:
    """Arm Linux parent-death handling and replace this process with the server.

    Parameters:
        parent_pid: int identifying the owning pytest worker before spawning.
        command: list[str] containing the absolute executable and its arguments.

    Raises:
        OSError: Linux rejected the parent-death signal, or exec failed.
    """

    if sys.platform == "linux":
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0:  # PR_SET_PDEATHSIG
            errno = ctypes.get_errno()
            raise OSError(errno, os.strerror(errno))
        if os.getppid() != parent_pid:
            os.kill(os.getpid(), signal.SIGKILL)
    os.execv(command[0], command)


if __name__ == "__main__":
    main(int(sys.argv[1]), sys.argv[2:])
