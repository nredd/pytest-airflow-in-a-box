"""Bound process-global task side effects to one in-process execution.

References:
    https://docs.python.org/3/library/os.html#os.fchdir
    https://docs.python.org/3/library/os.html#os.environ
"""

from __future__ import annotations

import os
from collections.abc import Generator
from contextlib import contextmanager


@contextmanager
def isolated_process_state() -> Generator[None]:
    """Restore cwd and environment after execution, including exceptional exits.

    A directory descriptor survives a task renaming the original working directory.
    This scopes synchronous execution, not concurrent mutations from other threads.
    Restoration errors propagate rather than leaving a silently contaminated worker.

    Yields:
        None while task code owns the process cwd and environment.
    """

    env = dict(os.environ)
    cwd_fd = os.open(".", os.O_RDONLY)
    try:
        yield
    finally:
        try:
            os.fchdir(cwd_fd)
        finally:
            os.close(cwd_fd)
            os.environ.clear()
            os.environ.update(env)
