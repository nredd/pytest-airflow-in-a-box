"""Install the coverage subprocess-startup hook into the active environment.

The written ``.pth`` file runs ``coverage.process_startup()`` in every Python
process started with this environment. That call is a no-op unless the
``COVERAGE_PROCESS_START`` environment variable points at a coverage
configuration, so the hook is safe to leave installed permanently; with it,
subprocess pytester runs contribute their own suffixed data files.

References:
    https://coverage.readthedocs.io/en/latest/subprocess.html
    https://docs.python.org/3/library/site.html
"""

from __future__ import annotations

import logging
import sysconfig
from pathlib import Path

LOGGER = logging.getLogger(__name__)

PTH_FILE_NAME = "pytest_airflow_in_a_box_coverage.pth"
PTH_CONTENTS = "import coverage; coverage.process_startup()\n"


def install_coverage_pth() -> Path:
    """Write the subprocess-startup hook into this environment's site-packages.

    Returns:
        pathlib.Path containing the installed ``.pth`` file.

    Raises:
        OSError: The site-packages directory is not writable.
    """

    purelib = Path(sysconfig.get_paths()["purelib"]).resolve()
    pth_path = purelib / PTH_FILE_NAME
    if pth_path.is_file() and pth_path.read_text(encoding="utf-8") == PTH_CONTENTS:
        LOGGER.info(f"Coverage subprocess hook already installed: '{pth_path}'")
        return pth_path
    pth_path.write_text(PTH_CONTENTS, encoding="utf-8")
    LOGGER.info(f"Installed coverage subprocess hook: '{pth_path}'")
    return pth_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    install_coverage_pth()
