"""Child-side reporter for `airflow_isolated` batches.

Loaded explicitly (``-p pytest_airflow_in_a_box.isolated_child``) in the one-shot child
process an `airflow_isolated` batch spawns, never by entry point. It serializes every
runtest-phase report with pytest's own ``TestReport._to_json`` -- the serializer
pytest-xdist rides on, version-matched here because parent and child share one
interpreter and one pytest -- and writes a single JSON envelope at session finish for
the parent to replay.

References:
    https://docs.pytest.org/en/stable/reference/reference.html#pytest.TestReport
    https://github.com/nredd/pytest-airflow-in-a-box/issues/115
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import pytest

LOGGER = logging.getLogger(__name__)

ISOLATED_REPORT_PATH_ENVIRONMENT_VARIABLE = "PYTEST_AIRFLOW_IN_A_BOX_ISOLATED_REPORT_PATH"
REPORT_FORMAT = "pytest-airflow-in-a-box-isolated-report"
REPORT_VERSION = 1
_WRITER_PLUGIN_NAME = "pytest-airflow-in-a-box-isolated-writer"


class IsolatedReportWriter:
    """Accumulate serialized phase reports and write one envelope at session finish."""

    def __init__(self, path: Path) -> None:
        """Bind the writer to its envelope destination.

        Parameters:
            path: pathlib.Path receiving the JSON report envelope.
        """

        self._path = path
        self._reports: list[dict[str, object]] = []

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        """Serialize one setup/call/teardown phase report.

        Parameters:
            report: pytest.TestReport for one runtest phase.
        """

        self._reports.append(report._to_json())

    def pytest_sessionfinish(self, session: pytest.Session, exitstatus: int) -> None:
        """Write the report envelope for the parent process to replay.

        Parameters:
            session: pytest.Session that finished running the batch.
            exitstatus: int containing pytest's raw session exit status.

        Raises:
            pytest.UsageError: The envelope destination cannot be created or written.
        """

        del session
        envelope = {
            "format": REPORT_FORMAT,
            "version": REPORT_VERSION,
            "exit_status": int(exitstatus),
            "reports": self._reports,
        }
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            # `default=repr` keeps a non-JSON-serializable report payload -- e.g. a
            # `record_property` value holding an arbitrary object, legal in plain
            # pytest -- from crashing the whole batch; the value degrades to its repr.
            self._path.write_text(
                json.dumps(envelope, indent=2, sort_keys=True, default=repr) + "\n",
                encoding="utf-8",
            )
        except OSError as error:
            raise pytest.UsageError(
                f"Could not write isolated report `{self._path}`: {error}"
            ) from error
        LOGGER.debug(f"Wrote isolated report envelope: '{self._path}'")


def pytest_configure(config: pytest.Config) -> None:
    """Register the report writer against the destination named in the environment.

    Parameters:
        config: pytest.Config for the isolated child session.

    Raises:
        pytest.UsageError: The report destination environment variable is unset.
    """

    destination = os.environ.get(ISOLATED_REPORT_PATH_ENVIRONMENT_VARIABLE)
    if not destination:
        raise pytest.UsageError(
            f"`{ISOLATED_REPORT_PATH_ENVIRONMENT_VARIABLE}` must name the isolated report "
            "destination; this plugin is only loaded by `airflow_isolated` child processes"
        )
    config.pluginmanager.register(IsolatedReportWriter(Path(destination)), _WRITER_PLUGIN_NAME)


__all__ = (
    "ISOLATED_REPORT_PATH_ENVIRONMENT_VARIABLE",
    "REPORT_FORMAT",
    "REPORT_VERSION",
    "IsolatedReportWriter",
    "pytest_configure",
)
