"""The `airflow-migration-diff` console-script orchestrator (issue #44).

Provisions Airflow 2.x and 3.x environments with `uv`, records outcomes on each through
issue #42's `--airflow-record`/`--airflow-baseline` contract, and prints the categorized
migration diff. This subpackage is independent of the installed-plugin fixture surface
and is never imported by `plugin.py`; it is reached only through the
`airflow-migration-diff` console script (`migration.cli:main`).

References:
    https://github.com/nredd/pytest-airflow-in-a-box/issues/44
"""

from __future__ import annotations

from pytest_airflow_in_a_box.migration.cli import (
    EXIT_OK,
    EXIT_REGRESSIONS,
    EXIT_TOOLING_ERROR,
    ResolvedConfig,
    build_parser,
    execute,
    main,
    resolve_config,
)
from pytest_airflow_in_a_box.migration.provision import (
    ProvisionedEnvironment,
    provision_environment,
)
from pytest_airflow_in_a_box.migration.render import render_diff
from pytest_airflow_in_a_box.migration.run import compute_diff, run_record
from pytest_airflow_in_a_box.migration.types import (
    ArtifactError,
    CategoryCounts,
    DiffResult,
    MigrationToolingError,
    PluginSpecResolutionError,
    ProvisioningError,
    RecordRunError,
    UvNotFoundError,
)

__all__ = (
    "EXIT_OK",
    "EXIT_REGRESSIONS",
    "EXIT_TOOLING_ERROR",
    "ArtifactError",
    "CategoryCounts",
    "DiffResult",
    "MigrationToolingError",
    "PluginSpecResolutionError",
    "ProvisionedEnvironment",
    "ProvisioningError",
    "RecordRunError",
    "ResolvedConfig",
    "UvNotFoundError",
    "build_parser",
    "compute_diff",
    "execute",
    "main",
    "provision_environment",
    "render_diff",
    "resolve_config",
    "run_record",
)
