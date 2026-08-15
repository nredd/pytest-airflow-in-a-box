"""Console entry point: `airflow-migration-diff`.

Provisions a disposable Airflow 2.x environment and a disposable Airflow 3.x
environment with `uv`, records outcomes on each (`--airflow-record`, issue #42), and
prints the categorized migration diff. Exit code 0 means no regressions, 1 means
regressions were found, and 2 means the orchestrator itself failed -- `uv` missing, a
provisioning failure, a record run that crashed before writing its artifact, or an
absent/malformed/incomplete artifact. A tooling failure (2) and a clean regression-free
run (0) are both "not migration outcomes"; only 1 describes the migration itself.

References:
    https://docs.python.org/3/library/argparse.html
    https://github.com/nredd/pytest-airflow-in-a-box/issues/42
    https://github.com/nredd/pytest-airflow-in-a-box/issues/44
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from importlib.metadata import version as distribution_version
from pathlib import Path

from pytest_airflow_in_a_box._compat.capabilities import (
    MIN_V2_PYTHON,
    SUPPORTED_RELEASES_BY_FAMILY,
    AirflowFamily,
    max_v2_python,
)
from pytest_airflow_in_a_box.migration import provision, render, run
from pytest_airflow_in_a_box.migration.provision import ProvisionedEnvironment
from pytest_airflow_in_a_box.migration.types import (
    ComputeCategoryDiff,
    MigrationToolingError,
    ProvisioningError,
    UvNotFoundError,
)

EXIT_OK = 0
EXIT_REGRESSIONS = 1
EXIT_TOOLING_ERROR = 2

WORK_DIR_PREFIX = "airflow-migration-diff-"
# The plugin has no `apache-airflow-core` release above 3.x's certified ceiling to fall
# back to, so an out-of-range interpreter (this package's own 3.10-3.14 support matrix)
# falls back to a known-good 3.12. The 2.x default clamps to the requested release's own
# bounds instead, because 2.7/2.8 cap at 3.11 and 2.9+ reach 3.12.
_FALLBACK_PYTHON = "3.12"
_MIN_V3_PYTHON = (3, 10)
_MAX_V3_PYTHON = (3, 14)


@dataclass(frozen=True)
class ResolvedConfig:
    """Every `airflow-migration-diff` setting after defaulting and validation.

    Parameters:
        project_dir: pathlib.Path containing the consumer project to test.
        work_dir: pathlib.Path containing this run's fresh scratch directory.
        keep_work_dir: bool indicating whether to skip removing `work_dir` on exit.
        airflow2_version: str containing the Airflow 2.x release to install.
        airflow3_version: str containing the Airflow 3.x release to install.
        python_airflow2: str containing the `X.Y` Python version for the 2.x venv.
        python_airflow3: str containing the `X.Y` Python version for the 3.x venv.
        plugin_spec: str containing the `uv pip install` spec for the plugin itself.
        plugin_spec_is_default: bool indicating whether `plugin_spec` was defaulted.
        uv_path: pathlib.Path containing the resolved `uv` executable.
        baseline_artifact: pathlib.Path the 2.x record run writes to.
        live_artifact: pathlib.Path the 3.x record run writes to.
        allow_incomplete_baseline: bool overriding the incomplete-baseline error.
        allow_incomplete_live: bool overriding the incomplete-live error.
        pytest_args: tuple[str, ...] forwarded verbatim to both record invocations.
    """

    project_dir: Path
    work_dir: Path
    keep_work_dir: bool
    airflow2_version: str
    airflow3_version: str
    python_airflow2: str
    python_airflow3: str
    plugin_spec: str
    plugin_spec_is_default: bool
    uv_path: Path
    baseline_artifact: Path
    live_artifact: Path
    allow_incomplete_baseline: bool
    allow_incomplete_live: bool
    pytest_args: tuple[str, ...]


def _split_pytest_args(argv: Sequence[str]) -> tuple[list[str], list[str]]:
    """Split raw argv into this program's own arguments and forwarded pytest arguments.

    Parameters:
        argv: Sequence[str] containing the raw command-line arguments.

    Returns:
        tuple[list[str], list[str]] containing the program's own arguments, then every
        argument after a literal `--` (empty when no `--` is present).
    """

    argv = list(argv)
    if "--" in argv:
        index = argv.index("--")
        return argv[:index], argv[index + 1 :]
    return argv, []


def _max_release(family: AirflowFamily) -> str:
    """Return the newest certified release for one Airflow family.

    Parameters:
        family: AirflowFamily selecting `SUPPORTED_RELEASES_BY_FAMILY`'s row.

    Returns:
        str containing the dot-joined newest certified release.
    """

    release = max(SUPPORTED_RELEASES_BY_FAMILY[family])
    return ".".join(str(part) for part in release)


def _default_python_v2(current: tuple[int, int], airflow2_version: str) -> str:
    """Default `--python-airflow2` to the current interpreter, clamped for that release.

    The ceiling is the requested release's own, not the family's: 2.7.3 and 2.8.4 cap at
    3.11 and publish no 3.12 constraints file, so a family-wide clamp would hand a 3.12
    interpreter straight through and fail deep inside provisioning.

    Parameters:
        current: tuple[int, int] containing the running interpreter's `(major, minor)`.
        airflow2_version: str containing the already-defaulted 2.x release to install.

    Returns:
        str containing the current interpreter's `X.Y` version when it falls inside
        `[MIN_V2_PYTHON, max_v2_python(airflow2_version)]`, else the nearer bound. A
        clamp rather than one shared fallback constant, because there is no single
        version every certified 2.x release accepts.
    """

    maximum = max_v2_python(airflow2_version)
    if current > maximum:
        return f"{maximum[0]}.{maximum[1]}"
    if current < MIN_V2_PYTHON:
        return f"{MIN_V2_PYTHON[0]}.{MIN_V2_PYTHON[1]}"
    return f"{current[0]}.{current[1]}"


def _default_python_v3(current: tuple[int, int]) -> str:
    """Default `--python-airflow3` to the current interpreter within its support matrix.

    Parameters:
        current: tuple[int, int] containing the running interpreter's `(major, minor)`.

    Returns:
        str containing the current interpreter's `X.Y` version when it falls inside
        `[3.10, 3.14]`, else the `_FALLBACK_PYTHON` constant.
    """

    if _MIN_V3_PYTHON <= current <= _MAX_V3_PYTHON:
        return f"{current[0]}.{current[1]}"
    return _FALLBACK_PYTHON


def _default_plugin_spec() -> str:
    """Default `--plugin-spec` to this checkout's installed plugin version.

    Returns:
        str containing a pinned `uv pip install` spec for the installed version.
    """

    return f"pytest-airflow-in-a-box=={distribution_version('pytest-airflow-in-a-box')}"


def _default_make_temp_dir() -> str:
    """Create a fresh temporary base work directory.

    Returns:
        str containing the created directory's path.
    """

    return tempfile.mkdtemp(prefix=WORK_DIR_PREFIX)


def build_parser() -> argparse.ArgumentParser:
    """Build the `airflow-migration-diff` argument parser.

    Returns:
        argparse.ArgumentParser accepting every documented option.
    """

    parser = argparse.ArgumentParser(
        prog="airflow-migration-diff",
        description=(
            "Provision Airflow 2.x and 3.x environments with uv, record outcomes on "
            "each, and print the categorized migration diff. Pass pytest arguments "
            "after a literal `--`."
        ),
    )
    parser.add_argument(
        "--project-dir",
        type=Path,
        default=None,
        help="Consumer project to test, installed into both environments. Default: cwd.",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=None,
        help="Scratch directory base; a fresh run subdirectory is created inside it. "
        "Default: a new temporary directory.",
    )
    parser.add_argument(
        "--keep-work-dir",
        action="store_true",
        help="Do not remove the run's scratch directory on exit.",
    )
    parser.add_argument(
        "--airflow2-version",
        default=None,
        help="Airflow 2.x release to install. Default: the newest certified 2.x release.",
    )
    parser.add_argument(
        "--airflow3-version",
        default=None,
        help="Airflow 3.x release to install. Default: the newest certified 3.x release.",
    )
    parser.add_argument(
        "--python-airflow2",
        default=None,
        help="Python `X.Y` version for the 2.x environment. Default: the current "
        "interpreter, clamped to the 2.x-supported range.",
    )
    parser.add_argument(
        "--python-airflow3",
        default=None,
        help="Python `X.Y` version for the 3.x environment. Default: the current "
        "interpreter, when supported.",
    )
    parser.add_argument(
        "--plugin-spec",
        default=None,
        help="`uv pip install` spec for pytest-airflow-in-a-box itself. Default: this "
        "checkout's installed version -- pass explicitly for an unreleased version.",
    )
    parser.add_argument(
        "--uv-path",
        type=Path,
        default=None,
        help="Path to the `uv` executable. Default: resolved from `$PATH`.",
    )
    parser.add_argument(
        "--baseline-artifact",
        type=Path,
        default=None,
        help="Path the 2.x record run writes to. Default: under the run's work directory.",
    )
    parser.add_argument(
        "--live-artifact",
        type=Path,
        default=None,
        help="Path the 3.x record run writes to. Default: under the run's work directory.",
    )
    parser.add_argument(
        "--allow-incomplete-baseline",
        action="store_true",
        help="Do not fail when the baseline artifact recorded `complete: false`.",
    )
    parser.add_argument(
        "--allow-incomplete-live",
        action="store_true",
        help="Do not fail when the live artifact recorded `complete: false`.",
    )
    return parser


def _make_run_dir(base_work_dir: Path) -> Path:
    """Create this run's fresh, uniquely-suffixed scratch subdirectory.

    Parameters:
        base_work_dir: pathlib.Path containing the base work directory (already
            created, either user-supplied via `--work-dir` or freshly made by
            `make_temp_dir`).

    Returns:
        pathlib.Path containing the created per-run subdirectory.

    Raises:
        ProvisioningError: The directory could not be created (for example, a full
            disk or a permissions failure), so this never collides with
            `EXIT_REGRESSIONS` by escaping as a raw, exit-code-1 `OSError`.
    """

    run_dir = base_work_dir / f"run-{uuid.uuid4().hex[:8]}"
    try:
        run_dir.mkdir(parents=True)
    except OSError as error:
        raise ProvisioningError(
            f"Could not create the run's work directory '{run_dir}': {error}"
        ) from error
    return run_dir


def resolve_config(
    args: argparse.Namespace,
    pytest_args: Sequence[str],
    *,
    which: Callable[[str], str | None] = shutil.which,
    make_temp_dir: Callable[[], str] = _default_make_temp_dir,
    current_python: tuple[int, int] | None = None,
) -> ResolvedConfig:
    """Resolve every default and validate `uv` availability.

    Parameters:
        args: argparse.Namespace containing the parsed command-line options.
        pytest_args: Sequence[str] to forward verbatim to both record invocations.
        which: Callable resolving an executable name to a path, or None when absent.
        make_temp_dir: Callable creating a fresh temporary base work directory.
        current_python: tuple[int, int] | None containing the running interpreter's
            `(major, minor)`, or None to read `sys.version_info`.

    Returns:
        ResolvedConfig containing every defaulted and validated setting.

    Raises:
        UvNotFoundError: No `uv` executable was resolved.
    """

    resolved_python = current_python or (sys.version_info.major, sys.version_info.minor)
    project_dir = (args.project_dir or Path.cwd()).resolve()

    if args.uv_path is not None:
        uv_path = args.uv_path.resolve()
    else:
        which_uv = which("uv")
        if which_uv is None:
            raise UvNotFoundError(
                "No `uv` executable found on `$PATH`; install uv "
                "(https://docs.astral.sh/uv/) or pass --uv-path."
            )
        uv_path = Path(which_uv).resolve()

    if args.work_dir is not None:
        base_work_dir = args.work_dir.resolve()
        try:
            base_work_dir.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise ProvisioningError(
                f"Could not create --work-dir '{base_work_dir}': {error}"
            ) from error
    else:
        base_work_dir = Path(make_temp_dir()).resolve()
    work_dir = _make_run_dir(base_work_dir)

    plugin_spec_is_default = args.plugin_spec is None
    baseline_artifact = args.baseline_artifact or (work_dir / "baseline.json")
    live_artifact = args.live_artifact or (work_dir / "live.json")
    airflow2_version = args.airflow2_version or _max_release(AirflowFamily.V2)

    return ResolvedConfig(
        project_dir=project_dir,
        work_dir=work_dir,
        keep_work_dir=args.keep_work_dir,
        airflow2_version=airflow2_version,
        airflow3_version=args.airflow3_version or _max_release(AirflowFamily.V3),
        python_airflow2=args.python_airflow2
        or _default_python_v2(resolved_python, airflow2_version),
        python_airflow3=args.python_airflow3 or _default_python_v3(resolved_python),
        plugin_spec=args.plugin_spec or _default_plugin_spec(),
        plugin_spec_is_default=plugin_spec_is_default,
        uv_path=uv_path,
        baseline_artifact=baseline_artifact.resolve(),
        live_artifact=live_artifact.resolve(),
        allow_incomplete_baseline=args.allow_incomplete_baseline,
        allow_incomplete_live=args.allow_incomplete_live,
        pytest_args=tuple(pytest_args),
    )


def execute(
    cfg: ResolvedConfig,
    *,
    provision_environment: Callable[..., ProvisionedEnvironment] = provision.provision_environment,
    run_record: Callable[..., None] = run.run_record,
    compute_category_diff: ComputeCategoryDiff = run.default_compute_category_diff,
) -> tuple[int, str]:
    """Run the full provision -> record -> categorize -> render pipeline.

    Parameters:
        cfg: ResolvedConfig containing every defaulted and validated setting.
        provision_environment: Callable provisioning one `uv` virtual environment.
        run_record: Callable running one `pytest --airflow-record` invocation.
        compute_category_diff: ComputeCategoryDiff loading and categorizing both
            recorded artifacts.

    Returns:
        tuple[int, str] containing the process exit code (`EXIT_OK` or
        `EXIT_REGRESSIONS`) and the rendered report text.

    Raises:
        MigrationToolingError: Provisioning, recording, or category computation failed.
    """

    baseline_env = provision_environment(
        family=AirflowFamily.V2,
        airflow_version=cfg.airflow2_version,
        python_version=cfg.python_airflow2,
        plugin_spec=cfg.plugin_spec,
        plugin_spec_is_default=cfg.plugin_spec_is_default,
        project_dir=cfg.project_dir,
        work_dir=cfg.work_dir,
        uv_path=cfg.uv_path,
    )
    live_env = provision_environment(
        family=AirflowFamily.V3,
        airflow_version=cfg.airflow3_version,
        python_version=cfg.python_airflow3,
        plugin_spec=cfg.plugin_spec,
        plugin_spec_is_default=cfg.plugin_spec_is_default,
        project_dir=cfg.project_dir,
        work_dir=cfg.work_dir,
        uv_path=cfg.uv_path,
    )

    run_record(
        env=baseline_env,
        project_dir=cfg.project_dir,
        record_path=cfg.baseline_artifact,
        baseline_path=None,
        pytest_args=cfg.pytest_args,
    )
    run_record(
        env=live_env,
        project_dir=cfg.project_dir,
        record_path=cfg.live_artifact,
        baseline_path=cfg.baseline_artifact,
        pytest_args=cfg.pytest_args,
    )

    diff = run.compute_diff(
        baseline_path=cfg.baseline_artifact,
        live_path=cfg.live_artifact,
        allow_incomplete_baseline=cfg.allow_incomplete_baseline,
        allow_incomplete_live=cfg.allow_incomplete_live,
        compute_category_diff=compute_category_diff,
    )

    report = render.render_diff(diff, baseline_env=baseline_env, live_env=live_env)
    exit_code = EXIT_REGRESSIONS if diff.has_regressions else EXIT_OK
    return exit_code, report


def main(argv: list[str] | None = None) -> int:
    """Run the `airflow-migration-diff` console entry point.

    Parameters:
        argv: list[str] | None containing raw arguments, or None to read `sys.argv[1:]`.

    Returns:
        int containing the process exit code: `EXIT_OK`, `EXIT_REGRESSIONS`, or
        `EXIT_TOOLING_ERROR`.
    """

    raw_argv = list(argv) if argv is not None else sys.argv[1:]
    own_argv, pytest_args = _split_pytest_args(raw_argv)
    args = build_parser().parse_args(own_argv)

    try:
        cfg = resolve_config(args, pytest_args)
    except MigrationToolingError as error:
        sys.stderr.write(f"airflow-migration-diff: {error}\n")
        return EXIT_TOOLING_ERROR
    except Exception as error:
        # Broad on purpose: resolving configuration must never escape as a raw,
        # unhandled exception. Python's default unhandled-exception exit status is 1 --
        # identical to EXIT_REGRESSIONS -- so any exception here, however unexpected,
        # is reported as a tooling failure rather than silently misread as "a migration
        # regression was found." No `--work-dir` was necessarily created yet at every
        # point in `resolve_config`, so there is nothing to clean up here.
        sys.stderr.write(
            f"airflow-migration-diff: unexpected failure resolving configuration: {error}\n"
        )
        return EXIT_TOOLING_ERROR

    try:
        exit_code, report = execute(cfg)
    except MigrationToolingError as error:
        sys.stderr.write(f"airflow-migration-diff: {error}\n")
        return EXIT_TOOLING_ERROR
    except Exception as error:
        # Broad on purpose, same rationale as above: an unexpected exception here must
        # never collide with EXIT_REGRESSIONS by falling through to Python's default
        # unhandled-exception exit status of 1.
        sys.stderr.write(f"airflow-migration-diff: unexpected failure: {error}\n")
        return EXIT_TOOLING_ERROR
    else:
        sys.stdout.write(report)
        return exit_code
    finally:
        if not cfg.keep_work_dir:
            shutil.rmtree(cfg.work_dir, ignore_errors=True)


__all__ = (
    "EXIT_OK",
    "EXIT_REGRESSIONS",
    "EXIT_TOOLING_ERROR",
    "ResolvedConfig",
    "build_parser",
    "execute",
    "main",
    "resolve_config",
)
