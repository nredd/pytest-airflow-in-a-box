"""Construct Apache Airflow Dag bags across certified releases.

Airflow is imported only after bootstrap and capability validation are complete.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html
"""

from __future__ import annotations

import time
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

from pytest_airflow_in_a_box._compat.capabilities import DagBagLocation, resolve_capabilities
from pytest_airflow_in_a_box._compat.parse_time import parse_time_supervision

if TYPE_CHECKING:
    from collections.abc import Sequence

    from airflow.models.dagbag import DagBag

    from pytest_airflow_in_a_box._compat.parse_time import ParseTimeComms


class DagBagConstructionError(RuntimeError):
    """Report failure to construct a Dag bag from a validated directory."""


class DagBagShardStat(NamedTuple):
    """One imported file's parse timing, duck-type compatible with Airflow's own stat shape.

    Parameters:
        file: str containing the file path relative to the shard's Dag folder.
        duration: datetime.timedelta measuring the file's import wall time.
        dag_num: int counting Dags bagged from the file.
        task_num: int counting tasks across those Dags.
    """

    file: str
    duration: timedelta
    dag_num: int
    task_num: int


def _validate_dag_location(path: str | Path) -> Path:
    """Resolve and validate a Dag location shared by every construction entry point.

    Parameters:
        path: str | pathlib.Path naming a candidate Dag directory or Dag file.

    Returns:
        pathlib.Path containing the resolved, validated location.

    Raises:
        FileNotFoundError: The resolved Dag location does not exist.
        ValueError: The resolved Dag location is neither a directory nor a file.
    """

    resolved_path = Path(str(path)).resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Dag location does not exist: '{resolved_path}'")
    if not resolved_path.is_dir() and not resolved_path.is_file():
        raise ValueError(f"Dag location is neither a directory nor a file: '{resolved_path}'")
    return resolved_path


def _build_models_dag_bag(path: Path, *, include_examples: bool) -> DagBag:
    """Construct a Dag bag through Airflow 3.1's canonical module.

    Parameters:
        path: pathlib.Path containing Dag files.
        include_examples: bool indicating constructor support for example control.

    Returns:
        airflow.models.dagbag.DagBag containing parsed Dags and import errors.
    """

    # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
    from airflow.models.dagbag import DagBag

    if include_examples:
        # Airflow 3.3 removes the second parameter, so a positional call keeps static
        # checking valid against every supported installed release.
        return DagBag(path, False, False, False)
    return DagBag(dag_folder=path, safe_mode=False, load_op_links=False)


def _build_dag_processing_dag_bag(path: Path, *, include_examples: bool) -> DagBag:
    """Construct a Dag bag through Airflow 3.2 and newer's canonical module.

    Parameters:
        path: pathlib.Path containing Dag files.
        include_examples: bool indicating constructor support for example control.

    Returns:
        airflow.models.dagbag.DagBag containing parsed Dags and import errors.
    """

    # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
    from airflow.dag_processing.dagbag import DagBag

    if include_examples:
        # Airflow 3.3 removes the second parameter, so a positional call keeps static
        # checking valid against every supported installed release.
        return DagBag(path, False, False, False)
    return DagBag(dag_folder=path, safe_mode=False, load_op_links=False)


def build_dag_bag(path: str | Path, *, comms: ParseTimeComms | None = None) -> DagBag:
    """Validate a Dag location and parse it with the certified Airflow interface.

    This is the one place the plugin executes user Dag modules, so it is also where
    parse-time Variable and Connection resolution is installed.

    Parameters:
        path: str | pathlib.Path naming an existing Dag directory or Dag file.
        comms: ParseTimeComms | None answering Variable and Connection lookups that
            run at Dag top level, or None to leave Airflow's own resolution untouched.

    Returns:
        airflow.models.dagbag.DagBag containing parsed Dags and import errors.

    Raises:
        FileNotFoundError: The resolved Dag location does not exist.
        ValueError: The resolved Dag location is neither a directory nor a file.
        AirflowCompatibilityError: The installed Airflow interface is unsupported.
        DagBagConstructionError: Airflow cannot construct the Dag bag.
    """

    resolved_path = _validate_dag_location(path)
    capabilities = resolve_capabilities()
    # The supervision block wraps the try rather than the reverse: its own setup and
    # teardown are not Dag construction, and relabeling them `DagBagConstructionError`
    # would blame the Dag folder for a failure that has nothing to do with it.
    with parse_time_supervision(comms):
        try:
            if capabilities.dag_bag_location is DagBagLocation.MODELS:
                return _build_models_dag_bag(
                    resolved_path,
                    include_examples=capabilities.dag_bag_supports_include_examples,
                )
            return _build_dag_processing_dag_bag(
                resolved_path,
                include_examples=capabilities.dag_bag_supports_include_examples,
            )
        except Exception as error:
            raise DagBagConstructionError(
                f"Could not construct an Airflow Dag bag from '{resolved_path}': {error}"
            ) from error


def list_dag_file_paths(path: str | Path, *, safe_mode: bool = False) -> list[str]:
    """Walk one Dag location for importable files, honoring ``.airflowignore``, without importing.

    Dispatches on the same certified module split `build_dag_bag` uses: Airflow 3.2's
    ``DagBag.collect_dags()`` discovers files through
    ``get_importer_registry().list_dag_files()`` (extensible to a plugin-registered Dag
    importer), while 3.1.x's discovers them through ``airflow.utils.file.list_py_file_paths``
    (Python and zip files only, no importer registry). Calling the exact function each
    release's own ``collect_dags()`` calls internally keeps this walk byte-identical to what
    a single-process serial parse would discover: ``list_py_file_paths`` still exists and
    still runs unmodified on 3.2+ (confirmed against the installed 3.3.0, mirrored at
    ``airflow.utils.file.find_dag_file_paths``), but calling it unconditionally on every
    release would silently miss a file only a custom-registered importer surfaces there.

    Parameters:
        path: str | pathlib.Path naming an existing Dag directory or Dag file.
        safe_mode: bool matching `build_dag_bag`'s own hardcoded file-selection behavior;
            `True` here would silently narrow the file set fan-out sees relative to a
            serial parse.

    Returns:
        list[str] containing every discovered file path.

    Raises:
        FileNotFoundError: The resolved Dag location does not exist.
        ValueError: The resolved Dag location is neither a directory nor a file.
        AirflowCompatibilityError: The installed Airflow interface is unsupported.
    """

    resolved_path = _validate_dag_location(path)
    capabilities = resolve_capabilities()
    if capabilities.dag_bag_location is DagBagLocation.MODELS:
        # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
        from airflow.utils.file import list_py_file_paths

        return list_py_file_paths(resolved_path, safe_mode=safe_mode)
    from airflow.dag_processing.importers import get_importer_registry

    return get_importer_registry().list_dag_files(resolved_path, safe_mode=safe_mode)


def build_partial_dag_bag(
    dag_folder: str | Path,
    file_paths: Sequence[str],
    *,
    comms: ParseTimeComms | None = None,
) -> tuple[DagBag, tuple[DagBagShardStat, ...]]:
    """Construct one Dag bag rooted at `dag_folder`, importing only `file_paths`.

    `dag_folder` must be the true configured Dag location, not a subdirectory: a
    single, correct, root-rooted ``.airflowignore`` walk (`list_dag_file_paths`) is
    meant to feed disjoint shards of an already-correct file list into this function,
    and a subdirectory root would also compute wrong relative filelocs for the
    returned stats.

    Constructs the Dag bag with ``collect_dags=False`` (skips Airflow's own walk and
    import, but still stashes `dag_folder` for relative-path bookkeeping) via the same
    MODELS/DAG_PROCESSING dispatch `build_dag_bag` uses, then calls
    ``process_file(path, only_if_updated=True, safe_mode=False)`` for each path in
    `file_paths`, hand-building the stat Airflow's own ``collect_dags()`` would have
    built -- ``dagbag_stats`` is only ever populated inside that method, so a
    ``collect_dags=False`` instance never gets one. Each stat's `file` is always
    computed relative to `dag_folder` (Airflow 3.2's shape); that field is display-only
    data for slowpoke/budget reporting, never a correctness-critical merge key, so one
    consistent computation on every certified release is a deliberate simplification,
    not a compatibility gap.

    Parameters:
        dag_folder: str | pathlib.Path naming the Dag corpus's true root.
        file_paths: Sequence[str] naming this shard's files, from `list_dag_file_paths`.
        comms: ParseTimeComms | None answering Variable and Connection lookups that
            run at Dag top level, or None to leave Airflow's own resolution untouched.

    Returns:
        tuple[DagBag, tuple[DagBagShardStat, ...]] containing the Dag bag holding only
        `file_paths`' parsed Dags and import errors, and one stat per file in
        `file_paths`, in processing order.

    Raises:
        FileNotFoundError: The resolved Dag location does not exist.
        ValueError: The resolved Dag location is neither a directory nor a file.
        AirflowCompatibilityError: The installed Airflow interface is unsupported.
        DagBagConstructionError: Airflow cannot construct or populate the Dag bag.
    """

    resolved_path = _validate_dag_location(dag_folder)
    capabilities = resolve_capabilities()
    with parse_time_supervision(comms):
        try:
            if capabilities.dag_bag_location is DagBagLocation.MODELS:
                from airflow.models.dagbag import DagBag
            else:
                from airflow.dag_processing.dagbag import DagBag
            dag_bag = DagBag(
                dag_folder=resolved_path,
                safe_mode=False,
                load_op_links=False,
                collect_dags=False,
            )
            stats: list[DagBagShardStat] = []
            for file_path in file_paths:
                started = time.perf_counter()
                found_dags = dag_bag.process_file(file_path, only_if_updated=True, safe_mode=False)
                duration = timedelta(seconds=time.perf_counter() - started)
                try:
                    relative_file = Path(file_path).relative_to(resolved_path).as_posix()
                except ValueError:
                    relative_file = Path(file_path).as_posix()
                stats.append(
                    DagBagShardStat(
                        file=relative_file,
                        duration=duration,
                        dag_num=len(found_dags),
                        task_num=sum(len(dag.tasks) for dag in found_dags),
                    )
                )
        except Exception as error:
            raise DagBagConstructionError(
                f"Could not construct a partial Dag bag from '{resolved_path}': {error}"
            ) from error
    return dag_bag, tuple(stats)


__all__ = (
    "DagBagConstructionError",
    "DagBagShardStat",
    "build_dag_bag",
    "build_partial_dag_bag",
    "list_dag_file_paths",
)
