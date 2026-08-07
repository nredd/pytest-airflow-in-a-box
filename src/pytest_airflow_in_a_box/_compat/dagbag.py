"""Construct Apache Airflow Dag bags across certified releases.

Airflow is imported only after bootstrap and capability validation are complete.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pytest_airflow_in_a_box._compat.capabilities import DagBagLocation, resolve_capabilities

if TYPE_CHECKING:
    from airflow.models.dagbag import DagBag


class DagBagConstructionError(RuntimeError):
    """Report failure to construct a Dag bag from a validated directory."""


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


def build_dag_bag(path: str | Path) -> DagBag:
    """Validate a Dag location and parse it with the certified Airflow interface.

    Parameters:
        path: str | pathlib.Path naming an existing Dag directory or Dag file.

    Returns:
        airflow.models.dagbag.DagBag containing parsed Dags and import errors.

    Raises:
        FileNotFoundError: The resolved Dag location does not exist.
        ValueError: The resolved Dag location is neither a directory nor a file.
        AirflowCompatibilityError: The installed Airflow interface is unsupported.
        DagBagConstructionError: Airflow cannot construct the Dag bag.
    """

    resolved_path = Path(str(path)).resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Dag location does not exist: '{resolved_path}'")
    if not resolved_path.is_dir() and not resolved_path.is_file():
        raise ValueError(f"Dag location is neither a directory nor a file: '{resolved_path}'")

    capabilities = resolve_capabilities()
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


__all__ = ("DagBagConstructionError", "build_dag_bag")
