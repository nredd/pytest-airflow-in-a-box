"""Validate pinned param cases against a Dag's declared params.

Mirrors the runtime merge a triggered run performs -- declared params
overlaid with the run's conf, then schema-validated -- while additionally
rejecting keys the Dag never declares, because in a pinned test case an
undeclared key is a typo, not runtime freedom.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/params.html
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

from pytest_airflow_in_a_box._compat.capabilities import ParamsLocation, resolve_capabilities


class ParamsCaseError(Exception):
    """Report one pinned param case that a Dag's schema rejects."""


def _param_symbols() -> tuple[Any, type[Exception]]:
    """Resolve the certified params-validation symbols for the installed family.

    Returns:
        tuple[Any, type[Exception]] containing `ParamsDict` and `ParamValidationError`.
    """

    # Deferred to preserve bootstrap safety and avoid Airflow's module import cost. The
    # 2.x modules are dynamically resolved so static checking stays valid against an
    # installed 3.x tree, which has neither of them.
    if resolve_capabilities().params_location is ParamsLocation.MODELS:
        exceptions: Any = import_module("airflow.exceptions")
        models_param: Any = import_module(ParamsLocation.MODELS.value)
        return models_param.ParamsDict, exceptions.ParamValidationError
    from airflow.sdk.definitions.param import ParamsDict, ParamValidationError

    return ParamsDict, ParamValidationError


def validate_dag_params(dag: Any, conf: dict[str, Any]) -> None:
    """Validate one pinned conf against a Dag's declared params.

    Parameters:
        dag: Any containing a parsed Airflow Dag.
        conf: dict[str, Any] containing the pinned param values.

    Raises:
        ParamsCaseError: A key is undeclared or a value fails its schema.
    """

    params_dict_class, param_validation_error = _param_symbols()

    declared = dag.params
    unknown = sorted(set(conf) - set(declared))
    if unknown:
        names = ", ".join(f"`{name}`" for name in unknown)
        declared_names = ", ".join(f"`{name}`" for name in sorted(declared)) or "none"
        raise ParamsCaseError(
            f"Dag `{dag.dag_id}` declares no params {names}; declared params: {declared_names}"
        )
    merged = params_dict_class(declared, suppress_exception=False)
    try:
        # `update` validates eagerly on assignment; `validate` re-checks the
        # merged result, covering both paths across releases.
        merged.update(conf)
        merged.validate()
    except param_validation_error as error:
        raise ParamsCaseError(f"Dag `{dag.dag_id}` rejected the pinned params: {error}") from error


__all__ = ("ParamsCaseError", "validate_dag_params")
