"""Validate pinned param cases against a Dag's declared params.

Mirrors the runtime merge a triggered run performs -- declared params
overlaid with the run's conf, then schema-validated -- while additionally
rejecting keys the Dag never declares, because in a pinned test case an
undeclared key is a typo, not runtime freedom.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/params.html
"""

from __future__ import annotations

from typing import Any


class ParamsCaseError(Exception):
    """Report one pinned param case that a Dag's schema rejects."""


def validate_dag_params(dag: Any, conf: dict[str, Any]) -> None:
    """Validate one pinned conf against a Dag's declared params.

    Parameters:
        dag: Any containing a parsed Airflow Dag.
        conf: dict[str, Any] containing the pinned param values.

    Raises:
        ParamsCaseError: A key is undeclared or a value fails its schema.
    """

    # Deferred to preserve bootstrap safety and avoid Airflow's module import cost.
    from airflow.sdk.definitions.param import ParamsDict, ParamValidationError

    declared = dag.params
    unknown = sorted(set(conf) - set(declared))
    if unknown:
        names = ", ".join(f"`{name}`" for name in unknown)
        declared_names = ", ".join(f"`{name}`" for name in sorted(declared)) or "none"
        raise ParamsCaseError(
            f"Dag `{dag.dag_id}` declares no params {names}; declared params: {declared_names}"
        )
    merged = ParamsDict(declared, suppress_exception=False)
    try:
        # `update` validates eagerly on assignment; `validate` re-checks the
        # merged result, covering both paths across releases.
        merged.update(conf)
        merged.validate()
    except ParamValidationError as error:
        raise ParamsCaseError(f"Dag `{dag.dag_id}` rejected the pinned params: {error}") from error


__all__ = ("ParamsCaseError", "validate_dag_params")
