"""Probe-double coverage for the Airflow 2.x branch of `_compat.params`."""

from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import Any

import pytest

from pytest_airflow_in_a_box._compat import params as params_module
from pytest_airflow_in_a_box._compat.capabilities import _CERTIFIED_CAPABILITIES
from pytest_airflow_in_a_box._compat.params import ParamsCaseError, validate_dag_params


class _FakeParamValidationError(Exception):
    """Stand in for the 2.x `airflow.exceptions.ParamValidationError`."""


class _FakeParamsDict(dict):
    """Record the 2.x validation flow with schema-free semantics."""

    def __init__(self, declared: dict[str, Any], suppress_exception: bool) -> None:
        super().__init__(declared)
        assert suppress_exception is False

    def validate(self) -> None:
        """Reject one poison value to exercise the rejection path."""

        if self.get("factor") == "poison":
            raise _FakeParamValidationError("poison value")


@pytest.fixture
def v2_param_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    """Expose fake 2.x param modules behind the certified 2.11.2 contract.

    Parameters:
        monkeypatch: pytest.MonkeyPatch replacing the resolver and modules.
    """

    capabilities = _CERTIFIED_CAPABILITIES[(2, 11, 2)]
    monkeypatch.setattr(params_module, "resolve_capabilities", lambda: capabilities)
    monkeypatch.setitem(
        sys.modules, "airflow.models.param", SimpleNamespace(ParamsDict=_FakeParamsDict)
    )
    monkeypatch.setitem(
        sys.modules,
        "airflow.exceptions",
        SimpleNamespace(ParamValidationError=_FakeParamValidationError),
    )


@pytest.mark.usefixtures("v2_param_modules")
def test_v2_params_accept_a_declared_case() -> None:
    """Validate a pinned case through the 2.x module locations."""

    dag = SimpleNamespace(dag_id="v2_params", params={"factor": 2})

    validate_dag_params(dag, {"factor": 3})


@pytest.mark.usefixtures("v2_param_modules")
def test_v2_params_reject_a_failing_schema() -> None:
    """Wrap the 2.x validation error in the family-independent case error."""

    dag = SimpleNamespace(dag_id="v2_params", params={"factor": 2})

    with pytest.raises(ParamsCaseError, match="rejected the pinned params"):
        validate_dag_params(dag, {"factor": "poison"})
