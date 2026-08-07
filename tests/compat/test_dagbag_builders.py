"""Test Dag bag construction dispatch against stub Airflow modules."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, ClassVar

import pytest

from pytest_airflow_in_a_box._compat import dagbag as dagbag_module
from pytest_airflow_in_a_box._compat.capabilities import DagBagLocation
from pytest_airflow_in_a_box._compat.dagbag import DagBagConstructionError, build_dag_bag


class _RecordingDagBag:
    """DagBag double recording its constructor arguments."""

    calls: ClassVar[list[tuple[tuple[Any, ...], dict[str, Any]]]] = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Record one construction.

        Parameters:
            args: Any positional constructor arguments.
            kwargs: Any keyword constructor arguments.
        """

        type(self).calls.append((args, kwargs))


@pytest.fixture
def stub_dagbag_modules(monkeypatch: pytest.MonkeyPatch) -> type[_RecordingDagBag]:
    """Install recording DagBag stubs for both canonical modules.

    Parameters:
        monkeypatch: pytest.MonkeyPatch scoping the module substitution.

    Returns:
        type[_RecordingDagBag] whose ``calls`` list captures constructions.
    """

    _RecordingDagBag.calls = []
    for name in ("airflow.models.dagbag", "airflow.dag_processing.dagbag"):
        module = ModuleType(name)
        module.__dict__["DagBag"] = _RecordingDagBag
        monkeypatch.setitem(sys.modules, name, module)
    return _RecordingDagBag


def test_models_builder_arguments(
    tmp_path: Path,
    stub_dagbag_modules: type[_RecordingDagBag],
) -> None:
    """Use the positional example-control call only when supported."""

    dagbag_module._build_models_dag_bag(tmp_path, include_examples=True)
    dagbag_module._build_models_dag_bag(tmp_path, include_examples=False)

    assert stub_dagbag_modules.calls == [
        ((tmp_path, False, False, False), {}),
        ((), {"dag_folder": tmp_path, "safe_mode": False, "load_op_links": False}),
    ]


def test_dag_processing_builder_arguments(
    tmp_path: Path,
    stub_dagbag_modules: type[_RecordingDagBag],
) -> None:
    """Use the positional example-control call only when supported."""

    dagbag_module._build_dag_processing_dag_bag(tmp_path, include_examples=True)
    dagbag_module._build_dag_processing_dag_bag(tmp_path, include_examples=False)

    assert stub_dagbag_modules.calls == [
        ((tmp_path, False, False, False), {}),
        ((), {"dag_folder": tmp_path, "safe_mode": False, "load_op_links": False}),
    ]


def test_build_dag_bag_dispatches_on_certified_location(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_dagbag_modules: type[_RecordingDagBag],
) -> None:
    """Select the models module when capabilities certify that location."""

    monkeypatch.setattr(
        dagbag_module,
        "resolve_capabilities",
        lambda: SimpleNamespace(
            dag_bag_location=DagBagLocation.MODELS,
            dag_bag_supports_include_examples=False,
        ),
    )

    result = build_dag_bag(tmp_path)

    assert isinstance(result, stub_dagbag_modules)
    assert stub_dagbag_modules.calls == [
        ((), {"dag_folder": tmp_path.resolve(), "safe_mode": False, "load_op_links": False}),
    ]


def test_build_dag_bag_rejects_special_files(tmp_path: Path) -> None:
    """Reject a location that is neither a directory nor a regular file."""

    fifo = tmp_path / "pipe"
    os.mkfifo(fifo)

    with pytest.raises(ValueError, match="neither a directory nor a file"):
        build_dag_bag(fifo)


def test_build_dag_bag_wraps_construction_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Wrap constructor failures with the resolved location."""

    def explode(path: Path, *, include_examples: bool) -> Any:
        del path, include_examples
        raise RuntimeError("constructor exploded")

    monkeypatch.setattr(dagbag_module, "_build_dag_processing_dag_bag", explode)
    monkeypatch.setattr(
        dagbag_module,
        "resolve_capabilities",
        lambda: SimpleNamespace(
            dag_bag_location=DagBagLocation.DAG_PROCESSING,
            dag_bag_supports_include_examples=False,
        ),
    )

    with pytest.raises(DagBagConstructionError, match="constructor exploded"):
        build_dag_bag(tmp_path)
