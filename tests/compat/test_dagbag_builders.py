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
from pytest_airflow_in_a_box._compat.dagbag import (
    DagBagConstructionError,
    build_dag_bag,
    build_partial_dag_bag,
    list_dag_file_paths,
)


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


class _DiscoveryCalls:
    """Recorded arguments from stubbed walk-only Dag discovery functions."""

    def __init__(self) -> None:
        """Start with no recorded calls."""

        self.list_py_file_paths: list[tuple[Any, bool]] = []
        self.list_dag_files: list[tuple[Any, bool]] = []


@pytest.fixture
def stub_file_discovery_modules(monkeypatch: pytest.MonkeyPatch) -> _DiscoveryCalls:
    """Install recording stubs for both canonical walk-only Dag discovery locations.

    Parameters:
        monkeypatch: pytest.MonkeyPatch scoping the module substitution.

    Returns:
        _DiscoveryCalls recording every stubbed discovery call.
    """

    calls = _DiscoveryCalls()

    def fake_list_py_file_paths(directory: Any, *, safe_mode: bool) -> list[str]:
        calls.list_py_file_paths.append((directory, safe_mode))
        return ["/dags/models.py"]

    class _FakeRegistry:
        def list_dag_files(self, directory: Any, *, safe_mode: bool) -> list[str]:
            calls.list_dag_files.append((directory, safe_mode))
            return ["/dags/dag_processing.py"]

    utils_file_module = ModuleType("airflow.utils.file")
    utils_file_module.__dict__["list_py_file_paths"] = fake_list_py_file_paths
    monkeypatch.setitem(sys.modules, "airflow.utils.file", utils_file_module)

    importers_module = ModuleType("airflow.dag_processing.importers")
    importers_module.__dict__["get_importer_registry"] = lambda: _FakeRegistry()
    monkeypatch.setitem(sys.modules, "airflow.dag_processing.importers", importers_module)

    return calls


def test_list_dag_file_paths_dispatches_on_models_location(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_file_discovery_modules: _DiscoveryCalls,
) -> None:
    """Discover files through Airflow's own walk-only helper on the models location."""

    monkeypatch.setattr(
        dagbag_module,
        "resolve_capabilities",
        lambda: SimpleNamespace(dag_bag_location=DagBagLocation.MODELS),
    )

    result = list_dag_file_paths(tmp_path)

    assert result == ["/dags/models.py"]
    assert stub_file_discovery_modules.list_py_file_paths == [(tmp_path.resolve(), False)]
    assert stub_file_discovery_modules.list_dag_files == []


def test_list_dag_file_paths_dispatches_on_dag_processing_location(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_file_discovery_modules: _DiscoveryCalls,
) -> None:
    """Discover files through the importer registry on the dag-processing location."""

    monkeypatch.setattr(
        dagbag_module,
        "resolve_capabilities",
        lambda: SimpleNamespace(dag_bag_location=DagBagLocation.DAG_PROCESSING),
    )

    result = list_dag_file_paths(tmp_path, safe_mode=True)

    assert result == ["/dags/dag_processing.py"]
    assert stub_file_discovery_modules.list_dag_files == [(tmp_path.resolve(), True)]
    assert stub_file_discovery_modules.list_py_file_paths == []


def test_list_dag_file_paths_rejects_special_files(tmp_path: Path) -> None:
    """Reject a location that is neither a directory nor a regular file."""

    fifo = tmp_path / "pipe"
    os.mkfifo(fifo)

    with pytest.raises(ValueError, match="neither a directory nor a file"):
        list_dag_file_paths(fifo)


def test_list_dag_file_paths_rejects_missing_location(tmp_path: Path) -> None:
    """Reject a Dag location that does not exist."""

    missing = tmp_path / "does-not-exist"

    with pytest.raises(FileNotFoundError, match="does not exist"):
        list_dag_file_paths(missing)


class _FakeParsedDag:
    """Minimal Dag double exposing only what `build_partial_dag_bag` reads."""

    def __init__(self, dag_id: str, *, task_count: int) -> None:
        """Create a fake parsed Dag with a given task count.

        Parameters:
            dag_id: str naming the fake Dag.
            task_count: int counting placeholder tasks to attach.
        """

        self.dag_id = dag_id
        self.tasks = [object() for _ in range(task_count)]


class _PartialRecordingDagBag:
    """DagBag double supporting `collect_dags=False` plus `process_file`, recording calls."""

    calls: ClassVar[list[tuple[tuple[Any, ...], dict[str, Any]]]] = []
    process_file_calls: ClassVar[list[tuple[str, bool, bool]]] = []
    responses: ClassVar[dict[str, list[_FakeParsedDag] | Exception]] = {}

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Record one construction and start with an empty parsed state.

        Parameters:
            args: Any positional constructor arguments.
            kwargs: Any keyword constructor arguments.
        """

        type(self).calls.append((args, kwargs))
        self.dags: dict[str, _FakeParsedDag] = {}
        self.import_errors: dict[str, str] = {}

    def process_file(
        self, filepath: str, only_if_updated: bool = True, safe_mode: bool = True
    ) -> list[_FakeParsedDag]:
        """Return (or raise) the configured response for one shard file.

        Parameters:
            filepath: str naming the file being processed.
            only_if_updated: bool recorded for assertion, unused by the double.
            safe_mode: bool recorded for assertion, unused by the double.

        Returns:
            list[_FakeParsedDag] configured for `filepath` in `responses`.

        Raises:
            Exception: The configured response for `filepath` is an exception.
        """

        type(self).process_file_calls.append((filepath, only_if_updated, safe_mode))
        outcome = type(self).responses.get(filepath, [])
        if isinstance(outcome, Exception):
            raise outcome
        for dag in outcome:
            self.dags[dag.dag_id] = dag
        return outcome


@pytest.fixture
def stub_partial_dagbag_modules(monkeypatch: pytest.MonkeyPatch) -> type[_PartialRecordingDagBag]:
    """Install recording DagBag-with-`process_file` stubs for both canonical modules.

    Parameters:
        monkeypatch: pytest.MonkeyPatch scoping the module substitution.

    Returns:
        type[_PartialRecordingDagBag] whose class attributes capture constructions,
        `process_file` calls, and configured `process_file` responses.
    """

    _PartialRecordingDagBag.calls = []
    _PartialRecordingDagBag.process_file_calls = []
    _PartialRecordingDagBag.responses = {}
    for name in ("airflow.models.dagbag", "airflow.dag_processing.dagbag"):
        module = ModuleType(name)
        module.__dict__["DagBag"] = _PartialRecordingDagBag
        monkeypatch.setitem(sys.modules, name, module)
    return _PartialRecordingDagBag


def test_build_partial_dag_bag_constructs_with_collect_dags_disabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_partial_dagbag_modules: type[_PartialRecordingDagBag],
) -> None:
    """Construct the underlying Dag bag with `collect_dags=False` and no shard files."""

    monkeypatch.setattr(
        dagbag_module,
        "resolve_capabilities",
        lambda: SimpleNamespace(dag_bag_location=DagBagLocation.DAG_PROCESSING),
    )

    dag_bag, stats = build_partial_dag_bag(tmp_path, [])

    assert stub_partial_dagbag_modules.calls == [
        (
            (),
            {
                "dag_folder": tmp_path.resolve(),
                "safe_mode": False,
                "load_op_links": False,
                "collect_dags": False,
            },
        )
    ]
    assert dag_bag.dags == {}
    assert stats == ()


def test_build_partial_dag_bag_processes_each_shard_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_partial_dagbag_modules: type[_PartialRecordingDagBag],
) -> None:
    """Call `process_file` once per shard path and build one stat entry per file."""

    monkeypatch.setattr(
        dagbag_module,
        "resolve_capabilities",
        lambda: SimpleNamespace(dag_bag_location=DagBagLocation.MODELS),
    )
    inside = tmp_path / "inside.py"
    inside.write_text("", encoding="utf-8")
    dag = _FakeParsedDag("inside_dag", task_count=2)
    stub_partial_dagbag_modules.responses = {str(inside): [dag]}

    dag_bag, stats = build_partial_dag_bag(tmp_path, [str(inside)])

    assert stub_partial_dagbag_modules.process_file_calls == [(str(inside), True, False)]
    assert dag_bag.dags == {"inside_dag": dag}
    assert len(stats) == 1
    assert stats[0].file == "inside.py"
    assert stats[0].dag_num == 1
    assert stats[0].task_num == 2


def test_build_partial_dag_bag_falls_back_when_file_outside_dag_folder(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_partial_dagbag_modules: type[_PartialRecordingDagBag],
) -> None:
    """Report the absolute path when a shard file is not under `dag_folder`."""

    monkeypatch.setattr(
        dagbag_module,
        "resolve_capabilities",
        lambda: SimpleNamespace(dag_bag_location=DagBagLocation.MODELS),
    )
    resolved_root = tmp_path.resolve()
    outside = resolved_root.parent / "elsewhere.py"

    _, stats = build_partial_dag_bag(tmp_path, [str(outside)])

    assert stats[0].file == outside.as_posix()
    assert stub_partial_dagbag_modules.process_file_calls == [(str(outside), True, False)]


def test_build_partial_dag_bag_wraps_process_file_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_partial_dagbag_modules: type[_PartialRecordingDagBag],
) -> None:
    """Wrap a `process_file` failure the same way construction failures are wrapped."""

    monkeypatch.setattr(
        dagbag_module,
        "resolve_capabilities",
        lambda: SimpleNamespace(dag_bag_location=DagBagLocation.DAG_PROCESSING),
    )
    broken = tmp_path / "broken.py"
    stub_partial_dagbag_modules.responses = {str(broken): RuntimeError("boom")}

    with pytest.raises(DagBagConstructionError, match="boom"):
        build_partial_dag_bag(tmp_path, [str(broken)])
