"""Test exact-release Airflow capability probing and validation."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Callable, Iterator
from types import SimpleNamespace

import pytest

from pytest_airflow_in_a_box import _compat
from pytest_airflow_in_a_box._compat import capabilities as capability_module
from pytest_airflow_in_a_box._compat.capabilities import (
    AIRFLOW_DISTRIBUTION,
    AirflowCapabilities,
    AirflowCompatibilityError,
    DagBagLocation,
    TaskInstanceRunner,
)


class _OldDagBag:
    """Fake pre-3.3 DagBag at the old module location."""

    def __init__(self, dag_folder: str | None = None, include_examples: bool = True) -> None:
        self.dag_folder = dag_folder
        self.include_examples = include_examples


class _NewDagBag:
    """Fake pre-3.3 DagBag at the new module location."""

    def __init__(self, dag_folder: str | None = None, include_examples: bool = True) -> None:
        self.dag_folder = dag_folder
        self.include_examples = include_examples


class _NewestDagBag:
    """Fake 3.3 DagBag without the removed examples argument."""

    def __init__(self, dag_folder: str | None = None) -> None:
        self.dag_folder = dag_folder


class _LegacyTaskInstance:
    """Fake TaskInstance exposing the legacy execution method."""

    def run(self) -> None:
        """Represent legacy task execution."""

    def refresh_from_task(self, task: object, pool_override: str | None = None) -> None:
        """Represent the original task refresh signature."""

        del task, pool_override


class _SdkTaskInstance:
    """Fake TaskInstance using Task SDK execution."""

    def refresh_from_task(self, task: object, pool_override: str | None = None) -> None:
        """Represent the task refresh signature before Airflow 3.3."""

        del task, pool_override


class _DagRunAwareTaskInstance:
    """Fake Airflow 3.3 TaskInstance accepting a DagRun."""

    def refresh_from_task(
        self,
        task: object,
        pool_override: str | None = None,
        *,
        dag_run: object | None = None,
    ) -> None:
        """Represent the Airflow 3.3 task refresh signature."""

        del task, pool_override, dag_run


def _callable_symbol() -> None:
    """Provide a callable placeholder for required functions."""


def _model(*fields: str) -> type:
    """Create a fake Pydantic model exposing selected field names.

    Parameters:
        fields: str names to expose through ``model_fields``.

    Returns:
        type containing a Pydantic-compatible field mapping.
    """

    return type("FakeModel", (), {"model_fields": dict.fromkeys(fields, object())})


def _base_modules() -> dict[str, SimpleNamespace]:
    """Build fake modules containing every invariant required Airflow symbol.

    Returns:
        dict[str, types.SimpleNamespace] keyed by private module path.
    """

    generic_class = type("GenericAirflowSymbol", (), {})
    return {
        "airflow.sdk": SimpleNamespace(DAG=generic_class),
        "airflow.utils.db": SimpleNamespace(initdb=_callable_symbol),
        "airflow.utils.session": SimpleNamespace(create_session=_callable_symbol),
        "airflow.models.dagrun": SimpleNamespace(DagRun=generic_class),
        "airflow.models.serialized_dag": SimpleNamespace(SerializedDagModel=generic_class),
        "airflow.models.dag": SimpleNamespace(DagModel=generic_class),
        "airflow.models.dag_version": SimpleNamespace(DagVersion=generic_class),
        "airflow.models.dagbundle": SimpleNamespace(DagBundleModel=generic_class),
        "airflow.serialization.serialized_objects": SimpleNamespace(
            LazyDeserializedDAG=generic_class
        ),
        "airflow.sdk.definitions.dag": SimpleNamespace(_run_task=_callable_symbol),
        "airflow.sdk.execution_time.task_runner": SimpleNamespace(
            RuntimeTaskInstance=generic_class,
            parse=_callable_symbol,
            run=_callable_symbol,
        ),
        "airflow.sdk.execution_time.comms": SimpleNamespace(
            CommsDecoder=generic_class,
            BundleInfo=generic_class,
            ToSupervisor=object(),
        ),
        "airflow.sdk.execution_time.xcom": SimpleNamespace(XCom=generic_class),
    }


def _fake_modules(release: tuple[int, int, int]) -> dict[str, SimpleNamespace]:
    """Build the exact fake private interface for one certified release.

    Parameters:
        release: tuple[int, int, int] identifying the certified contract.

    Returns:
        dict[str, types.SimpleNamespace] keyed by private module path.
    """

    modules = _base_modules()
    generic_class = type("GenericGeneratedSymbol", (), {})
    generated_fields = ("queue",) if release[:2] == (3, 3) else ()
    modules["airflow.sdk.api.datamodels._generated"] = SimpleNamespace(
        DagRun=generic_class,
        DagRunState=generic_class,
        TaskInstance=_model(*generated_fields),
        TaskInstanceState=generic_class,
        TIRunContext=generic_class,
    )
    sentry_fields = ("sentry_integration",) if release[:2] != (3, 1) else ()
    modules["airflow.sdk.execution_time.comms"].StartupDetails = _model(*sentry_fields)

    if release[:2] == (3, 1):
        modules["airflow.models.dagbag"] = SimpleNamespace(DagBag=_OldDagBag)
        modules["airflow.models.taskinstance"] = SimpleNamespace(TaskInstance=_LegacyTaskInstance)
        modules["airflow.serialization.serialized_objects"].SerializedDAG = generic_class
    else:
        dag_bag = _NewDagBag if release[:2] == (3, 2) else _NewestDagBag
        task_instance = _SdkTaskInstance if release[:2] == (3, 2) else _DagRunAwareTaskInstance
        modules["airflow.dag_processing.dagbag"] = SimpleNamespace(DagBag=dag_bag)
        modules["airflow.models.taskinstance"] = SimpleNamespace(TaskInstance=task_instance)
        modules["airflow.serialization.definitions.dag"] = SimpleNamespace(
            SerializedDAG=generic_class
        )
    return modules


def _install_fake_environment(
    monkeypatch: pytest.MonkeyPatch,
    version: str,
    modules: dict[str, SimpleNamespace],
    meta_version: str | None = None,
) -> None:
    """Patch metadata and imports to expose one isolated fake Airflow installation.

    Parameters:
        monkeypatch: pytest.MonkeyPatch applying replacements.
        version: str returned as installed `apache-airflow-core` package metadata.
        modules: dict[str, types.SimpleNamespace] containing fake Airflow modules.
        meta_version: str | None returned as installed `apache-airflow` meta-distribution
            metadata; None reports the meta-distribution as absent.
    """

    def fake_version(distribution_name: str) -> str:
        """Return fake metadata for exactly the two known Airflow distributions."""

        if distribution_name == capability_module.AIRFLOW_META_DISTRIBUTION:
            if meta_version is None:
                raise capability_module.metadata.PackageNotFoundError(distribution_name)
            return meta_version
        assert distribution_name == AIRFLOW_DISTRIBUTION
        return version

    def fake_import_module(name: str, package: str | None = None) -> object:
        """Resolve fake modules with importlib-compatible failure behavior."""

        del package
        try:
            return modules[name]
        except KeyError as error:
            raise ModuleNotFoundError(f"No module named '{name}'", name=name) from error

    monkeypatch.setattr(capability_module.metadata, "version", fake_version)
    monkeypatch.setattr(capability_module, "import_module", fake_import_module)


@pytest.fixture(autouse=True)
def _reset_capability_cache() -> Iterator[None]:
    """Isolate the process-local successful-resolution cache."""

    capability_module._reset_capabilities_for_testing()
    yield
    capability_module._reset_capabilities_for_testing()


def test_compat_package_import_does_not_import_airflow() -> None:
    """Keep compatibility metadata import-safe before bootstrap."""
    script = (
        "import sys; import pytest_airflow_in_a_box._compat; "
        "raise SystemExit('airflow' in sys.modules)"
    )

    subprocess.check_output([sys.executable, "-c", script], text=True)
    assert _compat.resolve_capabilities is capability_module.resolve_capabilities


@pytest.mark.parametrize(
    ("version", "release", "expected"),
    [
        (
            "3.1.0",
            (3, 1, 0),
            AirflowCapabilities(
                release=(3, 1, 0),
                dag_bag_location=DagBagLocation.MODELS,
                dag_bag_supports_include_examples=True,
                task_instance_runner=TaskInstanceRunner.LEGACY_RUN,
                refresh_from_task_supports_dag_run=False,
                startup_details_supports_sentry=False,
                runtime_task_instance_supports_queue=False,
            ),
        ),
        (
            "3.1.1",
            (3, 1, 1),
            AirflowCapabilities(
                release=(3, 1, 1),
                dag_bag_location=DagBagLocation.MODELS,
                dag_bag_supports_include_examples=True,
                task_instance_runner=TaskInstanceRunner.LEGACY_RUN,
                refresh_from_task_supports_dag_run=False,
                startup_details_supports_sentry=False,
                runtime_task_instance_supports_queue=False,
            ),
        ),
        (
            "3.1.2",
            (3, 1, 2),
            AirflowCapabilities(
                release=(3, 1, 2),
                dag_bag_location=DagBagLocation.MODELS,
                dag_bag_supports_include_examples=True,
                task_instance_runner=TaskInstanceRunner.LEGACY_RUN,
                refresh_from_task_supports_dag_run=False,
                startup_details_supports_sentry=False,
                runtime_task_instance_supports_queue=False,
            ),
        ),
        (
            "3.1.3",
            (3, 1, 3),
            AirflowCapabilities(
                release=(3, 1, 3),
                dag_bag_location=DagBagLocation.MODELS,
                dag_bag_supports_include_examples=True,
                task_instance_runner=TaskInstanceRunner.LEGACY_RUN,
                refresh_from_task_supports_dag_run=False,
                startup_details_supports_sentry=False,
                runtime_task_instance_supports_queue=False,
            ),
        ),
        (
            "3.1.5",
            (3, 1, 5),
            AirflowCapabilities(
                release=(3, 1, 5),
                dag_bag_location=DagBagLocation.MODELS,
                dag_bag_supports_include_examples=True,
                task_instance_runner=TaskInstanceRunner.LEGACY_RUN,
                refresh_from_task_supports_dag_run=False,
                startup_details_supports_sentry=False,
                runtime_task_instance_supports_queue=False,
            ),
        ),
        (
            "3.1.6",
            (3, 1, 6),
            AirflowCapabilities(
                release=(3, 1, 6),
                dag_bag_location=DagBagLocation.MODELS,
                dag_bag_supports_include_examples=True,
                task_instance_runner=TaskInstanceRunner.LEGACY_RUN,
                refresh_from_task_supports_dag_run=False,
                startup_details_supports_sentry=False,
                runtime_task_instance_supports_queue=False,
            ),
        ),
        (
            "3.1.7",
            (3, 1, 7),
            AirflowCapabilities(
                release=(3, 1, 7),
                dag_bag_location=DagBagLocation.MODELS,
                dag_bag_supports_include_examples=True,
                task_instance_runner=TaskInstanceRunner.LEGACY_RUN,
                refresh_from_task_supports_dag_run=False,
                startup_details_supports_sentry=False,
                runtime_task_instance_supports_queue=False,
            ),
        ),
        (
            "3.1.8",
            (3, 1, 8),
            AirflowCapabilities(
                release=(3, 1, 8),
                dag_bag_location=DagBagLocation.MODELS,
                dag_bag_supports_include_examples=True,
                task_instance_runner=TaskInstanceRunner.LEGACY_RUN,
                refresh_from_task_supports_dag_run=False,
                startup_details_supports_sentry=False,
                runtime_task_instance_supports_queue=False,
            ),
        ),
        (
            "3.2.0",
            (3, 2, 0),
            AirflowCapabilities(
                release=(3, 2, 0),
                dag_bag_location=DagBagLocation.DAG_PROCESSING,
                dag_bag_supports_include_examples=True,
                task_instance_runner=TaskInstanceRunner.SDK_RUN_TASK,
                refresh_from_task_supports_dag_run=False,
                startup_details_supports_sentry=True,
                runtime_task_instance_supports_queue=False,
            ),
        ),
        (
            "3.2.1",
            (3, 2, 1),
            AirflowCapabilities(
                release=(3, 2, 1),
                dag_bag_location=DagBagLocation.DAG_PROCESSING,
                dag_bag_supports_include_examples=True,
                task_instance_runner=TaskInstanceRunner.SDK_RUN_TASK,
                refresh_from_task_supports_dag_run=False,
                startup_details_supports_sentry=True,
                runtime_task_instance_supports_queue=False,
            ),
        ),
        (
            "3.2.2.dev4+vendor.1",
            (3, 2, 2),
            AirflowCapabilities(
                release=(3, 2, 2),
                dag_bag_location=DagBagLocation.DAG_PROCESSING,
                dag_bag_supports_include_examples=True,
                task_instance_runner=TaskInstanceRunner.SDK_RUN_TASK,
                refresh_from_task_supports_dag_run=False,
                startup_details_supports_sentry=True,
                runtime_task_instance_supports_queue=False,
            ),
        ),
        (
            "3.3.0-dev+LOCAL-build_2",
            (3, 3, 0),
            AirflowCapabilities(
                release=(3, 3, 0),
                dag_bag_location=DagBagLocation.DAG_PROCESSING,
                dag_bag_supports_include_examples=False,
                task_instance_runner=TaskInstanceRunner.SDK_RUN_TASK,
                refresh_from_task_supports_dag_run=True,
                startup_details_supports_sentry=True,
                runtime_task_instance_supports_queue=True,
            ),
        ),
    ],
)
def test_resolves_certified_release_capabilities(
    monkeypatch: pytest.MonkeyPatch,
    version: str,
    release: tuple[int, int, int],
    expected: AirflowCapabilities,
) -> None:
    """Accept certified exact releases with standard development and local suffixes."""

    _install_fake_environment(monkeypatch, version, _fake_modules(release))

    assert capability_module.resolve_capabilities() == expected


@pytest.mark.parametrize("version", ["3.2.3", "3.3.1", "4.0.0"])
def test_rejects_uncertified_release(monkeypatch: pytest.MonkeyPatch, version: str) -> None:
    """Name the installed and complete supported versions for valid but uncertified releases."""

    _install_fake_environment(monkeypatch, version, {})

    with pytest.raises(AirflowCompatibilityError) as caught:
        capability_module.resolve_capabilities()

    message = str(caught.value)
    assert f"installed version '{version}'" in message
    assert (
        "3.1.0, 3.1.1, 3.1.2, 3.1.3, 3.1.5, 3.1.6, 3.1.7, 3.1.8, 3.2.0, 3.2.1, 3.2.2, 3.3.0"
        in message
    )
    assert isinstance(caught.value.__cause__, ValueError)


@pytest.mark.parametrize("version", ["garbage", "3.3", "3.3.0rc1", "3.3.0.post1"])
def test_rejects_malformed_or_disallowed_version(
    monkeypatch: pytest.MonkeyPatch, version: str
) -> None:
    """Reject malformed, prerelease, and postrelease metadata forms."""

    _install_fake_environment(monkeypatch, version, {})

    with pytest.raises(AirflowCompatibilityError, match="parsing installed version") as caught:
        capability_module.resolve_capabilities()

    assert version in str(caught.value)
    assert isinstance(caught.value.__cause__, ValueError)


def test_airflow_free_environment_names_the_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    """Point an Airflow-free environment at the `airflow3` extra with failure context."""

    missing = capability_module.metadata.PackageNotFoundError(AIRFLOW_DISTRIBUTION)

    def missing_version(distribution_name: str) -> str:
        """Raise the representative importlib metadata failure for every distribution."""

        del distribution_name
        raise missing

    monkeypatch.setattr(capability_module.metadata, "version", missing_version)

    with pytest.raises(AirflowCompatibilityError, match="No Airflow distribution") as caught:
        capability_module.resolve_capabilities()

    message = str(caught.value)
    assert "pytest-airflow-in-a-box[airflow3]" in message
    assert caught.value.__cause__ is missing


def test_wraps_unexpected_metadata_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Retain metadata failure context for non-PackageNotFoundError failures."""

    unexpected = RuntimeError("metadata backend exploded")

    def broken_version(distribution_name: str) -> str:
        """Raise a non-PackageNotFoundError metadata failure."""

        del distribution_name
        raise unexpected

    monkeypatch.setattr(capability_module.metadata, "version", broken_version)

    with pytest.raises(AirflowCompatibilityError, match="<not installed>") as caught:
        capability_module.resolve_capabilities()

    assert caught.value.__cause__ is unexpected


def test_airflow2_environment_points_at_the_tier_issue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tell an Airflow 2.x environment the tier is planned and name the 3.x fix."""

    def fake_version(distribution_name: str) -> str:
        """Expose only the 2.x monolith distribution."""

        if distribution_name == capability_module.AIRFLOW_META_DISTRIBUTION:
            return "2.11.2"
        raise capability_module.metadata.PackageNotFoundError(distribution_name)

    monkeypatch.setattr(capability_module.metadata, "version", fake_version)

    with pytest.raises(AirflowCompatibilityError, match=r"Airflow 2\.x is installed") as caught:
        capability_module.resolve_capabilities()

    message = str(caught.value)
    assert "'2.11.2'" in message
    assert "issues/25" in message
    assert "pytest-airflow-in-a-box[airflow3]" in message
    assert isinstance(caught.value.__cause__, capability_module.metadata.PackageNotFoundError)


def test_meta_without_core_is_reported_broken(monkeypatch: pytest.MonkeyPatch) -> None:
    """Report a 3.x meta-distribution without the core as a broken installation."""

    def fake_version(distribution_name: str) -> str:
        """Expose only the 3.x meta-distribution."""

        if distribution_name == capability_module.AIRFLOW_META_DISTRIBUTION:
            return "3.3.0"
        raise capability_module.metadata.PackageNotFoundError(distribution_name)

    monkeypatch.setattr(capability_module.metadata, "version", fake_version)

    with pytest.raises(AirflowCompatibilityError, match="Broken Airflow installation") as caught:
        capability_module.resolve_capabilities()

    assert "'3.3.0'" in str(caught.value)
    assert isinstance(caught.value.__cause__, capability_module.metadata.PackageNotFoundError)


@pytest.mark.parametrize(
    "meta_version",
    ["2.11.2", "1!2.11.2", "0.1.dev0+g1234567", "unversioned"],
)
def test_corrupt_dual_family_environment_is_rejected(
    monkeypatch: pytest.MonkeyPatch, meta_version: str
) -> None:
    """Fail closed on any non-3.x or unparseable meta-distribution next to the core."""

    _install_fake_environment(monkeypatch, "3.3.0", {}, meta_version=meta_version)

    with pytest.raises(AirflowCompatibilityError, match="Corrupt Airflow installation") as caught:
        capability_module.resolve_capabilities()

    assert f"'{meta_version}'" in str(caught.value)


def test_meta_metadata_failure_is_treated_as_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolve normally on a healthy core when the meta-distribution read explodes."""

    modules = _fake_modules((3, 3, 0))

    def fake_version(distribution_name: str) -> str:
        """Fail the meta-distribution lookup with a non-PackageNotFoundError error."""

        if distribution_name == capability_module.AIRFLOW_META_DISTRIBUTION:
            raise RuntimeError("metadata backend exploded")
        assert distribution_name == AIRFLOW_DISTRIBUTION
        return "3.3.0"

    def fake_import_module(name: str, package: str | None = None) -> object:
        """Resolve fake modules with importlib-compatible failure behavior."""

        del package
        try:
            return modules[name]
        except KeyError as error:
            raise ModuleNotFoundError(f"No module named '{name}'", name=name) from error

    monkeypatch.setattr(capability_module.metadata, "version", fake_version)
    monkeypatch.setattr(capability_module, "import_module", fake_import_module)

    resolved = capability_module.resolve_capabilities()

    assert resolved.release == (3, 3, 0)


def test_meta_metadata_failure_keeps_the_no_airflow_diagnosis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep the friendly no-Airflow diagnosis when the meta-distribution read explodes."""

    missing = capability_module.metadata.PackageNotFoundError(AIRFLOW_DISTRIBUTION)

    def fake_version(distribution_name: str) -> str:
        """Fail both distribution lookups in distinct, realistic ways."""

        if distribution_name == capability_module.AIRFLOW_META_DISTRIBUTION:
            raise RuntimeError("metadata backend exploded")
        raise missing

    monkeypatch.setattr(capability_module.metadata, "version", fake_version)

    with pytest.raises(AirflowCompatibilityError, match="No Airflow distribution") as caught:
        capability_module.resolve_capabilities()

    assert caught.value.__cause__ is missing


def test_meta_alongside_core_is_the_normal_3x_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolve normally when the 3.x meta-package accompanies the core."""

    modules = _fake_modules((3, 3, 0))
    _install_fake_environment(monkeypatch, "3.3.0", modules, meta_version="3.3.0")

    resolved = capability_module.resolve_capabilities()

    assert resolved.release == (3, 3, 0)


def test_missing_symbol_is_named_and_failure_is_not_cached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Name an unavailable fixture dependency and retry after the installation is repaired."""

    modules = _fake_modules((3, 3, 0))
    del modules["airflow.sdk"].DAG
    _install_fake_environment(monkeypatch, "3.3.0", modules)

    with pytest.raises(AirflowCompatibilityError, match=r"airflow\.sdk\.DAG") as caught:
        capability_module.resolve_capabilities()

    assert isinstance(caught.value.__cause__, AttributeError)
    modules["airflow.sdk"].DAG = type("DAG", (), {})
    assert capability_module.resolve_capabilities().release == (3, 3, 0)


def _replace_dag_bag_with_old_location(modules: dict[str, SimpleNamespace]) -> None:
    """Mutate a fake release to report the legacy canonical DagBag path."""

    del modules["airflow.dag_processing.dagbag"]
    modules["airflow.models.dagbag"] = SimpleNamespace(DagBag=_OldDagBag)


def _add_include_examples(modules: dict[str, SimpleNamespace]) -> None:
    """Mutate Airflow 3.3 to restore its removed DagBag argument."""

    modules["airflow.dag_processing.dagbag"].DagBag = _NewDagBag


def _restore_legacy_runner(modules: dict[str, SimpleNamespace]) -> None:
    """Mutate a Task SDK release to expose legacy TaskInstance execution."""

    modules["airflow.models.taskinstance"].TaskInstance = _LegacyTaskInstance


def _remove_dag_run_refresh(modules: dict[str, SimpleNamespace]) -> None:
    """Mutate Airflow 3.3 to remove its DagRun-aware refresh signature."""

    modules["airflow.models.taskinstance"].TaskInstance = _SdkTaskInstance


def _remove_sentry_field(modules: dict[str, SimpleNamespace]) -> None:
    """Mutate a sentry-aware release to remove the field."""

    modules["airflow.sdk.execution_time.comms"].StartupDetails = _model()


def _remove_queue_field(modules: dict[str, SimpleNamespace]) -> None:
    """Mutate Airflow 3.3 to remove the runtime DTO queue field."""

    modules["airflow.sdk.api.datamodels._generated"].TaskInstance = _model()


def _backport_serialized_dag_location(modules: dict[str, SimpleNamespace]) -> None:
    """Mutate Airflow 3.1 to add the newer SerializedDAG path."""

    modules["airflow.serialization.definitions.dag"] = SimpleNamespace(
        SerializedDAG=type("SerializedDAG", (), {})
    )


@pytest.mark.parametrize(
    ("release", "mutate", "symbol"),
    [
        ((3, 2, 2), _replace_dag_bag_with_old_location, "DagBag canonical location"),
        ((3, 3, 0), _add_include_examples, "DagBag.__init__.include_examples"),
        ((3, 2, 2), _restore_legacy_runner, "TaskInstance task runner"),
        ((3, 3, 0), _remove_dag_run_refresh, "TaskInstance.refresh_from_task.dag_run"),
        ((3, 2, 2), _remove_sentry_field, "StartupDetails.sentry_integration"),
        ((3, 3, 0), _remove_queue_field, "TaskInstance DTO queue"),
        ((3, 1, 8), _backport_serialized_dag_location, "SerializedDAG canonical location"),
    ],
)
def test_rejects_vendor_contract_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    release: tuple[int, int, int],
    mutate: Callable[[dict[str, SimpleNamespace]], None],
    symbol: str,
) -> None:
    """Reject capability changes even when metadata claims a certified base release."""

    modules = _fake_modules(release)
    mutate(modules)
    version = ".".join(str(part) for part in release) + "+vendor"
    _install_fake_environment(monkeypatch, version, modules)

    with pytest.raises(AirflowCompatibilityError, match=symbol) as caught:
        capability_module.resolve_capabilities()

    assert caught.value.__cause__ is not None
    assert (
        "supported `apache-airflow-core` versions: "
        "3.1.0, 3.1.1, 3.1.2, 3.1.3, 3.1.5, 3.1.6, 3.1.7, 3.1.8, 3.2.0, 3.2.1, 3.2.2, 3.3.0"
        in str(caught.value)
    )


def test_non_callable_run_task_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """Require the selected private Task SDK execution symbol to be callable."""

    modules = _fake_modules((3, 2, 2))
    modules["airflow.sdk.definitions.dag"]._run_task = object()
    _install_fake_environment(monkeypatch, "3.2.2", modules)

    with pytest.raises(AirflowCompatibilityError, match="validating callable Airflow symbol"):
        capability_module.resolve_capabilities()


def test_invalid_signature_is_wrapped(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wrap an uninspectable required callable with its qualified symbol name."""

    modules = _fake_modules((3, 3, 0))
    modules["airflow.dag_processing.dagbag"].DagBag = object()
    _install_fake_environment(monkeypatch, "3.3.0", modules)

    with pytest.raises(AirflowCompatibilityError, match="inspecting signature of") as caught:
        capability_module.resolve_capabilities()

    assert isinstance(caught.value.__cause__, TypeError)


def test_signature_introspection_failure_is_wrapped(monkeypatch: pytest.MonkeyPatch) -> None:
    """Retain inspect failures raised after a callable passes structural validation."""

    modules = _fake_modules((3, 3, 0))
    failure = ValueError("invalid callable signature")
    _install_fake_environment(monkeypatch, "3.3.0", modules)

    def broken_signature(symbol: object) -> object:
        """Raise a representative inspect failure for any callable."""

        del symbol
        raise failure

    monkeypatch.setattr(capability_module, "signature", broken_signature)

    with pytest.raises(AirflowCompatibilityError, match="inspecting signature of") as caught:
        capability_module.resolve_capabilities()

    assert caught.value.__cause__ is failure


def test_invalid_model_fields_are_wrapped(monkeypatch: pytest.MonkeyPatch) -> None:
    """Require Pydantic model metadata for structural capability probes."""

    modules = _fake_modules((3, 3, 0))
    modules["airflow.sdk.execution_time.comms"].StartupDetails = object
    _install_fake_environment(monkeypatch, "3.3.0", modules)

    with pytest.raises(AirflowCompatibilityError, match="model_fields") as caught:
        capability_module.resolve_capabilities()

    assert isinstance(caught.value.__cause__, TypeError)


def test_unexpected_canonical_import_failure_is_wrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not treat arbitrary import initialization failures as absent newer modules."""

    modules = _fake_modules((3, 1, 8))
    failure = RuntimeError("broken module initialization")

    def broken_import(name: str, package: str | None = None) -> object:
        """Fail unexpectedly while probing the newer canonical DagBag module."""

        del package
        if name == "airflow.dag_processing.dagbag":
            raise failure
        return modules[name]

    def fake_version(distribution_name: str) -> str:
        """Return certified metadata for the import failure test."""

        if distribution_name == capability_module.AIRFLOW_META_DISTRIBUTION:
            raise capability_module.metadata.PackageNotFoundError(distribution_name)
        assert distribution_name == AIRFLOW_DISTRIBUTION
        return "3.1.8"

    monkeypatch.setattr(capability_module.metadata, "version", fake_version)
    monkeypatch.setattr(capability_module, "import_module", broken_import)

    with pytest.raises(
        AirflowCompatibilityError, match="probing canonical Airflow symbol"
    ) as caught:
        capability_module.resolve_capabilities()

    assert caught.value.__cause__ is failure


def test_unexpected_serialized_dag_import_failure_is_wrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retain initialization failures from the newer SerializedDAG module."""

    modules = _fake_modules((3, 2, 2))
    failure = RuntimeError("broken serialization module initialization")

    def broken_import(name: str, package: str | None = None) -> object:
        """Fail unexpectedly while probing the canonical SerializedDAG module."""

        del package
        if name == "airflow.serialization.definitions.dag":
            raise failure
        return modules[name]

    def fake_version(distribution_name: str) -> str:
        """Return certified metadata for the serialization failure test."""

        if distribution_name == capability_module.AIRFLOW_META_DISTRIBUTION:
            raise capability_module.metadata.PackageNotFoundError(distribution_name)
        assert distribution_name == AIRFLOW_DISTRIBUTION
        return "3.2.2"

    monkeypatch.setattr(capability_module.metadata, "version", fake_version)
    monkeypatch.setattr(capability_module, "import_module", broken_import)

    with pytest.raises(AirflowCompatibilityError, match="SerializedDAG") as caught:
        capability_module.resolve_capabilities()

    assert caught.value.__cause__ is failure


def test_success_is_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cache only a complete successful resolution."""

    _install_fake_environment(monkeypatch, "3.3.0", _fake_modules((3, 3, 0)))
    first = capability_module.resolve_capabilities()

    def unexpected_metadata_read(distribution_name: str) -> str:
        """Fail if cached resolution consults package metadata again."""

        raise AssertionError(f"Unexpected metadata read for '{distribution_name}'")

    monkeypatch.setattr(capability_module.metadata, "version", unexpected_metadata_read)

    assert capability_module.resolve_capabilities() is first


def test_real_current_airflow_resolves(pytester: pytest.Pytester) -> None:
    """Validate every required symbol against current Airflow in an isolated process."""

    pytester.makepyfile(
        """
        from pytest_airflow_in_a_box._compat import resolve_capabilities

        def test_current_airflow():
            capabilities = resolve_capabilities()
            assert capabilities.release in {
                (3, 1, 0),
                (3, 1, 1),
                (3, 1, 2),
                (3, 1, 3),
                (3, 1, 5),
                (3, 1, 6),
                (3, 1, 7),
                (3, 1, 8),
                (3, 2, 0),
                (3, 2, 1),
                (3, 2, 2),
                (3, 3, 0),
            }
            assert resolve_capabilities() is capabilities
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)
