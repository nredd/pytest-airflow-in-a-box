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
    AirflowFamily,
    ApiSurface,
    AssetUniqueKeyLocation,
    DagBagLocation,
    DagRunInterface,
    ExecutorContract,
    ParamsLocation,
    PluginsManagerShape,
    SecretsResolution,
    SharedModuleLoading,
    TaskInstanceRunner,
    TimezoneLocation,
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


class _V31Executor:
    """Fake pre-3.2 BaseExecutor exposing the boolean sentry flag."""

    supports_sentry = False


class _V32Executor:
    """Fake 3.2/3.2.x BaseExecutor exposing the renamed string sentry flag."""

    sentry_integration = ""


class _V33Executor:
    """Fake 3.3 BaseExecutor adding the connection-test flag."""

    sentry_integration = ""
    supports_connection_test = False


def _task_instance_mutation_hook_without_dag_run(task_instance: object) -> None:
    """Represent the pre-3.3 `task_instance_mutation_hook` hookspec signature."""


def _task_instance_mutation_hook_with_dag_run(task_instance: object, dag_run: object) -> None:
    """Represent the Airflow 3.3 `task_instance_mutation_hook` hookspec signature."""


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
        "airflow.sdk.definitions.asset": SimpleNamespace(AssetUniqueKey=generic_class),
        "airflow.sdk.definitions.param": SimpleNamespace(ParamsDict=generic_class),
        "airflow.sdk.timezone": SimpleNamespace(utcnow=_callable_symbol),
        "airflow.utils.db": SimpleNamespace(initdb=_callable_symbol),
        "airflow.utils.session": SimpleNamespace(create_session=_callable_symbol),
        "airflow.models.dagrun": SimpleNamespace(DagRun=generic_class),
        "airflow.models.serialized_dag": SimpleNamespace(SerializedDagModel=generic_class),
        "airflow.models.dag": SimpleNamespace(DagModel=generic_class),
        "airflow.models.dag_version": SimpleNamespace(DagVersion=generic_class),
        "airflow.models.dagbundle": SimpleNamespace(DagBundleModel=generic_class),
        "airflow.serialization.serialized_objects": SimpleNamespace(
            LazyDeserializedDAG=generic_class,
            encode_timetable=_callable_symbol,
            decode_timetable=_callable_symbol,
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
        "airflow.sdk.execution_time.context": SimpleNamespace(
            _get_variable=_callable_symbol,
            _get_connection=_callable_symbol,
            set_current_context=_callable_symbol,
        ),
        "airflow.sdk.execution_time.cache": SimpleNamespace(SecretCache=generic_class),
        "airflow.assets.evaluation": SimpleNamespace(AssetEvaluator=generic_class),
        "airflow.models.asset": SimpleNamespace(AssetDagRunQueue=generic_class),
        "airflow.timetables.simple": SimpleNamespace(AssetTriggeredTimetable=generic_class),
        "airflow.sdk.listener": SimpleNamespace(get_listener_manager=_callable_symbol),
    }


def _fake_v2_modules() -> dict[str, SimpleNamespace]:
    """Build the exact fake private interface for a certified Airflow 2.x release.

    Returns:
        dict[str, types.SimpleNamespace] keyed by private module path. The 2.x surface
        is uniform across all certified releases, so no release parameter exists.
    """

    generic_class = type("GenericAirflowSymbol", (), {})
    return {
        "airflow.utils.db": SimpleNamespace(initdb=_callable_symbol),
        "airflow.utils.session": SimpleNamespace(create_session=_callable_symbol),
        "airflow.models.dagrun": SimpleNamespace(DagRun=generic_class),
        "airflow.models.serialized_dag": SimpleNamespace(SerializedDagModel=generic_class),
        "airflow.models.dag": SimpleNamespace(DagModel=generic_class, DAG=generic_class),
        "airflow.models.dagbag": SimpleNamespace(DagBag=_OldDagBag),
        "airflow.models.taskinstance": SimpleNamespace(TaskInstance=_LegacyTaskInstance),
        "airflow.serialization.serialized_objects": SimpleNamespace(SerializedDAG=generic_class),
        "airflow.models.param": SimpleNamespace(ParamsDict=generic_class),
        "airflow.exceptions": SimpleNamespace(
            ParamValidationError=generic_class,
            RemovedInAirflow3Warning=generic_class,
            AirflowProviderDeprecationWarning=generic_class,
        ),
        "airflow.utils.timezone": SimpleNamespace(
            utcnow=_callable_symbol,
            coerce_datetime=_callable_symbol,
            convert_to_utc=_callable_symbol,
        ),
        "airflow.models.dataset": SimpleNamespace(
            DatasetModel=generic_class,
            DatasetDagRunQueue=generic_class,
            DagScheduleDatasetReference=generic_class,
        ),
        "airflow.timetables.simple": SimpleNamespace(DatasetTriggeredTimetable=generic_class),
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
        # `airflow.sdk.listener` does not exist at all on 3.1.x; verified against the
        # installed 3.1.0 in an isolated environment -- see `PROVENANCE.md`.
        del modules["airflow.sdk.listener"]
    else:
        dag_bag = _NewDagBag if release[:2] == (3, 2) else _NewestDagBag
        task_instance = _SdkTaskInstance if release[:2] == (3, 2) else _DagRunAwareTaskInstance
        modules["airflow.dag_processing.dagbag"] = SimpleNamespace(DagBag=dag_bag)
        modules["airflow.models.taskinstance"] = SimpleNamespace(TaskInstance=task_instance)
        modules["airflow.serialization.definitions.dag"] = SimpleNamespace(
            SerializedDAG=generic_class
        )
        modules["airflow.serialization.definitions.assets"] = SimpleNamespace(
            SerializedAssetUniqueKey=generic_class
        )
    executor_by_minor = {(3, 1): _V31Executor, (3, 2): _V32Executor}
    modules["airflow.executors.base_executor"] = SimpleNamespace(
        BaseExecutor=executor_by_minor.get(release[:2], _V33Executor)
    )
    modules["airflow.policies"] = SimpleNamespace(
        task_instance_mutation_hook=(
            _task_instance_mutation_hook_with_dag_run
            if release[:2] == (3, 3)
            else _task_instance_mutation_hook_without_dag_run
        )
    )
    return modules


def _install_fake_environment(
    monkeypatch: pytest.MonkeyPatch,
    version: str | None,
    modules: dict[str, SimpleNamespace],
    meta_version: str | None = None,
    meta_error: Exception | None = None,
    python_version: tuple[int, int] = (3, 12),
) -> None:
    """Patch metadata and imports to expose one isolated fake Airflow installation.

    Parameters:
        monkeypatch: pytest.MonkeyPatch applying replacements.
        version: str | None returned as installed `apache-airflow-core` package
            metadata; None reports the core distribution as absent (a 2.x or
            Airflow-free environment).
        modules: dict[str, types.SimpleNamespace] containing fake Airflow modules.
        meta_version: str | None returned as installed `apache-airflow` meta-distribution
            metadata; None reports the meta-distribution as absent.
        meta_error: Exception | None raised by the meta-distribution lookup instead of
            returning or reporting absence; takes precedence over `meta_version`.
        python_version: tuple[int, int] reported as the running interpreter -- part of
            the faked environment because the real host may be 3.13/3.14, where the
            ragged-matrix guard would otherwise fire inside every fake 2.x scenario.
    """

    monkeypatch.setattr(capability_module, "_running_python", lambda: python_version)

    def fake_version(distribution_name: str) -> str:
        """Return fake metadata for exactly the two known Airflow distributions."""

        if distribution_name == capability_module.AIRFLOW_META_DISTRIBUTION:
            if meta_error is not None:
                raise meta_error
            if meta_version is None:
                raise capability_module.metadata.PackageNotFoundError(distribution_name)
            return meta_version
        assert distribution_name == AIRFLOW_DISTRIBUTION
        if version is None:
            raise capability_module.metadata.PackageNotFoundError(distribution_name)
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
                family=AirflowFamily.V3,
                has_task_sdk=True,
                uses_structlog=True,
                has_dag_versioning=True,
                dagrun_interface=DagRunInterface.LOGICAL_DATE,
                api_surface=ApiSurface.API_SERVER,
                params_location=ParamsLocation.SDK,
                timezone_location=TimezoneLocation.SDK,
                secrets_resolution=SecretsResolution.SUPERVISOR_COMMS,
                max_python=None,
                dag_requires_start_date=False,
                asset_unique_key_location=AssetUniqueKeyLocation.SDK,
                executor_contract=ExecutorContract.V3_1,
                sdk_listener_manager_available=False,
                task_instance_mutation_hook_supports_dag_run=False,
                plugins_manager=PluginsManagerShape.MODULE_GLOBALS,
                shared_module_loading=SharedModuleLoading.SINGLE,
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
                family=AirflowFamily.V3,
                has_task_sdk=True,
                uses_structlog=True,
                has_dag_versioning=True,
                dagrun_interface=DagRunInterface.LOGICAL_DATE,
                api_surface=ApiSurface.API_SERVER,
                params_location=ParamsLocation.SDK,
                timezone_location=TimezoneLocation.SDK,
                secrets_resolution=SecretsResolution.SUPERVISOR_COMMS,
                max_python=None,
                dag_requires_start_date=False,
                asset_unique_key_location=AssetUniqueKeyLocation.SDK,
                executor_contract=ExecutorContract.V3_1,
                sdk_listener_manager_available=False,
                task_instance_mutation_hook_supports_dag_run=False,
                plugins_manager=PluginsManagerShape.MODULE_GLOBALS,
                shared_module_loading=SharedModuleLoading.SINGLE,
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
                family=AirflowFamily.V3,
                has_task_sdk=True,
                uses_structlog=True,
                has_dag_versioning=True,
                dagrun_interface=DagRunInterface.LOGICAL_DATE,
                api_surface=ApiSurface.API_SERVER,
                params_location=ParamsLocation.SDK,
                timezone_location=TimezoneLocation.SDK,
                secrets_resolution=SecretsResolution.SUPERVISOR_COMMS,
                max_python=None,
                dag_requires_start_date=False,
                asset_unique_key_location=AssetUniqueKeyLocation.SDK,
                executor_contract=ExecutorContract.V3_1,
                sdk_listener_manager_available=False,
                task_instance_mutation_hook_supports_dag_run=False,
                plugins_manager=PluginsManagerShape.MODULE_GLOBALS,
                shared_module_loading=SharedModuleLoading.SINGLE,
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
                family=AirflowFamily.V3,
                has_task_sdk=True,
                uses_structlog=True,
                has_dag_versioning=True,
                dagrun_interface=DagRunInterface.LOGICAL_DATE,
                api_surface=ApiSurface.API_SERVER,
                params_location=ParamsLocation.SDK,
                timezone_location=TimezoneLocation.SDK,
                secrets_resolution=SecretsResolution.SUPERVISOR_COMMS,
                max_python=None,
                dag_requires_start_date=False,
                asset_unique_key_location=AssetUniqueKeyLocation.SDK,
                executor_contract=ExecutorContract.V3_1,
                sdk_listener_manager_available=False,
                task_instance_mutation_hook_supports_dag_run=False,
                plugins_manager=PluginsManagerShape.MODULE_GLOBALS,
                shared_module_loading=SharedModuleLoading.SINGLE,
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
                family=AirflowFamily.V3,
                has_task_sdk=True,
                uses_structlog=True,
                has_dag_versioning=True,
                dagrun_interface=DagRunInterface.LOGICAL_DATE,
                api_surface=ApiSurface.API_SERVER,
                params_location=ParamsLocation.SDK,
                timezone_location=TimezoneLocation.SDK,
                secrets_resolution=SecretsResolution.SUPERVISOR_COMMS,
                max_python=None,
                dag_requires_start_date=False,
                asset_unique_key_location=AssetUniqueKeyLocation.SDK,
                executor_contract=ExecutorContract.V3_1,
                sdk_listener_manager_available=False,
                task_instance_mutation_hook_supports_dag_run=False,
                plugins_manager=PluginsManagerShape.MODULE_GLOBALS,
                shared_module_loading=SharedModuleLoading.SINGLE,
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
                family=AirflowFamily.V3,
                has_task_sdk=True,
                uses_structlog=True,
                has_dag_versioning=True,
                dagrun_interface=DagRunInterface.LOGICAL_DATE,
                api_surface=ApiSurface.API_SERVER,
                params_location=ParamsLocation.SDK,
                timezone_location=TimezoneLocation.SDK,
                secrets_resolution=SecretsResolution.SUPERVISOR_COMMS,
                max_python=None,
                dag_requires_start_date=False,
                asset_unique_key_location=AssetUniqueKeyLocation.SDK,
                executor_contract=ExecutorContract.V3_1,
                sdk_listener_manager_available=False,
                task_instance_mutation_hook_supports_dag_run=False,
                plugins_manager=PluginsManagerShape.MODULE_GLOBALS,
                shared_module_loading=SharedModuleLoading.SINGLE,
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
                family=AirflowFamily.V3,
                has_task_sdk=True,
                uses_structlog=True,
                has_dag_versioning=True,
                dagrun_interface=DagRunInterface.LOGICAL_DATE,
                api_surface=ApiSurface.API_SERVER,
                params_location=ParamsLocation.SDK,
                timezone_location=TimezoneLocation.SDK,
                secrets_resolution=SecretsResolution.SUPERVISOR_COMMS,
                max_python=None,
                dag_requires_start_date=False,
                asset_unique_key_location=AssetUniqueKeyLocation.SDK,
                executor_contract=ExecutorContract.V3_1,
                sdk_listener_manager_available=False,
                task_instance_mutation_hook_supports_dag_run=False,
                plugins_manager=PluginsManagerShape.MODULE_GLOBALS,
                shared_module_loading=SharedModuleLoading.SINGLE,
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
                family=AirflowFamily.V3,
                has_task_sdk=True,
                uses_structlog=True,
                has_dag_versioning=True,
                dagrun_interface=DagRunInterface.LOGICAL_DATE,
                api_surface=ApiSurface.API_SERVER,
                params_location=ParamsLocation.SDK,
                timezone_location=TimezoneLocation.SDK,
                secrets_resolution=SecretsResolution.SUPERVISOR_COMMS,
                max_python=None,
                dag_requires_start_date=False,
                asset_unique_key_location=AssetUniqueKeyLocation.SDK,
                executor_contract=ExecutorContract.V3_1,
                sdk_listener_manager_available=False,
                task_instance_mutation_hook_supports_dag_run=False,
                plugins_manager=PluginsManagerShape.MODULE_GLOBALS,
                shared_module_loading=SharedModuleLoading.SINGLE,
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
                family=AirflowFamily.V3,
                has_task_sdk=True,
                uses_structlog=True,
                has_dag_versioning=True,
                dagrun_interface=DagRunInterface.LOGICAL_DATE,
                api_surface=ApiSurface.API_SERVER,
                params_location=ParamsLocation.SDK,
                timezone_location=TimezoneLocation.SDK,
                secrets_resolution=SecretsResolution.SUPERVISOR_COMMS,
                max_python=None,
                dag_requires_start_date=False,
                asset_unique_key_location=AssetUniqueKeyLocation.SERIALIZATION,
                executor_contract=ExecutorContract.V3_2,
                sdk_listener_manager_available=True,
                task_instance_mutation_hook_supports_dag_run=False,
                plugins_manager=PluginsManagerShape.CACHED_FUNCTIONS,
                shared_module_loading=SharedModuleLoading.DUPLICATED,
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
                family=AirflowFamily.V3,
                has_task_sdk=True,
                uses_structlog=True,
                has_dag_versioning=True,
                dagrun_interface=DagRunInterface.LOGICAL_DATE,
                api_surface=ApiSurface.API_SERVER,
                params_location=ParamsLocation.SDK,
                timezone_location=TimezoneLocation.SDK,
                secrets_resolution=SecretsResolution.SUPERVISOR_COMMS,
                max_python=None,
                dag_requires_start_date=False,
                asset_unique_key_location=AssetUniqueKeyLocation.SERIALIZATION,
                executor_contract=ExecutorContract.V3_2,
                sdk_listener_manager_available=True,
                task_instance_mutation_hook_supports_dag_run=False,
                plugins_manager=PluginsManagerShape.CACHED_FUNCTIONS,
                shared_module_loading=SharedModuleLoading.DUPLICATED,
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
                family=AirflowFamily.V3,
                has_task_sdk=True,
                uses_structlog=True,
                has_dag_versioning=True,
                dagrun_interface=DagRunInterface.LOGICAL_DATE,
                api_surface=ApiSurface.API_SERVER,
                params_location=ParamsLocation.SDK,
                timezone_location=TimezoneLocation.SDK,
                secrets_resolution=SecretsResolution.SUPERVISOR_COMMS,
                max_python=None,
                dag_requires_start_date=False,
                asset_unique_key_location=AssetUniqueKeyLocation.SERIALIZATION,
                executor_contract=ExecutorContract.V3_2,
                sdk_listener_manager_available=True,
                task_instance_mutation_hook_supports_dag_run=False,
                plugins_manager=PluginsManagerShape.CACHED_FUNCTIONS,
                shared_module_loading=SharedModuleLoading.DUPLICATED,
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
                family=AirflowFamily.V3,
                has_task_sdk=True,
                uses_structlog=True,
                has_dag_versioning=True,
                dagrun_interface=DagRunInterface.LOGICAL_DATE,
                api_surface=ApiSurface.API_SERVER,
                params_location=ParamsLocation.SDK,
                timezone_location=TimezoneLocation.SDK,
                secrets_resolution=SecretsResolution.SUPERVISOR_COMMS,
                max_python=None,
                dag_requires_start_date=False,
                asset_unique_key_location=AssetUniqueKeyLocation.SERIALIZATION,
                executor_contract=ExecutorContract.V3_3,
                sdk_listener_manager_available=True,
                task_instance_mutation_hook_supports_dag_run=True,
                plugins_manager=PluginsManagerShape.CACHED_FUNCTIONS,
                shared_module_loading=SharedModuleLoading.DUPLICATED,
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


@pytest.mark.parametrize(
    ("version", "detail"),
    [
        ("3.0.2", "below the certified floor"),
        ("4.0.0", "outside the supported 3.x family"),
        ("5.1.0", "outside the supported 3.x family"),
    ],
)
def test_rejects_release_outside_the_probe_and_degrade_window(
    monkeypatch: pytest.MonkeyPatch, version: str, detail: str
) -> None:
    """Keep hard-failing below the certified floor and outside the 3.x major.

    Since #212 an *uncertified* 3.x release at or above the floor probes instead of
    failing; these releases are known-unsupported rather than unknown, so the loud
    rejection (naming the installed and supported versions) survives for them.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing the fake environment.
        version: str containing the rejected metadata version.
        detail: str containing the expected rejection reason fragment.
    """

    _install_fake_environment(monkeypatch, version, {})

    with pytest.raises(AirflowCompatibilityError) as caught:
        capability_module.resolve_capabilities()

    message = str(caught.value)
    assert f"installed version '{version}'" in message
    assert (
        "3.1.0, 3.1.1, 3.1.2, 3.1.3, 3.1.5, 3.1.6, 3.1.7, 3.1.8, 3.2.0, 3.2.1, 3.2.2, 3.3.0"
        in message
    )
    assert detail in message
    assert isinstance(caught.value.__cause__, ValueError)


def test_uncertified_v3_release_resolves_by_probing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve a forward uncertified 3.x release on the `PROBED` tier, skipping verification.

    The fake world deliberately carries the 3.1-era shape under a fake `3.3.2` version:
    on the certified tier `_verify_contract` would reject the observed
    `dag_bag_location`/`sdk_listener_manager_available` against any 3.3 row, so a
    successful resolution here proves verification is skipped and the observations win.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing the fake environment.
    """

    _install_fake_environment(monkeypatch, "3.3.2", _fake_modules((3, 1, 0)))

    capabilities = capability_module.resolve_capabilities()

    assert capabilities.certification is capability_module.CertificationTier.PROBED
    assert capabilities.release == (3, 3, 2)
    assert capabilities.dag_bag_location is DagBagLocation.MODELS
    assert capabilities.sdk_listener_manager_available is False
    # Release-derived fields still derive from the release, not the observed world.
    assert capabilities.plugins_manager is PluginsManagerShape.CACHED_FUNCTIONS
    assert capabilities.shared_module_loading is SharedModuleLoading.DUPLICATED


def test_uncertified_gap_release_resolves_by_probing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve a gap release inside the certified range (`3.1.4`) on the `PROBED` tier.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing the fake environment.
    """

    _install_fake_environment(monkeypatch, "3.1.4", _fake_modules((3, 1, 0)))

    capabilities = capability_module.resolve_capabilities()

    assert capabilities.certification is capability_module.CertificationTier.PROBED
    assert capabilities.release == (3, 1, 4)
    assert capabilities.plugins_manager is PluginsManagerShape.MODULE_GLOBALS


def test_uncertified_release_missing_symbol_still_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep the structural floor on the `PROBED` tier: a missing symbol hard-fails.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing the fake environment.
    """

    _install_fake_environment(monkeypatch, "3.3.2", {})

    with pytest.raises(AirflowCompatibilityError):
        capability_module.resolve_capabilities()


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ("3.3.1", capability_module.CertificationTier.CERTIFIED),
        ("3.4.0", capability_module.CertificationTier.PROBED),
        ("3.1.4", capability_module.CertificationTier.PROBED),
        ("3.0.2", None),
        ("4.0.0", None),
        ("garbage", None),
    ],
)
def test_installed_certification_classifies_v3_from_metadata(
    monkeypatch: pytest.MonkeyPatch,
    version: str,
    expected: capability_module.CertificationTier | None,
) -> None:
    """Classify the 3.x tier from metadata alone, never raising.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing the fake environment.
        version: str containing the fake core metadata version.
        expected: CertificationTier | None containing the expected classification.
    """

    _install_fake_environment(monkeypatch, version, {})

    assert capability_module.installed_certification() is expected


@pytest.mark.parametrize(
    ("meta_version", "expected"),
    [
        ("2.10.5", capability_module.CertificationTier.CERTIFIED),
        ("2.6.0", None),
    ],
)
def test_installed_certification_classifies_v2_from_metadata(
    monkeypatch: pytest.MonkeyPatch,
    meta_version: str,
    expected: capability_module.CertificationTier | None,
) -> None:
    """Classify the closed 2.x set: certified releases only, no `PROBED` tier.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing the fake environment.
        meta_version: str containing the fake meta-distribution version.
        expected: CertificationTier | None containing the expected classification.
    """

    _install_fake_environment(monkeypatch, None, {}, meta_version=meta_version)

    assert capability_module.installed_certification() is expected


def test_installed_certification_is_none_for_a_corrupt_dual_family_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Classify a dual-family install as None, not `PROBED`.

    Reviewer-found hole: without the corruption rejection, an uncertified core next to
    a leftover 2.x monolith warned with a pin-or-upgrade remedy where the real remedy
    is recreating the environment -- `resolve_capabilities()` rejects it outright.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing the fake environment.
    """

    _install_fake_environment(monkeypatch, "3.4.0", {}, meta_version="2.11.2")

    assert capability_module.installed_certification() is None


def test_installed_certification_is_none_for_an_over_cap_v2_python(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Classify a 2.x release running above its Python ceiling as None, not `CERTIFIED`.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing the fake environment.
    """

    _install_fake_environment(monkeypatch, None, {}, meta_version="2.7.3", python_version=(3, 12))

    assert capability_module.installed_certification() is None


def test_installed_certification_is_none_without_airflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Classify an Airflow-free environment as None instead of raising.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing the fake environment.
    """

    _install_fake_environment(monkeypatch, None, {})

    assert capability_module.installed_certification() is None


def test_classify_installed_certification_survives_racing_metadata_mutations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stay never-raising when the environment mutates between classification reads.

    The uncached classifier re-reads metadata after `installed_family` classified the
    family; both re-read branches (a vanished 2.x meta version and a 3.x core read
    that starts raising) must degrade to None, not crash. Driven through the private
    classifier on purpose: the public wrapper caches its first answer, so a second
    scenario in the same test would never re-classify.

    Parameters:
        monkeypatch: pytest.MonkeyPatch faking the racing classification reads.
    """

    monkeypatch.setattr(
        capability_module, "installed_family", lambda: capability_module.AirflowFamily.V2
    )
    monkeypatch.setattr(
        capability_module,
        "_meta_distribution",
        lambda: capability_module._MetaDistribution(version=None, major=None),
    )
    assert capability_module._classify_installed_certification() is None

    monkeypatch.setattr(
        capability_module, "installed_family", lambda: capability_module.AirflowFamily.V3
    )

    def raising_version(distribution_name: str) -> str:
        """Raise the representative metadata failure for every distribution."""

        raise OSError(f"unreadable metadata for '{distribution_name}'")

    monkeypatch.setattr(capability_module.metadata, "version", raising_version)
    assert capability_module._classify_installed_certification() is None


def test_installed_certification_caches_its_first_classification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Classify once and trust forever, so reports cannot drift from the resolved tier.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing then mutating the fake environment.
    """

    _install_fake_environment(monkeypatch, "3.3.1", {})
    assert (
        capability_module.installed_certification()
        is capability_module.CertificationTier.CERTIFIED
    )

    _install_fake_environment(monkeypatch, "3.4.0", {})

    assert (
        capability_module.installed_certification()
        is capability_module.CertificationTier.CERTIFIED
    )


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


@pytest.mark.parametrize(
    ("meta_version", "release", "max_python", "requires_start_date"),
    [
        ("2.7.3", (2, 7, 3), (3, 11), True),
        ("2.8.4", (2, 8, 4), (3, 11), False),
        ("2.9.3", (2, 9, 3), (3, 12), False),
        ("2.10.5", (2, 10, 5), (3, 12), False),
        ("2.11.2", (2, 11, 2), (3, 12), False),
    ],
)
def test_resolves_certified_v2_release_capabilities(
    monkeypatch: pytest.MonkeyPatch,
    meta_version: str,
    release: tuple[int, int, int],
    max_python: tuple[int, int],
    requires_start_date: bool,
) -> None:
    """Resolve the full certified 2.x contract from a monolith-only environment.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing the fake environment.
        meta_version: str reported as the installed `apache-airflow` version.
        release: tuple[int, int, int] expected as the parsed base release.
        max_python: tuple[int, int] expected as the release's own Python ceiling.
        requires_start_date: bool expected as the schedule-free `start_date` demand.
    """

    _install_fake_environment(
        monkeypatch,
        None,
        _fake_v2_modules(),
        meta_version=meta_version,
        python_version=(3, 11),
    )

    resolved = capability_module.resolve_capabilities()

    assert resolved == AirflowCapabilities(
        release=release,
        family=AirflowFamily.V2,
        dag_bag_location=DagBagLocation.MODELS,
        dag_bag_supports_include_examples=True,
        task_instance_runner=TaskInstanceRunner.LEGACY_RUN,
        refresh_from_task_supports_dag_run=False,
        startup_details_supports_sentry=None,
        runtime_task_instance_supports_queue=None,
        has_task_sdk=False,
        uses_structlog=False,
        has_dag_versioning=False,
        dagrun_interface=DagRunInterface.EXECUTION_DATE,
        api_surface=ApiSurface.WEBSERVER,
        params_location=ParamsLocation.MODELS,
        timezone_location=TimezoneLocation.UTILS,
        secrets_resolution=SecretsResolution.METASTORE,
        max_python=max_python,
        dag_requires_start_date=requires_start_date,
        asset_unique_key_location=None,
        executor_contract=None,
        sdk_listener_manager_available=False,
        task_instance_mutation_hook_supports_dag_run=False,
        plugins_manager=None,
        shared_module_loading=None,
    )


def test_certified_v2_releases_track_the_python_ceiling_table() -> None:
    """Derive the certified 2.x release list from the per-release ceiling table.

    The two cannot drift because `SUPPORTED_RELEASES_BY_FAMILY` reads its 2.x row from
    `V2_MAX_PYTHON_BY_RELEASE`; this pins that they stay wired that way, and that every
    certified release carries its ceiling on the resolved row.
    """

    releases = capability_module.SUPPORTED_RELEASES_BY_FAMILY[AirflowFamily.V2]

    assert releases == tuple(capability_module.V2_MAX_PYTHON_BY_RELEASE)
    assert {
        release: capability_module._CERTIFIED_CAPABILITIES[release].max_python
        for release in releases
    } == capability_module.V2_MAX_PYTHON_BY_RELEASE


@pytest.mark.parametrize("meta_version", ["2.6.3", "2.8.1", "2.10.4", "2.11.3"])
def test_rejects_uncertified_v2_release(
    monkeypatch: pytest.MonkeyPatch, meta_version: str
) -> None:
    """Name both families' supported versions for valid but uncertified 2.x releases."""

    _install_fake_environment(monkeypatch, None, {}, meta_version=meta_version)

    with pytest.raises(AirflowCompatibilityError) as caught:
        capability_module.resolve_capabilities()

    message = str(caught.value)
    assert f"installed version '{meta_version}'" in message
    assert "2.7.3, 2.8.4, 2.9.3, 2.10.5, 2.11.2" in message


@pytest.mark.parametrize(
    ("meta_version", "match"),
    [
        ("2.11", "parsing installed version"),
        ("2.x.1", "Broken Airflow installation"),
    ],
)
def test_v2_unparseable_certified_version_is_named(
    monkeypatch: pytest.MonkeyPatch, meta_version: str, match: str
) -> None:
    """Reject 2.x versions outside the strict release pattern with named reasons.

    A PEP 440 version missing the strict `x.y.z` shape fails the release parse; a
    version PEP 440 cannot read at all classifies as a broken installation.
    """

    _install_fake_environment(monkeypatch, None, {}, meta_version=meta_version)

    with pytest.raises(AirflowCompatibilityError, match=match):
        capability_module.resolve_capabilities()


def test_installed_family_classifies_v3(monkeypatch: pytest.MonkeyPatch) -> None:
    """Classify a core install as the 3.x family."""

    _install_fake_environment(monkeypatch, "3.3.0", {})

    assert capability_module.installed_family() is AirflowFamily.V3


def test_installed_family_classifies_v2(monkeypatch: pytest.MonkeyPatch) -> None:
    """Classify a monolith-only install as the 2.x family."""

    _install_fake_environment(monkeypatch, None, {}, meta_version="2.11.2")

    assert capability_module.installed_family() is AirflowFamily.V2


def test_installed_family_reports_none_without_airflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Report no family for an Airflow-free environment."""

    _install_fake_environment(monkeypatch, None, {})

    assert capability_module.installed_family() is None


def test_installed_family_survives_metadata_failure(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Report no family instead of raising when the core metadata read explodes."""

    def broken_version(distribution_name: str) -> str:
        """Fail every metadata lookup with a non-PackageNotFoundError error."""

        del distribution_name
        raise RuntimeError("metadata backend exploded")

    monkeypatch.setattr(capability_module.metadata, "version", broken_version)

    with caplog.at_level("WARNING", logger=capability_module.LOGGER.name):
        assert capability_module.installed_family() is None

    assert "metadata backend exploded" in caplog.text


def test_installed_family_warns_on_unreadable_meta_metadata(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Warn, not raise, when only the meta-distribution is present but unreadable."""

    _install_fake_environment(
        monkeypatch, None, {}, meta_error=RuntimeError("half-clobbered dist-info")
    )

    with caplog.at_level("WARNING", logger=capability_module.LOGGER.name):
        assert capability_module.installed_family() is None

    assert "half-clobbered dist-info" in caplog.text


def test_running_python_reports_the_interpreter() -> None:
    """Report the live interpreter's major and minor version."""

    assert capability_module._running_python() == sys.version_info[:2]


@pytest.mark.parametrize("python_version", [(3, 13), (3, 14), (4, 0)])
def test_v2_python_cap_is_enforced(
    monkeypatch: pytest.MonkeyPatch, python_version: tuple[int, int]
) -> None:
    """Reject a certified 2.x release on a Python the family never supported.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing the fake environment.
        python_version: tuple[int, int] reported as the running interpreter.
    """

    _install_fake_environment(
        monkeypatch, None, {}, meta_version="2.11.2", python_version=python_version
    )

    with pytest.raises(AirflowCompatibilityError, match="does not support Python") as caught:
        capability_module.resolve_capabilities()

    message = str(caught.value)
    assert "'2.11.2'" in message
    assert "3.10-3.12" in message


@pytest.mark.parametrize("meta_version", ["2.7.3", "2.8.4"])
def test_v2_python_cap_is_per_release(monkeypatch: pytest.MonkeyPatch, meta_version: str) -> None:
    """Reject a pre-2.9 release on CPython 3.12, which the family as a whole supports.

    The releases below 2.9 declare `requires-python` `~=3.8,<3.12` and publish no 3.12
    constraints file, so a family-wide ceiling of 3.12 would let them through and fail
    somewhere deeper.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing the fake environment.
        meta_version: str reported as the installed `apache-airflow` version.
    """

    _install_fake_environment(
        monkeypatch, None, {}, meta_version=meta_version, python_version=(3, 12)
    )

    with pytest.raises(AirflowCompatibilityError, match="does not support Python") as caught:
        capability_module.resolve_capabilities()

    message = str(caught.value)
    assert f"'{meta_version}'" in message
    assert "3.10-3.11" in message


@pytest.mark.parametrize("meta_version", ["2.7.3", "2.8.4"])
def test_v2_python_cap_admits_the_release_ceiling(
    monkeypatch: pytest.MonkeyPatch, meta_version: str
) -> None:
    """Accept a pre-2.9 release on its own ceiling rather than clamping it further.

    Parameters:
        monkeypatch: pytest.MonkeyPatch installing the fake environment.
        meta_version: str reported as the installed `apache-airflow` version.
    """

    _install_fake_environment(
        monkeypatch,
        None,
        _fake_v2_modules(),
        meta_version=meta_version,
        python_version=(3, 11),
    )

    assert capability_module.resolve_capabilities().max_python == (3, 11)


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ("2.7.3", (3, 11)),
        ("2.8.4", (3, 11)),
        ("2.9.3", (3, 12)),
        ("2.11.2", (3, 12)),
        # Uncertified and unparseable versions take the lowest certified ceiling: every
        # certified release accepts it, and guessing high is the failure mode the
        # per-release table exists to remove.
        ("2.10.4", (3, 11)),
        ("2.11", (3, 11)),
        ("not-a-version", (3, 11)),
    ],
)
def test_max_v2_python_reports_the_release_ceiling(
    version: str, expected: tuple[int, int]
) -> None:
    """Report each 2.x version's Python ceiling, conservatively for unknown versions.

    Parameters:
        version: str containing the Airflow 2.x version to look up.
        expected: tuple[int, int] expected as that version's ceiling.
    """

    assert capability_module.max_v2_python(version) == expected


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


def test_corruption_check_escape_hatch_bypasses_the_rejection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolve a self-verified dual-metadata environment when the escape hatch is set.

    Parameters:
        monkeypatch: pytest.MonkeyPatch isolating metadata and environment state.
    """

    modules = _fake_modules((3, 3, 0))
    _install_fake_environment(monkeypatch, "3.3.0", modules, meta_version="0.1.dev0+g1234567")
    monkeypatch.setenv(capability_module.SKIP_CORRUPTION_CHECK_ENVIRONMENT_VARIABLE, "1")

    resolved = capability_module.resolve_capabilities()

    assert resolved.release == (3, 3, 0)


def test_airflow1_environment_fails_certification_naming_supported_versions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject a legacy 1.x monolith through certification, never as `Airflow 2.x`.

    Parameters:
        monkeypatch: pytest.MonkeyPatch isolating metadata state.
    """

    def fake_version(distribution_name: str) -> str:
        """Expose only a legacy 1.x monolith distribution.

        Parameters:
            distribution_name: str naming the queried distribution.

        Returns:
            str containing the fake metadata version.

        Raises:
            importlib.metadata.PackageNotFoundError: The distribution is not the
                legacy monolith.
        """

        if distribution_name == capability_module.AIRFLOW_META_DISTRIBUTION:
            return "1.10.15"
        raise capability_module.metadata.PackageNotFoundError(distribution_name)

    monkeypatch.setattr(capability_module.metadata, "version", fake_version)

    with pytest.raises(AirflowCompatibilityError, match="base release is not certified") as caught:
        capability_module.resolve_capabilities()

    message = str(caught.value)
    assert "'1.10.15'" in message
    assert "Airflow 2.x" not in message
    assert "2.9.3" in message


def test_unreadable_meta_metadata_next_to_core_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Treat an unreadable meta-distribution next to the core as corruption."""

    _install_fake_environment(
        monkeypatch,
        "3.3.0",
        {},
        meta_error=RuntimeError("metadata backend exploded"),
    )

    with pytest.raises(AirflowCompatibilityError, match="Corrupt Airflow installation") as caught:
        capability_module.resolve_capabilities()

    assert "RuntimeError: metadata backend exploded" in str(caught.value)


def test_unreadable_meta_metadata_without_core_names_the_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Name the metadata read failure instead of claiming Airflow is absent."""

    missing = capability_module.metadata.PackageNotFoundError(AIRFLOW_DISTRIBUTION)

    def fake_version(distribution_name: str) -> str:
        """Fail both distribution lookups in distinct, realistic ways."""

        if distribution_name == capability_module.AIRFLOW_META_DISTRIBUTION:
            raise RuntimeError("metadata backend exploded")
        raise missing

    monkeypatch.setattr(capability_module.metadata, "version", fake_version)

    with pytest.raises(AirflowCompatibilityError, match="Broken Airflow installation") as caught:
        capability_module.resolve_capabilities()

    message = str(caught.value)
    assert "RuntimeError: metadata backend exploded" in message
    assert "pytest-airflow-in-a-box[airflow3]" in message
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


def _downgrade_executor_sentry_flag(modules: dict[str, SimpleNamespace]) -> None:
    """Mutate a 3.2+ release to report the 3.1 boolean sentry flag."""

    modules["airflow.executors.base_executor"] = SimpleNamespace(BaseExecutor=_V31Executor)


def _add_connection_test_support(modules: dict[str, SimpleNamespace]) -> None:
    """Mutate a 3.2 release to report the 3.3 connection-test flag."""

    modules["airflow.executors.base_executor"] = SimpleNamespace(BaseExecutor=_V33Executor)


def _remove_sdk_listener_manager(modules: dict[str, SimpleNamespace]) -> None:
    """Mutate a 3.x release to remove the SDK listener manager module."""

    del modules["airflow.sdk.listener"]


def _add_dag_run_to_mutation_hook(modules: dict[str, SimpleNamespace]) -> None:
    """Mutate a pre-3.3 release to report the 3.3 `task_instance_mutation_hook` shape."""

    modules["airflow.policies"] = SimpleNamespace(
        task_instance_mutation_hook=_task_instance_mutation_hook_with_dag_run
    )


def _remove_dag_run_from_mutation_hook(modules: dict[str, SimpleNamespace]) -> None:
    """Mutate Airflow 3.3 to report the pre-3.3 `task_instance_mutation_hook` shape."""

    modules["airflow.policies"] = SimpleNamespace(
        task_instance_mutation_hook=_task_instance_mutation_hook_without_dag_run
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
        ((3, 2, 2), _downgrade_executor_sentry_flag, "BaseExecutor attribute contract"),
        ((3, 2, 2), _add_connection_test_support, "BaseExecutor attribute contract"),
        ((3, 3, 0), _remove_sdk_listener_manager, "airflow.sdk.listener manager"),
        (
            (3, 2, 2),
            _add_dag_run_to_mutation_hook,
            "task_instance_mutation_hook.dag_run",
        ),
        (
            (3, 3, 0),
            _remove_dag_run_from_mutation_hook,
            "task_instance_mutation_hook.dag_run",
        ),
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


def test_unexpected_asset_unique_key_import_failure_is_wrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retain initialization failures from the newer asset unique-key module."""

    modules = _fake_modules((3, 2, 2))
    failure = RuntimeError("broken asset serialization module initialization")

    def broken_import(name: str, package: str | None = None) -> object:
        """Fail unexpectedly while probing the canonical asset unique-key module."""

        del package
        if name == "airflow.serialization.definitions.assets":
            raise failure
        return modules[name]

    def fake_version(distribution_name: str) -> str:
        """Return certified metadata for the asset unique-key failure test."""

        if distribution_name == capability_module.AIRFLOW_META_DISTRIBUTION:
            raise capability_module.metadata.PackageNotFoundError(distribution_name)
        assert distribution_name == AIRFLOW_DISTRIBUTION
        return "3.2.2"

    monkeypatch.setattr(capability_module.metadata, "version", fake_version)
    monkeypatch.setattr(capability_module, "import_module", broken_import)

    with pytest.raises(AirflowCompatibilityError, match="SerializedAssetUniqueKey") as caught:
        capability_module.resolve_capabilities()

    assert caught.value.__cause__ is failure


def test_executor_contract_rejects_neither_sentry_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """Name the probed symbol when a vendor executor drops both sentry flags."""

    modules = _fake_modules((3, 3, 0))
    modules["airflow.executors.base_executor"] = SimpleNamespace(
        BaseExecutor=type("BareExecutor", (), {})
    )
    _install_fake_environment(monkeypatch, "3.3.0+vendor", modules)

    with pytest.raises(
        AirflowCompatibilityError,
        match=r"airflow\.executors\.base_executor\.BaseExecutor\.sentry_integration",
    ) as caught:
        capability_module.resolve_capabilities()

    assert isinstance(caught.value.__cause__, ValueError)


def test_sdk_listener_manager_import_failure_is_wrapped(monkeypatch: pytest.MonkeyPatch) -> None:
    """Retain initialization failures from the SDK listener manager module."""

    modules = _fake_modules((3, 3, 0))
    failure = RuntimeError("broken sdk listener module initialization")

    def broken_import(name: str, package: str | None = None) -> object:
        """Fail unexpectedly while probing the SDK listener manager module."""

        del package
        if name == "airflow.sdk.listener":
            raise failure
        return modules[name]

    def fake_version(distribution_name: str) -> str:
        """Return certified metadata for the SDK listener manager failure test."""

        del distribution_name
        return "3.3.0"

    monkeypatch.setattr(capability_module.metadata, "version", fake_version)
    monkeypatch.setattr(capability_module, "import_module", broken_import)

    with pytest.raises(
        AirflowCompatibilityError, match=r"airflow\.sdk\.listener\.get_listener_manager"
    ) as caught:
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
                (3, 3, 1),
            }
            assert resolve_capabilities() is capabilities
        """
    )

    result = pytester.runpytest_subprocess("-q")

    result.assert_outcomes(passed=1)
