"""Probe and validate the private Airflow surface used by test fixtures.

No Airflow module is imported until ``resolve_capabilities`` runs after bootstrap.

References:
    https://docs.python.org/3/library/importlib.html#importlib.import_module
    https://docs.python.org/3/library/importlib.metadata.html
    https://docs.python.org/3/library/inspect.html#inspect.signature
    https://docs.pydantic.dev/latest/concepts/models/#model-methods-and-properties
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from importlib import import_module, metadata
from inspect import signature
from typing import NoReturn

Release = tuple[int, int, int]

SUPPORTED_RELEASES: tuple[Release, ...] = (
    (3, 1, 0),
    (3, 1, 8),
    (3, 2, 0),
    (3, 2, 2),
    (3, 3, 0),
)
SUPPORTED_VERSIONS = tuple(
    ".".join(str(part) for part in release) for release in SUPPORTED_RELEASES
)
AIRFLOW_DISTRIBUTION = "apache-airflow-core"
VERSION_PATTERN = re.compile(
    r"^(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)"
    r"(?:[-_.]?dev(?:[-_.]?\d+)?)?(?:\+[a-z0-9]+(?:[-_.][a-z0-9]+)*)?$",
    re.IGNORECASE,
)


class AirflowCompatibilityError(RuntimeError):
    """Report an unsupported or structurally incompatible Airflow installation."""


class DagBagLocation(str, Enum):
    """Closed set of certified canonical ``DagBag`` module locations."""

    MODELS = "airflow.models.dagbag"
    DAG_PROCESSING = "airflow.dag_processing.dagbag"


class TaskInstanceRunner(str, Enum):
    """Closed set of certified task-instance execution entry points."""

    LEGACY_RUN = "TaskInstance.run"
    SDK_RUN_TASK = "airflow.sdk.definitions.dag._run_task"


class _SerializedDagLocation(str, Enum):
    """Closed set of certified ``SerializedDAG`` module locations."""

    SERIALIZED_OBJECTS = "airflow.serialization.serialized_objects"
    DEFINITIONS = "airflow.serialization.definitions.dag"


@dataclass(frozen=True)
class AirflowCapabilities:
    """Immutable metadata describing a validated Airflow private interface.

    Parameters:
        release: tuple[int, int, int] containing the certified base release.
        dag_bag_location: DagBagLocation naming the canonical import location.
        dag_bag_supports_include_examples: bool indicating constructor support.
        task_instance_runner: TaskInstanceRunner selecting the execution entry point.
        refresh_from_task_supports_dag_run: bool indicating ``dag_run`` keyword support.
        startup_details_supports_sentry: bool indicating the sentry model field.
        runtime_task_instance_supports_queue: bool indicating the runtime DTO queue field.
    """

    release: Release
    dag_bag_location: DagBagLocation
    dag_bag_supports_include_examples: bool
    task_instance_runner: TaskInstanceRunner
    refresh_from_task_supports_dag_run: bool
    startup_details_supports_sentry: bool
    runtime_task_instance_supports_queue: bool


_CERTIFIED_CAPABILITIES = {
    (3, 1, 0): AirflowCapabilities(
        release=(3, 1, 0),
        dag_bag_location=DagBagLocation.MODELS,
        dag_bag_supports_include_examples=True,
        task_instance_runner=TaskInstanceRunner.LEGACY_RUN,
        refresh_from_task_supports_dag_run=False,
        startup_details_supports_sentry=False,
        runtime_task_instance_supports_queue=False,
    ),
    (3, 1, 8): AirflowCapabilities(
        release=(3, 1, 8),
        dag_bag_location=DagBagLocation.MODELS,
        dag_bag_supports_include_examples=True,
        task_instance_runner=TaskInstanceRunner.LEGACY_RUN,
        refresh_from_task_supports_dag_run=False,
        startup_details_supports_sentry=False,
        runtime_task_instance_supports_queue=False,
    ),
    (3, 2, 0): AirflowCapabilities(
        release=(3, 2, 0),
        dag_bag_location=DagBagLocation.DAG_PROCESSING,
        dag_bag_supports_include_examples=True,
        task_instance_runner=TaskInstanceRunner.SDK_RUN_TASK,
        refresh_from_task_supports_dag_run=False,
        startup_details_supports_sentry=True,
        runtime_task_instance_supports_queue=False,
    ),
    (3, 2, 2): AirflowCapabilities(
        release=(3, 2, 2),
        dag_bag_location=DagBagLocation.DAG_PROCESSING,
        dag_bag_supports_include_examples=True,
        task_instance_runner=TaskInstanceRunner.SDK_RUN_TASK,
        refresh_from_task_supports_dag_run=False,
        startup_details_supports_sentry=True,
        runtime_task_instance_supports_queue=False,
    ),
    (3, 3, 0): AirflowCapabilities(
        release=(3, 3, 0),
        dag_bag_location=DagBagLocation.DAG_PROCESSING,
        dag_bag_supports_include_examples=False,
        task_instance_runner=TaskInstanceRunner.SDK_RUN_TASK,
        refresh_from_task_supports_dag_run=True,
        startup_details_supports_sentry=True,
        runtime_task_instance_supports_queue=True,
    ),
}
_CERTIFIED_SERIALIZED_DAG_LOCATIONS = {
    (3, 1, 0): _SerializedDagLocation.SERIALIZED_OBJECTS,
    (3, 1, 8): _SerializedDagLocation.SERIALIZED_OBJECTS,
    (3, 2, 0): _SerializedDagLocation.DEFINITIONS,
    (3, 2, 2): _SerializedDagLocation.DEFINITIONS,
    (3, 3, 0): _SerializedDagLocation.DEFINITIONS,
}
_REQUIRED_SYMBOLS = (
    ("airflow.sdk", "DAG"),
    ("airflow.utils.db", "initdb"),
    ("airflow.utils.session", "create_session"),
    ("airflow.models.dagrun", "DagRun"),
    ("airflow.models.serialized_dag", "SerializedDagModel"),
    ("airflow.models.dag", "DagModel"),
    ("airflow.models.dag_version", "DagVersion"),
    ("airflow.models.dagbundle", "DagBundleModel"),
    ("airflow.serialization.serialized_objects", "LazyDeserializedDAG"),
    ("airflow.sdk.execution_time.task_runner", "RuntimeTaskInstance"),
    ("airflow.sdk.execution_time.task_runner", "parse"),
    ("airflow.sdk.execution_time.task_runner", "run"),
    ("airflow.sdk.execution_time.comms", "CommsDecoder"),
    ("airflow.sdk.execution_time.comms", "BundleInfo"),
    ("airflow.sdk.execution_time.comms", "ToSupervisor"),
    ("airflow.sdk.execution_time.xcom", "XCom"),
    ("airflow.sdk.api.datamodels._generated", "DagRun"),
    ("airflow.sdk.api.datamodels._generated", "DagRunState"),
    ("airflow.sdk.api.datamodels._generated", "TaskInstanceState"),
    ("airflow.sdk.api.datamodels._generated", "TIRunContext"),
)

_CAPABILITIES: AirflowCapabilities | None = None


def _raise_compatibility_error(
    installed_version: str,
    operation: str,
    symbol: str,
    error: Exception,
) -> NoReturn:
    """Raise one actionable compatibility error while retaining its cause.

    Parameters:
        installed_version: str reported by package metadata, or an absence marker.
        operation: str describing the failed validation operation.
        symbol: str naming the distribution, module, field, or callable involved.
        error: Exception that caused validation to fail.

    Raises:
        AirflowCompatibilityError: Always, chained from ``error``.
    """

    supported = ", ".join(SUPPORTED_VERSIONS)
    raise AirflowCompatibilityError(
        f"Apache Airflow compatibility validation failed for installed version "
        f"'{installed_version}' while {operation} `{symbol}`: {error}. Install one of the "
        f"supported `{AIRFLOW_DISTRIBUTION}` versions: {supported}."
    ) from error


def _installed_release() -> tuple[str, Release]:
    """Read and validate the installed Airflow base release without importing Airflow.

    Returns:
        tuple[str, Release] containing the metadata version and parsed base release.

    Raises:
        AirflowCompatibilityError: Package metadata is absent, malformed, or unsupported.
    """

    try:
        installed_version = metadata.version(AIRFLOW_DISTRIBUTION)
    except Exception as error:
        _raise_compatibility_error(
            "<not installed>", "reading package metadata for", AIRFLOW_DISTRIBUTION, error
        )

    match = VERSION_PATTERN.fullmatch(installed_version)
    if match is None:
        error = ValueError("version is not a standard final, development, or local release")
        _raise_compatibility_error(
            installed_version, "parsing installed version for", AIRFLOW_DISTRIBUTION, error
        )

    release = (
        int(match.group("major")),
        int(match.group("minor")),
        int(match.group("patch")),
    )
    if release not in SUPPORTED_RELEASES:
        error = ValueError("base release is not certified")
        _raise_compatibility_error(
            installed_version, "validating installed version for", AIRFLOW_DISTRIBUTION, error
        )
    return installed_version, release


def _resolve_symbol(module_name: str, symbol_name: str, installed_version: str) -> object:
    """Import one required Airflow symbol and wrap import or lookup failures.

    Parameters:
        module_name: str containing the private Airflow module path.
        symbol_name: str naming the required module attribute.
        installed_version: str reported by package metadata.

    Returns:
        object containing the resolved symbol.

    Raises:
        AirflowCompatibilityError: The module or symbol cannot be resolved.
    """

    qualified_name = f"{module_name}.{symbol_name}"
    try:
        module = import_module(module_name)
        return getattr(module, symbol_name)
    except Exception as error:
        _raise_compatibility_error(
            installed_version, "resolving required Airflow symbol", qualified_name, error
        )


def _signature_has_parameter(
    symbol: object, qualified_name: str, parameter: str, version: str
) -> bool:
    """Probe whether a callable signature contains one named parameter.

    Parameters:
        symbol: object containing the callable to inspect.
        qualified_name: str naming the callable for diagnostics.
        parameter: str naming the parameter to probe.
        version: str reported by package metadata.

    Returns:
        bool indicating whether the parameter is present.

    Raises:
        AirflowCompatibilityError: The callable signature cannot be inspected.
    """

    if not callable(symbol):
        error = TypeError("symbol is not callable")
        _raise_compatibility_error(version, "inspecting signature of", qualified_name, error)
    try:
        return parameter in signature(symbol).parameters
    except (TypeError, ValueError) as error:
        _raise_compatibility_error(version, "inspecting signature of", qualified_name, error)


def _model_has_field(symbol: object, qualified_name: str, field: str, version: str) -> bool:
    """Probe one Pydantic model field without constructing the model.

    Parameters:
        symbol: object containing the Pydantic model class.
        qualified_name: str naming the model for diagnostics.
        field: str naming the field to probe.
        version: str reported by package metadata.

    Returns:
        bool indicating whether the model field is present.

    Raises:
        AirflowCompatibilityError: The symbol does not expose Pydantic ``model_fields``.
    """

    model_fields = getattr(symbol, "model_fields", None)
    if not isinstance(model_fields, dict):
        error = TypeError("symbol does not expose a `model_fields` dictionary")
        _raise_compatibility_error(
            version, "probing Pydantic model fields on", qualified_name, error
        )
    return field in model_fields


def _probe_dag_bag(version: str) -> tuple[DagBagLocation, object]:
    """Resolve the canonical DagBag location by capability rather than release.

    Parameters:
        version: str reported by package metadata.

    Returns:
        tuple[DagBagLocation, object] containing the location and class.

    Raises:
        AirflowCompatibilityError: Neither certified DagBag location resolves.
    """

    module_name = DagBagLocation.DAG_PROCESSING.value
    try:
        module = import_module(module_name)
        dag_bag = module.DagBag
    except (ImportError, AttributeError):
        return DagBagLocation.MODELS, _resolve_symbol(
            DagBagLocation.MODELS.value, "DagBag", version
        )
    except Exception as error:
        _raise_compatibility_error(
            version, "probing canonical Airflow symbol", f"{module_name}.DagBag", error
        )
    return DagBagLocation.DAG_PROCESSING, dag_bag


def _probe_serialized_dag(version: str) -> _SerializedDagLocation:
    """Resolve the canonical SerializedDAG location by capability.

    Parameters:
        version: str reported by package metadata.

    Returns:
        _SerializedDagLocation naming the resolved module.

    Raises:
        AirflowCompatibilityError: Neither certified SerializedDAG location resolves.
    """

    module_name = _SerializedDagLocation.DEFINITIONS.value
    try:
        module = import_module(module_name)
        _ = module.SerializedDAG
    except (ImportError, AttributeError):
        _resolve_symbol(_SerializedDagLocation.SERIALIZED_OBJECTS.value, "SerializedDAG", version)
        return _SerializedDagLocation.SERIALIZED_OBJECTS
    except Exception as error:
        _raise_compatibility_error(
            version, "probing canonical Airflow symbol", f"{module_name}.SerializedDAG", error
        )
    return _SerializedDagLocation.DEFINITIONS


def _probe_task_instance_runner(task_instance: object, version: str) -> TaskInstanceRunner:
    """Resolve legacy or Task SDK task execution behavior.

    Parameters:
        task_instance: object containing the ORM TaskInstance class.
        version: str reported by package metadata.

    Returns:
        TaskInstanceRunner selecting the callable execution path.

    Raises:
        AirflowCompatibilityError: Neither execution entry point is callable.
    """

    legacy_run = getattr(task_instance, "run", None)
    if callable(legacy_run):
        return TaskInstanceRunner.LEGACY_RUN

    qualified_name = TaskInstanceRunner.SDK_RUN_TASK.value
    module_name, _, symbol_name = qualified_name.rpartition(".")
    run_task = _resolve_symbol(module_name, symbol_name, version)
    if not callable(run_task):
        error = TypeError("resolved symbol is not callable")
        _raise_compatibility_error(
            version, "validating callable Airflow symbol", qualified_name, error
        )
    return TaskInstanceRunner.SDK_RUN_TASK


def _verify_value(
    observed: object,
    expected: object,
    symbol: str,
    installed_version: str,
) -> None:
    """Verify one probe against its certified contract value.

    Parameters:
        observed: object containing the probed value.
        expected: object containing the certified value.
        symbol: str naming the capability or private symbol.
        installed_version: str reported by package metadata.

    Raises:
        AirflowCompatibilityError: The installation differs from the certified contract.
    """

    if observed == expected:
        return
    error = ValueError(f"expected {expected!r}, observed {observed!r}")
    _raise_compatibility_error(
        installed_version, "verifying certified capability contract for", symbol, error
    )


def _verify_contract(
    observed: AirflowCapabilities,
    serialized_dag_location: _SerializedDagLocation,
    installed_version: str,
) -> None:
    """Verify all probes against the exact certified release contract.

    Parameters:
        observed: AirflowCapabilities produced from runtime probes.
        serialized_dag_location: _SerializedDagLocation produced from import probes.
        installed_version: str reported by package metadata.

    Raises:
        AirflowCompatibilityError: Any probe differs from its certified value.
    """

    expected = _CERTIFIED_CAPABILITIES[observed.release]
    checks = (
        (observed.dag_bag_location, expected.dag_bag_location, "DagBag canonical location"),
        (
            observed.dag_bag_supports_include_examples,
            expected.dag_bag_supports_include_examples,
            "DagBag.__init__.include_examples",
        ),
        (
            observed.task_instance_runner,
            expected.task_instance_runner,
            "TaskInstance task runner",
        ),
        (
            observed.refresh_from_task_supports_dag_run,
            expected.refresh_from_task_supports_dag_run,
            "TaskInstance.refresh_from_task.dag_run",
        ),
        (
            observed.startup_details_supports_sentry,
            expected.startup_details_supports_sentry,
            "StartupDetails.sentry_integration",
        ),
        (
            observed.runtime_task_instance_supports_queue,
            expected.runtime_task_instance_supports_queue,
            "TaskInstance DTO queue",
        ),
        (
            serialized_dag_location,
            _CERTIFIED_SERIALIZED_DAG_LOCATIONS[observed.release],
            "SerializedDAG canonical location",
        ),
    )
    for actual, certified, symbol in checks:
        _verify_value(actual, certified, symbol, installed_version)


def _resolve_uncached(installed_version: str, release: Release) -> AirflowCapabilities:
    """Import, probe, and validate every Airflow dependency used by eventual fixtures.

    Parameters:
        installed_version: str reported by package metadata.
        release: tuple[int, int, int] containing the certified base release.

    Returns:
        AirflowCapabilities containing validated metadata only.

    Raises:
        AirflowCompatibilityError: A symbol is unavailable or a probe violates the contract.
    """

    dag_bag_location, dag_bag = _probe_dag_bag(installed_version)
    task_instance = _resolve_symbol(
        "airflow.models.taskinstance", "TaskInstance", installed_version
    )
    startup_details = _resolve_symbol(
        "airflow.sdk.execution_time.comms", "StartupDetails", installed_version
    )
    runtime_task_instance_dto = _resolve_symbol(
        "airflow.sdk.api.datamodels._generated", "TaskInstance", installed_version
    )
    serialized_dag_location = _probe_serialized_dag(installed_version)

    observed = AirflowCapabilities(
        release=release,
        dag_bag_location=dag_bag_location,
        dag_bag_supports_include_examples=_signature_has_parameter(
            dag_bag, f"{dag_bag_location.value}.DagBag", "include_examples", installed_version
        ),
        task_instance_runner=_probe_task_instance_runner(task_instance, installed_version),
        refresh_from_task_supports_dag_run=_signature_has_parameter(
            getattr(task_instance, "refresh_from_task", None),
            "airflow.models.taskinstance.TaskInstance.refresh_from_task",
            "dag_run",
            installed_version,
        ),
        startup_details_supports_sentry=_model_has_field(
            startup_details,
            "airflow.sdk.execution_time.comms.StartupDetails",
            "sentry_integration",
            installed_version,
        ),
        runtime_task_instance_supports_queue=_model_has_field(
            runtime_task_instance_dto,
            "airflow.sdk.api.datamodels._generated.TaskInstance",
            "queue",
            installed_version,
        ),
    )

    for module_name, symbol_name in _REQUIRED_SYMBOLS:
        _resolve_symbol(module_name, symbol_name, installed_version)

    _verify_contract(observed, serialized_dag_location, installed_version)
    return observed


def resolve_capabilities() -> AirflowCapabilities:
    """Resolve and cache the validated Airflow compatibility contract after bootstrap.

    Returns:
        AirflowCapabilities containing immutable capability metadata.

    Raises:
        AirflowCompatibilityError: The Airflow release or private interface is unsupported.
    """

    global _CAPABILITIES
    if _CAPABILITIES is not None:
        return _CAPABILITIES

    installed_version, release = _installed_release()
    capabilities = _resolve_uncached(installed_version, release)
    _CAPABILITIES = capabilities
    return capabilities


def _reset_capabilities_for_testing() -> None:
    """Clear the successful-resolution cache for isolated compatibility tests."""

    global _CAPABILITIES
    _CAPABILITIES = None


__all__ = (
    "AirflowCapabilities",
    "AirflowCompatibilityError",
    "DagBagLocation",
    "TaskInstanceRunner",
    "resolve_capabilities",
)
