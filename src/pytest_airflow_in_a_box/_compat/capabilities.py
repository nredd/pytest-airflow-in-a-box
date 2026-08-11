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
import sys
from dataclasses import dataclass
from enum import Enum
from importlib import import_module, metadata
from inspect import signature
from typing import NoReturn

from packaging.version import InvalidVersion, Version

Release = tuple[int, int, int]


class AirflowFamily(str, Enum):
    """Closed set of Airflow distribution families, valued by distribution name."""

    V2 = "apache-airflow"
    V3 = "apache-airflow-core"


SUPPORTED_RELEASES_BY_FAMILY: dict[AirflowFamily, tuple[Release, ...]] = {
    AirflowFamily.V3: (
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
    ),
    # Certified per issue #41: the final 2.x release, Composer 3's exact 2.10 patch, and
    # the oldest line still shipped by a managed vendor. Spike-verified 2026-08-11.
    AirflowFamily.V2: (
        (2, 9, 3),
        (2, 10, 5),
        (2, 11, 2),
    ),
}
SUPPORTED_RELEASES = SUPPORTED_RELEASES_BY_FAMILY[AirflowFamily.V3]
SUPPORTED_VERSIONS = tuple(
    ".".join(str(part) for part in release) for release in SUPPORTED_RELEASES
)
SUPPORTED_VERSIONS_V2 = tuple(
    ".".join(str(part) for part in release)
    for release in SUPPORTED_RELEASES_BY_FAMILY[AirflowFamily.V2]
)
# The maximum Python minor each family supports; 2.x never runs on 3.13+ and its
# `requires-python` uses bare `!=3.13` exclusions pip does not enforce (see #41).
MAX_V2_PYTHON = (3, 12)
AIRFLOW_DISTRIBUTION = AirflowFamily.V3.value
AIRFLOW_META_DISTRIBUTION = AirflowFamily.V2.value
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


class DagRunInterface(str, Enum):
    """Closed set of certified ``create_dagrun`` date-keyword interfaces."""

    EXECUTION_DATE = "execution_date"
    LOGICAL_DATE = "logical_date"


class ApiSurface(str, Enum):
    """Closed set of certified REST API server entry points."""

    WEBSERVER = "airflow webserver"
    API_SERVER = "airflow api-server"


class ParamsLocation(str, Enum):
    """Closed set of certified params-validation module locations."""

    MODELS = "airflow.models.param"
    SDK = "airflow.sdk.definitions.param"


class TimezoneLocation(str, Enum):
    """Closed set of certified timezone-helper module locations."""

    UTILS = "airflow.utils.timezone"
    SDK = "airflow.sdk.timezone"


@dataclass(frozen=True)
class AirflowCapabilities:
    """Immutable metadata describing a validated Airflow private interface.

    Parameters:
        release: tuple[int, int, int] containing the certified base release.
        family: AirflowFamily naming the installed distribution family.
        dag_bag_location: DagBagLocation naming the canonical import location.
        dag_bag_supports_include_examples: bool indicating constructor support.
        task_instance_runner: TaskInstanceRunner selecting the execution entry point.
        refresh_from_task_supports_dag_run: bool indicating ``dag_run`` keyword support.
        startup_details_supports_sentry: bool | None indicating the sentry model field;
            None on 2.x, which has no Task SDK to probe.
        runtime_task_instance_supports_queue: bool | None indicating the runtime DTO
            queue field; None on 2.x, which has no Task SDK to probe.
        has_task_sdk: bool indicating the `airflow.sdk` execution stack exists.
        uses_structlog: bool indicating task logs flow through structlog.
        has_dag_versioning: bool indicating DAG bundle/version models exist.
        dagrun_interface: DagRunInterface selecting the ``create_dagrun`` date keyword.
        api_surface: ApiSurface naming the REST API server entry point.
        params_location: ParamsLocation naming the params-validation module.
        timezone_location: TimezoneLocation naming the timezone-helper module.
    """

    release: Release
    family: AirflowFamily
    dag_bag_location: DagBagLocation
    dag_bag_supports_include_examples: bool
    task_instance_runner: TaskInstanceRunner
    refresh_from_task_supports_dag_run: bool
    startup_details_supports_sentry: bool | None
    runtime_task_instance_supports_queue: bool | None
    has_task_sdk: bool
    uses_structlog: bool
    has_dag_versioning: bool
    dagrun_interface: DagRunInterface
    api_surface: ApiSurface
    params_location: ParamsLocation
    timezone_location: TimezoneLocation


def _certify_v3(
    release: Release,
    *,
    dag_bag_location: DagBagLocation,
    dag_bag_supports_include_examples: bool,
    task_instance_runner: TaskInstanceRunner,
    refresh_from_task_supports_dag_run: bool,
    startup_details_supports_sentry: bool,
    runtime_task_instance_supports_queue: bool,
) -> AirflowCapabilities:
    """Build one certified 3.x contract row with the family-static fields filled.

    Parameters:
        release: tuple[int, int, int] containing the certified base release.
        dag_bag_location: DagBagLocation naming the canonical import location.
        dag_bag_supports_include_examples: bool indicating constructor support.
        task_instance_runner: TaskInstanceRunner selecting the execution entry point.
        refresh_from_task_supports_dag_run: bool indicating ``dag_run`` keyword support.
        startup_details_supports_sentry: bool indicating the sentry model field.
        runtime_task_instance_supports_queue: bool indicating the runtime DTO queue field.

    Returns:
        AirflowCapabilities containing the complete certified contract.
    """

    return AirflowCapabilities(
        release=release,
        family=AirflowFamily.V3,
        dag_bag_location=dag_bag_location,
        dag_bag_supports_include_examples=dag_bag_supports_include_examples,
        task_instance_runner=task_instance_runner,
        refresh_from_task_supports_dag_run=refresh_from_task_supports_dag_run,
        startup_details_supports_sentry=startup_details_supports_sentry,
        runtime_task_instance_supports_queue=runtime_task_instance_supports_queue,
        has_task_sdk=True,
        uses_structlog=True,
        has_dag_versioning=True,
        dagrun_interface=DagRunInterface.LOGICAL_DATE,
        api_surface=ApiSurface.API_SERVER,
        params_location=ParamsLocation.SDK,
        timezone_location=TimezoneLocation.SDK,
    )


def _certify_v2(release: Release) -> AirflowCapabilities:
    """Build one certified 2.x contract row.

    Every probed value is uniform across the certified 2.x releases -- the Phase 1a
    spike (2026-08-11) observed identical signatures on 2.9.3, 2.10.5, and 2.11.2 for
    every symbol the plugin touches.

    Parameters:
        release: tuple[int, int, int] containing the certified base release.

    Returns:
        AirflowCapabilities containing the complete certified contract.
    """

    return AirflowCapabilities(
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
    )


_CERTIFIED_CAPABILITIES = (
    {release: _certify_v2(release) for release in SUPPORTED_RELEASES_BY_FAMILY[AirflowFamily.V2]}
    | {
        release: _certify_v3(
            release,
            dag_bag_location=DagBagLocation.MODELS,
            dag_bag_supports_include_examples=True,
            task_instance_runner=TaskInstanceRunner.LEGACY_RUN,
            refresh_from_task_supports_dag_run=False,
            startup_details_supports_sentry=False,
            runtime_task_instance_supports_queue=False,
        )
        for release in SUPPORTED_RELEASES_BY_FAMILY[AirflowFamily.V3]
        if release < (3, 2, 0)
    }
    | {
        (3, 2, 0): _certify_v3(
            (3, 2, 0),
            dag_bag_location=DagBagLocation.DAG_PROCESSING,
            dag_bag_supports_include_examples=True,
            task_instance_runner=TaskInstanceRunner.SDK_RUN_TASK,
            refresh_from_task_supports_dag_run=False,
            startup_details_supports_sentry=True,
            runtime_task_instance_supports_queue=False,
        ),
        (3, 2, 1): _certify_v3(
            (3, 2, 1),
            dag_bag_location=DagBagLocation.DAG_PROCESSING,
            dag_bag_supports_include_examples=True,
            task_instance_runner=TaskInstanceRunner.SDK_RUN_TASK,
            refresh_from_task_supports_dag_run=False,
            startup_details_supports_sentry=True,
            runtime_task_instance_supports_queue=False,
        ),
        (3, 2, 2): _certify_v3(
            (3, 2, 2),
            dag_bag_location=DagBagLocation.DAG_PROCESSING,
            dag_bag_supports_include_examples=True,
            task_instance_runner=TaskInstanceRunner.SDK_RUN_TASK,
            refresh_from_task_supports_dag_run=False,
            startup_details_supports_sentry=True,
            runtime_task_instance_supports_queue=False,
        ),
        (3, 3, 0): _certify_v3(
            (3, 3, 0),
            dag_bag_location=DagBagLocation.DAG_PROCESSING,
            dag_bag_supports_include_examples=False,
            task_instance_runner=TaskInstanceRunner.SDK_RUN_TASK,
            refresh_from_task_supports_dag_run=True,
            startup_details_supports_sentry=True,
            runtime_task_instance_supports_queue=True,
        ),
    }
)
_CERTIFIED_SERIALIZED_DAG_LOCATIONS = {
    (2, 9, 3): _SerializedDagLocation.SERIALIZED_OBJECTS,
    (2, 10, 5): _SerializedDagLocation.SERIALIZED_OBJECTS,
    (2, 11, 2): _SerializedDagLocation.SERIALIZED_OBJECTS,
    (3, 1, 0): _SerializedDagLocation.SERIALIZED_OBJECTS,
    (3, 1, 1): _SerializedDagLocation.SERIALIZED_OBJECTS,
    (3, 1, 2): _SerializedDagLocation.SERIALIZED_OBJECTS,
    (3, 1, 3): _SerializedDagLocation.SERIALIZED_OBJECTS,
    (3, 1, 5): _SerializedDagLocation.SERIALIZED_OBJECTS,
    (3, 1, 6): _SerializedDagLocation.SERIALIZED_OBJECTS,
    (3, 1, 7): _SerializedDagLocation.SERIALIZED_OBJECTS,
    (3, 1, 8): _SerializedDagLocation.SERIALIZED_OBJECTS,
    (3, 2, 0): _SerializedDagLocation.DEFINITIONS,
    (3, 2, 1): _SerializedDagLocation.DEFINITIONS,
    (3, 2, 2): _SerializedDagLocation.DEFINITIONS,
    (3, 3, 0): _SerializedDagLocation.DEFINITIONS,
}
_COMMON_REQUIRED_SYMBOLS = (
    ("airflow.utils.db", "initdb"),
    ("airflow.utils.session", "create_session"),
    ("airflow.models.dagrun", "DagRun"),
    ("airflow.models.serialized_dag", "SerializedDagModel"),
    ("airflow.models.dag", "DagModel"),
)
_V3_REQUIRED_SYMBOLS = (
    ("airflow.sdk", "DAG"),
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
# Spike-verified present on 2.9.3/2.10.5/2.11.2 (2026-08-11); the surface Phase 2's
# fixture branches consume.
_V2_REQUIRED_SYMBOLS = (
    ("airflow.models.dag", "DAG"),
    ("airflow.models.param", "ParamsDict"),
    ("airflow.exceptions", "ParamValidationError"),
    ("airflow.exceptions", "RemovedInAirflow3Warning"),
    ("airflow.exceptions", "AirflowProviderDeprecationWarning"),
    ("airflow.utils.timezone", "utcnow"),
    ("airflow.utils.timezone", "coerce_datetime"),
    ("airflow.utils.timezone", "convert_to_utc"),
    ("airflow.models.dataset", "DatasetModel"),
)
_REQUIRED_SYMBOLS_BY_FAMILY = {
    AirflowFamily.V2: _COMMON_REQUIRED_SYMBOLS + _V2_REQUIRED_SYMBOLS,
    AirflowFamily.V3: _COMMON_REQUIRED_SYMBOLS + _V3_REQUIRED_SYMBOLS,
}

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
    supported_v2 = ", ".join(SUPPORTED_VERSIONS_V2)
    raise AirflowCompatibilityError(
        f"Apache Airflow compatibility validation failed for installed version "
        f"'{installed_version}' while {operation} `{symbol}`: {error}. Install one of the "
        f"supported `{AIRFLOW_DISTRIBUTION}` versions: {supported}, or one of the "
        f"supported `{AIRFLOW_META_DISTRIBUTION}` versions: {supported_v2}."
    ) from error


@dataclass(frozen=True)
class _MetaDistribution:
    """Observed `apache-airflow` meta-distribution state.

    Parameters:
        version: str | None containing the metadata version, or None when the
            meta-distribution is absent or unreadable.
        major: int | None containing the PEP 440 major component, or None when no
            version is available or it does not parse.
        unreadable: str | None describing a metadata read failure other than a clean
            not-installed, or None when the read succeeded or found nothing.
    """

    version: str | None
    major: int | None
    unreadable: str | None = None


def _meta_distribution() -> _MetaDistribution:
    """Read the `apache-airflow` meta-distribution state without importing Airflow.

    A read failure other than a clean not-installed is reported as `unreadable` rather
    than propagated: the module's contract of raising only `AirflowCompatibilityError`
    must survive a broken metadata backend, and a half-clobbered dist-info is itself a
    realistic signature of the dual-family corruption the callers guard against, so the
    failure text is preserved for their messages instead of being swallowed.

    Returns:
        _MetaDistribution containing the observed version string, PEP 440 major, and
        any read-failure description.
    """

    try:
        installed_version = metadata.version(AIRFLOW_META_DISTRIBUTION)
    except metadata.PackageNotFoundError:
        return _MetaDistribution(version=None, major=None)
    except Exception as error:
        return _MetaDistribution(
            version=None, major=None, unreadable=f"{type(error).__name__}: {error}"
        )
    try:
        parsed = Version(installed_version)
    except InvalidVersion:
        return _MetaDistribution(version=installed_version, major=None)
    return _MetaDistribution(version=installed_version, major=parsed.release[0])


def _reject_corrupt_environment() -> None:
    """Reject a non-3.x `apache-airflow` coexisting with the installed 3.x core.

    Both majors install the same `airflow` package, so their coexistence is file-level
    corruption that pip does not detect. This check runs before family classification;
    a 3.x meta-package alongside the core is the normal shape, not an error. The check
    fails closed: a meta-distribution version that does not parse, or parses below
    major 3 (including dev fallbacks like '0.1.dev0'), is treated as corruption and the
    offending version is named so a false positive stays debuggable.

    Raises:
        AirflowCompatibilityError: A non-3.x or unparseable `apache-airflow`
            distribution is installed next to `apache-airflow-core`.
    """

    meta = _meta_distribution()
    if meta.unreadable is not None:
        raise AirflowCompatibilityError(
            f"Corrupt Airflow installation: `{AIRFLOW_META_DISTRIBUTION}` is present "
            f"but its metadata is unreadable ({meta.unreadable}) next to "
            f"`{AIRFLOW_DISTRIBUTION}` -- a half-clobbered install is the signature of "
            f"both Airflow families sharing one environment. Recreate the environment "
            f"and install `pytest-airflow-in-a-box[airflow3]` for a supported "
            f"Airflow 3.x."
        )
    if meta.version is None or (meta.major is not None and meta.major >= 3):
        return
    raise AirflowCompatibilityError(
        f"Corrupt Airflow installation: `{AIRFLOW_META_DISTRIBUTION}` '{meta.version}' "
        f"coexists with `{AIRFLOW_DISTRIBUTION}` -- both install the `airflow` package, "
        f"so any 2.x files are silently overwritten by the 3.x core. Recreate the "
        f"environment and install `pytest-airflow-in-a-box[airflow3]` for a supported "
        f"Airflow 3.x. If `{AIRFLOW_META_DISTRIBUTION}` is a source checkout with a dev "
        f"fallback version, install the core distribution alone instead."
    )


def installed_family() -> AirflowFamily | None:
    """Classify the installed Airflow family from metadata alone, without importing Airflow.

    This is the one probe callable from pre-import bootstrap. It never raises: a corrupt
    or Airflow-free environment classifies best-effort (core metadata wins) or None, and
    `resolve_capabilities()` remains the authority that rejects invalid environments
    with actionable errors once a test actually needs Airflow.

    Returns:
        AirflowFamily | None naming the installed family, or None when neither
        distribution is readable.
    """

    try:
        metadata.version(AIRFLOW_DISTRIBUTION)
    except metadata.PackageNotFoundError:
        meta = _meta_distribution()
        if meta.major is not None and meta.major < 3:
            return AirflowFamily.V2
        return None
    except Exception:
        return None
    return AirflowFamily.V3


def _running_python() -> tuple[int, int]:
    """Report the running Python major and minor version.

    Returns:
        tuple[int, int] containing `sys.version_info[:2]`.
    """

    return sys.version_info[:2]


def _installed_v2_release(
    error: metadata.PackageNotFoundError,
) -> tuple[str, Release, AirflowFamily]:
    """Accept a certified Airflow 2.x monolith when the 3.x core is absent.

    Parameters:
        error: importlib.metadata.PackageNotFoundError raised for the core distribution.

    Returns:
        tuple[str, Release, AirflowFamily] containing the metadata version, parsed base
        release, and `AirflowFamily.V2`.

    Raises:
        AirflowCompatibilityError: No usable Airflow family is installed, the running
            Python exceeds the 2.x cap, or the 2.x release is not certified.
    """

    meta = _meta_distribution()
    if meta.unreadable is not None:
        raise AirflowCompatibilityError(
            f"Broken Airflow installation: `{AIRFLOW_META_DISTRIBUTION}` is present "
            f"but its metadata is unreadable ({meta.unreadable}), and "
            f"`{AIRFLOW_DISTRIBUTION}` is absent. Recreate the environment and install "
            f"`pytest-airflow-in-a-box[airflow3]` for a supported Airflow 3.x."
        ) from error
    if meta.version is None:
        raise AirflowCompatibilityError(
            f"No Airflow distribution is installed (`{AIRFLOW_DISTRIBUTION}` and "
            f"`{AIRFLOW_META_DISTRIBUTION}` are both absent). The plugin does not "
            f"depend on Airflow directly -- install the "
            f"`pytest-airflow-in-a-box[airflow3]` or `pytest-airflow-in-a-box[airflow2]` "
            f"extra, or pin Airflow yourself."
        ) from error
    if meta.major is None or meta.major >= 3:
        raise AirflowCompatibilityError(
            f"Broken Airflow installation: `{AIRFLOW_META_DISTRIBUTION}` "
            f"'{meta.version}' is installed without `{AIRFLOW_DISTRIBUTION}`. Reinstall "
            f"via `pytest-airflow-in-a-box[airflow3]` so the meta-package pins a "
            f"coherent core + task-sdk pair."
        ) from error
    if _running_python() > MAX_V2_PYTHON:
        running = ".".join(str(part) for part in _running_python())
        raise AirflowCompatibilityError(
            f"Apache Airflow 2.x (`{AIRFLOW_META_DISTRIBUTION}` '{meta.version}') does "
            f"not support Python '{running}' -- its `requires-python` caps at "
            f"{'.'.join(str(part) for part in MAX_V2_PYTHON)} but uses bare `!=` "
            f"exclusions the installer does not enforce. Use Python 3.10-3.12 for the "
            f"2.x tier, or upgrade to Airflow 3."
        ) from error

    match = VERSION_PATTERN.fullmatch(meta.version)
    if match is None:
        parse_error = ValueError("version is not a standard final, development, or local release")
        _raise_compatibility_error(
            meta.version, "parsing installed version for", AIRFLOW_META_DISTRIBUTION, parse_error
        )
    release = (
        int(match.group("major")),
        int(match.group("minor")),
        int(match.group("patch")),
    )
    if release not in SUPPORTED_RELEASES_BY_FAMILY[AirflowFamily.V2]:
        certified_error = ValueError("base release is not certified")
        _raise_compatibility_error(
            meta.version,
            "validating installed version for",
            AIRFLOW_META_DISTRIBUTION,
            certified_error,
        )
    return meta.version, release, AirflowFamily.V2


def _installed_release() -> tuple[str, Release, AirflowFamily]:
    """Read and validate the installed Airflow base release without importing Airflow.

    The 3.x core distribution decides the family: when present it must certify against
    the 3.x contract table (after the corrupt-environment check), and when absent the
    2.x monolith is accepted through `_installed_v2_release`.

    Returns:
        tuple[str, Release, AirflowFamily] containing the metadata version, parsed base
        release, and distribution family.

    Raises:
        AirflowCompatibilityError: The environment is corrupt or Airflow-free, or the
            package metadata is absent, malformed, or unsupported.
    """

    try:
        installed_version = metadata.version(AIRFLOW_DISTRIBUTION)
    except metadata.PackageNotFoundError as error:
        return _installed_v2_release(error)
    except Exception as error:
        _raise_compatibility_error(
            "<not installed>", "reading package metadata for", AIRFLOW_DISTRIBUTION, error
        )
    _reject_corrupt_environment()

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
    return installed_version, release, AirflowFamily.V3


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
        (observed.family, expected.family, "Airflow distribution family"),
    )
    for actual, certified, symbol in checks:
        _verify_value(actual, certified, symbol, installed_version)
    _verify_value(observed, expected, "complete capability contract", installed_version)


def _resolve_uncached(
    installed_version: str, release: Release, family: AirflowFamily
) -> AirflowCapabilities:
    """Import, probe, and validate every Airflow dependency used by eventual fixtures.

    Both families share the DagBag, task-runner, and serialization probes; the Task SDK
    probes run only on 3.x because the modules they inspect do not exist on 2.x, so the
    corresponding capability fields observe as None there. Family-static fields (Task
    SDK presence, structlog, DAG versioning, interface selections) come from the family
    itself and are cross-checked against the certified row like every probed value.

    Parameters:
        installed_version: str reported by package metadata.
        release: tuple[int, int, int] containing the certified base release.
        family: AirflowFamily naming the installed distribution family.

    Returns:
        AirflowCapabilities containing validated metadata only.

    Raises:
        AirflowCompatibilityError: A symbol is unavailable or a probe violates the contract.
    """

    is_v3 = family is AirflowFamily.V3
    dag_bag_location, dag_bag = _probe_dag_bag(installed_version)
    task_instance = _resolve_symbol(
        "airflow.models.taskinstance", "TaskInstance", installed_version
    )
    startup_details_supports_sentry: bool | None = None
    runtime_task_instance_supports_queue: bool | None = None
    if is_v3:
        startup_details = _resolve_symbol(
            "airflow.sdk.execution_time.comms", "StartupDetails", installed_version
        )
        runtime_task_instance_dto = _resolve_symbol(
            "airflow.sdk.api.datamodels._generated", "TaskInstance", installed_version
        )
        startup_details_supports_sentry = _model_has_field(
            startup_details,
            "airflow.sdk.execution_time.comms.StartupDetails",
            "sentry_integration",
            installed_version,
        )
        runtime_task_instance_supports_queue = _model_has_field(
            runtime_task_instance_dto,
            "airflow.sdk.api.datamodels._generated.TaskInstance",
            "queue",
            installed_version,
        )
    serialized_dag_location = _probe_serialized_dag(installed_version)

    observed = AirflowCapabilities(
        release=release,
        family=family,
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
        startup_details_supports_sentry=startup_details_supports_sentry,
        runtime_task_instance_supports_queue=runtime_task_instance_supports_queue,
        has_task_sdk=is_v3,
        uses_structlog=is_v3,
        has_dag_versioning=is_v3,
        dagrun_interface=(
            DagRunInterface.LOGICAL_DATE if is_v3 else DagRunInterface.EXECUTION_DATE
        ),
        api_surface=ApiSurface.API_SERVER if is_v3 else ApiSurface.WEBSERVER,
        params_location=ParamsLocation.SDK if is_v3 else ParamsLocation.MODELS,
        timezone_location=TimezoneLocation.SDK if is_v3 else TimezoneLocation.UTILS,
    )

    for module_name, symbol_name in _REQUIRED_SYMBOLS_BY_FAMILY[family]:
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

    installed_version, release, family = _installed_release()
    capabilities = _resolve_uncached(installed_version, release, family)
    _CAPABILITIES = capabilities
    return capabilities


def _reset_capabilities_for_testing() -> None:
    """Clear the successful-resolution cache for isolated compatibility tests."""

    global _CAPABILITIES
    _CAPABILITIES = None


__all__ = (
    "AirflowCapabilities",
    "AirflowCompatibilityError",
    "AirflowFamily",
    "ApiSurface",
    "DagBagLocation",
    "DagRunInterface",
    "ParamsLocation",
    "TaskInstanceRunner",
    "TimezoneLocation",
    "resolve_capabilities",
)
