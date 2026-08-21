"""Probe and validate the private Airflow surface used by test fixtures.

No Airflow module is imported until ``resolve_capabilities`` runs after bootstrap.

References:
    https://docs.python.org/3/library/importlib.html#importlib.import_module
    https://docs.python.org/3/library/importlib.metadata.html
    https://docs.python.org/3/library/inspect.html#inspect.signature
    https://docs.pydantic.dev/latest/concepts/models/#model-methods-and-properties
"""

from __future__ import annotations

import logging
import os
import re
import sys
from dataclasses import dataclass, fields
from enum import Enum
from importlib import import_module, metadata
from inspect import signature
from typing import NoReturn

from packaging.version import InvalidVersion, Version

LOGGER = logging.getLogger(__name__)

Release = tuple[int, int, int]


class AirflowFamily(str, Enum):
    """Closed set of Airflow distribution families, valued by distribution name."""

    V2 = "apache-airflow"
    V3 = "apache-airflow-core"


# The certified 2.x releases, each carrying its own Python ceiling. Ordering is the
# certified order, so this mapping is also the family's `SUPPORTED_RELEASES` source and
# the two cannot drift. Certified per issue #41 (the final 2.x release, Composer 3's
# exact 2.10 patch, and the oldest line still shipped by a managed vendor) and issue
# #139, which reaches back to the last release of the 2.8 and 2.7 lines as a proof that
# the plugin spans the whole 2.x era. Spike-verified 2026-08-11 (2.9/2.10/2.11) and
# 2026-08-14 (2.7/2.8).
#
# The ceiling is per release, not per family: 2.7.3 and 2.8.4 declare `requires-python`
# `~=3.8,<3.12` and publish no 3.12 constraints file, while 2.9.3 and later reach 3.12.
# A family-wide cap let 2.7.3 sail past the guard on CPython 3.12 and fail opaquely
# somewhere deeper.
V2_MAX_PYTHON_BY_RELEASE: dict[Release, tuple[int, int]] = {
    (2, 7, 3): (3, 11),
    (2, 8, 4): (3, 11),
    (2, 9, 3): (3, 12),
    (2, 10, 5): (3, 12),
    (2, 11, 2): (3, 12),
}
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
        (3, 3, 1),
    ),
    AirflowFamily.V2: tuple(V2_MAX_PYTHON_BY_RELEASE),
}
SUPPORTED_RELEASES = SUPPORTED_RELEASES_BY_FAMILY[AirflowFamily.V3]
SUPPORTED_VERSIONS = tuple(
    ".".join(str(part) for part in release) for release in SUPPORTED_RELEASES
)
SUPPORTED_VERSIONS_V2 = tuple(
    ".".join(str(part) for part in release)
    for release in SUPPORTED_RELEASES_BY_FAMILY[AirflowFamily.V2]
)
# The floor of the Python range the 2.x family supports. 2.7.3 and 2.8.4 themselves
# reach down to 3.8, but this package's own `requires-python` is `>=3.10`, so a running
# interpreter can never be below the floor and comparing against it would be dead code.
# It is interpolated into the over-cap message so the reported range is the usable one.
# The ceiling is per release -- see `V2_MAX_PYTHON_BY_RELEASE`.
MIN_V2_PYTHON = (3, 10)
AIRFLOW_DISTRIBUTION = AirflowFamily.V3.value
AIRFLOW_META_DISTRIBUTION = AirflowFamily.V2.value
# Escape hatch for the fail-closed metadata corruption check: an Airflow source
# checkout can legitimately expose `apache-airflow` with a dev-fallback version next to
# a real core. Set to "1" by a consumer who has verified their environment themselves.
SKIP_CORRUPTION_CHECK_ENVIRONMENT_VARIABLE = "PYTEST_AIRFLOW_IN_A_BOX_SKIP_CORRUPTION_CHECK"
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


class AssetUniqueKeyLocation(str, Enum):
    """Closed set of certified asset-condition unique-key module locations.

    3.1.x evaluates asset conditions against the SDK's own ``AssetUniqueKey``; 3.2
    split asset serialization out of ``airflow.serialization.serialized_objects`` the
    same way it split ``SerializedDAG``, and condition evaluation moved onto the
    resulting ``SerializedAssetUniqueKey``. 2.x has no unique-key type at all --
    ``BaseDataset.evaluate`` keys its status dict by dataset URI string directly.
    """

    SDK = "airflow.sdk.definitions.asset"
    SERIALIZATION = "airflow.serialization.definitions.assets"


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


class SecretsResolution(str, Enum):
    """Closed set of certified Variable and Connection resolution endpoints.

    Names how a lookup reaches storage when no supervisor process is running, which is
    the situation every Dag parse under test is in. 2.x reads the metastore backend
    directly; 3.x routes through the Task SDK's supervisor endpoint, which is why
    `_compat.parse_time` has to install one.
    """

    METASTORE = "airflow.secrets.metastore.MetastoreBackend"
    SUPERVISOR_COMMS = "airflow.sdk.execution_time.task_runner.SUPERVISOR_COMMS"


class ExecutorContract(str, Enum):
    """Closed set of certified `BaseExecutor` attribute contracts.

    Names the certified attribute table `pytest_airflow_in_a_box.components.check_component`
    matches a custom executor against. 3.2 renamed 3.1's `supports_sentry: bool` flag to
    `sentry_integration: str` and added `supports_callbacks`/`supports_multi_team`; 3.3 added
    `supports_connection_test` on top of the 3.2 shape. The three certified releases are
    otherwise attribute-compatible for the fields this plugin checks.
    """

    V3_1 = "3.1"
    V3_2 = "3.2"
    V3_3 = "3.3"


class PluginsManagerShape(str, Enum):
    """Closed set of certified `airflow.plugins_manager` cache mechanisms.

    Names the total 3.1 -> 3.2 API break in which the module-global cache Airflow's
    plugin loading used through 3.1.x (`plugins`, `loaded_plugins`, and sixteen sibling
    globals in `airflow.plugins_manager`, each `None`/empty until first load, reset by
    hand rather than by any cache primitive) was deleted outright and replaced by eleven
    independent `functools.cache`-decorated functions, plus a second, structurally
    identical split on `airflow.sdk.plugins_manager` -- a module that does not exist at
    all on 3.1.x, since the Task SDK carries no plugin-loading surface of its own before
    3.2. `pytest_airflow_in_a_box._compat.components` certifies the exact clearable-name
    set for each shape and hard-fails on a mismatch; see that module's docstring for why
    the check resolves lazily rather than inside `resolve_capabilities()`.
    """

    MODULE_GLOBALS = "module-globals"
    CACHED_FUNCTIONS = "cached-functions"


class SharedModuleLoading(str, Enum):
    """Closed set of certified `_get_grouped_entry_points` vendoring shapes.

    Names the 3.2+ double-vendoring of Airflow's entry-point discovery cache. Through
    3.1.x, `_get_grouped_entry_points` (`functools.cache`-decorated) lives once, at
    `airflow.utils.entry_points`. 3.2 moved it to `airflow._shared.module_loading` and
    gave the Task SDK its own independently-cached, identically-named copy at
    `airflow.sdk._shared.module_loading` -- confirmed distinct module objects on the
    installed 3.3.0, so clearing one never clears the other. This field makes "clear it
    twice on 3.2+" structural (the seam function returns one or two modules to walk)
    rather than a fact `pytest_airflow_in_a_box._compat.components` has to remember.
    """

    SINGLE = "single"
    DUPLICATED = "duplicated"


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
        secrets_resolution: SecretsResolution naming how a supervisor-free Variable or
            Connection lookup reaches storage.
        max_python: tuple[int, int] | None containing the highest CPython `(major,
            minor)` this release supports; None on 3.x, whose `requires-python` bounds
            the installer actually enforces.
        dag_requires_start_date: bool indicating that constructing a Dag without a
            `start_date` fails even when the Dag declares no schedule.
        asset_unique_key_location: AssetUniqueKeyLocation | None naming the module
            asset-condition evaluation imports its unique-key type from; None on 2.x,
            which evaluates dataset conditions by URI string with no unique-key type.
        executor_contract: ExecutorContract | None naming the certified `BaseExecutor`
            attribute contract `check_component` matches a custom executor against;
            None on 2.x, whose executor interface predates the certified 3.x table.
        sdk_listener_manager_available: bool indicating whether `airflow.sdk.listener`
            exposes its own listener manager, separate from the core one at
            `airflow.listeners.listener`. Constant True on every certified 3.x release
            and False on 2.x, which has no Task SDK and so no second manager to split
            hookspecs across; kept as a probed tripwire rather than a bare family
            constant because the dual-manager split is the single most common
            real-world listener bug (register in the wrong manager and half the hooks
            silently never fire), so a future release silently collapsing back to one
            manager must fail loudly here instead of going unnoticed.
        task_instance_mutation_hook_supports_dag_run: bool indicating whether the
            `airflow.policies.task_instance_mutation_hook` hookspec declares a
            `dag_run` parameter. False on every certified 3.1.x and 3.2.x release and
            on 2.x, True from 3.3 onward. `pytest_airflow_in_a_box.components`'s
            policy checks derive the legitimate hookimpl argument set from the live
            installed hookspec directly (mirroring the listener checks), so nothing
            here reads this field back -- it exists as a certified tripwire the same
            way `sdk_listener_manager_available` does, so a future release silently
            reverting the parameter is caught at `resolve_capabilities()` time rather
            than only inside a user's own conformance check output.
        plugins_manager: PluginsManagerShape | None naming the certified
            `airflow.plugins_manager` cache mechanism; None on 2.x. Derived from
            `release` alone (the 3.1 -> 3.2 break applies uniformly), not independently
            probed: probing it for real means importing `airflow.plugins_manager` and
            `airflow.sdk.plugins_manager`, which `resolve_capabilities()` must not do
            for every session regardless of whether a test ever touches a custom
            component. `pytest_airflow_in_a_box._compat.components` verifies the live
            shape lazily, on first `airflow_components` use; see its module docstring.
        shared_module_loading: SharedModuleLoading | None naming the certified
            `_get_grouped_entry_points` vendoring shape; None on 2.x. Derived from
            `release` alone, for the same reason `plugins_manager` is: the real
            verification is deferred and lazy, not eager here.
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
    secrets_resolution: SecretsResolution
    max_python: tuple[int, int] | None
    dag_requires_start_date: bool
    asset_unique_key_location: AssetUniqueKeyLocation | None
    executor_contract: ExecutorContract | None
    sdk_listener_manager_available: bool
    task_instance_mutation_hook_supports_dag_run: bool
    plugins_manager: PluginsManagerShape | None
    shared_module_loading: SharedModuleLoading | None


# Airflow below 2.8 raises `DAG is missing the start_date parameter` from
# `DAG.add_task` whenever neither the Dag nor the task carries a `start_date`. 2.8.0
# moved the check into `DAG.__init__` and narrowed it to Dags that actually declare a
# scheduling argument, which is the behavior every later release keeps.
#
# References:
#     https://github.com/apache/airflow/blob/2.7.3/airflow/models/dag.py#L2555
#     https://github.com/apache/airflow/blob/2.8.4/airflow/models/dag.py#L557
V2_START_DATE_REQUIRED_BELOW: Release = (2, 8, 0)


def _dag_requires_start_date(family: AirflowFamily, release: Release) -> bool:
    """Report whether Dag construction demands a `start_date` regardless of schedule.

    Parameters:
        family: AirflowFamily naming the installed distribution family.
        release: tuple[int, int, int] containing the certified base release.

    Returns:
        bool marking the release as one that rejects a schedule-free Dag without a
        `start_date`.
    """

    return family is AirflowFamily.V2 and release < V2_START_DATE_REQUIRED_BELOW


# The 3.1 -> 3.2 plugins-manager and shared-module-loading break shares one boundary;
# see `PluginsManagerShape` and `SharedModuleLoading`. Both fields are derived from
# `release` alone, on both the certified and the observed side (`_certify_v3` and
# `_resolve_uncached` both call these), rather than independently probed: verifying the
# live shape means importing `airflow.plugins_manager` and `airflow.sdk.plugins_manager`,
# a cost `resolve_capabilities()` must not pay for every session. The real verification
# happens lazily in `pytest_airflow_in_a_box._compat.components`, on first
# `airflow_components` use -- see that module's docstring for the full rationale. Unlike
# the independently-probed 3.x-varying fields (`executor_contract`,
# `sdk_listener_manager_available`, ...), a mismatch here is therefore a
# self-consistency guard against a typo in this file, not a real runtime observation --
# the same honesty distinction `_verify_contract` already draws for `max_python` and
# `dag_requires_start_date`.
PLUGINS_MANAGER_BREAK: Release = (3, 2, 0)


def _plugins_manager_shape(release: Release) -> PluginsManagerShape:
    """Derive the certified `plugins_manager` shape for one 3.x release.

    Parameters:
        release: tuple[int, int, int] containing the certified base release.

    Returns:
        PluginsManagerShape naming the release's cache mechanism.
    """

    if release < PLUGINS_MANAGER_BREAK:
        return PluginsManagerShape.MODULE_GLOBALS
    return PluginsManagerShape.CACHED_FUNCTIONS


def _shared_module_loading(release: Release) -> SharedModuleLoading:
    """Derive the certified `shared_module_loading` shape for one 3.x release.

    Parameters:
        release: tuple[int, int, int] containing the certified base release.

    Returns:
        SharedModuleLoading naming the release's entry-point cache vendoring.
    """

    if release < PLUGINS_MANAGER_BREAK:
        return SharedModuleLoading.SINGLE
    return SharedModuleLoading.DUPLICATED


def _certify_v3(
    release: Release,
    *,
    dag_bag_location: DagBagLocation,
    dag_bag_supports_include_examples: bool,
    task_instance_runner: TaskInstanceRunner,
    refresh_from_task_supports_dag_run: bool,
    startup_details_supports_sentry: bool,
    runtime_task_instance_supports_queue: bool,
    asset_unique_key_location: AssetUniqueKeyLocation,
    executor_contract: ExecutorContract,
    sdk_listener_manager_available: bool,
    task_instance_mutation_hook_supports_dag_run: bool,
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
        asset_unique_key_location: AssetUniqueKeyLocation naming the canonical import
            location for asset-condition evaluation's unique-key type.
        executor_contract: ExecutorContract naming the certified `BaseExecutor`
            attribute contract for this release.
        sdk_listener_manager_available: bool indicating whether `airflow.sdk.listener`
            exists and exposes a listener manager on this release. False on 3.1.x, whose
            `airflow.sdk` carries no listener manager module at all; True from 3.2
            onward, alongside the rest of that release's Task SDK listener-architecture
            changes (`dag_bag_location`, `task_instance_runner`,
            `asset_unique_key_location`).
        task_instance_mutation_hook_supports_dag_run: bool indicating whether
            `airflow.policies.task_instance_mutation_hook` declares a `dag_run`
            parameter on this release. False on 3.1.x and 3.2.x, True from 3.3 onward.

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
        secrets_resolution=SecretsResolution.SUPERVISOR_COMMS,
        max_python=None,
        dag_requires_start_date=False,
        asset_unique_key_location=asset_unique_key_location,
        executor_contract=executor_contract,
        sdk_listener_manager_available=sdk_listener_manager_available,
        task_instance_mutation_hook_supports_dag_run=(
            task_instance_mutation_hook_supports_dag_run
        ),
        plugins_manager=_plugins_manager_shape(release),
        shared_module_loading=_shared_module_loading(release),
    )


def _certify_v2(release: Release) -> AirflowCapabilities:
    """Build one certified 2.x contract row.

    Every probed value is uniform across the certified 2.x releases -- the Phase 1a
    spike (2026-08-11) observed identical signatures on 2.9.3, 2.10.5, and 2.11.2, and
    the #139 reach-back spike (2026-08-14) observed the same ones on 2.7.3 and 2.8.4,
    for every symbol the plugin touches. Two fields do vary by release rather than by
    family and are derived rather than passed: the Python ceiling comes from
    `V2_MAX_PYTHON_BY_RELEASE`, and the schedule-free `start_date` demand from
    `_dag_requires_start_date`.

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
        secrets_resolution=SecretsResolution.METASTORE,
        max_python=V2_MAX_PYTHON_BY_RELEASE[release],
        dag_requires_start_date=_dag_requires_start_date(AirflowFamily.V2, release),
        asset_unique_key_location=None,
        executor_contract=None,
        sdk_listener_manager_available=False,
        task_instance_mutation_hook_supports_dag_run=False,
        plugins_manager=None,
        shared_module_loading=None,
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
            asset_unique_key_location=AssetUniqueKeyLocation.SDK,
            executor_contract=ExecutorContract.V3_1,
            sdk_listener_manager_available=False,
            task_instance_mutation_hook_supports_dag_run=False,
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
            asset_unique_key_location=AssetUniqueKeyLocation.SERIALIZATION,
            executor_contract=ExecutorContract.V3_2,
            sdk_listener_manager_available=True,
            task_instance_mutation_hook_supports_dag_run=False,
        ),
        (3, 2, 1): _certify_v3(
            (3, 2, 1),
            dag_bag_location=DagBagLocation.DAG_PROCESSING,
            dag_bag_supports_include_examples=True,
            task_instance_runner=TaskInstanceRunner.SDK_RUN_TASK,
            refresh_from_task_supports_dag_run=False,
            startup_details_supports_sentry=True,
            runtime_task_instance_supports_queue=False,
            asset_unique_key_location=AssetUniqueKeyLocation.SERIALIZATION,
            executor_contract=ExecutorContract.V3_2,
            sdk_listener_manager_available=True,
            task_instance_mutation_hook_supports_dag_run=False,
        ),
        (3, 2, 2): _certify_v3(
            (3, 2, 2),
            dag_bag_location=DagBagLocation.DAG_PROCESSING,
            dag_bag_supports_include_examples=True,
            task_instance_runner=TaskInstanceRunner.SDK_RUN_TASK,
            refresh_from_task_supports_dag_run=False,
            startup_details_supports_sentry=True,
            runtime_task_instance_supports_queue=False,
            asset_unique_key_location=AssetUniqueKeyLocation.SERIALIZATION,
            executor_contract=ExecutorContract.V3_2,
            sdk_listener_manager_available=True,
            task_instance_mutation_hook_supports_dag_run=False,
        ),
        (3, 3, 0): _certify_v3(
            (3, 3, 0),
            dag_bag_location=DagBagLocation.DAG_PROCESSING,
            dag_bag_supports_include_examples=False,
            task_instance_runner=TaskInstanceRunner.SDK_RUN_TASK,
            refresh_from_task_supports_dag_run=True,
            startup_details_supports_sentry=True,
            runtime_task_instance_supports_queue=True,
            asset_unique_key_location=AssetUniqueKeyLocation.SERIALIZATION,
            executor_contract=ExecutorContract.V3_3,
            sdk_listener_manager_available=True,
            task_instance_mutation_hook_supports_dag_run=True,
        ),
        (3, 3, 1): _certify_v3(
            (3, 3, 1),
            dag_bag_location=DagBagLocation.DAG_PROCESSING,
            dag_bag_supports_include_examples=False,
            task_instance_runner=TaskInstanceRunner.SDK_RUN_TASK,
            refresh_from_task_supports_dag_run=True,
            startup_details_supports_sentry=True,
            runtime_task_instance_supports_queue=True,
            asset_unique_key_location=AssetUniqueKeyLocation.SERIALIZATION,
            executor_contract=ExecutorContract.V3_3,
            sdk_listener_manager_available=True,
            task_instance_mutation_hook_supports_dag_run=True,
        ),
    }
)
_CERTIFIED_SERIALIZED_DAG_LOCATIONS = {
    (2, 7, 3): _SerializedDagLocation.SERIALIZED_OBJECTS,
    (2, 8, 4): _SerializedDagLocation.SERIALIZED_OBJECTS,
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
    (3, 3, 1): _SerializedDagLocation.DEFINITIONS,
}


@dataclass(frozen=True)
class CertifiedCaches:
    """Certified cache-clearable names for one shape: always-present plus release-optional.

    A two-frozenset certification, not a bare set: keying the certified tables by
    capability enum rather than by release means one row spans several point releases,
    and upstream adds cache functions mid-line (`get_deadline_references_plugins` in
    3.2.2, `get_windows_plugins` in 3.3.0). `required` names must all be present on
    every release the row covers; `optional` names are certified-but-absent-on-some
    releases -- tolerated when missing, verified and cleared when present. Any observed
    name outside `required | optional` is still a hard failure, so the under-clear
    guarantee (an uncertified upstream cache can never slip past verification silently)
    survives the relaxation.

    Parameters:
        required: frozenset[str] containing names present on every covered release.
        optional: frozenset[str] containing names certified for the shape but absent on
            some covered releases.
    """

    required: frozenset[str]
    optional: frozenset[str] = frozenset()


# Certified `functools.cache`-decorated callable names in `airflow.plugins_manager`
# (core) and `airflow.sdk.plugins_manager` (Task SDK), keyed by `PluginsManagerShape`
# rather than by release -- two rows cover every certified 3.x release, mirroring
# `_CERTIFIED_SERIALIZED_DAG_LOCATIONS` above. `pytest_airflow_in_a_box._compat.components`
# enumerates the *observed* set on the installed release by introspection (any attribute
# exposing `.cache_clear`) and hard-fails unless the observed set is a superset of the
# row's `required` names and a subset of `required | optional` -- the check that
# actually matters going forward, since 3.2+ is still under active development and
# upstream can add another cache function in a release this plugin has not certified
# yet.
#
# The MODULE_GLOBALS row cannot be verified the same way: a plain module-level global
# carries no structural marker distinguishing "lazily-cached plugin state" from any other
# module attribute, so there is no safe way to *discover* it by introspection the way
# `.cache_clear` discovers a `functools.cache` function. It exists purely as a permanently
# fixed, hand-verified fact about a release line Airflow will never again modify (3.1.x
# is fully released and closed), transcribed by reading the installed
# `apache-airflow-core==3.1.0` wheel's `airflow/plugins_manager.py` directly (see
# PROVENANCE.md) -- `airflow.sdk.plugins_manager` does not exist at all on 3.1.x (the
# paired `apache-airflow-task-sdk==1.1.0` wheel ships no `plugins_manager.py`), hence the
# empty SDK-side set. Verification for this row is PRESENCE-only (every certified name
# must exist), not symmetric-difference: there is no future 3.1.x release that could add
# an uncertified name for this check to catch.
CERTIFIED_CORE_PLUGINS_MANAGER_CACHES: dict[PluginsManagerShape, CertifiedCaches] = {
    PluginsManagerShape.MODULE_GLOBALS: CertifiedCaches(
        required=frozenset(
            {
                "plugins",
                "loaded_plugins",
                "import_errors",
                "macros_modules",
                "admin_views",
                "flask_blueprints",
                "fastapi_apps",
                "fastapi_root_middlewares",
                "external_views",
                "react_apps",
                "menu_links",
                "flask_appbuilder_views",
                "flask_appbuilder_menu_links",
                "global_operator_extra_links",
                "operator_extra_links",
                "registered_operator_link_classes",
                "timetable_classes",
                "hook_lineage_reader_classes",
                "priority_weight_strategy_classes",
            }
        )
    ),
    PluginsManagerShape.CACHED_FUNCTIONS: CertifiedCaches(
        # The nine names present on every CACHED_FUNCTIONS release, transcribed from the
        # `apache-airflow-core==3.2.0` wheel's `airflow/plugins_manager.py` and
        # re-verified against the installed 3.3.0; see PROVENANCE.md.
        required=frozenset(
            {
                "_get_plugins",
                "_get_ui_plugins",
                "get_flask_plugins",
                "get_fastapi_plugins",
                "_get_extra_operators_links_plugins",
                "get_timetables_plugins",
                "get_partition_mapper_plugins",
                "integrate_macros_plugins",
                "get_priority_weight_strategy_plugins",
            }
        ),
        # Certified mid-line additions: absent on the earlier CACHED_FUNCTIONS releases,
        # present (and cleared) from the release named beside each.
        optional=frozenset(
            {
                "get_deadline_references_plugins",  # added in 3.2.2
                "get_windows_plugins",  # added in 3.3.0
            }
        ),
    ),
}
CERTIFIED_SDK_PLUGINS_MANAGER_CACHES: dict[PluginsManagerShape, CertifiedCaches] = {
    PluginsManagerShape.MODULE_GLOBALS: CertifiedCaches(required=frozenset()),
    # All three names are present from `apache-airflow-task-sdk==1.2.0` (the wheel
    # paired with core 3.2.0, the first CACHED_FUNCTIONS release) onward; verified by
    # reading that wheel's `airflow/sdk/plugins_manager.py` directly. See PROVENANCE.md.
    PluginsManagerShape.CACHED_FUNCTIONS: CertifiedCaches(
        required=frozenset(
            {"_get_plugins", "integrate_macros_plugins", "get_hook_lineage_readers_plugins"}
        )
    ),
}
# `_get_grouped_entry_points` itself is byte-for-byte identical on every certified
# release -- only its module location (and, on 3.2+, its duplication into a second,
# independently-`functools.cache`d Task SDK copy) changes. Verified against the
# installed 3.3.0 (`airflow._shared.module_loading` and
# `airflow.sdk._shared.module_loading`, confirmed distinct module objects) and against
# the installed `apache-airflow-core==3.1.0` wheel (`airflow.utils.entry_points`, the
# SINGLE-shape module -- 3.1.x's Task SDK has no plugin or listener surface at all, so
# there is nothing to duplicate). `pytest_airflow_in_a_box._compat.components`'s
# `_shared_module_loading_modules()` seam returns one module for SINGLE and two for
# DUPLICATED; this table certifies the one function name common to both.
CERTIFIED_SHARED_MODULE_LOADING_CACHES: dict[SharedModuleLoading, CertifiedCaches] = {
    SharedModuleLoading.SINGLE: CertifiedCaches(required=frozenset({"_get_grouped_entry_points"})),
    SharedModuleLoading.DUPLICATED: CertifiedCaches(
        required=frozenset({"_get_grouped_entry_points"})
    ),
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
    ("airflow.sdk.definitions.param", "ParamsDict"),
    ("airflow.sdk.timezone", "utcnow"),
    ("airflow.models.dag_version", "DagVersion"),
    ("airflow.models.dagbundle", "DagBundleModel"),
    ("airflow.serialization.serialized_objects", "LazyDeserializedDAG"),
    # Defined in `serialized_objects` itself on 3.1.x and re-exported there from
    # `airflow.serialization.encoders`/`decoders` on 3.2+, so this one location is
    # stable across every certified 3.x release; see PROVENANCE.md.
    ("airflow.serialization.serialized_objects", "encode_timetable"),
    ("airflow.serialization.serialized_objects", "decode_timetable"),
    ("airflow.sdk.execution_time.task_runner", "RuntimeTaskInstance"),
    ("airflow.sdk.execution_time.task_runner", "parse"),
    ("airflow.sdk.execution_time.task_runner", "run"),
    ("airflow.sdk.execution_time.comms", "CommsDecoder"),
    ("airflow.sdk.execution_time.comms", "BundleInfo"),
    ("airflow.sdk.execution_time.comms", "ToSupervisor"),
    ("airflow.sdk.execution_time.xcom", "XCom"),
    # The two private lookups that route a supervisor-free Variable or Connection read
    # through `SUPERVISOR_COMMS`, plus the cache sitting in front of them. This is the
    # exact surface `_compat.parse_time` shims, and apache/airflow#61630 announces it is
    # about to move to a lazy-init `InProcessExecutionAPI`; requiring the symbols turns
    # that move into a loud compatibility failure rather than a silent no-op shim.
    ("airflow.sdk.execution_time.context", "_get_variable"),
    ("airflow.sdk.execution_time.context", "_get_connection"),
    # `task_context_in_process` wraps the caller's block in `set_current_context` so
    # hand-driven `execute()` bodies can call `airflow.sdk.get_current_context()`.
    ("airflow.sdk.execution_time.context", "set_current_context"),
    ("airflow.sdk.execution_time.cache", "SecretCache"),
    ("airflow.sdk.api.datamodels._generated", "DagRun"),
    ("airflow.sdk.api.datamodels._generated", "DagRunState"),
    ("airflow.sdk.api.datamodels._generated", "TaskInstanceState"),
    ("airflow.sdk.api.datamodels._generated", "TIRunContext"),
    ("airflow.assets.evaluation", "AssetEvaluator"),
    ("airflow.models.asset", "AssetDagRunQueue"),
    ("airflow.timetables.simple", "AssetTriggeredTimetable"),
)
# Spike-verified present on 2.9.3/2.10.5/2.11.2 (2026-08-11) and on 2.7.3/2.8.4
# (2026-08-14, #139); the surface Phase 2's fixture branches consume.
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
    ("airflow.models.dataset", "DatasetDagRunQueue"),
    ("airflow.models.dataset", "DagScheduleDatasetReference"),
    ("airflow.timetables.simple", "DatasetTriggeredTimetable"),
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
    offending version is named so a false positive stays debuggable. Setting
    `PYTEST_AIRFLOW_IN_A_BOX_SKIP_CORRUPTION_CHECK=1` bypasses the check for
    environments (an Airflow source checkout with a dev fallback version) the consumer
    has verified themselves.

    Raises:
        AirflowCompatibilityError: A non-3.x or unparseable `apache-airflow`
            distribution is installed next to `apache-airflow-core`.
    """

    if os.environ.get(SKIP_CORRUPTION_CHECK_ENVIRONMENT_VARIABLE) == "1":
        return
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
        f"so the older family's files are silently overwritten by the 3.x core. "
        f"Recreate the environment and install `pytest-airflow-in-a-box[airflow3]` for "
        f"a supported Airflow 3.x. If `{AIRFLOW_META_DISTRIBUTION}` is a source "
        f"checkout with a dev fallback version, install the core distribution alone or "
        f"set `{SKIP_CORRUPTION_CHECK_ENVIRONMENT_VARIABLE}=1` after verifying the "
        f"environment yourself."
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
        if meta.unreadable is not None:
            LOGGER.warning(
                f"Could not classify the Airflow family: `{AIRFLOW_META_DISTRIBUTION}` "
                f"metadata is unreadable ({meta.unreadable})."
            )
        return None
    except Exception as error:
        LOGGER.warning(
            f"Could not classify the Airflow family: reading `{AIRFLOW_DISTRIBUTION}` "
            f"metadata failed ({type(error).__name__}: {error})."
        )
        return None
    return AirflowFamily.V3


def v2_gate_message(surface: str, detail: str) -> str | None:
    """Build the unavailable-on-2.x message for a 3.x-only fixture surface.

    Classification uses the import-free family probe so a 3.x session requesting the
    surface stays as cheap as before the 2.x tier existed.

    Parameters:
        surface: str naming the requested fixture surface.
        detail: str explaining why 2.x lacks it and what to use instead.

    Returns:
        str | None containing the actionable message, or None off the 2.x family.
    """

    if installed_family() is not AirflowFamily.V2:
        return None
    return (
        f"`{surface}` is unavailable on Apache Airflow 2.x: {detail} The 2.x tier "
        f"scope is tracked in "
        f"https://github.com/nredd/pytest-airflow-in-a-box/issues/25."
    )


def _running_python() -> tuple[int, int]:
    """Report the running Python major and minor version.

    Returns:
        tuple[int, int] containing `sys.version_info[:2]`.
    """

    return sys.version_info[:2]


def max_v2_python(version: str) -> tuple[int, int]:
    """Report the Python ceiling for one Airflow 2.x version string.

    An uncertified or unparseable version falls back to the lowest ceiling any certified
    release declares, which every certified release supports: the caller (the
    `airflow-migration-diff` CLI) accepts an arbitrary `--airflow2-version`, and the
    provisioned environment's own `resolve_capabilities()` is the authority that rejects
    it. Guessing high would reintroduce exactly the failure this mapping exists to stop.

    Parameters:
        version: str containing an Airflow 2.x version, certified or not.

    Returns:
        tuple[int, int] containing the highest CPython `(major, minor)` that version
        supports.
    """

    match = VERSION_PATTERN.fullmatch(version)
    if match is None:
        return min(V2_MAX_PYTHON_BY_RELEASE.values())
    release = (
        int(match.group("major")),
        int(match.group("minor")),
        int(match.group("patch")),
    )
    return V2_MAX_PYTHON_BY_RELEASE.get(release, min(V2_MAX_PYTHON_BY_RELEASE.values()))


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
            f"the `pytest-airflow-in-a-box[airflow3]` or "
            f"`pytest-airflow-in-a-box[airflow2]` extra."
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
            f"via the `pytest-airflow-in-a-box[airflow3]` or "
            f"`pytest-airflow-in-a-box[airflow2]` extra for a coherent installation."
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
    release_max_python = V2_MAX_PYTHON_BY_RELEASE[release]
    if _running_python() > release_max_python:
        running = ".".join(str(part) for part in _running_python())
        minimum = ".".join(str(part) for part in MIN_V2_PYTHON)
        maximum = ".".join(str(part) for part in release_max_python)
        raise AirflowCompatibilityError(
            f"Apache Airflow 2.x (`{AIRFLOW_META_DISTRIBUTION}` '{meta.version}') does "
            f"not support Python '{running}': the certified ceiling for this release "
            f"is {maximum}. Some 2.x releases spell that bound with bare `!=` "
            f"exclusions the installer does not enforce against patch releases, which "
            f"is why this check exists at all. Use Python {minimum}-{maximum} for "
            f"Airflow '{meta.version}', or upgrade to Airflow 3."
        ) from error
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


def _probe_asset_unique_key_location(version: str) -> AssetUniqueKeyLocation:
    """Resolve the canonical asset-condition unique-key location by capability.

    Parameters:
        version: str reported by package metadata.

    Returns:
        AssetUniqueKeyLocation naming the resolved module.

    Raises:
        AirflowCompatibilityError: Neither certified location resolves.
    """

    module_name = AssetUniqueKeyLocation.SERIALIZATION.value
    try:
        module = import_module(module_name)
        _ = module.SerializedAssetUniqueKey
    except (ImportError, AttributeError):
        _resolve_symbol(AssetUniqueKeyLocation.SDK.value, "AssetUniqueKey", version)
        return AssetUniqueKeyLocation.SDK
    except Exception as error:
        _raise_compatibility_error(
            version,
            "probing canonical Airflow symbol",
            f"{module_name}.SerializedAssetUniqueKey",
            error,
        )
    return AssetUniqueKeyLocation.SERIALIZATION


def _probe_executor_contract(version: str) -> ExecutorContract:
    """Resolve the certified `BaseExecutor` attribute contract by capability.

    Distinguishes the three certified 3.x executor shapes by attributes Airflow itself
    added or renamed: 3.1 exposes `supports_sentry: bool`; 3.2 renamed it to
    `sentry_integration: str` and added `supports_callbacks`/`supports_multi_team`; 3.3
    additionally added `supports_connection_test`. This is a genuine runtime
    observation of the installed `BaseExecutor`, not a release-keyed lookup, so a
    maintainer certifying a new release with the wrong contract is caught here rather
    than trusted silently.

    Parameters:
        version: str reported by package metadata.

    Returns:
        ExecutorContract naming the resolved certified attribute contract.

    Raises:
        AirflowCompatibilityError: Neither certified sentry flag is present.
    """

    base_executor = _resolve_symbol("airflow.executors.base_executor", "BaseExecutor", version)
    if hasattr(base_executor, "supports_sentry"):
        return ExecutorContract.V3_1
    if not hasattr(base_executor, "sentry_integration"):
        error = ValueError("neither `sentry_integration` nor `supports_sentry` is present")
        _raise_compatibility_error(
            version,
            "probing canonical Airflow symbol",
            "airflow.executors.base_executor.BaseExecutor.sentry_integration",
            error,
        )
    if hasattr(base_executor, "supports_connection_test"):
        return ExecutorContract.V3_3
    return ExecutorContract.V3_2


def _probe_sdk_listener_manager_available(version: str) -> bool:
    """Probe whether `airflow.sdk.listener` exposes its own listener manager.

    A missing module or callable observes as False rather than raising: the certified
    3.x value is True, so `_verify_contract` is what turns a real removal into a loud
    `AirflowCompatibilityError` instead of this probe raising directly, mirroring how
    `_probe_dag_bag` falls back instead of raising when the newer location is absent.

    Parameters:
        version: str reported by package metadata.

    Returns:
        bool indicating whether a callable `get_listener_manager` was observed.

    Raises:
        AirflowCompatibilityError: Resolving the module raises something other than
            an import or attribute failure.
    """

    try:
        module = import_module("airflow.sdk.listener")
        get_listener_manager = module.get_listener_manager
    except (ImportError, AttributeError):
        return False
    except Exception as error:
        _raise_compatibility_error(
            version,
            "probing canonical Airflow symbol",
            "airflow.sdk.listener.get_listener_manager",
            error,
        )
    return callable(get_listener_manager)


def _probe_task_instance_mutation_hook_supports_dag_run(installed_version: str) -> bool:
    """Probe whether the `task_instance_mutation_hook` hookspec declares `dag_run`.

    Parameters:
        installed_version: str reported by package metadata.

    Returns:
        bool indicating whether the real, installed hookspec accepts a `dag_run`
        parameter.

    Raises:
        AirflowCompatibilityError: The hookspec cannot be resolved or inspected.
    """

    hook = _resolve_symbol("airflow.policies", "task_instance_mutation_hook", installed_version)
    return _signature_has_parameter(
        hook,
        "airflow.policies.task_instance_mutation_hook",
        "dag_run",
        installed_version,
    )


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


# Human-readable error labels for the genuinely probed fields; a mismatch here is a
# real runtime observation contradicting the certified row.
_PROBED_FIELD_LABELS: dict[str, str] = {
    "dag_bag_location": "DagBag canonical location",
    "dag_bag_supports_include_examples": "DagBag.__init__.include_examples",
    "task_instance_runner": "TaskInstance task runner",
    "refresh_from_task_supports_dag_run": "TaskInstance.refresh_from_task.dag_run",
    "startup_details_supports_sentry": "StartupDetails.sentry_integration",
    "runtime_task_instance_supports_queue": "TaskInstance DTO queue",
    "asset_unique_key_location": "Asset unique-key canonical location",
    "executor_contract": "BaseExecutor attribute contract",
    "sdk_listener_manager_available": "airflow.sdk.listener manager",
    "task_instance_mutation_hook_supports_dag_run": "task_instance_mutation_hook.dag_run",
}


def _verify_contract(
    observed: AirflowCapabilities,
    serialized_dag_location: _SerializedDagLocation,
    installed_version: str,
) -> None:
    """Verify all probes against the exact certified release contract.

    Iterating the dataclass fields keeps the comparison complete by construction: a
    field added to `AirflowCapabilities` is compared without touching this function.
    Honesty note on strength: only the `_PROBED_FIELD_LABELS` fields (plus the
    serialized-Dag location) are real runtime observations that can contradict the
    certified row. The family-derived fields (`family`, `has_task_sdk`,
    `uses_structlog`, `has_dag_versioning`, `dagrun_interface`, `api_surface`,
    `params_location`, `timezone_location`, `secrets_resolution`) are computed from the
    same family on both
    sides, so their comparison is a self-consistency guard, not a probe. `max_python`
    and `dag_requires_start_date` are release-derived rather than family-derived, but
    both sides read them from the same declaration (`V2_MAX_PYTHON_BY_RELEASE` and
    `_dag_requires_start_date`), so they are guards of the same kind. The real
    enforcement for the module-valued fields is `_REQUIRED_SYMBOLS_BY_FAMILY`, which
    imports each named module and fails resolution when upstream moves it;
    `api_surface` is exercised only when the API fixtures actually launch the server.

    Parameters:
        observed: AirflowCapabilities produced from runtime probes.
        serialized_dag_location: _SerializedDagLocation produced from import probes.
        installed_version: str reported by package metadata.

    Raises:
        AirflowCompatibilityError: Any probe differs from its certified value.
    """

    expected = _CERTIFIED_CAPABILITIES[observed.release]
    _verify_value(
        serialized_dag_location,
        _CERTIFIED_SERIALIZED_DAG_LOCATIONS[observed.release],
        "SerializedDAG canonical location",
        installed_version,
    )
    for field in fields(observed):
        _verify_value(
            getattr(observed, field.name),
            getattr(expected, field.name),
            _PROBED_FIELD_LABELS.get(field.name, f"capability `{field.name}`"),
            installed_version,
        )


def _resolve_uncached(
    installed_version: str, release: Release, family: AirflowFamily
) -> AirflowCapabilities:
    """Import, probe, and validate every Airflow dependency used by eventual fixtures.

    Both families share the DagBag, task-runner, and serialization probes; the Task SDK
    probes run only on 3.x because the modules they inspect do not exist on 2.x, so the
    corresponding capability fields observe as None there. Family-static fields (Task
    SDK presence, structlog, DAG versioning, interface selections) come from the family
    itself; see `_verify_contract` for what their comparison does and does not prove.

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
    asset_unique_key_location: AssetUniqueKeyLocation | None = None
    executor_contract: ExecutorContract | None = None
    sdk_listener_manager_available = False
    task_instance_mutation_hook_supports_dag_run = False
    if is_v3:
        asset_unique_key_location = _probe_asset_unique_key_location(installed_version)
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
        executor_contract = _probe_executor_contract(installed_version)
        sdk_listener_manager_available = _probe_sdk_listener_manager_available(installed_version)
        task_instance_mutation_hook_supports_dag_run = (
            _probe_task_instance_mutation_hook_supports_dag_run(installed_version)
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
        secrets_resolution=(
            SecretsResolution.SUPERVISOR_COMMS if is_v3 else SecretsResolution.METASTORE
        ),
        max_python=None if is_v3 else V2_MAX_PYTHON_BY_RELEASE[release],
        dag_requires_start_date=_dag_requires_start_date(family, release),
        asset_unique_key_location=asset_unique_key_location,
        executor_contract=executor_contract,
        sdk_listener_manager_available=sdk_listener_manager_available,
        task_instance_mutation_hook_supports_dag_run=(
            task_instance_mutation_hook_supports_dag_run
        ),
        plugins_manager=_plugins_manager_shape(release) if is_v3 else None,
        shared_module_loading=_shared_module_loading(release) if is_v3 else None,
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
    "AssetUniqueKeyLocation",
    "DagBagLocation",
    "DagRunInterface",
    "ExecutorContract",
    "ParamsLocation",
    "PluginsManagerShape",
    "SecretsResolution",
    "SharedModuleLoading",
    "TaskInstanceRunner",
    "TimezoneLocation",
    "installed_family",
    "resolve_capabilities",
)
