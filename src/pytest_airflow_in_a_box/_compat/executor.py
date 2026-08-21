"""Drive one persisted DagRun through a real Airflow executor.

The other adapter over `pytest_airflow_in_a_box._compat.taskrun.drive_dag_run`. Where
`execute_dag_run` runs every task in this process, this one hands each instance to the
executor the caller configured, exactly as the scheduler does: a `workloads.ExecuteTask`
is queued, the executor heartbeats, and a supervised worker subprocess reports its own
final state to the Task Execution API. The plugin's live api-server fixture is what
serves that API -- upstream has no way to stand one up inside a test process, which is
why `dag.test(use_executor=True)` cannot work there (apache/airflow#59074).

Deliberately not built on `dag.test`. That entry point is mid-move upstream
(apache/airflow#61803, #54658) and clears task instances, owns run-id derivation, and
swallows task exceptions. Driving `BaseExecutor` directly costs one loop and keeps every
`DagRunResult` guarantee the in-process path already makes.

Dispatch is one instance at a time, in the same dependency-safe topological order the
in-process path uses. That trades an executor's concurrency for determinism and for
reusing the mapped-expansion and settling logic unchanged; `SettleInstance` can widen to
a batch later without changing any public signature.

Airflow 3.x only: `workloads.ExecuteTask`, `queue_workload`, `jwt_generator` and the
Task Execution API all arrived with AIP-72. The fixture layer gates on that before
reaching this module.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html
    https://github.com/apache/airflow/issues/59074
"""

from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pytest_airflow_in_a_box._compat.taskrun import (
    DagRunDriveError,
    dependencies_met,
    drive_dag_run,
)
from pytest_airflow_in_a_box.config import airflow_config
from pytest_airflow_in_a_box.results import task_key

if TYPE_CHECKING:
    from collections.abc import Iterator

    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk.types import Operator
    from sqlalchemy.orm import Session

    from pytest_airflow_in_a_box._compat.taskrun import SettleInstance
    from pytest_airflow_in_a_box.results import DagRunResult

LOGGER = logging.getLogger(__name__)

DEFAULT_EXECUTOR_TIMEOUT = 300.0
LOCAL_BUNDLE_CLASSPATH = "airflow.dag_processing.bundles.local.LocalDagBundle"
BUNDLE_CONFIG_OPTION = ("dag_processor", "dag_bundle_config_list")

# One instance is dispatched at a time, so this is the latency added to every task, not
# a per-run cost. Small enough to stay invisible next to a supervisor subprocess's own
# startup, large enough not to spin a core while that subprocess parses a Dag file.
_POLL_INTERVAL_SECONDS = 0.05

# States an instance can still leave on its own once queued. Anything else -- terminal,
# `deferred`, `up_for_retry`, `up_for_reschedule` -- is as far as one dispatch takes it,
# because resuming from those is a triggerer's or a scheduler's job, not an executor's.
_IN_FLIGHT_STATE_VALUES = frozenset({"queued", "restarting", "running", "scheduled"})

# `BaseExecutor.start` is a no-op default, but `end` and `terminate` raise
# `NotImplementedError` outright, so a custom executor that skipped them explodes at
# teardown *after* a run that otherwise succeeded. Naming the method beats surfacing a
# bare `NotImplementedError` from inside a `finally`.
_LIFECYCLE_HINT = (
    "`BaseExecutor.{name}` raises `NotImplementedError` by default, so every executor "
    "must override it. Add a `{name}` to the executor class; "
    "`pytest_airflow_in_a_box.check_component` reports the rest of the class shape."
)


class ExecutorRunError(DagRunDriveError):
    """Report an executor that could not be resolved, started, or driven to a result."""


def _resolve_executor(executor: object, *, parallelism: int) -> Any:
    """Turn a caller's executor selector into a live, bounded executor instance.

    Accepts what the surrounding configuration surfaces already accept: an alias
    registered through `airflow_components.executor`, a dotted import path, the executor
    class itself, or an instance the caller built. A class is instantiated here rather
    than through `ExecutorLoader.load_executor` so `parallelism` can be bounded --
    `BaseExecutor`'s default reads `[core] parallelism`, which is 32, and a
    forking executor pre-spawns that many workers the moment it starts.

    An instance is returned untouched: the caller who built it chose its parallelism.

    Parameters:
        executor: object containing an alias, dotted path, executor class, or instance.
        parallelism: int bounding a class-or-alias executor's worker pool.

    Returns:
        Any containing the live executor instance.

    Raises:
        ExecutorRunError: The selector names nothing importable, or names something that
            is not an executor.
    """

    from airflow.executors.base_executor import BaseExecutor

    if isinstance(executor, BaseExecutor):
        return executor
    executor_type = executor if isinstance(executor, type) else None
    if executor_type is None:
        if not isinstance(executor, str):
            raise ExecutorRunError(
                f"`executor` must be an alias, a dotted import path, a `BaseExecutor` "
                f"subclass, or a `BaseExecutor` instance, not {type(executor).__name__!r}"
            )
        executor_type = _import_executor_type(executor)
    if not issubclass(executor_type, BaseExecutor):
        raise ExecutorRunError(
            f"`{executor_type.__module__}.{executor_type.__qualname__}` is not a "
            f"`BaseExecutor` subclass"
        )
    return executor_type(parallelism=parallelism)


def _import_executor_type(name: str) -> type:
    """Resolve an executor alias or dotted import path to its class.

    Aliases resolve through `ExecutorLoader`, the registry `airflow_components.executor`
    writes into, so a sandbox-registered executor and a `[core] executor` alias reach
    this the same way. An unregistered name is retried as a dotted import path, which is
    what `[core] executor` itself accepts. Both failures are reported together: "not an
    alias" alone would be misleading for a typo'd import path, and vice versa.

    The alias lookup yields `ExecutorName.module_path` and the import is done here with
    `importlib`, rather than through `ExecutorLoader.import_executor_cls`, whose
    `(class, source)` tuple is one more Airflow-internal shape to track across releases.

    Parameters:
        name: str containing a registered alias or a dotted import path.

    Returns:
        type containing the resolved executor class.

    Raises:
        ExecutorRunError: Neither the alias registry nor a direct import resolves it.
    """

    from airflow.executors.executor_loader import ExecutorLoader

    resolution_errors: list[str] = []
    module_path = name
    try:
        module_path = ExecutorLoader.lookup_executor_name_by_str(name).module_path
    # Broad on purpose: `ExecutorLoader` raises `UnknownExecutorException`,
    # `AirflowConfigException`, `ImportError` and `ValueError` across the certified
    # releases for what is one condition -- "that is not a registered alias".
    except Exception as error:
        resolution_errors.append(f"as a registered alias: {error}")
    resolved = _import_dotted(module_path, resolution_errors)
    if resolved is not None:
        return resolved
    detail = "; ".join(resolution_errors)
    raise ExecutorRunError(f"Could not resolve executor '{name}'. Tried {detail}")


def _import_dotted(path: str, resolution_errors: list[str]) -> type | None:
    """Import one dotted path to a class, recording why it did not resolve.

    Parameters:
        path: str containing a dotted path whose last component names a class.
        resolution_errors: list[str] receiving a description of any failure.

    Returns:
        type | None containing the imported class, or `None` when it did not resolve.
    """

    module_name, separator, attribute = path.rpartition(".")
    if not separator:
        resolution_errors.append(f"as an import path: '{path}' has no module component")
        return None
    try:
        resolved = getattr(import_module(module_name), attribute)
    # Broad on purpose: a bad path raises `ModuleNotFoundError` or `AttributeError`, and
    # a module whose own imports are broken raises anything at all.
    except Exception as error:
        resolution_errors.append(f"as an import path: {error}")
        return None
    if not isinstance(resolved, type):
        resolution_errors.append(f"as an import path: '{path}' does not name a class")
        return None
    return resolved


def relative_dag_path(dag: Any, dags_folder: Path) -> Path:
    """Locate a Dag file inside the folder its bundle will expose to task workers.

    An executor-driven task runs in a fresh subprocess that re-imports the Dag *file*
    from a configured bundle, so a Dag this process merely holds in memory is not
    enough -- the file has to be inside the bundled folder. That is what makes a
    `dag_maker` Dag impossible here: it is defined in a test function body, and a
    standalone re-import finds nothing.

    Parameters:
        dag: Any containing the authoring Dag whose `fileloc` is being located.
        dags_folder: pathlib.Path containing the folder the bundle will expose.

    Returns:
        pathlib.Path containing the Dag file's path relative to `dags_folder`.

    Raises:
        ExecutorRunError: The Dag has no file location, or its file is not inside
            `dags_folder`.
    """

    fileloc = getattr(dag, "fileloc", "")
    if not fileloc:
        raise ExecutorRunError(
            f"Dag '{dag.dag_id}' has no `fileloc`, so no task worker can re-import it. "
            f"An executor-driven run needs a Dag defined in a file inside the Dag "
            f"folder; `dag_maker` Dags are defined in a test body and cannot qualify."
        )
    try:
        return Path(fileloc).resolve().relative_to(Path(dags_folder).resolve())
    except ValueError as error:
        raise ExecutorRunError(
            f"Dag '{dag.dag_id}' lives at '{fileloc}', which is outside the Dag folder "
            f"'{dags_folder}'. An executor-driven run re-imports the Dag file in a task "
            f"worker subprocess, so the file must be inside the folder `dag_bag` "
            f"parses -- set `--dag-folder` or the `airflow_dags_folder` ini option to "
            f"the folder holding this Dag."
        ) from error


def bundle_overrides(bundle_name: str, dags_folder: Path) -> dict[tuple[str, str], str]:
    """Build the configuration exposing one Dag folder to task workers under a bundle.

    Task workers resolve bundles from *configuration*, never from the metadata database,
    so the `DagBundleModel` row this plugin already writes per Dag is invisible to them.
    This adds a matching `LocalDagBundle` entry pointing at the Dag folder, leaving any
    bundle the consumer configured in place. Re-registering the same name replaces this
    plugin's own earlier entry rather than accumulating duplicates.

    Parameters:
        bundle_name: str naming the bundle, matching the persisted `DagModel.bundle_name`.
        dags_folder: pathlib.Path containing the folder to expose.

    Returns:
        dict[tuple[str, str], str] mapping the bundle configuration option to its value.
    """

    from airflow.configuration import conf

    configured: list[dict[str, Any]] = []
    # Broad on purpose: an unset, empty, or malformed consumer value must not stop a
    # test run here. Airflow itself re-reads and rejects the merged list downstream.
    try:
        existing = conf.getjson(*BUNDLE_CONFIG_OPTION)
    except Exception as error:  # pragma: no cover - defensive, see comment above
        LOGGER.debug(f"Could not read the configured Dag bundle list: {error}")
    else:
        if isinstance(existing, list):
            configured = [
                entry
                for entry in existing
                if not (isinstance(entry, dict) and entry.get("name") == bundle_name)
            ]
    configured.append(
        {
            "name": bundle_name,
            "classpath": LOCAL_BUNDLE_CLASSPATH,
            "kwargs": {"path": str(Path(dags_folder).resolve())},
        }
    )
    return {BUNDLE_CONFIG_OPTION: json.dumps(configured)}


def _lifecycle(executor: Any, name: str) -> None:
    """Call one executor lifecycle method, naming it when the class did not override it.

    Parameters:
        executor: Any containing the live executor instance.
        name: str naming the lifecycle method to call.

    Raises:
        ExecutorRunError: The executor inherits `BaseExecutor`'s raising default.
    """

    try:
        getattr(executor, name)()
    except NotImplementedError as error:
        raise ExecutorRunError(
            f"`{type(executor).__name__}.{name}()` is not implemented. "
            f"{_LIFECYCLE_HINT.format(name=name)}"
        ) from error


@contextmanager
def executor_runtime(
    dag: Any,
    *,
    executor: object,
    api_server_url: str,
    dags_folder: Path,
    bundle_name: str,
    timeout: float,
) -> Iterator[SettleInstance]:
    """Stand up everything one executor-driven DagRun needs, and yield its settle step.

    On entry: the executor is resolved and started, the Dag file is located inside
    `dags_folder`, and the Task Execution API URL plus a `LocalDagBundle` entry for
    `bundle_name` are published as configuration. Publishing through configuration is
    what reaches task worker subprocesses, which inherit the environment rather than
    this process's objects. On exit the executor is ended and every override is
    restored exactly, whether or not the run succeeded.

    Every dependency is a parameter: no fixture is looked up and no option is read here,
    so this is exercisable with a fake executor and no live server.

    Parameters:
        dag: Any containing the authoring Dag whose file task workers will re-import.
        executor: object containing an alias, dotted path, executor class, or instance.
        api_server_url: str containing the live api-server's base URL, whose
            `/execution` app task workers report to.
        dags_folder: pathlib.Path containing the folder exposed as the bundle.
        bundle_name: str naming the bundle, matching the persisted `DagModel.bundle_name`.
        timeout: float seconds allowed for any one instance to reach a settled state.

    Yields:
        pytest_airflow_in_a_box._compat.taskrun.SettleInstance dispatching one instance.

    Raises:
        ExecutorRunError: The executor cannot be resolved or started, or the Dag file is
            not inside `dags_folder`.
        ComponentContractError: The executor's class shape is broken in a way that would
            fail mid-run.
    """

    from pytest_airflow_in_a_box.components import ComponentKind, check_component

    dag_rel_path = relative_dag_path(dag, dags_folder)
    resolved = _resolve_executor(executor, parallelism=1)
    check_component(resolved, kind=ComponentKind.EXECUTOR).raise_for_problems()

    overrides: dict[tuple[str, str], str | None] = {
        ("api", "base_url"): api_server_url,
        ("core", "execution_api_server_url"): f"{api_server_url.rstrip('/')}/execution/",
    }
    overrides.update(bundle_overrides(bundle_name, dags_folder))
    LOGGER.info(
        f"Driving Dag '{dag.dag_id}' through {type(resolved).__name__} against "
        f"'{api_server_url}' (bundle '{bundle_name}')"
    )
    with airflow_config(overrides):
        _lifecycle(resolved, "start")
        try:
            yield _settler(
                resolved,
                dag_rel_path=dag_rel_path,
                bundle_name=bundle_name,
                timeout=timeout,
            )
        finally:
            _lifecycle(resolved, "end")


def _settler(
    executor: Any,
    *,
    dag_rel_path: Path,
    bundle_name: str,
    timeout: float,
) -> SettleInstance:
    """Build the settle step dispatching one instance through a started executor.

    Parameters:
        executor: Any containing the started executor instance.
        dag_rel_path: pathlib.Path containing the Dag file relative to its bundle.
        bundle_name: str naming the bundle exposing the Dag folder.
        timeout: float seconds allowed for one instance to reach a settled state.

    Returns:
        pytest_airflow_in_a_box._compat.taskrun.SettleInstance dispatching one instance.
    """

    def settle(ti: TaskInstance, task: Operator, *, session: Session | None) -> None:
        """Queue one instance to the executor and wait for it to settle.

        Parameters:
            ti: airflow.models.taskinstance.TaskInstance to dispatch.
            task: airflow.sdk.types.Operator, unused: the worker re-imports it itself.
            session: sqlalchemy.orm.Session | None owning the persisted metadata.

        Raises:
            ExecutorRunError: No session was supplied, or the instance never settled.
            BaseException: The executor reported the task's own failure.
        """

        del task
        if session is None:
            raise ExecutorRunError(
                "An executor-driven run needs a metadata session: the task instance has "
                "to be committed as `queued` before a task worker can pick it up"
            )
        if not dependencies_met(ti, session=session):
            # Leaving the state untouched is the documented `SettleInstance` contract
            # for a blocked instance; `drive_dag_run` reads it as "not executed".
            return
        _queue(executor, ti, session, dag_rel_path=dag_rel_path, bundle_name=bundle_name)
        _await_settled(executor, ti, session, timeout=timeout)

    return settle


def _queue(
    executor: Any,
    ti: TaskInstance,
    session: Session,
    *,
    dag_rel_path: Path,
    bundle_name: str,
) -> None:
    """Commit one instance as `queued` and hand its workload to the executor.

    `bundle_info` and `dag_rel_path` are passed explicitly rather than derived from
    `ti.dag_model`, so the persisted metadata rows stay byte-identical to the ones an
    in-process run writes and nothing about the Dag object is mutated.

    Parameters:
        executor: Any containing the started executor instance.
        ti: airflow.models.taskinstance.TaskInstance to dispatch.
        session: sqlalchemy.orm.Session owning the persisted metadata.
        dag_rel_path: pathlib.Path containing the Dag file relative to its bundle.
        bundle_name: str naming the bundle exposing the Dag folder.
    """

    from airflow.executors import workloads
    from airflow.executors.workloads import BundleInfo
    from airflow.utils.state import TaskInstanceState

    instance: Any = ti
    instance.try_number += 1
    instance.state = TaskInstanceState.QUEUED
    # The task worker is a separate process reading this row through the API server, so
    # the transition has to be committed before the workload is handed over.
    session.commit()
    workload = workloads.ExecuteTask.make(
        instance,
        dag_rel_path=dag_rel_path,
        generator=executor.jwt_generator,
        bundle_info=BundleInfo(name=bundle_name),
    )
    executor.queue_workload(workload, session=session)
    session.commit()


def _await_settled(
    executor: Any,
    ti: TaskInstance,
    session: Session,
    *,
    timeout: float,
) -> None:
    """Heartbeat the executor until one instance settles, then surface its outcome.

    The task worker reports its own final state to the Task Execution API, so the
    metadata row is authoritative and the executor's event buffer is only consulted for
    what the row cannot carry: the exception object a local executor caught. When one is
    reported it is raised so `drive_dag_run` records it against the instance's key, and
    `DagRunResult.errors` carries it exactly as the in-process path would.

    Parameters:
        executor: Any containing the started executor instance.
        ti: airflow.models.taskinstance.TaskInstance being awaited.
        session: sqlalchemy.orm.Session owning the persisted metadata.
        timeout: float seconds allowed before the instance is declared stuck.

    Raises:
        ExecutorRunError: The instance never left an in-flight state.
        BaseException: The executor reported the task's own failure.
    """

    instance: Any = ti
    deadline = time.monotonic() + timeout
    reported: BaseException | None = None
    while True:
        executor.heartbeat()
        reported = _drain_events(executor, ti) or reported
        session.expire_all()
        session.refresh(instance)
        state = getattr(instance.state, "value", instance.state)
        if state not in _IN_FLIGHT_STATE_VALUES:
            if reported is not None:
                raise reported
            return
        if time.monotonic() >= deadline:
            raise ExecutorRunError(
                f"Task instance '{task_key(str(instance.task_id), int(instance.map_index))}' "
                f"of Dag '{instance.dag_id}' was still '{state}' after {timeout:g}s under "
                f"{type(executor).__name__}. Either the executor never reported it, or "
                f"the task needs longer than `--airflow-executor-timeout` allows."
            )
        time.sleep(_POLL_INTERVAL_SECONDS)


def _drain_events(executor: Any, ti: TaskInstance) -> BaseException | None:
    """Flush the executor's event buffer, returning any exception reported for `ti`.

    Parameters:
        executor: Any containing the started executor instance.
        ti: airflow.models.taskinstance.TaskInstance whose events are wanted.

    Returns:
        BaseException | None containing the exception the executor attached, if any.
    """

    instance: Any = ti
    identity = (str(instance.task_id), int(instance.map_index))
    reported: BaseException | None = None
    for key, (state, info) in executor.get_event_buffer().items():
        if (str(getattr(key, "task_id", "")), int(getattr(key, "map_index", -1))) != identity:
            continue
        LOGGER.debug(f"Executor reported '{state}' for {identity}")
        if isinstance(info, BaseException):
            reported = info
    return reported


def execute_dag_run_via_executor(
    dag_run: DagRun,
    dag: Any,
    *,
    executor: object,
    api_server_url: str,
    dags_folder: Path,
    bundle_name: str,
    session: Session,
    timeout: float = DEFAULT_EXECUTOR_TIMEOUT,
) -> DagRunResult:
    """Execute every task instance of one DagRun through an executor and snapshot it.

    The executor-driven adapter over `drive_dag_run`, returning the same
    `DagRunResult` the in-process path returns, with the same ordering, mapped-task
    expansion, and settling semantics. Two differences are inherent to running tasks in
    another process and are worth asserting around rather than fighting: task bodies
    execute in supervised worker subprocesses, so `result.errors` carries only what the
    executor itself attached to a failure -- the full traceback is in the worker's log
    under the run's logs folder -- and instances are dispatched one at a time, so an
    executor's own concurrency is not exercised.

    Parameters:
        dag_run: airflow.models.dagrun.DagRun whose task instances are executed.
        dag: Any containing the authoring Dag whose file task workers re-import.
        executor: object containing an alias, dotted path, executor class, or instance.
        api_server_url: str containing the live api-server's base URL.
        dags_folder: pathlib.Path containing the folder exposed as the bundle.
        bundle_name: str naming the bundle, matching the persisted `DagModel.bundle_name`.
        session: sqlalchemy.orm.Session owning the persisted metadata.
        timeout: float seconds allowed for any one instance to settle.

    Returns:
        pytest_airflow_in_a_box.results.DagRunResult containing the settled outcome.

    Raises:
        ExecutorRunError: The executor cannot be resolved, started, or driven to a
            result, or the Dag file is not inside `dags_folder`.
        ComponentContractError: The executor's class shape is broken.
    """

    with executor_runtime(
        dag,
        executor=executor,
        api_server_url=api_server_url,
        dags_folder=dags_folder,
        bundle_name=bundle_name,
        timeout=timeout,
    ) as settle:
        return drive_dag_run(dag_run, dag, session, settle)


__all__ = (
    "DEFAULT_EXECUTOR_TIMEOUT",
    "ExecutorRunError",
    "bundle_overrides",
    "execute_dag_run_via_executor",
    "executor_runtime",
    "relative_dag_path",
)
