"""Test the executor-driven DagRun adapter without a live server or a real executor.

`_compat.executor` takes every dependency as a parameter, so all of its resolution,
validation, configuration and lifecycle behaviour is reachable with a stub executor and
a made-up URL. `tests/enduser/test_executor_run.py` covers the parts that genuinely need
a running api-server and real worker subprocesses.

References:
    https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, NamedTuple

import pytest

from pytest_airflow_in_a_box._compat import executor as compat_executor
from pytest_airflow_in_a_box._compat.executor import (
    ExecutorRunError,
    bundle_overrides,
    executor_runtime,
    relative_dag_path,
)

pytestmark = pytest.mark.requires_airflow3


def _recording_executor() -> Any:
    """Build a minimal, genuinely conformant executor that records what it is asked to do.

    A real `BaseExecutor` subclass rather than a mock, so `executor_runtime`'s
    `check_component` pre-flight runs for real against it. Built inside a function
    because the base class is a 3.x-only import.

    Returns:
        Any containing the recording executor instance.
    """

    from airflow.executors.base_executor import BaseExecutor

    class RecordingExecutor(BaseExecutor):
        """Record lifecycle and queueing calls without running anything."""

        def __init__(self) -> None:
            """Initialize empty call recording."""

            super().__init__(parallelism=1)
            self.calls: list[str] = []
            self.dispatched: list[Any] = []
            self.heartbeats = 0
            self.events: dict[Any, tuple[Any, Any]] = {}

        def start(self) -> None:
            """Record the start call."""

            self.calls.append("start")

        def sync(self) -> None:
            """Report nothing; nothing is ever actually dispatched."""

        def _process_workloads(self, workload_items: Any) -> None:
            """Record the workloads without running them.

            Parameters:
                workload_items: Any containing the workloads handed over.
            """

            self.dispatched.extend(workload_items)

        def heartbeat(self) -> None:
            """Record one heartbeat."""

            self.heartbeats += 1

        def get_event_buffer(self, dag_ids: Any = None) -> dict[Any, tuple[Any, Any]]:
            """Return and flush the recorded events.

            Parameters:
                dag_ids: Any, unused: this executor never filters.

            Returns:
                dict[Any, tuple[Any, Any]] containing the flushed events.
            """

            del dag_ids
            events, self.events = self.events, {}
            return events

        def end(self) -> None:
            """Record the end call."""

            self.calls.append("end")

        def terminate(self) -> None:
            """Report nothing; nothing is ever in flight."""

    return RecordingExecutor()


def _dag(fileloc: str, dag_id: str = "probe") -> Any:
    """Build the minimal Dag stand-in `relative_dag_path` reads.

    Parameters:
        fileloc: str containing the Dag's file location.
        dag_id: str identifying the Dag in failure messages.

    Returns:
        Any exposing `dag_id` and `fileloc`.
    """

    return SimpleNamespace(dag_id=dag_id, fileloc=fileloc)


def test_relative_dag_path_locates_a_dag_inside_the_folder(tmp_path: Path) -> None:
    """Return the Dag file's path relative to the folder its bundle will expose.

    Parameters:
        tmp_path: pathlib.Path containing the Dag folder.
    """

    nested = tmp_path / "team" / "etl.py"
    nested.parent.mkdir()
    nested.touch()

    assert relative_dag_path(_dag(str(nested)), tmp_path) == Path("team/etl.py")


def test_relative_dag_path_rejects_a_dag_with_no_file(tmp_path: Path) -> None:
    """Name the missing `fileloc` rather than letting a worker fail to import nothing.

    Parameters:
        tmp_path: pathlib.Path containing the Dag folder.
    """

    with pytest.raises(ExecutorRunError, match="has no `fileloc`"):
        relative_dag_path(_dag(""), tmp_path)


def test_relative_dag_path_rejects_a_dag_outside_the_folder(tmp_path: Path) -> None:
    """Name the folder and the offending path when the Dag lives elsewhere.

    Parameters:
        tmp_path: pathlib.Path containing the Dag folder.
    """

    outside = tmp_path.parent / "elsewhere.py"
    outside.touch()
    folder = tmp_path / "dags"
    folder.mkdir()

    with pytest.raises(ExecutorRunError, match="outside the Dag folder"):
        relative_dag_path(_dag(str(outside)), folder)


def test_bundle_overrides_appends_to_the_configured_bundle_list(tmp_path: Path) -> None:
    """Expose the Dag folder without discarding a bundle the consumer configured.

    Parameters:
        tmp_path: pathlib.Path containing the Dag folder.
    """

    from pytest_airflow_in_a_box.config import airflow_config

    theirs = {"name": "theirs", "classpath": "x.Y", "kwargs": {}}
    with airflow_config({compat_executor.BUNDLE_CONFIG_OPTION: json.dumps([theirs])}):
        overrides = bundle_overrides("ours", tmp_path)

    entries = json.loads(overrides[compat_executor.BUNDLE_CONFIG_OPTION])
    assert [entry["name"] for entry in entries] == ["theirs", "ours"]
    assert entries[1]["classpath"] == compat_executor.LOCAL_BUNDLE_CLASSPATH
    assert entries[1]["kwargs"] == {"path": str(tmp_path.resolve())}


def test_bundle_overrides_replaces_its_own_earlier_entry(tmp_path: Path) -> None:
    """Keep re-registering one bundle name idempotent instead of accumulating duplicates.

    Parameters:
        tmp_path: pathlib.Path containing the Dag folder.
    """

    from pytest_airflow_in_a_box.config import airflow_config

    stale = {"name": "ours", "classpath": "x.Y", "kwargs": {"path": "/stale"}}
    with airflow_config({compat_executor.BUNDLE_CONFIG_OPTION: json.dumps([stale])}):
        overrides = bundle_overrides("ours", tmp_path)

    entries = json.loads(overrides[compat_executor.BUNDLE_CONFIG_OPTION])
    assert len(entries) == 1
    assert entries[0]["kwargs"] == {"path": str(tmp_path.resolve())}


def test_bundle_overrides_ignores_a_non_list_configured_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep a malformed consumer value from stopping the run; Airflow rejects it later.

    Parameters:
        tmp_path: pathlib.Path containing the Dag folder.
        monkeypatch: pytest.MonkeyPatch replacing the configuration reader.
    """

    from airflow.configuration import conf

    def malformed(*args: Any, **kwargs: Any) -> dict[str, str]:
        """Return a value that is valid JSON but not the expected list.

        Parameters:
            args: Any forwarded positional arguments, unused.
            kwargs: Any forwarded keyword arguments, unused.

        Returns:
            dict[str, str] standing in for a malformed consumer value.
        """

        del args, kwargs
        return {"not": "a list"}

    monkeypatch.setattr(conf, "getjson", malformed)

    entries = json.loads(bundle_overrides("ours", tmp_path)[compat_executor.BUNDLE_CONFIG_OPTION])

    assert [entry["name"] for entry in entries] == ["ours"]


def test_resolve_executor_returns_a_supplied_instance_untouched() -> None:
    """Leave an instance the caller built alone, including its chosen parallelism."""

    from airflow.executors.local_executor import LocalExecutor

    built = LocalExecutor(parallelism=7)

    resolved = compat_executor._resolve_executor(built, parallelism=1)

    assert resolved is built
    assert resolved.parallelism == 7


def test_resolve_executor_bounds_a_class_parallelism() -> None:
    """Instantiate a class with a bounded pool rather than `[core] parallelism`'s 32."""

    from airflow.executors.local_executor import LocalExecutor

    resolved = compat_executor._resolve_executor(LocalExecutor, parallelism=1)

    assert isinstance(resolved, LocalExecutor)
    assert resolved.parallelism == 1


def test_resolve_executor_accepts_a_core_alias() -> None:
    """Resolve a registered alias through `ExecutorLoader`, as `[core] executor` does."""

    from airflow.executors.local_executor import LocalExecutor

    assert isinstance(
        compat_executor._resolve_executor("LocalExecutor", parallelism=1), LocalExecutor
    )


def test_resolve_executor_accepts_a_dotted_import_path() -> None:
    """Resolve an unregistered dotted path, which `[core] executor` also accepts."""

    from airflow.executors.local_executor import LocalExecutor

    resolved = compat_executor._resolve_executor(
        "airflow.executors.local_executor.LocalExecutor", parallelism=1
    )

    assert isinstance(resolved, LocalExecutor)


def test_resolve_executor_rejects_a_non_selector() -> None:
    """Name the accepted selector shapes rather than failing deeper on an odd value."""

    with pytest.raises(ExecutorRunError, match="must be an alias"):
        compat_executor._resolve_executor(object(), parallelism=1)


def test_resolve_executor_rejects_a_class_that_is_not_an_executor() -> None:
    """Refuse a class that would explode the moment the driver called into it."""

    with pytest.raises(ExecutorRunError, match="is not a `BaseExecutor` subclass"):
        compat_executor._resolve_executor(SimpleNamespace, parallelism=1)


def test_resolve_executor_reports_both_resolution_attempts() -> None:
    """Explain the alias *and* the import-path failure, since either could be intended."""

    with pytest.raises(ExecutorRunError) as caught:
        compat_executor._resolve_executor("nope.NotThere", parallelism=1)

    message = str(caught.value)
    assert "as a registered alias" in message
    assert "as an import path" in message


def test_resolve_executor_reports_a_bare_name_with_no_module() -> None:
    """Say why a bare word cannot be an import path, instead of a bare `ImportError`."""

    with pytest.raises(ExecutorRunError, match="has no module component"):
        compat_executor._resolve_executor("NotAnExecutorAlias", parallelism=1)


def test_import_dotted_reports_a_path_that_does_not_name_a_class() -> None:
    """Refuse a dotted path resolving to something that cannot be instantiated."""

    errors: list[str] = []

    assert compat_executor._import_dotted("json.dumps", errors) is None
    assert "does not name a class" in errors[0]


def test_lifecycle_names_a_method_the_executor_did_not_override() -> None:
    """Turn `BaseExecutor`'s raising default into a message naming the missing method.

    `end` and `terminate` raise `NotImplementedError` on `BaseExecutor` itself, so an
    executor that skipped them fails at teardown, after an otherwise successful run.
    """

    class _Unfinished:
        """Reproduce `BaseExecutor`'s raising lifecycle default."""

        def end(self) -> None:
            """Raise exactly as the inherited default does."""

            raise NotImplementedError

    with pytest.raises(ExecutorRunError, match=r"`_Unfinished\.end\(\)` is not implemented"):
        compat_executor._lifecycle(_Unfinished(), "end")


def _pinned_resolution(monkeypatch: pytest.MonkeyPatch, executor: Any) -> None:
    """Pin executor resolution to one prepared instance.

    Resolution has its own tests; pinning it here keeps the runtime tests about
    lifecycle and configuration.

    Parameters:
        monkeypatch: pytest.MonkeyPatch replacing the resolver.
        executor: Any containing the instance to resolve to.
    """

    def resolve(*args: Any, **kwargs: Any) -> Any:
        """Return the prepared executor.

        Parameters:
            args: Any forwarded positional arguments, unused.
            kwargs: Any forwarded keyword arguments, unused.

        Returns:
            Any containing the prepared executor.
        """

        del args, kwargs
        return executor

    monkeypatch.setattr(compat_executor, "_resolve_executor", resolve)


def test_executor_runtime_ends_the_executor_even_when_the_run_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Release the executor on the failure path, not only on the happy one.

    Parameters:
        tmp_path: pathlib.Path containing the Dag folder.
        monkeypatch: pytest.MonkeyPatch replacing executor resolution.
    """

    executor = _recording_executor()
    _pinned_resolution(monkeypatch, executor)
    dag_file = tmp_path / "etl.py"
    dag_file.touch()

    with (
        pytest.raises(RuntimeError, match="boom"),
        executor_runtime(
            _dag(str(dag_file)),
            executor=executor,
            api_server_url="http://127.0.0.1:1",
            dags_folder=tmp_path,
            bundle_name="ours",
            timeout=1.0,
        ),
    ):
        raise RuntimeError("boom")

    assert executor.calls == ["start", "end"]


def test_executor_runtime_publishes_the_execution_api_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Publish the Task Execution API URL as configuration, which task workers inherit.

    Parameters:
        tmp_path: pathlib.Path containing the Dag folder.
        monkeypatch: pytest.MonkeyPatch replacing executor resolution.
    """

    executor = _recording_executor()
    _pinned_resolution(monkeypatch, executor)
    dag_file = tmp_path / "etl.py"
    dag_file.touch()

    with executor_runtime(
        _dag(str(dag_file)),
        executor=executor,
        api_server_url="http://127.0.0.1:9999/",
        dags_folder=tmp_path,
        bundle_name="ours",
        timeout=1.0,
    ):
        from airflow.configuration import conf

        assert conf.get("core", "execution_api_server_url") == "http://127.0.0.1:9999/execution/"
        assert conf.get("api", "base_url") == "http://127.0.0.1:9999/"
        bundles = json.loads(conf.get("dag_processor", "dag_bundle_config_list"))
        assert bundles[-1]["kwargs"] == {"path": str(tmp_path.resolve())}


def test_executor_runtime_refuses_an_executor_with_a_broken_class_shape(
    tmp_path: Path,
) -> None:
    """Fail the conformance pre-flight before a run, not deep inside a heartbeat.

    Parameters:
        tmp_path: pathlib.Path containing the Dag folder.
    """

    from pytest_airflow_in_a_box.components import ComponentContractError

    def build() -> Any:
        """Build an executor missing both required overrides.

        Returns:
            Any containing the non-conformant executor instance.
        """

        from airflow.executors.base_executor import BaseExecutor

        class Unfinished(BaseExecutor):
            """Override nothing `BaseExecutor` requires."""

        return Unfinished(parallelism=1)

    dag_file = tmp_path / "etl.py"
    dag_file.touch()

    with (
        pytest.raises(ComponentContractError, match="_process_workloads"),
        executor_runtime(
            _dag(str(dag_file)),
            executor=build(),
            api_server_url="http://127.0.0.1:1",
            dags_folder=tmp_path,
            bundle_name="ours",
            timeout=1.0,
        ),
    ):
        pytest.fail("the pre-flight should have refused this executor")


class _FakeInstance:
    """Stand in for a persisted task instance the driver dispatches and polls."""

    def __init__(self, states: list[str]) -> None:
        """Record the sequence of states successive refreshes will observe.

        Parameters:
            states: list[str] containing the states to report, last one repeating.
        """

        self.dag_id = "probe"
        self.task_id = "work"
        self.map_index = -1
        self.try_number = 0
        self._states = states
        self.state = states[0]

    def advance(self) -> None:
        """Adopt the next recorded state, holding on the last one."""

        if len(self._states) > 1:
            self._states.pop(0)
        self.state = self._states[0]


class _FakeSession:
    """Stand in for the metadata session, advancing the instance on every refresh."""

    def __init__(self, instance: _FakeInstance) -> None:
        """Bind the instance whose state successive refreshes advance.

        Parameters:
            instance: _FakeInstance whose state each refresh advances.
        """

        self.instance = instance
        self.commits = 0

    def commit(self) -> None:
        """Record one commit."""

        self.commits += 1

    def expire_all(self) -> None:
        """Report nothing; the fake instance holds its own state."""

    def refresh(self, instance: Any) -> None:
        """Advance the bound instance, as a real refresh would observe a worker's write.

        Parameters:
            instance: Any containing the instance being refreshed.
        """

        del instance
        self.instance.advance()


class _Key(NamedTuple):
    """Reproduce the hashable `TaskInstanceKey` shape `_drain_events` matches against."""

    task_id: str
    map_index: int


def _key(task_id: str = "work", map_index: int = -1) -> _Key:
    """Build one event-buffer key.

    Parameters:
        task_id: str identifying the task.
        map_index: int identifying the mapped expansion.

    Returns:
        _Key exposing `task_id` and `map_index`.
    """

    return _Key(task_id=task_id, map_index=map_index)


def test_settle_refuses_a_run_with_no_metadata_session() -> None:
    """Say why a session is required rather than failing later inside Airflow."""

    settle = compat_executor._settler(
        _recording_executor(), dag_rel_path=Path("etl.py"), bundle_name="ours", timeout=1.0
    )

    instance: Any = _FakeInstance(["queued"])
    # The session guard fires before `task` is looked at: the worker re-imports the
    # operator itself, so this side never needs one.
    task: Any = object()

    with pytest.raises(ExecutorRunError, match="needs a metadata session"):
        settle(instance, task, session=None)


def test_awaiting_a_settled_instance_raises_what_the_executor_reported() -> None:
    """Surface the executor's own exception so `DagRunResult.errors` can carry it."""

    executor = _recording_executor()
    instance: Any = _FakeInstance(["queued", "failed"])
    session: Any = _FakeSession(instance)
    boom = RuntimeError("the task raised")
    executor.events = {_key(): ("failed", boom)}

    with pytest.raises(RuntimeError, match="the task raised"):
        compat_executor._await_settled(executor, instance, session, timeout=5.0)


def test_awaiting_an_instance_that_never_settles_names_it() -> None:
    """Fail with the stuck instance and the option to raise, not a silent hang."""

    executor = _recording_executor()
    instance: Any = _FakeInstance(["queued"])
    session: Any = _FakeSession(instance)

    with pytest.raises(ExecutorRunError, match=r"'work'.*still 'queued'.*executor-timeout"):
        compat_executor._await_settled(executor, instance, session, timeout=0.0)


def test_awaiting_a_settled_instance_returns_without_an_exception() -> None:
    """Return quietly when the worker reported success through the metadata row."""

    executor = _recording_executor()
    instance: Any = _FakeInstance(["queued", "success"])
    session: Any = _FakeSession(instance)
    executor.events = {_key(): ("success", None)}

    assert compat_executor._await_settled(executor, instance, session, timeout=5.0) is None


def test_draining_events_ignores_another_instances_outcome() -> None:
    """Attribute a failure to the instance it belongs to, not whichever arrived first."""

    executor = _recording_executor()
    executor.events = {
        _key(task_id="other"): ("failed", RuntimeError("not mine")),
        _key(map_index=3): ("failed", RuntimeError("also not mine")),
    }

    instance: Any = _FakeInstance(["queued"])

    assert compat_executor._drain_events(executor, instance) is None
