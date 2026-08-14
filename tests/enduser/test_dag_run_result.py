"""Exercise whole-DagRun execution and bulk outcome assertions.

`BaseOperator` and `task` resolve dynamically ON PURPOSE: they moved from
`airflow.models`/`airflow.decorators` (2.x) to `airflow.sdk` (3.x), and
`dag_maker.run()` is family-branched in `_compat.taskrun`, so the whole module
collects and runs on both families. `airflow.triggers.base` and the matchers are
family-independent and stay static imports.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta
from importlib import import_module
from typing import Any

import pytest
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.state import DagRunState, TaskInstanceState

from pytest_airflow_in_a_box.matchers import (
    deferred,
    failed,
    not_run,
    skipped,
    succeeded,
    upstream_failed,
)
from pytest_airflow_in_a_box.types import DagMaker

pytestmark = [pytest.mark.compat, pytest.mark.db_test]

# Shared with the sibling contract modules; see `tests/enduser/_authoring.py`.
_resolve = import_module("_authoring")._resolve

_authoring = _resolve("airflow.sdk", "airflow.models")
BaseOperator = _authoring.BaseOperator
task = _resolve("airflow.sdk", "airflow.decorators").task


class ImmediateTrigger(BaseTrigger):
    """Emit one deterministic event."""

    def __init__(self, value: int) -> None:
        """Store the payload yielded by ``run``.

        Parameters:
            value: int contained in the emitted event payload.
        """

        super().__init__()
        self.value = value

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Return the importable classpath and constructor arguments.

        Returns:
            tuple[str, dict[str, Any]] persisted for the triggerer.
        """

        return f"{type(self).__module__}.{type(self).__qualname__}", {"value": self.value}

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Yield one deterministic event.

        Yields:
            airflow.triggers.base.TriggerEvent containing the stored payload.
        """

        yield TriggerEvent({"value": self.value})


class DeferredOperator(BaseOperator):
    """Defer once, then return the trigger payload."""

    def execute(self, context: Any) -> None:
        """Defer to ``ImmediateTrigger`` immediately.

        Parameters:
            context: Any containing the unused execution context.
        """

        del context
        self.defer(trigger=ImmediateTrigger(42), method_name="execute_complete")

    def execute_complete(self, context: Any, event: dict[str, Any]) -> int:
        """Return the resumed trigger payload.

        Parameters:
            context: Any containing the unused execution context.
            event: dict[str, Any] containing the trigger payload.

        Returns:
            int contained in the trigger payload.
        """

        del context
        return event["value"]


def _linear_dag(dag_maker: DagMaker, dag_id: str) -> None:
    """Author one produce/consume TaskFlow Dag.

    Parameters:
        dag_maker: DagMaker building the persisted Dag.
        dag_id: str identifying the Dag.
    """

    with dag_maker(dag_id=dag_id):

        @task
        def produce() -> int:
            return 21

        @task
        def consume(value: int) -> int:
            return value * 2

        produced: Any = produce()
        consume(produced)


def test_run_executes_the_whole_dag_and_snapshots(dag_maker: DagMaker) -> None:
    """Create the DagRun, run every task in order, and expose bulk outcomes."""

    _linear_dag(dag_maker, "result_linear")

    result = dag_maker.run()

    assert result.success
    assert result.state == DagRunState.SUCCESS
    assert result.states == {
        "produce": TaskInstanceState.SUCCESS,
        "consume": TaskInstanceState.SUCCESS,
    }
    assert result.xcoms == {"produce": 21, "consume": 42}
    assert result.order == ["produce", "consume"]
    assert result.errors == {}
    assert result["consume"].xcom == 42
    assert len(result.tis) == 2
    assert result == {"produce": succeeded(21), "consume": succeeded(42)}


def test_run_repr_renders_the_per_task_table(dag_maker: DagMaker) -> None:
    """Render the header and one aligned row per settled task instance."""

    _linear_dag(dag_maker, "result_repr")

    rendered = repr(dag_maker.run())

    lines = rendered.splitlines()
    assert lines[0].startswith("<DagRunResult 'result_repr' run_id='")
    assert lines[0].endswith("state=success>")
    assert lines[1].startswith("  produce  success")
    assert lines[1].endswith("xcom=21")
    assert lines[2].startswith("  consume  success")
    assert lines[2].endswith("xcom=42")


def test_run_captures_a_failing_task_scheduler_shaped(dag_maker: DagMaker) -> None:
    """Record the raised error, continue, and settle downstreams `upstream_failed`."""

    with dag_maker(dag_id="result_failure"):

        @task
        def produce() -> int:
            return 21

        @task
        def boom(value: int) -> int:
            raise ValueError(f"nope: '{value}'")

        @task
        def consume(value: int) -> int:
            return value * 2

        produced: Any = produce()
        exploded: Any = boom(produced)
        consume(exploded)

    result = dag_maker.run()

    assert not result.success
    assert result.state == DagRunState.FAILED
    assert result.states == {
        "produce": TaskInstanceState.SUCCESS,
        "boom": TaskInstanceState.FAILED,
        "consume": TaskInstanceState.UPSTREAM_FAILED,
    }
    assert result.order == ["produce", "boom"]
    assert isinstance(result.errors["boom"], ValueError)
    assert result == {
        "produce": succeeded(21),
        "boom": failed(ValueError),
        "consume": upstream_failed(),
    }


def test_run_expands_mapped_tasks_mid_run(dag_maker: DagMaker) -> None:
    """Expand an XCom-driven mapping after its producer succeeds and run every index."""

    with dag_maker(dag_id="result_mapping"):

        @task
        def produce() -> list[int]:
            return [7, 8, 9]

        @task
        def double(value: int) -> int:
            return value * 2

        double.expand(value=produce())

    result = dag_maker.run()

    assert result.success
    assert result.states == {
        "produce": TaskInstanceState.SUCCESS,
        "double[0]": TaskInstanceState.SUCCESS,
        "double[1]": TaskInstanceState.SUCCESS,
        "double[2]": TaskInstanceState.SUCCESS,
    }
    assert result.xcoms == {"produce": [7, 8, 9], "double": [14, 16, 18]}
    assert result.order == ["produce", "double[0]", "double[1]", "double[2]"]
    assert result["double", 1].xcom == 16
    with pytest.raises(KeyError, match=r"'double' is mapped"):
        _ = result["double"]


def test_run_records_branch_skips_without_executing_them(dag_maker: DagMaker) -> None:
    """Keep branch-skipped downstreams out of the execution order."""

    with dag_maker(dag_id="result_branch"):

        @task.branch
        def choose() -> str:
            return "chosen"

        @task
        def chosen() -> None:
            pass

        @task
        def rejected() -> None:
            pass

        choose() >> [chosen(), rejected()]

    result = dag_maker.run()

    assert result.success
    assert result.states == {
        "choose": TaskInstanceState.SUCCESS,
        "chosen": TaskInstanceState.SUCCESS,
        "rejected": TaskInstanceState.SKIPPED,
    }
    assert result.order == ["choose", "chosen"]
    assert result["rejected"] == skipped()


def test_run_leaves_a_deferred_task_visible(dag_maker: DagMaker) -> None:
    """Settle a deferring task as `deferred` and keep the DagRun running."""

    with dag_maker(dag_id="result_deferred"):
        DeferredOperator(task_id="deferred")

    result = dag_maker.run()

    assert not result.success
    assert result.state == DagRunState.RUNNING
    assert result.states == {"deferred": TaskInstanceState.DEFERRED}
    assert result.order == ["deferred"]


def test_run_resumes_a_deferred_task_with_the_triggerer(dag_maker: DagMaker) -> None:
    """Fire the persisted trigger inline and settle the deferring task."""

    with dag_maker(dag_id="result_triggerer"):
        DeferredOperator(task_id="deferred")

    result = dag_maker.run(run_triggerer=True, trigger_timeout=5.0)

    assert result.success
    assert result.xcoms == {"deferred": 42}


def test_run_commits_the_settled_state_for_other_sessions(
    dag_maker: DagMaker,
    session: Any,
) -> None:
    """Leave no open write transaction: settle results are visible cross-session."""

    from airflow.models.taskinstance import TaskInstance
    from sqlalchemy import select

    with dag_maker(dag_id="result_commit"):

        @task
        def boom() -> None:
            raise ValueError("nope")

        @task
        def consume() -> None:
            pass

        boom() >> consume()

    result = dag_maker.run()

    observed = {
        str(ti.task_id): ti.state
        for ti in session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == result.dag_id,
                TaskInstance.run_id == result.run_id,
            )
        )
    }
    assert observed == {
        "boom": TaskInstanceState.FAILED,
        "consume": TaskInstanceState.UPSTREAM_FAILED,
    }


def test_run_waits_for_upstreams_before_expanding_mapped_tasks(
    dag_maker: DagMaker,
) -> None:
    """Keep a mapped task pending, not `upstream_failed`, behind a deferred upstream."""

    with dag_maker(dag_id="result_deferred_mapping"):

        @task
        def double(value: int) -> int:
            return value * 2

        double.expand(value=DeferredOperator(task_id="defer").output)

    result = dag_maker.run()

    assert not result.success
    assert result.state == DagRunState.RUNNING
    assert result.states == {"defer": TaskInstanceState.DEFERRED, "double": None}
    assert result == {"defer": deferred(), "double": not_run()}


def test_run_success_follows_airflow_leaf_semantics(dag_maker: DagMaker) -> None:
    """Report `success` from the DagRun state even when an absorbed task failed."""

    with dag_maker(dag_id="result_leaf_semantics"):

        @task
        def boom() -> None:
            raise ValueError("nope")

        @task(trigger_rule="all_done")
        def cleanup() -> None:
            pass

        boom() >> cleanup()

    result = dag_maker.run()

    assert result.success
    assert result.states == {
        "boom": TaskInstanceState.FAILED,
        "cleanup": TaskInstanceState.SUCCESS,
    }
    assert isinstance(result.errors["boom"], ValueError)


def test_run_rejects_a_foreign_dag_run(dag_maker: DagMaker) -> None:
    """Refuse a DagRun the factory's current Dag does not own."""

    _linear_dag(dag_maker, "result_first_dag")
    first_dag_run = dag_maker.create_dagrun()
    _linear_dag(dag_maker, "result_second_dag")

    with pytest.raises(ValueError, match="is not owned by `dag_maker`"):
        dag_maker.run(first_dag_run)


def test_run_strands_a_retrying_task_visibly(dag_maker: DagMaker) -> None:
    """Settle a retry-configured failure as `up_for_retry` and keep the DagRun running."""

    with dag_maker(dag_id="result_retry"):

        @task(retries=1, retry_delay=timedelta(minutes=5))
        def boom() -> None:
            raise ValueError("nope")

        boom()

    result = dag_maker.run()

    assert not result.success
    assert result.state == DagRunState.RUNNING
    assert result.states == {"boom": TaskInstanceState.UP_FOR_RETRY}
    assert isinstance(result.errors["boom"], ValueError)
    assert result.order == ["boom"]


def test_run_accepts_an_explicit_dag_run(dag_maker: DagMaker) -> None:
    """Compose with `create_dagrun` for callers pinning run metadata."""

    _linear_dag(dag_maker, "result_explicit")
    dag_run = dag_maker.create_dagrun(run_id="pinned-run")

    result = dag_maker.run(dag_run)

    assert result.success
    assert result.run_id == "pinned-run"
    assert result.dag_run is dag_run


def test_run_rejects_conflicting_dag_run_arguments(dag_maker: DagMaker) -> None:
    """Refuse an explicit DagRun combined with creation keyword arguments."""

    _linear_dag(dag_maker, "result_conflict")
    dag_run = dag_maker.create_dagrun()

    with pytest.raises(ValueError, match="`dag_run_kwargs` cannot be supplied"):
        dag_maker.run(dag_run, dag_run_kwargs={"run_id": "other"})
