"""Keep persisted execution attempts consistent with scheduled task contexts."""

from __future__ import annotations

from datetime import timedelta
from importlib import import_module
from typing import Any

import pytest
from test_triggers import DeferredOperator

from piab.matchers import succeeded
from piab.types import DagMaker

pytestmark = [pytest.mark.compat, pytest.mark.requires_airflow3]

_task = import_module("_authoring")._resolve("airflow.sdk", "airflow.decorators").task


@pytest.mark.db_test
@pytest.mark.parametrize("retries", [0, 1])
@pytest.mark.parametrize("explicit_run", [False, True])
def test_first_execution_starts_at_one(
    dag_maker: DagMaker, retries: int, explicit_run: bool
) -> None:
    """Start execution at one without mutating a newly created DagRun.

    Parameters:
        dag_maker: DagMaker building and executing the persisted Dag.
        retries: int specifying the operator's retry allowance.
        explicit_run: bool selecting a caller-created DagRun.
    """

    with dag_maker(dag_id=f"compat_attempt_{retries}_{explicit_run}", schedule=None):

        @_task(retries=retries)
        def inspect_attempt(ti: Any) -> dict[str, int]:
            """Return the counters visible inside the running task."""

            return {"try_number": ti.try_number, "max_tries": ti.max_tries}

        inspect_attempt()

    dag_run = dag_maker.create_dagrun() if explicit_run else None
    if dag_run is not None:
        assert [ti.try_number for ti in dag_run.get_task_instances()] == [0]

    result = dag_maker.run(dag_run)

    assert result == {"inspect_attempt": succeeded({"try_number": 1, "max_tries": retries})}
    assert result["inspect_attempt"].ti.try_number == 1


@pytest.mark.db_test
def test_retry_and_cleared_execution_keep_attempt_history(dag_maker: DagMaker) -> None:
    """Advance genuine retries and cleared executions instead of resetting history.

    Parameters:
        dag_maker: DagMaker building and executing the persisted Dag.
    """

    with dag_maker(dag_id="compat_attempt_history", schedule=None):

        @_task(retries=1, retry_delay=timedelta(0))
        def flaky(ti: Any) -> dict[str, int]:
            """Fail attempt one, then return the counters of later executions."""

            if ti.try_number == 1:
                raise ValueError("first attempt")
            return {"try_number": ti.try_number, "max_tries": ti.max_tries}

        flaky()

    dag_run = dag_maker.create_dagrun()
    with pytest.raises(ValueError, match="first attempt"):
        dag_maker.run_ti("flaky", dag_run)
    ti = dag_maker.run_ti("flaky", dag_run, ignore_ti_state=True, ignore_task_deps=True)
    assert ti.try_number == 2
    assert ti.xcom_pull(task_ids="flaky", session=dag_maker.session) == {
        "try_number": 2,
        "max_tries": 1,
    }
    # Running an already successful task without bypassing dependencies does nothing.
    dag_maker.run_ti("flaky", dag_run)
    assert ti.try_number == 2
    ti.set_state(None, session=dag_maker.session)
    dag_maker.session.commit()
    ti = dag_maker.run_ti("flaky", dag_run)
    assert ti.try_number == 3


class AttemptDeferredOperator(DeferredOperator):
    """Assert both halves of a deferred execution share attempt one."""

    def execute(self, context: Any) -> None:
        """Check the initial attempt before deferring to an immediate trigger.

        Parameters:
            context: Any containing the running task instance.
        """

        assert context["ti"].try_number == 1
        super().execute(context)

    def execute_complete(self, context: Any, event: dict[str, Any]) -> int:
        """Return the attempt number observed after trigger resumption.

        Parameters:
            context: Any containing the resumed task instance.
            event: dict[str, Any] containing the emitted trigger payload.

        Returns:
            int containing the resumed attempt number.
        """

        del event
        return context["ti"].try_number


@pytest.mark.db_test
def test_deferral_resumption_keeps_the_same_attempt(dag_maker: DagMaker) -> None:
    """Resume a deferred task without consuming a retry.

    Parameters:
        dag_maker: DagMaker building and executing the persisted Dag.
    """

    with dag_maker(dag_id="compat_attempt_deferred", schedule=None):
        AttemptDeferredOperator(task_id="deferred")

    result = dag_maker.run(run_triggerer=True)

    assert result == {"deferred": succeeded(1)}
    assert result["deferred"].ti.try_number == 1
