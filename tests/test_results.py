"""Test DagRunResult snapshots and assertion rendering without Airflow."""

from __future__ import annotations

from typing import Any

import pytest

from pytest_airflow_in_a_box import plugin
from pytest_airflow_in_a_box.results import (
    DagRunResult,
    TaskResult,
    assertrepr_compare,
    task_key,
)


class _State:
    """Mimic an Airflow state enum exposing ``value``."""

    def __init__(self, value: str) -> None:
        """Store the display value.

        Parameters:
            value: str containing the state value.
        """

        self.value = value


def _task_result(
    task_id: str,
    *,
    map_index: int = -1,
    state: Any = "success",
    xcom: Any = None,
    error: BaseException | None = None,
) -> TaskResult:
    """Build one snapshot task result with a marker task instance.

    Parameters:
        task_id: str identifying the task.
        map_index: int identifying the mapped instance, or ``-1`` when unmapped.
        state: Any containing the settled state.
        xcom: Any containing the pulled ``return_value`` XCom.
        error: BaseException | None captured from the task body.

    Returns:
        TaskResult containing the supplied fields.
    """

    return TaskResult(
        task_id=task_id,
        map_index=map_index,
        state=state,
        xcom=xcom,
        error=error,
        ti=object(),
    )


def _dag_run_result(*tasks: TaskResult, executed: tuple[str, ...] = ()) -> DagRunResult:
    """Build one snapshot around the supplied task results.

    Parameters:
        tasks: TaskResult entries in graph order.
        executed: tuple[str, ...] containing executed task keys in order.

    Returns:
        DagRunResult containing the supplied tasks.
    """

    return DagRunResult(
        dag_run=object(),
        dag_id="unit_dag",
        run_id="unit_run",
        state=_State("failed"),
        success=False,
        tasks=tasks,
        executed=executed,
    )


def test_task_key_renders_unmapped_and_mapped_identities() -> None:
    """Render bare task ids for unmapped instances and indexed keys for mapped ones."""

    assert task_key("produce", -1) == "produce"
    assert task_key("double", 2) == "double[2]"


def test_task_result_repr_prefers_error_over_xcom() -> None:
    """Render the captured error, the xcom, or neither, in that priority order."""

    plain = _task_result("idle", state=None)
    valued = _task_result("produce", xcom=21)
    failed = _task_result("boom", state="failed", xcom=1, error=ValueError("nope"))

    assert repr(plain) == "TaskResult('idle', state=None)"
    assert repr(valued) == "TaskResult('produce', state=success, xcom=21)"
    assert repr(failed) == "TaskResult('boom', state=failed, error=ValueError('nope'))"


def test_task_result_repr_truncates_long_details() -> None:
    """Bound one oversized xcom repr so assertion output stays scannable."""

    result = _task_result("produce", xcom="x" * 200)

    assert repr(result).endswith(f"xcom='{'x' * 59}...)")


def test_order_states_errors_and_tis_expose_snapshot_fields() -> None:
    """Expose executed order, per-key states, captured errors, and raw instances."""

    error = ValueError("nope")
    produce = _task_result("produce", xcom=21)
    boom = _task_result("boom", state=_State("failed"), error=error)
    result = _dag_run_result(produce, boom, executed=("produce", "boom"))

    assert result.order == ["produce", "boom"]
    assert result.states == {"produce": "success", "boom": boom.state}
    assert result.errors == {"boom": error}
    assert result.tis == (produce.ti, boom.ti)


def test_xcoms_aggregate_mapped_values_and_drop_unpushed_tasks() -> None:
    """Map unmapped values by task id, mapped values to ordered lists, and drop `None`."""

    result = _dag_run_result(
        _task_result("produce", xcom=21),
        _task_result("silent"),
        _task_result("double", map_index=1, xcom=16),
        _task_result("double", map_index=0, xcom=14),
        _task_result("quiet", map_index=0),
        _task_result("quiet", map_index=1),
    )

    assert result.xcoms == {"produce": 21, "double": [14, 16]}


def test_getitem_addresses_unmapped_tasks_and_mapped_instances() -> None:
    """Return one snapshot by bare task id or by ``(task_id, map_index)``."""

    produce = _task_result("produce", xcom=21)
    mapped = _task_result("double", map_index=1, xcom=16)
    result = _dag_run_result(produce, mapped)

    assert result["produce"] is produce
    assert result["double", 1] is mapped


def test_getitem_rejects_absent_and_ambiguous_keys() -> None:
    """Name the available keys for absent tasks and expanded mapped tasks."""

    result = _dag_run_result(
        _task_result("double", map_index=0),
        _task_result("double", map_index=1),
    )

    with pytest.raises(KeyError, match=r"'missing' is absent.*double\[0\]"):
        _ = result["missing"]
    with pytest.raises(KeyError, match=r"map index '9' is absent"):
        _ = result["double", 9]
    with pytest.raises(KeyError, match=r"'double' is mapped.*double\[1\]"):
        _ = result["double"]


def test_getitem_rejects_a_bare_id_for_a_single_mapped_instance() -> None:
    """Keep bare-id access unambiguous even when only one mapped instance exists."""

    result = _dag_run_result(_task_result("double", map_index=0))

    with pytest.raises(KeyError, match=r"'double' is mapped"):
        _ = result["double"]


def test_equality_compares_per_task_outcomes_against_a_mapping() -> None:
    """Match exact key coverage, reject key or value mismatches and non-mappings."""

    produce = _task_result("produce", xcom=21)
    consume = _task_result("consume", xcom=42)
    result = _dag_run_result(produce, consume)

    assert result == {"produce": produce, "consume": consume}
    assert result != {"produce": produce}
    assert result != {"produce": produce, "consume": _task_result("consume", xcom=41)}
    assert result != 5


def test_repr_renders_an_aligned_per_task_table() -> None:
    """Render the header plus one aligned row per instance with error or xcom detail."""

    result = _dag_run_result(
        _task_result("produce", xcom=21),
        _task_result("boom", state=_State("failed"), error=ValueError("nope")),
        _task_result("consume", state=_State("upstream_failed")),
    )

    assert repr(result).splitlines() == [
        "<DagRunResult 'unit_dag' run_id='unit_run' state=failed>",
        "  produce  success          xcom=21",
        "  boom     failed           error=ValueError('nope')",
        "  consume  upstream_failed",
    ]


def test_repr_without_tasks_is_the_bare_header() -> None:
    """Render only the header when the DagRun settled with no task instances."""

    assert repr(_dag_run_result()) == "<DagRunResult 'unit_dag' run_id='unit_run' state=failed>"


def test_assertrepr_compare_explains_each_key_category() -> None:
    """Label matching, mismatched, absent, and unexpected task keys."""

    produce = _task_result("produce", xcom=21)
    boom = _task_result("boom", state=_State("failed"), error=ValueError("nope"))
    result = _dag_run_result(produce, boom)
    expected = {"produce": produce, "missing": produce}

    lines = assertrepr_compare("==", result, expected)

    assert lines == [
        "DagRunResult does not match the expected task outcomes:",
        "  boom: not in the expected mapping; got "
        "TaskResult('boom', state=failed, error=ValueError('nope'))",
        "  missing: absent from the DagRun; expected "
        "TaskResult('produce', state=success, xcom=21)",
        "  produce: matches TaskResult('produce', state=success, xcom=21)",
    ]


def test_assertrepr_compare_labels_value_mismatches() -> None:
    """Show the expected and actual snapshots for one mismatched key."""

    produce = _task_result("produce", xcom=21)
    expected = {"produce": _task_result("produce", xcom=99)}

    lines = assertrepr_compare("==", expected, _dag_run_result(produce))

    assert lines is not None
    assert lines[1] == (
        "  produce: expected TaskResult('produce', state=success, xcom=99), "
        "got TaskResult('produce', state=success, xcom=21)"
    )


def test_assertrepr_compare_ignores_unrelated_comparisons() -> None:
    """Keep pytest's default rendering for other operators and operand shapes."""

    result = _dag_run_result(_task_result("produce"))

    assert assertrepr_compare("!=", result, {}) is None
    assert assertrepr_compare("==", result, [1]) is None
    assert assertrepr_compare("==", {"a": 1}, {"b": 2}) is None


def test_plugin_hook_delegates_to_assertrepr_compare() -> None:
    """Route pytest's comparison hook through the snapshot renderer."""

    config: Any = None
    result = _dag_run_result(_task_result("produce", xcom=21))

    lines = plugin.pytest_assertrepr_compare(config, "==", result, {})

    assert lines is not None
    assert lines[0] == "DagRunResult does not match the expected task outcomes:"
    assert plugin.pytest_assertrepr_compare(config, "==", 1, 2) is None
