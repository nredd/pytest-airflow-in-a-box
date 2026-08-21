"""Test static conformance checks for custom Airflow components."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar

import pytest
from airflow.executors.base_executor import BaseExecutor
from airflow.listeners import hookimpl
from airflow.plugins_manager import AirflowPlugin
from airflow.policies import hookimpl as policy_hookimpl
from airflow.providers.sqlite.get_provider_info import get_provider_info as _real_provider_info
from airflow.sdk.bases.notifier import BaseNotifier
from airflow.sdk.bases.xcom import BaseXCom
from airflow.secrets.base_secrets import BaseSecretsBackend
from airflow.task.priority_strategy import PriorityWeightStrategy
from airflow.timetables.base import DataInterval, Timetable

from pytest_airflow_in_a_box.components import (
    CertificationTier,
    ComponentContractError,
    ComponentKind,
    ComponentProblem,
    ComponentReport,
    check_component,
)


class _CleanTimetable(Timetable):
    """Conformant custom timetable: every check should pass."""

    def infer_manual_data_interval(self, *, run_after: Any) -> DataInterval:
        return DataInterval.exact(run_after)

    def next_dagrun_info(self, *, last_automated_data_interval: Any, restriction: Any) -> None:
        del last_automated_data_interval, restriction
        return

    def serialize(self) -> dict[str, Any]:
        return {"marker": "clean"}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> _CleanTimetable:
        del data
        return cls()


def _make_local_timetable() -> type:
    """Build a timetable class whose `__qualname__` contains `<locals>`."""

    class _LocalTimetable(Timetable):
        """Defined inside a function on purpose, for `timetable-local-qualname`."""

        def serialize(self) -> dict[str, Any]:
            return {"unpaired": True}

    return _LocalTimetable


class _NotJsonTimetable(Timetable):
    """Serializes to a value `json.dumps` rejects."""

    def infer_manual_data_interval(self, *, run_after: Any) -> DataInterval:
        return DataInterval.exact(run_after)

    def next_dagrun_info(self, *, last_automated_data_interval: Any, restriction: Any) -> None:
        del last_automated_data_interval, restriction
        return

    def serialize(self) -> dict[str, Any]:
        return {"bad": object()}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> _NotJsonTimetable:
        del data
        return cls()


class _CleanListener:
    """Conformant custom listener: every check should pass."""

    calls: ClassVar[list[str]] = []

    @hookimpl
    def on_task_instance_success(self, previous_state: Any, task_instance: Any) -> None:
        del previous_state, task_instance


class _BrokenListener:
    """Listener exercising every listener check at once."""

    @hookimpl
    def on_totally_made_up_event(self, x: Any) -> None:
        del x

    @hookimpl
    def on_task_instance_success(
        self, previous_state: Any, task_instance: Any, bogus: Any
    ) -> None:
        del previous_state, task_instance, bogus

    @hookimpl
    def on_dag_run_success(self, dag_run: Any, msg: Any) -> None:
        del dag_run, msg


class _CleanExecutor(BaseExecutor):
    """Conformant custom executor: every check should pass."""

    def sync(self) -> None:
        pass

    def _process_workloads(self, workload_items: Any) -> None:
        del workload_items

    def end(self) -> None:
        pass


class _BrokenExecutor(BaseExecutor):
    """Executor exercising every executor check at once."""

    is_single_threaded = True
    # Wrong on every certified contract, not just one: on 3.1 `sentry_integration` is
    # simply the wrong name (3.1 wants `supports_sentry`); on 3.2+ it is the right name
    # holding the wrong type (an int, not `str`).
    sentry_integration = 123


class _CleanXCom(BaseXCom):
    """Conformant custom XCom backend: every check should pass."""

    @staticmethod
    def serialize_value(
        value: Any,
        *,
        key: str | None = None,
        task_id: str | None = None,
        dag_id: str | None = None,
        run_id: str | None = None,
        map_index: int | None = None,
    ) -> str:
        del value, key, task_id, dag_id, run_id, map_index
        return "clean"

    @staticmethod
    def deserialize_value(result: Any) -> Any:
        del result
        return None


def _broken_xcom_serialize_value(self: Any, value: Any) -> str:
    """Forgot `@staticmethod`; wrong shape entirely -- for `_BrokenXCom` below."""
    del self, value
    return "broken"


def _broken_xcom_orm_deserialize_value(self: Any) -> Any:
    """Removed in Airflow 3; nothing calls this -- for `_BrokenXCom` below."""
    del self


# Built via `type()` rather than a `class` statement: a real `class _BrokenXCom(BaseXCom)`
# with this exact shape is a genuine Liskov substitution violation -- an instance method
# where the base has a `@staticmethod`, missing five required-by-call-site keyword
# parameters -- which `ty` correctly refuses to let a real subclass declare. `type()`
# builds the identical runtime object `_check_xcom_backend_signature` inspects, without
# going through `ty`'s override check.
_BrokenXCom = type(
    "_BrokenXCom",
    (BaseXCom,),
    {
        "__doc__": "XCom backend exercising every XCom check at once.",
        "serialize_value": _broken_xcom_serialize_value,
        "orm_deserialize_value": _broken_xcom_orm_deserialize_value,
    },
)


class _CleanWeightStrategy(PriorityWeightStrategy):
    """Conformant custom weight strategy: every check should pass."""

    def get_weight(self, ti: Any) -> int:
        del ti
        return 1

    def __hash__(self) -> int:
        return hash(self.__class__.__name__)


class _AbstractWeightStrategy(PriorityWeightStrategy):
    """Weight strategy that never implements the required `get_weight`."""


class _HashOfNoneWeightStrategy(PriorityWeightStrategy):
    """Weight strategy that implements `get_weight` but never overrides `__hash__`."""

    def get_weight(self, ti: Any) -> int:
        del ti
        return 1


class _CleanNotifier(BaseNotifier):
    """Conformant custom notifier: every check should pass."""

    template_fields: Sequence[str] = ("message",)

    def __init__(self, message: str = "hello") -> None:
        super().__init__()
        self.message = message

    def notify(self, context: Any) -> None:
        del context


class _BrokenNotifier(BaseNotifier):
    """Notifier exercising every notifier check at once."""

    template_fields: Sequence[str] = ("never_set",)


class _CleanSecretsBackend(BaseSecretsBackend):
    """Conformant custom secrets backend: every check should pass."""

    def get_variable(self, key: str, team_name: str | None = None) -> str | None:
        del key, team_name
        return None


class _BrokenSecretsBackend(BaseSecretsBackend):
    """Secrets backend annotated to never return `None` on a miss."""

    def get_variable(self, key: str, team_name: str | None = None) -> str:
        del team_name
        raise KeyError(key)


class _CleanPolicy:
    """Conformant custom policy: every check should pass on every certified release.

    Declares only `task_instance` -- `task_instance_mutation_hook` gained `dag_run`
    only in Airflow 3.3, so a hookimpl declaring it would fail registration on 3.1/3.2.
    """

    @policy_hookimpl
    def task_instance_mutation_hook(self, task_instance: Any) -> None:
        del task_instance


class _BrokenPolicy:
    """Policy exercising every policy check at once."""

    @policy_hookimpl
    def totally_made_up_hook(self, x: Any) -> None:
        del x

    @policy_hookimpl
    def task_instance_mutation_hook(self, task_instance: Any, bogus: Any) -> None:
        del task_instance, bogus


class _CleanPlugin(AirflowPlugin):
    """Conformant custom plugin: every check should pass."""

    name = "clean_plugin"


class _BrokenPlugin(AirflowPlugin):
    """Plugin exercising the plugin check: never sets `name`."""


def test_check_component_reports_no_problems_for_a_clean_timetable() -> None:
    """Accept a fully conformant module-level timetable."""

    report = check_component(_CleanTimetable)

    assert report.ok is True
    assert report.problems == ()
    assert report.summary() == "`_CleanTimetable`: no problems found."


def test_check_component_flags_local_qualname_and_missing_methods() -> None:
    """Flag a locally defined timetable missing required overrides and its pair."""

    local_timetable = _make_local_timetable()

    report = check_component(local_timetable)

    codes = {problem.code for problem in report.problems}
    assert codes == {
        "timetable-local-qualname",
        "timetable-missing-protocol-method",
        "timetable-serialize-pair-incomplete",
    }
    assert report.ok is False


def test_check_component_flags_non_json_serialize_only_on_an_instance() -> None:
    """Call `serialize()` only when given an instance, never a bare class."""

    class_report = check_component(_NotJsonTimetable)
    instance_report = check_component(_NotJsonTimetable())

    assert "timetable-serialize-not-json" not in {p.code for p in class_report.problems}
    assert [p.code for p in instance_report.problems] == ["timetable-serialize-not-json"]


def test_check_component_reports_no_problems_for_a_clean_listener() -> None:
    """Accept a listener whose one hookimpl matches a real hookspec exactly."""

    report = check_component(_CleanListener)

    assert report.ok is True


def test_check_component_flags_every_listener_problem() -> None:
    """Flag an unmatched hookspec, an unknown argument, and (on 3.2+) a core-only hook.

    `on_dag_run_success` has no hookspec in the SDK listener manager only where that
    manager exists at all (3.2+); on 3.1.x, which has one manager registering every
    hookspec, it is not a real problem, so `listener-core-manager-only` is conditioned
    on `AirflowCapabilities.sdk_listener_manager_available` here.
    """

    from pytest_airflow_in_a_box._compat import resolve_capabilities

    report = check_component(_BrokenListener)

    codes = {problem.code for problem in report.problems}
    expected = {"listener-no-matching-hookspec", "listener-unknown-argument"}
    if resolve_capabilities().sdk_listener_manager_available:
        expected.add("listener-core-manager-only")
    assert codes == expected


def test_check_component_reports_no_problems_for_a_clean_executor() -> None:
    """Accept an executor overriding every required method with no stale attributes."""

    report = check_component(_CleanExecutor)

    assert report.ok is True


def test_check_component_flags_every_executor_problem() -> None:
    """Flag missing overrides, a stale attribute, and a wrong-contract sentry flag."""

    report = check_component(_BrokenExecutor)

    codes = {problem.code for problem in report.problems}
    assert codes == {
        "executor-missing-override",
        "executor-stale-attribute",
        "executor-flag-wrong-type",
    }
    missing = {
        problem.message
        for problem in report.problems
        if problem.code == "executor-missing-override"
    }
    assert missing == {
        "`_BrokenExecutor` does not override `sync`.",
        "`_BrokenExecutor` does not override `_process_workloads`.",
        "`_BrokenExecutor` does not override `end`.",
    }
    assert report.ok is False


def test_check_component_flags_an_executor_missing_only_end() -> None:
    """Catch an executor that overrides `sync`/`_process_workloads` but not `end`.

    The exact class shape reported in #231: a "clean-looking" executor that overrides
    every method `BaseExecutor` used to require, which used to pass `check_component`
    clean and only blew up at teardown.
    """

    class _MissingEnd(BaseExecutor):
        def sync(self) -> None:
            pass

        def _process_workloads(self, workload_items: Any) -> None:
            del workload_items

    report = check_component(_MissingEnd, kind=ComponentKind.EXECUTOR)

    assert [problem.code for problem in report.problems] == ["executor-missing-override"]
    assert report.problems[0].message == "`_MissingEnd` does not override `end`."


def test_check_component_reports_no_problems_for_a_clean_xcom_backend() -> None:
    """Accept an XCom backend whose overrides accept the real base's call shape."""

    report = check_component(_CleanXCom)

    assert report.ok is True


def test_check_component_flags_every_xcom_problem() -> None:
    """Flag a broken `serialize_value` shape and the removed `orm_deserialize_value`."""

    report = check_component(_BrokenXCom)

    codes = {problem.code for problem in report.problems}
    assert codes == {"xcom-backend-signature", "xcom-orm-deserialize-removed"}
    assert report.ok is False


def test_check_component_flags_an_xcom_backend_that_is_not_a_real_subclass() -> None:
    """Flag a forced-kind component that does not subclass `BaseXCom` at all."""

    class _NotAnXComBackend:
        """Plain class that does not subclass `BaseXCom`."""

    report = check_component(_NotAnXComBackend, kind=ComponentKind.XCOM)

    assert {problem.code for problem in report.problems} == {"xcom-backend-signature"}


def test_check_component_reports_no_problems_for_a_clean_weight_strategy() -> None:
    """Accept a weight strategy implementing `get_weight` and `__hash__`."""

    report = check_component(_CleanWeightStrategy)

    assert report.ok is True


def test_check_component_flags_an_abstract_weight_strategy() -> None:
    """Flag a weight strategy that never implements the abstract `get_weight`."""

    report = check_component(_AbstractWeightStrategy)

    codes = {problem.code for problem in report.problems}
    assert "weight-strategy-abstract" in codes
    assert "weight-strategy-hash-of-none" in codes
    assert report.ok is False


def test_check_component_flags_a_weight_strategy_hash_of_none() -> None:
    """Flag a weight strategy that implements `get_weight` but not `__hash__`."""

    report = check_component(_HashOfNoneWeightStrategy)

    assert {problem.code for problem in report.problems} == {"weight-strategy-hash-of-none"}


def test_check_component_reports_no_problems_for_a_clean_notifier() -> None:
    """Accept a notifier overriding `notify` whose `template_fields` all resolve."""

    assert check_component(_CleanNotifier).ok is True
    assert check_component(_CleanNotifier()).ok is True


def test_check_component_flags_notifier_missing_notify_on_class_or_instance() -> None:
    """Flag a notifier that never overrides `notify`, class or instance alike."""

    class_report = check_component(_BrokenNotifier)
    instance_report = check_component(_BrokenNotifier())

    assert "notifier-missing-notify" in {p.code for p in class_report.problems}
    assert "notifier-missing-notify" in {p.code for p in instance_report.problems}


def test_check_component_flags_unresolvable_template_fields_only_on_an_instance() -> None:
    """Call `template_fields` resolution only when given an instance, never a class."""

    class_report = check_component(_BrokenNotifier)
    instance_report = check_component(_BrokenNotifier())

    assert "notifier-template-fields-unresolvable" not in {p.code for p in class_report.problems}
    assert "notifier-template-fields-unresolvable" in {p.code for p in instance_report.problems}


def test_check_component_reports_no_problems_for_a_clean_secrets_backend() -> None:
    """Accept a secrets backend whose overridden getter is annotated `str | None`."""

    report = check_component(_CleanSecretsBackend)

    assert report.ok is True


def test_check_component_flags_a_secrets_backend_getter_that_cannot_return_none() -> None:
    """Flag a secrets backend getter annotated to never return `None`."""

    report = check_component(_BrokenSecretsBackend)

    assert {problem.code for problem in report.problems} == {"secrets-backend-raises-on-miss"}


def test_check_component_reports_no_problems_for_a_clean_policy() -> None:
    """Accept a policy hookimpl matching a real hookspec with no unknown arguments."""

    report = check_component(_CleanPolicy)

    assert report.ok is True


def test_check_component_flags_every_policy_problem() -> None:
    """Flag an unmatched hookspec name and an unknown hookimpl argument."""

    report = check_component(_BrokenPolicy)

    codes = {problem.code for problem in report.problems}
    assert codes == {"policy-unknown-hookspec", "policy-argument-name-mismatch"}
    assert report.ok is False


def test_check_component_reports_no_problems_for_a_clean_plugin() -> None:
    """Accept a plugin that sets `name`."""

    report = check_component(_CleanPlugin)

    assert report.ok is True


def test_check_component_flags_a_plugin_missing_name() -> None:
    """Flag a plugin that never sets `name`."""

    report = check_component(_BrokenPlugin)

    assert {problem.code for problem in report.problems} == {"plugin-name-missing"}


def test_check_component_reports_no_problems_for_a_clean_provider() -> None:
    """Accept the real, correctly configured `apache-airflow-providers-sqlite` provider.

    Uses a real shipped provider rather than a local fixture: a fabricated
    `get_provider_info` would need a `package-name` matching some real installed
    distribution to pass `provider-package-name-mismatch`, and that distribution would
    also need a real `apache_airflow_provider` entry point to pass
    `provider-no-entry-point` -- properties only a real, already-published provider
    package actually has.
    """

    report = check_component(_real_provider_info)

    assert report.ok is True


def test_check_component_names_a_provider_by_its_own_function_name() -> None:
    """Name a provider report after the callable's own name, not `\"function\"`.

    Regression test: `_as_type(component)` falls back to `type(component).__name__` for
    a bare callable that is neither a class nor a built instance, which for every
    real-world provider is the unhelpful, indistinguishable generic string `\"function\"`.
    A class or instance is unaffected -- see `test_component_report_summary_lists_every_
    problem` and the class-based tests above, none of which name-check `component_name`
    because it was already correct for them.
    """

    report = check_component(_real_provider_info)

    assert report.component_name == "get_provider_info"


def test_check_component_flags_a_provider_info_schema_violation() -> None:
    """Flag a `get_provider_info` callable missing schema-required fields.

    Defined and called from this test module -- a sibling of `src/`, not a file the
    editable install's own `.pth` root exposes -- so `_provider_owning_distribution`
    cannot attribute it to any installed distribution, and the package-name-mismatch
    and no-entry-point checks silently skip rather than firing on an unrelated,
    incidental "distribution".
    """

    def get_provider_info() -> dict[str, Any]:
        return {"package-name": "apache-airflow-providers-sqlite"}

    report = check_component(get_provider_info, kind=ComponentKind.PROVIDER)

    assert {problem.code for problem in report.problems} == {"provider-info-schema"}


def test_check_component_classifies_by_kind_automatically() -> None:
    """Auto-detect a component's kind when none is given explicitly."""

    assert check_component(_CleanTimetable).ok is True
    assert check_component(_CleanListener).ok is True
    assert check_component(_CleanExecutor).ok is True
    assert check_component(_CleanXCom).ok is True
    assert check_component(_CleanWeightStrategy).ok is True
    assert check_component(_CleanNotifier).ok is True
    assert check_component(_CleanSecretsBackend).ok is True
    assert check_component(_CleanPolicy).ok is True
    assert check_component(_CleanPlugin).ok is True
    assert check_component(_real_provider_info).ok is True


def test_check_component_forced_kind_overrides_classification() -> None:
    """Run a kind's checks even when the component would not classify as it."""

    class _NotAnExecutor:
        """Plain class that does not subclass `BaseExecutor` at all."""

    report = check_component(_NotAnExecutor(), kind=ComponentKind.EXECUTOR)

    assert report.ok is False
    assert {problem.code for problem in report.problems} == {"executor-missing-override"}


def test_check_component_returns_a_clean_report_for_an_unrecognized_component() -> None:
    """Return an empty, clean report rather than raising for an unmatched component."""

    report = check_component(object())

    assert report.ok is True
    assert report.problems == ()


def test_raise_for_problems_raises_component_contract_error() -> None:
    """Raise `ComponentContractError` carrying the rendered summary."""

    report = check_component(_BrokenExecutor)

    with pytest.raises(ComponentContractError) as caught:
        report.raise_for_problems()

    assert str(caught.value) == report.summary()
    assert "executor-missing-override" in str(caught.value)


def test_raise_for_problems_is_a_no_op_when_ok() -> None:
    """Do nothing when the report found no problems."""

    check_component(_CleanExecutor).raise_for_problems()


def test_component_report_summary_lists_every_problem() -> None:
    """Render one line per problem, each carrying its code and message."""

    report = ComponentReport(
        component_name="Fake",
        problems=(ComponentProblem(code="fake-code", message="fake message", hint="fake hint"),),
    )

    summary = report.summary()

    assert "`Fake`: 1 problem(s) found:" in summary
    assert "[fake-code] fake message" in summary


def test_component_report_summary_flags_the_probed_tier() -> None:
    """Append the uncertified-release line on the `PROBED` tier, clean report included."""

    clean = ComponentReport(
        component_name="Fake", problems=(), certification=CertificationTier.PROBED
    )
    dirty = ComponentReport(
        component_name="Fake",
        problems=(ComponentProblem(code="fake-code", message="fake message", hint="fake hint"),),
        certification=CertificationTier.PROBED,
    )

    assert "uncertified Apache Airflow release" in clean.summary()
    assert "uncertified Apache Airflow release" in dirty.summary()
    assert "[fake-code] fake message" in dirty.summary()


def test_check_component_stamps_the_certification_tier() -> None:
    """Stamp every report with the metadata-classified tier -- `CERTIFIED` on this repo."""

    report = check_component(_CleanExecutor)

    assert report.certification is CertificationTier.CERTIFIED


def test_component_kind_values_are_stable_strings() -> None:
    """Keep `ComponentKind` values as the plain strings the private registry keys by."""

    assert ComponentKind.TIMETABLE.value == "timetable"
    assert ComponentKind.LISTENER.value == "listener"
    assert ComponentKind.EXECUTOR.value == "executor"
    assert ComponentKind.XCOM.value == "xcom"
    assert ComponentKind.WEIGHT_STRATEGY.value == "weight-strategy"
    assert ComponentKind.NOTIFIER.value == "notifier"
    assert ComponentKind.SECRETS_BACKEND.value == "secrets-backend"
    assert ComponentKind.POLICY.value == "policy"
    assert ComponentKind.PLUGIN.value == "plugin"
    assert ComponentKind.PROVIDER.value == "provider"
