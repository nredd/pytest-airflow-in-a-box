# Checking components

Airflow validates many extension points only when the scheduler, worker, or Dag processor
discovers them. Run `check_component` first to catch contract errors against the installed
Airflow release without starting a database or mutating Airflow's live registries:

```python
from pytest_airflow_in_a_box.components import check_component


def test_my_timetable_conforms():
    check_component(MyTimetable).raise_for_problems()
```

Use this check for timetables, listeners, executors, XCom backends, priority-weight strategies,
notifiers, secrets backends, policies, plugins, and providers. Test operators, hooks, sensors,
and `@task` decorators through the [fidelity ladder](ladder.md) instead.

## Choose the component kind

`check_component` normally detects the kind from inheritance, Airflow's pluggy markers, plugin
shape, or a callable named `get_provider_info`. Pass a class or an existing instance; the
checker never constructs a class, so required and side-effectful constructors are safe.

Force the kind when the component is intentionally duck-typed or still incomplete:

```python
from pytest_airflow_in_a_box.components import ComponentKind, check_component

report = check_component(MyListener, kind=ComponentKind.LISTENER)
```

This matters because an unrecognized component returns a clean report: no applicable checks is
not the same as a validated contract. `ComponentKind` contains `TIMETABLE`, `LISTENER`,
`EXECUTOR`, `XCOM`, `WEIGHT_STRATEGY`, `NOTIFIER`, `SECRETS_BACKEND`, `POLICY`, `PLUGIN`, and
`PROVIDER`.

## The report

Checks accumulate every finding instead of stopping at the first one:

```python
report = check_component(MyExecutor)
report.ok
report.problems  # tuple[ComponentProblem, ...]
report.summary()
report.certification  # CertificationTier | None
report.raise_for_problems()
```

Each `ComponentProblem` has a machine-readable `code`, a specific `message`, and a corrective
`hint`. `raise_for_problems()` raises `ComponentContractError` with the complete summary;
otherwise a report never fails the test by itself.

On an uncertified Airflow release, available checks still run against live capabilities and
`report.certification` is `PROBED`. Timetable checks work on both Airflow families. The other
kinds are Airflow 3 checks and produce no findings on Airflow 2.

## What gets checked

Every `ComponentProblem.code` value is part of the diagnostic contract:

| Kind | Problem codes |
| --- | --- |
| Timetable | `timetable-local-qualname`, `timetable-missing-protocol-method`, `timetable-serialize-pair-incomplete`, `timetable-serialize-not-json`, `timetable-round-trip-mismatch` |
| Listener | `listener-no-matching-hookspec`, `listener-unknown-argument`, `listener-core-manager-only`, `listener-sdk-manager-only` |
| Executor | `executor-missing-override`, `executor-stale-attribute`, `executor-flag-wrong-type` |
| XCom backend | `xcom-orm-deserialize-removed`, `xcom-backend-signature` |
| Priority-weight strategy | `weight-strategy-abstract`, `weight-strategy-hash-of-none` |
| Notifier | `notifier-missing-notify`, `notifier-template-fields-unresolvable` |
| Secrets backend | `secrets-backend-raises-on-miss` |
| Policy | `policy-unknown-hookspec`, `policy-argument-name-mismatch` |
| Plugin | `plugin-name-missing` |
| Provider | `provider-info-schema`, `provider-package-name-mismatch`, `provider-no-entry-point` |

### Timetables

Timetable checks catch local classes that Airflow cannot resolve, required scheduling methods
left on their raising defaults, incomplete `serialize`/`deserialize` pairs, and non-JSON state
returned by an instance. The [Timetables](custom-timetables.md) page covers logic tests,
serialization round trips, and automatic registration through `dag_maker`.

### Execution components

- **Executors:** required lifecycle overrides, ignored 2.x-era attributes, and the installed
  release's sentry capability name and type.
- **XCom backends:** the removed ORM deserializer and serialization methods that no longer
  accept the base class's real call shape.
- **Priority-weight strategies:** abstract implementations and classes that cannot serve as
  set or dictionary keys.

A clean report does not run a workload. Exercise an executor through
[`run_dag(..., executor=...)`](ladder.md#executor-driven-runs); the
[Cookbook](cookbook.md#a-minimal-serial-executor) contains a complete serial example.

### Observability components

- **Listeners and policies:** hook names and arguments against the installed hookspecs;
  listeners also report hooks available through only one listener manager.
- **Notifiers:** a missing synchronous `notify`; on an instance, unresolved `template_fields`.

Plain functions loaded through `airflow_local_settings.py` use the older policy mechanism and
are not policy components.

### Distribution components

- **Secrets backends:** declared lookup return types that exclude `None`. The checker never
  calls a backend or fabricates credentials, so an unannotated lookup produces no finding.
- **Plugins:** a missing plugin name.
- **Providers:** the installed provider-info schema, package-name agreement, and the
  `apache_airflow_provider` entry point.

Pass the provider's `get_provider_info` callable, not its result. The schema check invokes it
because `ProvidersManager` does too. Package and entry-point checks are skipped when the
callable cannot be attributed to an installed distribution. Use an isolated process to prove
the entry point itself resolves; see
[Registration and packaging](custom-components-wiring.md#isolated-entry-point-discovery).

## Shape is not registration

A clean report proves that the checked shape matches the installed release. It does not prove
that production can discover the component or that the component behaves correctly. Continue
with [Registration and packaging](custom-components-wiring.md), then exercise the behavior at
the appropriate rung of the [fidelity ladder](ladder.md).
