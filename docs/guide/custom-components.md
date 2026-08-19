# Custom components

`check_component` runs pure, static conformance checks against a custom `Timetable`,
listener, or `BaseExecutor` subclass -- no metadata database, no cache, no Airflow
bootstrap. Safe in a plain unit test or a pre-commit hook:

```python
from pytest_airflow_in_a_box.components import check_component


def test_my_timetable_conforms():
    check_component(MyTimetable).raise_for_problems()
```

`BaseExecutor` is not an ABC and `Timetable` is a `typing.Protocol`, and a listener carries
no base class at all -- nothing about any of the three is enforced when the class is
defined. A shape bug ships silently and only fails once a scheduler actually exercises it.

## The report

`check_component` accepts a bare class or an already-built instance interchangeably and
never constructs one itself, so it is safe to call on a component whose constructor is not
side-effect-free or takes required arguments. It returns a `ComponentReport`:

```python
report = check_component(MyExecutor)
report.ok  # bool: no problems found
report.problems  # tuple[ComponentProblem, ...] -- (code, message, hint) each
report.summary()  # human-readable, one line per problem
report.raise_for_problems()  # raises ComponentContractError when not ok
```

Checks are additive: each reports the problems it finds and never raises on the component
itself, so a wrong or overly strict check cannot fail an otherwise-passing suite -- only
`raise_for_problems()` (or asserting `.ok` yourself) turns a report into a test failure.

## Kind detection

Pass `kind=ComponentKind.TIMETABLE` / `.LISTENER` / `.EXECUTOR` to force a check set, or
omit it and let `check_component` classify the component itself:

- **Timetable** -- nominal `Timetable` inheritance. A purely duck-typed timetable that
  never inherits `Timetable` needs the explicit `kind=`; structural `isinstance` checks
  are not used because `Timetable` declares data attributes as well as methods, which
  makes `issubclass` against it always raise `TypeError`
- **Listener** -- at least one `@hookimpl`-decorated method
- **Executor** -- `BaseExecutor` subclassing

A component matching none of the three (with no `kind` given) returns a clean, empty
report rather than raising.

## Timetable checks

- `timetable-local-qualname` -- a timetable defined inside a function or method carries
  `<locals>` in `__qualname__`. Airflow's `find_registered_custom_timetable` matches a
  custom timetable by qualified name, so a `<locals>` class can never match; every DagRun
  using it raises `TimetableNotRegistered` permanently, not just in a test
- `timetable-missing-protocol-method` -- `infer_manual_data_interval` or `next_dagrun_info`
  is not overridden. Both default to `raise NotImplementedError()`; every other Protocol
  member (the data attributes, `serialize`/`deserialize`, `validate`, the partition hooks)
  has a usable default
- `timetable-serialize-pair-incomplete` -- exactly one of `serialize`/`deserialize` is
  overridden. The default `deserialize` reconstructs the class with `cls()`, silently
  dropping whatever state a custom `serialize` emits
- `timetable-serialize-not-json` -- an instance's `serialize()` does not return a
  JSON-serializable mapping. Only checked against an already-built instance; a bare class
  skips this one check, since calling `serialize()` needs a real instance and
  `check_component` never constructs one

## Listener checks

- `listener-no-matching-hookspec` -- a hookimpl method's name matches no hookspec
  registered by either listener manager. pluggy silently ignores it; the method never
  fires, with no warning -- the single most common real-world listener bug
- `listener-unknown-argument` -- a hookimpl method declares an argument name its matching
  hookspec does not have. pluggy hard-errors on this at registration time
- `listener-core-manager-only` / `listener-sdk-manager-only` -- a hookimpl matches a
  hookspec registered by only one manager. `airflow.listeners.listener` registers
  lifecycle, taskinstance, dagrun, asset, and import-error hookspecs;
  `airflow.sdk.listener` registers only lifecycle and taskinstance. Register a listener
  with only one manager and half its hooks are silently unreachable

Listener checks require Airflow 3.x and report no problems on 2.x, whose listener
architecture predates the Task SDK's separate manager entirely.

## Executor checks

- `executor-missing-override` -- `sync` or `_process_workloads` is not overridden. Neither
  is abstract: `sync`'s default silently does nothing, and `_process_workloads`'s default
  raises `NotImplementedError`
- `executor-stale-attribute` -- the executor sets `is_single_threaded`, `supports_pickling`,
  `change_sensitivity`, or `execute_async`. All four are still documented in older material
  but do not exist on `BaseExecutor` in Airflow 3.1-3.3, so Airflow silently ignores them
- `executor-flag-wrong-type` -- the sentry integration flag uses the wrong name or type for
  the installed release. 3.1 has `supports_sentry: bool`; 3.2 renamed it to
  `sentry_integration: str`, unchanged through 3.3

Executor checks also require Airflow 3.x and report no problems on 2.x.
