# Compatibility

Two ways you get here. Either you are deciding whether this plugin survives your next Airflow
bump, or a run just died with:

```console
ERROR: Apache Airflow compatibility validation failed for installed version '3.4.0' while
resolving required Airflow symbol `airflow.sdk.execution_time.task_runner.run`: ...
```

Both questions have the same answer: `_compat/` is the *only* place in this package that
touches Airflow, every symbol it touches is probed before use, and a symbol that moved fails
at resolve time with the module path in the message rather than three frames deep inside a
fixture.

## The surface it stands on

Airflow 3 has no supported testing API for what this plugin does. Driving a real
`RuntimeTaskInstance`, persisting a `DagRun` with a `DagVersion` and a `DagBundleModel`,
clearing the metadata DB table-group by table-group, evaluating an asset condition -- all of it
runs through modules Airflow does not document as public and does not promise across minors.

Measured on this tree:

- 17 modules, 12,547 lines under `src/pytest_airflow_in_a_box/_compat/`
- 94 distinct `airflow.*` module paths referenced. A handful are public (`airflow.sdk`,
  `airflow.providers.standard.operators.empty`); the bulk are not -- `airflow.models.*` ORM
  tables, `airflow.sdk.execution_time.*`, `airflow.sdk.api.datamodels._generated`,
  `airflow._shared.*`, `airflow.serialization.*`, `airflow.executors.*`, `airflow.policies`
- Certified across 18 Airflow releases -- the certified matrix lives in
  [Certification](certification.md)

The version drift is not hypothetical. `DagBag` moved from `airflow.models.dagbag` to
`airflow.dag_processing.dagbag`. `SerializedDAG` moved out of
`airflow.serialization.serialized_objects` into `airflow.serialization.definitions.dag` in 3.2,
and asset condition evaluation moved with it. 3.2 deleted `airflow.plugins_manager`'s
module-global cache outright and replaced it with independent `functools.cache` functions, then
gave the Task SDK a second, structurally identical copy that 3.1 does not have at all. 3.3 added
a `dag_run` parameter to the `task_instance_mutation_hook` hookspec. Each of those is one field
on one dataclass here, and nothing above `_compat/` knows any of it happened.

## One seam, and it is enforced

The rule is absolute: **any use of Airflow internals goes behind `_compat/`.** Outside that
package, the only `airflow` imports anywhere in `src/` are `TYPE_CHECKING`-only annotations in
`types.py`, `fixtures/dag.py`, and `fixtures/upstream.py` -- three imports that never execute.

`tests/compat/test_seam.py` is what makes that a fact instead of an intention. It walks the
shipped source with `ast` and fails on any runtime `airflow` import outside `_compat/` --
statements in function bodies and `except` handlers included, plus dynamic
`import_module("airflow...")` / `__import__` calls with a literal target. Its known-leak
allowlist is `frozenset()`, and the assertion exact-matches it, so the list can only ever
shrink.

Consequence, and the reason this is core rather than housekeeping: a new Airflow release lands
in one package. `plugin.py`, the fixtures, `db.py`, and `smoke.py` do not get a version branch.

## How a probe works

A probe is a live observation of the installed Airflow, not a version comparison. Four
primitives in `_compat/capabilities.py`:

- `_resolve_symbol(module, name, version)` -- import and `getattr`, wrapping any failure in
  `AirflowCompatibilityError` naming the module path
- `_signature_has_parameter(...)` -- `inspect.signature` over a constructor or method, e.g.
  does `DagBag.__init__` still take `include_examples`
- `_model_has_field(...)` -- Pydantic `model_fields` membership, e.g. does
  `airflow.sdk.execution_time.comms.StartupDetails` carry `sentry_integration`
- `_probe_*` -- try the newer location, fall back to the older, return the enum member naming
  which one answered

The fallback shape, verbatim in spirit from `_probe_dag_bag`:

```python
try:
    module = import_module("airflow.dag_processing.dagbag")
    dag_bag = module.DagBag
except (ImportError, AttributeError):
    return DagBagLocation.MODELS, _resolve_symbol("airflow.models.dagbag", "DagBag", version)
return DagBagLocation.DAG_PROCESSING, dag_bag
```

Every location a probe can return is a member of a closed `Enum` (`DagBagLocation`,
`TaskInstanceRunner`, `ParamsLocation`, `SecretsResolution`, `ExecutorContract`, ...). An
unrecognized third answer is not a new enum value, it is a failure.

The probes fold into one frozen `AirflowCapabilities` dataclass -- 25 fields -- resolved once
per process and cached by `resolve_capabilities()`. Nothing imports Airflow until that call,
which is why the plugin stays inert for non-Airflow test runs.

## Certified, then verified

Probing alone would let a maintainer's assumption pass silently. So every certified release also
has a hand-written row in `_CERTIFIED_CAPABILITIES`, and `_verify_contract` compares *every*
field of the observed contract against it, iterating `dataclasses.fields` so a newly added field
is compared without touching the comparison function. A mismatch fails the session.

The module is honest about what that proves, and so is this page. Only the ten fields in
`_PROBED_FIELD_LABELS` (plus the `SerializedDAG` location) are real runtime observations that
can contradict the certified row -- DagBag location, task-instance runner, the executor
attribute contract, the SDK listener manager, and so on. The family-derived fields
(`has_task_sdk`, `uses_structlog`, `dagrun_interface`, `api_surface`, `timezone_location`,
`secrets_resolution`, ...) are computed from the same family on both sides, so their comparison
is a self-consistency guard, not a probe.

The real floor under those is `_REQUIRED_SYMBOLS_BY_FAMILY`: a flat table of module/symbol
pairs that must import, 31 of them on Airflow 3.x and 17 on 2.x, resolved on every session
regardless of tier. Upstream moving `airflow.sdk.execution_time.context._get_variable` -- which
apache/airflow#61630 announces it intends to -- becomes a loud failure here instead of a shim
that silently stops shimming.

## When your Airflow is newer than the certified set

A 3.x release at or above the certified floor with no certified row does not brick the plugin.
It resolves on the `PROBED` tier: every probe and every required-symbol resolution still runs
and still hard-fails on a missing symbol, but `_verify_contract` is skipped because there is
nothing to verify against.

What you see, once per session:

```console
UncertifiedAirflowWarning: Apache Airflow '3.4.0' has no certified contract row in this version
of `pytest-airflow-in-a-box` (last certified release: '3.3.1'): capabilities were resolved by
live probing and the component sandbox degrades to generic snapshot/restore. ...
```

`pytest --airflow-doctor` prints every capability field plus a `DEGRADED:` bullet explaining the
tier. See [diagnosing a run](../reference/diagnostics.md).

The policy itself -- degrade above the certified set, hard-fail below the floor, weekly canary
-- is stated in [Certification](certification.md).

## `tests/enduser/` is the consumer contract

Unit tests of `_compat/` prove the shims behave. They do not prove a *user's* test still passes
after a version bump. `tests/enduser/` does that: 32 modules marked `compat`, written against
the public fixtures only, exercising operators, sensors, TaskFlow, triggers, assets,
callbacks, hooks, the REST API, `dag_corpus`, structlog capture, and executor runs.

It runs on the whole compat matrix -- 3.1.0 through 3.3.1 across CPython 3.10-3.14, plus macOS,
arm, musl, and pytest-floor legs. The Airflow 2.x legs run `tests/enduser` and *only*
`tests/enduser`: the inner unit suite imports 3.x-only modules at module scope and cannot
collect on 2.x, so the consumer contract is the whole 2.x signal.
`tests/enduser/conftest.py` drops the four 3.x-only modules by `collect_ignore` and
authors everything else dynamically through `tests/enduser/_authoring.py`, marking its 3.x-only
tests `requires_airflow3` so they are collected and skipped rather than never seen.

Adding a probe without adding an end-user test proves a symbol exists. It does not prove your
test survives.

## Adapted upstream code

`_compat/taskrun.py::run_task_instance` is adapted from Airflow's own
`devel-common/src/tests_common/test_utils/taskinstance.py`, and `_compat/asset_schedule.py`
from the scheduler's asset-triggered DagRun creation. Both carry exact upstream commits in
`PROVENANCE.md`, along with every Airflow source file the certified contract was read against.
That file is the audit trail for what "certified" means on any given release.

For where `_compat/` sits relative to the rest of the plugin: bootstrap owns the environment
before Airflow is ever imported ([`pytest-xdist`](bootstrap-env-ownership.md)), the
parse-time secrets shim is one of the private pieces this layer holds
([parse-time secret resolution](parse-time-secrets.md)), and
[Certification](certification.md) is the user-facing statement of the certified set.
