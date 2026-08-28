# Compatibility and certification

Use this page to answer two questions: whether an Airflow version is certified, and what the
plugin does when Airflow changes an internal interface.

The short answer is that every private Airflow dependency belongs in `_compat/`. Before a
fixture uses one, the compatibility layer resolves the installed Airflow family, probes the
interfaces it needs, and rejects an incompatible installation with the failing symbol in the
error message.

## Supported and certified

The plugin supports all major Apache Airflow versions. Certification is more specific: it
records the exact Airflow releases and Python combinations exercised by this repository.

- The package supports CPython 3.10–3.14 and pytest 8 or newer.
- Linux and macOS are supported. On Windows, use WSL2 or the devcontainer.
- Airflow 3 releases at or above 3.1.0 can resolve through the compatibility layer. Releases
  with a checked-in contract row are **certified**; other 3.x releases are **probed**.
- Airflow 2 is a closed certification tier: only 2.7.3, 2.8.4, 2.9.3, 2.10.5, and 2.11.2
  resolve. The first two support Python through 3.11; the others support Python through 3.12.

Run [`pytest --airflow-doctor`](../reference/diagnostics.md) in the environment you intend to
use. It reports the installed versions, certification tier, and resolved capabilities.

### What CI actually exercises

The compatibility workflow uses representative pairs rather than every Cartesian combination:

- Airflow 3.1–3.3 across CPython 3.10–3.14, including the pytest 8.0 floor;
- Linux, macOS, ARM Linux, and Alpine/musl;
- parallel runs under `-n auto --dist loadgroup`, plus one serial reference leg; and
- SQLite throughout the matrix, with a separate real-Docker Postgres job.

Airflow 3 legs run the complete suite. Airflow 2 legs run the consumer contract in
`tests/enduser/`, including one xdist leg, on Linux and SQLite. See
[GitHub Actions and reports](../guide/ci/github-action.md) for reproducing these environments.

### Airflow 2.x is a migration bridge, not a second home

The certified Airflow 2 tier exists so one consumer suite can remain green during a 2-to-3
migration. The shared surface includes `dag_maker`, `run_ti`, `dag_bag`, `run_dag`, cleanup,
seeding, configuration, and smoke checks.

Task SDK runners, structlog capture, the REST API, component sandbox, and executor-driven runs
are Airflow 3 only. They fail on Airflow 2 with an alternative where one exists. The
[Fixtures](../reference/fixtures.md) table is the per-feature source of truth.

## The surface it stands on

Airflow does not expose a public testing API for the work this plugin performs. Creating
scheduler metadata, running a `RuntimeTaskInstance`, clearing related ORM tables, and evaluating
asset schedules all require private interfaces that can move between releases.

That movement is expected. Across recent releases, `DagBag`, `SerializedDAG`, asset evaluation,
plugin-manager caches, the Task SDK runner, and executor and listener contracts have all changed
location or shape. The compatibility layer turns each variation into one capability; fixtures
consume that capability instead of branching on an Airflow version.

## One seam, and it is enforced

Any runtime use of Airflow internals must live under `src/pytest_airflow_in_a_box/_compat/`.
Outside that package, Airflow imports in shipped source are type-checking-only annotations.

`tests/compat/test_seam.py` enforces the boundary with an AST scan. It catches ordinary imports,
imports inside functions or exception handlers, and literal dynamic imports. The accepted-leak
set is empty. As a result, a new Airflow release should change the compatibility package and its
contract tests—not scatter version branches through fixtures, collection, or database code.

## How a probe works

A probe observes the installed code; it does not compare version strings. The capability layer
uses four patterns:

- resolve a required module attribute and wrap failure in `AirflowCompatibilityError`;
- inspect whether a callable accepts a parameter;
- inspect whether a Pydantic model exposes a field; or
- try the known locations or shapes and return a member of a closed enum.

The results form one immutable `AirflowCapabilities` value, cached once per process. Resolution
is lazy, so loading pytest without using an Airflow feature does not import Airflow.

Closed enums matter: if an interface has a third, unknown shape, resolution fails instead of
guessing. The error names the symbol or contract that moved. `--airflow-doctor` prints the same
resolved fields in a diagnostic report.

## Certified, then verified

A certified release has a hand-maintained row in `_CERTIFIED_CAPABILITIES`. Runtime probes are
compared with that row, including the canonical `SerializedDAG` location. A mismatch aborts the
session.

Certification combines two kinds of checks:

- Probes verify interface locations, callable parameters, model fields, and other structural
  differences that can be observed directly.
- `_REQUIRED_SYMBOLS_BY_FAMILY` imports every additional private symbol the plugin depends on.
  A moved or removed symbol fails even when it is not represented by a capability field.

Family-derived fields are consistency checks, not independent observations. The distinction is
intentional: a certified row documents the expected contract, while probes and required-symbol
checks enforce the portions that can be verified mechanically.

## When your Airflow is newer than the certified set

An uncertified Airflow 3 release at or above the supported floor resolves on the `PROBED` tier.
All capability probes and required-symbol checks still run, but no certified row exists for the
final comparison.

Pytest emits one `UncertifiedAirflowWarning`, `--airflow-doctor` reports `DEGRADED`, and the
component sandbox uses generic snapshot and restore. Tests may continue, but the plugin has not
yet certified that release's private interfaces. Pin a certified release when you require that
assurance.

The weekly Airflow canary installs the newest matching upstream release, runs the compatibility
suite, and deliberately fails its certification probe for an uncertified release. That failure
files an issue with the resolved environment and reports so certification work begins before a
routine upgrade reaches users.

## `tests/enduser/` is the consumer contract

Compatibility-unit tests prove individual shims. `tests/enduser/` proves that public fixtures
still behave as a consumer expects. Its dual-family tests cover whole-Dag runs, operators,
sensors, TaskFlow, mapping, triggers, callbacks, hooks, providers, configuration, corpus checks,
and migration behavior. Airflow 3-only modules add assets, REST API, structlog, and executor
coverage.

The complete directory runs on every Airflow 3 compatibility leg. Airflow 2 legs run only this
directory; four modules that import 3-only surfaces are excluded, while individual 3-only tests
are collected and skipped through `requires_airflow3`.

A probe can prove that a symbol exists. An end-user test proves that the public workflow built
on it still works.

## Adapted upstream code

Some compatibility implementations follow upstream Airflow behavior closely enough to require
an audit trail. [`PROVENANCE.md`](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/PROVENANCE.md)
records the upstream files and exact commits used by the adapted task-instance and asset-schedule
code, along with the sources consulted for certified contracts.

For the surrounding runtime boundary, see [Test Environments](test-environments.md): bootstrap
owns configuration before Airflow imports, and `_compat/` supplies the private mechanics after
that boundary.
