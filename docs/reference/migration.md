# Migration artifacts

The [Airflow 2->3 Migration guide](../guide/migration.md) covers the workflow. This page
defines the recorded artifact, comparison rules, and `airflow-migration-diff` interface. See
the complete [pytest migration-option catalog](ini-options.md#migration-runs) for the flags
used inside either environment.

The source contracts are
[`artifact.py`](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/src/pytest_airflow_in_a_box/artifact.py),
[`baseline.py`](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/src/pytest_airflow_in_a_box/baseline.py),
and the
[`migration` package](https://github.com/nredd/pytest-airflow-in-a-box/tree/main/src/pytest_airflow_in_a_box/migration).

## Categories

Comparison first projects outcomes to `pass` (`passed`, `xpassed`), `fail` (`failed`,
`error`, `xfailed`), or `neutral` (ordinary `skipped`). A node gated by
`requires_airflow2` or `requires_airflow3` on either side is always `gated` before that
projection.

| Baseline | Live | Category |
| --- | --- | --- |
| pass | pass or neutral | `still-passing` |
| pass | fail | `regression` |
| fail | pass | `fixed` |
| fail | fail or neutral | `broken-on-both` |
| neutral | pass or neutral | `still-passing` |
| neutral | fail | `broken-on-both` |

Baseline-only node IDs are `missing`; live-only IDs are `new`. All seven buckets are sorted.
Node IDs match exactly, so use stable parametrization IDs. The `@group` suffix added by xdist
`loadgroup` is removed before recording; literal `@` characters inside parametrization IDs
remain intact.

## Artifact schema

`--airflow-record=PATH` writes schema version 1 at session finish. A serial process or the
xdist controller writes exactly one file; workers forward their reports to the controller.
Ordinary test failures still produce a complete artifact. Interrupted and internal-error
sessions write `complete: false`; a hard process kill writes nothing.

| Field | Type | Meaning |
| --- | --- | --- |
| `schema_version` | `int` | Currently `1`; other versions are rejected |
| `plugin_version` | `str` | Recording plugin version |
| `airflow_version` / `airflow_family` | `str` | Recording Airflow identity, or `"unknown"` |
| `python_version` / `pytest_version` | `str` | Recording runtime |
| `created_at` | `str` | UTC ISO 8601 timestamp |
| `complete` | `bool` | Whether pytest avoided interruption or an internal error |
| `outcomes` | `dict[str, OutcomeEntry]` | Entries keyed by canonical node ID |

Each `OutcomeEntry` contains `outcome`, `phase`, `gated`, and `duration`. `outcome` is
`passed`, `failed`, `error`, `skipped`, `xfailed`, or `xpassed`. `phase` is `setup` or
`teardown` only for `error`; call exceptions are `failed`. `duration` sums every phase that
ran.

Precedence is worker crash, teardown error, setup error, setup skip, then call outcome. A
worker crash is `failed` because it has no attributable pytest phase. A rerun overwrites the
same node ID, leaving its final outcome.

Loading validates every required field and entry. Missing files, malformed JSON, wrong types,
unknown outcomes, inconsistent error phases, and schema mismatches are usage errors. The
writer creates missing parent directories and emits indented, key-sorted JSON.

`--airflow-baseline-allow-incomplete` accepts an incomplete baseline or prior-live artifact;
without it, either is a usage error. The orchestrator instead has separate
`--allow-incomplete-baseline` and `--allow-incomplete-live` switches.

Schema consumers may import `ARTIFACT_SCHEMA_VERSION`, `Outcome`, `OutcomeEntry`, `Artifact`,
`load_artifact`, and `write_artifact` from `pytest_airflow_in_a_box.artifact`, plus
`CategoryBuckets` and `compute_categories` from `pytest_airflow_in_a_box.baseline`.

## Selection and known regressions

`--airflow-baseline-select=MODE` deselects live items before execution and requires
`--airflow-baseline`:

| Mode | Baseline membership |
| --- | --- |
| `passing` | `passed` or `xpassed` |
| `failing` | `failed`, `error`, or `xfailed` |
| `new` | Node ID absent from the baseline |

A neutral baseline skip belongs to none of these modes.

`--airflow-baseline-xfail=PRIOR_LIVE.json` also requires the baseline. A node that passed in
the baseline and failed in the prior-live artifact receives a non-strict xfail. An existing
test-owned xfail marker wins. When a known regression passes, the terminal summary reports its
XPASS and prompts you to refresh the prior-live recording.

The baseline and prior-live files must be complete unless the pytest incomplete-artifact
override is enabled. Under xdist, workers apply selection and xfail marks during collection,
then forward reports; only the controller writes the artifact and renders the final comparison.
Comparing two artifacts from the same Airflow family is allowed and emits a warning.

## Orchestrator options

`airflow-migration-diff` owns these options; they are not pytest flags:

| Option | Default and behavior |
| --- | --- |
| `--project-dir=PATH` | Current directory. Installs it editable with `--no-deps` for `pyproject.toml`/`setup.py`, installs `requirements.txt` when that is the only project file, or performs no project install |
| `--work-dir=PATH` | A new temporary base. Every invocation creates a fresh `run-<id>` child inside the base |
| `--keep-work-dir` | Off; retains the run child when enabled |
| `--airflow2-version=VERSION` | Newest certified 2.x release |
| `--airflow3-version=VERSION` | Newest certified 3.x release |
| `--python-airflow2=X.Y` | Current interpreter, clamped to the requested 2.x release's Python bounds |
| `--python-airflow3=X.Y` | Current interpreter when supported; otherwise Python 3.12 |
| `--plugin-spec=SPEC` | Exact installed plugin version; pass a released version, VCS reference, or local path for another build |
| `--uv-path=PATH` | `uv` resolved from `PATH` |
| `--baseline-artifact=PATH` | `<run-dir>/baseline.json` |
| `--live-artifact=PATH` | `<run-dir>/live.json` |
| `--allow-incomplete-baseline` | Off; accepts `complete: false` for the 2.x artifact |
| `--allow-incomplete-live` | Off; accepts `complete: false` for the 3.x artifact |

Arguments after a literal `--` pass unchanged to both pytest invocations. The orchestrator
adds `--airflow-record` to both and `--airflow-baseline` to the live run.

Each environment is real and independent. The orchestrator fetches the selected release's
Apache constraints, installs Airflow under them, then installs the matching plugin family
extra unconstrained. It provisions both environments before running the 2.x baseline and 3.x
live recordings. Missing `uv`, failed constraints downloads or installs, an unresolved plugin
spec, a missing recording, or an invalid artifact exits `2`. Regressions exit `1`; no
regressions exits `0`. Ordinary pytest failures are comparison data as long as the artifact
was written.
