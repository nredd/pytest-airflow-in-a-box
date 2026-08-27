# Migration artifacts

The [Airflow 2->3 Migration guide](../guide/migration.md) covers the workflow. This page is the
contract for tooling built on recorded outcomes.

## Categories

Outcomes project to `pass` (`passed`, `xpassed`), `fail` (`failed`, `error`, `xfailed`), or
`neutral` (ordinary `skipped`). A node gated by `requires_airflow2` or `requires_airflow3` on
either side is always `gated` before projection.

| Baseline | Live | Category |
| --- | --- | --- |
| pass | pass or neutral | `still-passing` |
| pass | fail | `regression` |
| fail | pass | `fixed` |
| fail | fail or neutral | `broken-on-both` |
| neutral | pass or neutral | `still-passing` |
| neutral | fail | `broken-on-both` |

Baseline-only IDs are `missing`; live-only IDs are `new`. Node IDs match exactly. The
`@group` suffix added by xdist `loadgroup` is normalized away before recording.

## Artifact schema

`--airflow-record=PATH` writes schema version 1 once from the xdist controller or serial
process at session finish. Interrupted and internal-error sessions still write with
`complete: false`; a hard process kill writes nothing.

| Field | Type | Meaning |
| --- | --- | --- |
| `schema_version` | `int` | Currently `1`; mismatches are usage errors |
| `plugin_version` | `str` | Recording plugin version |
| `airflow_version` / `airflow_family` | `str` | Recording Airflow identity or `"unknown"` |
| `python_version` / `pytest_version` | `str` | Recording runtime |
| `created_at` | `str` | UTC ISO 8601 timestamp |
| `complete` | `bool` | False after interrupted/internal-error sessions |
| `outcomes` | `dict[str, OutcomeEntry]` | Entries keyed by exact node ID |

Each entry contains `outcome`, `phase`, `gated`, and total `duration`. Outcome is one of
`passed`, `failed`, `error`, `skipped`, `xfailed`, or `xpassed`; phase is `setup` or `teardown`
only for errors.

Outcome precedence is worker crash, teardown error, setup error, setup skip, then call report.
A call exception is `failed`, while setup/teardown exceptions are `error`. A rerun records only
its final outcome.

`--airflow-baseline-allow-incomplete` accepts an incomplete baseline or prior-live artifact;
without it, loading one is a usage error.

Public helpers live in `pytest_airflow_in_a_box.artifact` (`load_artifact`, `write_artifact`,
`Artifact`, `OutcomeEntry`) and `pytest_airflow_in_a_box.baseline`
(`compute_categories`).

## Orchestrator options

| Option | Default |
| --- | --- |
| `--project-dir` | Current directory; installed editable when it is a Python project |
| `--work-dir` | Fresh temporary directory |
| `--keep-work-dir` | Off |
| `--airflow2-version` / `--airflow3-version` | Newest certified release per family |
| `--python-airflow2` / `--python-airflow3` | Current interpreter, clamped to release bounds |
| `--plugin-spec` | Version installed beside the console script |
| `--uv-path` | Resolved from `PATH` |
| `--baseline-artifact` / `--live-artifact` | Files below the run work directory |
| `--allow-incomplete-baseline` / `--allow-incomplete-live` | Off |

Every venv, install, and pytest run is real. A missing `uv`, failed provision, missing record,
or malformed artifact exits 2; detected regressions exit 1.
