# Baseline artifact contract

Building your own tooling on a recorded run -- a dashboard, a bot that files one issue per
regression, a merge gate stricter than the exit code -- means depending on the artifact's shape
and on how an outcome was derived. Both are public and both are specified here. If you are only
running the workflow, [Diffing outcomes across the upgrade](outcome-diff.md) is the page you
want.

Two importable surfaces:

- [`pytest_airflow_in_a_box.artifact`](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/src/pytest_airflow_in_a_box/artifact.py)
  -- `Outcome`, `OutcomeEntry`, `Artifact`, `ARTIFACT_SCHEMA_VERSION`, `load_artifact`,
  `write_artifact`
- [`pytest_airflow_in_a_box.baseline`](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/src/pytest_airflow_in_a_box/baseline.py)
  -- `compute_categories(baseline, live) -> CategoryBuckets`, the pure function behind the
  terminal summary and behind `airflow-migration-diff`. Nothing re-derives a category anywhere
  else

## The seven categories

For nodeids collected on both sides, each outcome projects onto `pass` (`passed`/`xpassed`),
`fail` (`failed`/`error`/`xfailed`), or `neutral` (a non-gated `skipped`). A nodeid gated by a
`requires_airflow2`/`requires_airflow3` family marker on *either* side is always `gated`, checked
before the projection -- it is never a regression, a fix, or folded into `still-passing`/
`broken-on-both`.

| Baseline  | Live      | Category         |
|-----------|-----------|------------------|
| pass      | pass      | `still-passing`  |
| pass      | fail      | `regression`     |
| pass      | neutral   | `still-passing`  |
| fail      | pass      | `fixed`          |
| fail      | fail      | `broken-on-both` |
| fail      | neutral   | `broken-on-both` |
| neutral   | pass      | `still-passing`  |
| neutral   | fail      | `broken-on-both` |
| neutral   | neutral   | `still-passing`  |

A neutral outcome never lands in `regression` or `fixed` on either side -- a skip does not prove a
regression fixed, and it does not prove one introduced. `missing` (baseline-only) and `new`
(live-only) round out the seven; nodeids match exactly, with no fuzzy pairing, so a
version-dependent parametrize id shows up honestly as a `new`/`missing` pair rather than a false
regression. Pin `ids=` explicitly in a parametrized test if that matters to you.

Nodeids are normalized for one xdist detail before they reach the artifact: `--dist loadgroup`
rewrites a grouped item's nodeid to `<nodeid>@<group>`, and that suffix is stripped, so an
artifact recorded under `loadgroup` still compares against one recorded under any other dist mode.

## Artifact schema (v1)

`--airflow-record=PATH` writes this JSON at `pytest_sessionfinish`, once, from the xdist
controller (or the single process, when not running under `pytest-xdist`) -- test reports already
funnel there, so no worker coordination is needed. A `KeyboardInterrupt` or internal-error session
still writes, flagged `complete: false`. A hard kill of the whole pytest process (SIGKILL, OOM)
writes nothing -- there is no crash-safe partial artifact, by design. An individual `pytest-xdist`
*worker* subprocess crashing mid-test is a different, survivable case: the controller keeps
running, and the crashed nodeid still gets a real `failed` outcome in the artifact rather than
silently vanishing (see [Outcome derivation](#outcome-derivation)).

| Field             | Type                       | Notes                                                                 |
|-------------------|----------------------------|------------------------------------------------------------------------|
| `schema_version`  | `int`                      | `1`. A mismatch on load is a `pytest.UsageError`.                     |
| `plugin_version`  | `str`                      | Recording `pytest-airflow-in-a-box` version.                          |
| `airflow_version` | `str`                      | Recording Airflow distribution version, or `"unknown"`.               |
| `airflow_family`  | `str`                      | Raw `AirflowFamily` value (the distribution name), or `"unknown"`.    |
| `python_version`  | `str`                      | Recording Python version.                                             |
| `pytest_version`  | `str`                      | Recording pytest version.                                             |
| `created_at`      | `str`                      | UTC ISO 8601 timestamp.                                                |
| `complete`        | `bool`                     | `false` for an interrupted or internal-error session.                 |
| `outcomes`        | `dict[str, OutcomeEntry]`  | Keyed by nodeid, exact match only.                                     |

Each `OutcomeEntry`:

| Field      | Type            | Notes                                                                          |
|------------|-----------------|---------------------------------------------------------------------------------|
| `outcome`  | `str`           | One of `passed`, `failed`, `error`, `skipped`, `xfailed`, `xpassed`.            |
| `phase`    | `str \| None`   | `setup`/`teardown` when `outcome` is `error`; always `None` otherwise.          |
| `gated`    | `bool`          | Whether a `requires_airflow2`/`requires_airflow3` marker would gate this item.  |
| `duration` | `float`         | Sum, in seconds, of every phase report that actually ran.                       |

The file is written as indented, key-sorted JSON, so two artifacts diff readably in `git diff`.

## Outcome derivation

Reports fold in as setup/call/teardown arrive and finalize at the item's terminal report, in this
precedence:

1. A crashed `pytest-xdist` worker reports its in-flight nodeid once, with no setup/call/teardown
   phase to attribute the failure to: `failed`, `phase: null` -- not `error`, which stays reserved
   for a `setup`/`teardown` phase exception specifically.
2. Otherwise a failed teardown report: `error`, `phase: teardown`.
3. Otherwise a failed setup report: `error`, `phase: setup`. A call-phase exception is always
   `failed`, never `error` -- `phase` distinguishes fixture/session-level trouble from a genuine
   test failure.
4. No call report at all (setup skipped, no failure) is `skipped`.
5. Otherwise the call report decides: `passed` (`xpassed` if xfail-marked), `failed` (a strict-xfail
   unexpected pass is already reported `failed` by pytest itself, so no extra handling is needed),
   or `skipped` (`xfailed` if xfail-marked).

`gated` is computed by semantically recomputing the family-marker condition -- never by parsing
the skip reason -- so an environment-caused skip on a family-marked test is never mistagged as
gated. A pytest-rerunfailures run records only the final outcome after the last rerun; merging
multiple runs into one artifact is a deliberate v1 scope cut.
