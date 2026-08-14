# Migration outcome diff

"Which tests pass on 2.11 but fail on 3.x" cannot be an in-run switch -- one environment is one
Airflow, and the 2.x/3.x extras are mutually exclusive (see [Requirements](../index.md)). It can be
a two-run workflow the plugin owns: record outcomes in one environment, compare them against a
second.

This is the last and most expensive layer of the migration funnel -- it answers with real
pass/fail rather than a predicted break, so run ruff's `AIR3xx` rules and
`--airflow-migration-strict` first and let this catch the *executed* breakage they structurally
cannot see (see [Pairing with ruff's AIR rules](migration-strict.md#pairing-with-ruffs-air-rules)).
It does not subsume them: this layer only ever sees what your tests exercise.

## Quickstart

Record a baseline in the 2.x environment:

```console
pytest --airflow-record=baseline.json
```

Compare against it in the 3.x environment, recording the live side too:

```console
pytest --airflow-baseline=baseline.json --airflow-record=live.json
```

The terminal summary prints seven bucket counts plus nodeids for `regression`, `fixed`, `new`, and
`missing`. `missing` is informational -- those nodeids exist in the baseline but were not
collected live, so they cannot be re-run.

## Selecting by baseline outcome

`--airflow-baseline-select` filters collection to one baseline bucket, computed from the
*baseline* outcome (selection happens before live outcomes exist):

```console
pytest --airflow-baseline=baseline.json --airflow-baseline-select=passing
```

- `passing` -- every possible regression lives here; the migration iteration loop runs this
  (baseline `passed` or `xpassed`, the same `pass` projection the comparison uses)
- `failing` -- fixed/broken-on-both candidates (baseline `failed`, `error`, or `xfailed`, the same
  `fail` projection the comparison uses)
- `new` -- nodeids absent from the baseline

Composes with `-k`/`-m`. `missing` is not selectable -- nothing to collect. Only a genuinely
neutral baseline entry (a non-gated `skipped`) is eligible for none of the three selectors,
matching how the comparison folds a neutral outcome away from `regression`/`fixed`.

## Non-strict xfail during migration

`--airflow-baseline-xfail=PATH` takes a *prior live recording from this same environment*
(produced by `--airflow-record` alongside a `--airflow-baseline` run) and non-strict xfail-marks
every known regression -- a nodeid that passed (or xpassed) on the baseline and failed, errored, or
xfailed in that prior live run. The prior-live side accepts `xfailed` too, on purpose: once a
regression has been auto-marked once, re-recording the live side while it is still unfixed records
it as `xfailed`, not `failed`, and the marking must stay self-sustaining across repeated
`--airflow-baseline-xfail` runs rather than losing track of it:

```console
pytest --airflow-baseline=baseline.json --airflow-baseline-xfail=prior-live.json
```

This keeps migration CI green while a regression is being worked on. Each fix surfaces as an
XPASS, and the terminal summary prompts a re-record of the live artifact so the xfail set catches
up. A test's own `xfail` marker always wins -- the plugin never double-marks.

`--airflow-baseline-allow-incomplete` (also settable as the `airflow_baseline_allow_incomplete`
ini option) accepts a baseline or prior-live artifact recorded from a session that hit
`pytest.ExitCode.INTERRUPTED` or `pytest.ExitCode.INTERNAL_ERROR` (`complete: false`); without it,
loading such an artifact is a `pytest.UsageError`.

## The seven categories

For nodeids collected on both sides, each outcome projects onto `pass` (`passed`/`xpassed`),
`fail` (`failed`/`error`/`xfailed`), or `neutral` (a non-gated `skipped`). A nodeid gated by a
`requires_airflow2`/`requires_airflow3` family marker on either side is always `gated`, checked
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

`compute_categories(baseline, live) -> CategoryBuckets` in
[`pytest_airflow_in_a_box.baseline`](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/src/pytest_airflow_in_a_box/baseline.py)
is the pure function behind the terminal summary, importable directly by other tooling built on
top of the artifact.

## Artifact schema (v1)

`--airflow-record=PATH` writes this JSON at `pytest_sessionfinish`, once, from the xdist
controller (or the single process, when not running under `pytest-xdist`) -- test reports already
funnel there, so no worker coordination is needed. A `KeyboardInterrupt` or internal-error session
still writes, flagged `complete: false`. A hard kill of the whole pytest process (SIGKILL, OOM)
writes nothing -- there is no crash-safe partial artifact, by design. An individual `pytest-xdist`
*worker* subprocess crashing mid-test is a different, survivable case: the controller keeps
running, and the crashed nodeid still gets a real `failed` outcome in the artifact rather than
silently vanishing (see [Outcome derivation](#outcome-derivation)).

This is the public contract [`pytest_airflow_in_a_box.artifact`](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/src/pytest_airflow_in_a_box/artifact.py)
exposes for other tooling to build on: `Outcome`, `OutcomeEntry`, `Artifact`, and
`load_artifact`/`write_artifact`.

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

## Outcome derivation

Reports fold in as setup/call/teardown arrive and finalize at teardown:

1. A failed teardown report always wins: `error`, `phase: teardown`.
2. Otherwise a failed setup report: `error`, `phase: setup`. A call-phase exception is always
   `failed`, never `error` -- `phase` distinguishes fixture/session-level trouble from a genuine
   test failure.
3. Otherwise the call report decides: `passed` (`xpassed` if xfail-marked), `failed` (a strict-xfail
   unexpected pass is already reported `failed` by pytest itself, so no extra handling is needed),
   or `skipped` (`xfailed` if xfail-marked).
4. No call report at all (setup skipped, no failure) is `skipped`.
5. A crashed `pytest-xdist` worker reports its in-flight nodeid once, with no setup/call/teardown
   phase to attribute the failure to: `failed`, `phase: null` (not `error` -- that stays reserved
   for a `setup`/`teardown` phase exception specifically).

`gated` is computed by semantically recomputing the family-marker condition -- never by parsing
the skip reason -- so an environment-caused skip on a family-marked test is never mistagged as
gated. A pytest-rerunfailures run records only the final outcome after the last rerun; merging
multiple runs into one artifact is a deliberate v1 scope cut.

## Warnings, not errors

A same-family comparison (baseline recorded on the same `airflow_family` as the live run, e.g.
3.1 -> 3.3) logs a warning and prints a visible terminal-summary line, never an error -- it is a
legitimate way to check for regressions across a same-family upgrade too.
