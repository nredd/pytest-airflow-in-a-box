# Diffing outcomes across the upgrade

"Which tests pass on 2.11 but fail on 3.x" cannot be an in-run switch -- one environment is one
Airflow, and the `airflow2`/`airflow3` extras are declared mutually exclusive (see
[Certification](../../internals/certification.md)). It is a two-run workflow the
plugin owns: record outcomes in one environment, compare them against a second.

This is the last and most expensive layer of the migration funnel. Run
[ruff's `AIR3xx` rules](ruff-air-rules.md) and [`--airflow-migration-strict`](strict.md) first
and let this catch the *executed* breakage neither of them can see. It does not subsume
them: this layer only ever sees what your tests exercise.

## Record, then compare

Record a baseline in the 2.x environment:

```console
pytest --airflow-record=baseline.json
```

Compare against it in the 3.x environment, recording the live side too:

```console
pytest --airflow-baseline=baseline.json --airflow-record=live.json
```

The terminal summary prints all seven bucket counts -- `still-passing`, `broken-on-both`,
`gated`, `regression`, `fixed`, `new`, `missing` -- each followed by its own nodeids. `missing`
is informational: those nodeids exist in the baseline but were not collected live, so they
cannot be re-run.

Do not want to provision both environments by hand? That is what
[`airflow-migration-diff`](orchestrator.md) is for.

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

Composes with `-k`/`-m`. `missing` is not selectable -- nothing to collect. Only a
neutral baseline entry (a non-gated `skipped`) is eligible for none of the three selectors,
matching how the comparison folds a neutral outcome away from `regression`/`fixed`.

## Non-strict xfail during migration

`--airflow-baseline-xfail=PATH` takes a *prior live recording from this same environment*
(produced by `--airflow-record` alongside a `--airflow-baseline` run) and non-strict xfail-marks
every known regression -- a nodeid that passed (or xpassed) on the baseline and failed, errored, or
xfailed in that prior live run. The prior-live side accepts `xfailed` too, on purpose: once a
regression has been auto-marked, re-recording the live side while it is still unfixed records
it as `xfailed`, not `failed`, and the marking must stay self-sustaining across repeated
`--airflow-baseline-xfail` runs rather than losing track of it:

```console
pytest --airflow-baseline=baseline.json --airflow-baseline-xfail=prior-live.json
```

This keeps migration CI green while a regression is being worked on. Each fix shows up as an
XPASS, and the terminal summary names those nodeids and prompts a re-record of the live artifact
so the xfail set catches up. A test's own `xfail` marker always wins -- the plugin never
double-marks.

`--airflow-baseline-allow-incomplete` (also settable as the `airflow_baseline_allow_incomplete`
ini option) accepts a baseline or prior-live artifact recorded from a session that hit
`pytest.ExitCode.INTERRUPTED` or `pytest.ExitCode.INTERNAL_ERROR` (`complete: false`); without it,
loading such an artifact is a `pytest.UsageError`.

## A same-family comparison warns, it does not fail

Recording the baseline on the same `airflow_family` as the live run (3.1 -> 3.3, say) logs a
warning and prints a visible terminal-summary line, never an error -- checking for regressions
across a same-family upgrade is a legitimate use of these flags.

The recorded JSON's schema, the seven-category algorithm, and how a raw pytest report folds into
one outcome are all in the [baseline artifact contract](baseline-artifact.md).
