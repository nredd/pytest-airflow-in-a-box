# Airflow 2->3 Migration

You are on Airflow 2.x, you have to be on 3.x, and the question you cannot answer is *what
breaks*. This tier answers it in layers, cheapest first -- each layer is blind along a
*different* axis, so the last one does not subsume the first. Run all of them.

The workflow spine, in order: [migration-strict mode](#migration-strict-mode) ->
[ruff's AIR rules](#pairing-migration-strict-with-ruffs-air-rules) ->
[diffing outcomes across the upgrade](#diffing-outcomes-across-the-upgrade) ->
[driving both families in one run](#driving-both-families-in-one-run) ->
[running both families in CI](#running-both-families-in-ci). The
[baseline artifact contract](#baseline-artifact-contract) documents the JSON the diff layer
produces, for tooling built on top of it.

2.x support is a temporary bridge -- see
[Certification](../internals/certification.md#airflow-2x-is-a-migration-bridge-not-a-second-home)
-- so delete this subtree once the cutover lands.

## Migration-strict mode

`--airflow-migration-strict` turns an Airflow 2.11 test run into a forecast of 3.x breakage,
with no 3.x environment needed. Airflow 2.11 is deliberately saturated with deprecation
warnings pointing at what changes in 3.x; error-promoting the right two categories during the
runtest phase flags exactly the code paths a real 3.x migration will break, today, on the
2.x environment already in CI:

```console
pytest --airflow-migration-strict
```

or persistently via the `airflow_migration_strict` ini option. The command-line flag wins when
given; otherwise the ini value decides. Neither spelling appears in `README.md` -- `pytest
--help` is where you will find it.

Composes with the Airflow 2.x compatibility tier: predict a 3.x break here, verify the fix
against a real 3.x install separately. Single environment, no second Airflow install, matches
this plugin's existing "deprecations stay visible by design" stance.

### What gets promoted

Exactly two categories, both importable on every certified Airflow 2.x release and both
subclassing `DeprecationWarning`:

- `airflow.exceptions.RemovedInAirflow3Warning`
- `airflow.exceptions.AirflowProviderDeprecationWarning`

Plain `DeprecationWarning` is excluded on purpose. A lot of it reaches a test through
Airflow's own call frames without being an Airflow-authored migration signal -- third-party
library noise, stdlib deprecations, and the like. Only Airflow's own two public deprecation
categories are trustworthy enough to fail a test over.

### Test-phase only

Error promotion applies to the runtest phase only, never to collection or bootstrap. Airflow
2.11 emits the very same two categories from its own modules during import and Dag parsing --
an unqualified `error::` filter over them would abort the session before a single test ran.

That timing is the part you cannot hand-roll. pytest's `filterwarnings` grammar has no phase
field, so there is no line you can write in your own ini that means "errors during the run
only". pytest does re-read the ini list per warning context, though, so the plugin adds its
filters from `pytest_collection_finish` rather than `pytest_configure`: absent during
collection's own warning context, present for every runtest-phase warning context. A
module-level deprecation warning at Dag-file import time is reported, not fatal; the exact same
warning raised inside a test body fails that test.

Under `pytest-xdist` the mutation runs once per worker process, because each worker parses its
own copy of the ini list.

### Allowlisting a specific warning

No bespoke allowlist option exists, because pytest's own `filterwarnings` precedence already
does the job: the plugin *prepends* its two error filters ahead of every user-supplied line, and
pytest applies `filterwarnings` lines in order with later lines winning. A later, more specific
line downgrades the plugin's default:

```ini
[pytest]
filterwarnings =
    ignore:some specific known-fine message:airflow.exceptions.RemovedInAirflow3Warning
```

or per test:

```python
@pytest.mark.filterwarnings("ignore::airflow.exceptions.RemovedInAirflow3Warning")
def test_uses_a_known_deprecation(): ...
```

### Airflow 3.x and no-Airflow environments

There is nothing to forecast off the Airflow 2.x family: the flag becomes a no-op, reported
once via a `MigrationStrictNoOpWarning` at configure time so an enabled flag left over from a
2.x-only CI leg is never silently inert. `--airflow-doctor` also reports whether the mode is
enabled and, when it is enabled off 2.x, that it is currently a no-op.

## Pairing migration-strict with ruff's AIR rules

A lint rule reads every line you wrote. [`--airflow-migration-strict`](#migration-strict-mode)
only sees the lines your tests execute. Turning both on costs one config block and closes the
larger of the two blind spots, so do it before you provision a second Airflow.

ruff's Airflow ruleset (`AIR301`/`AIR302` for hard 3.0 removals and core-to-provider moves,
`AIR311`/`AIR312` for suggested updates that still have a 3.0 compat layer) attacks the same
2.x -> 3.x problem this plugin does, one layer earlier:

| Layer                        | Sees                                                       | Misses                                                     |
|------------------------------|------------------------------------------------------------|------------------------------------------------------------|
| `AIR3xx` (ruff)              | Every symbol the pinned ruff knows is bad, executed or not | Provider-issued deprecations, anything spelled dynamically |
| `--airflow-migration-strict` | Airflow's own deprecation warnings on executed paths       | Code no test reaches, breakage Airflow never warned about  |
| `airflow-migration-diff`     | Real pass/fail on a real 3.x install                       | Anything your tests never exercise, or a renamed nodeid    |

The funnel does not narrow monotonically. Only ruff sees code no test executes, so a fully green
[`airflow-migration-diff`](#driving-both-families-in-one-run) proves nothing about the untested
half of your Dags.

Where the layers overlap, the overlap is confirmation, not conflict: ruff never executes your
code and the plugin never parses your source, so there is no code-level interaction to reason
about. A symbol ruff flags and a warning `--airflow-migration-strict` promotes are two
independent witnesses to the same break. The one real coordination cost is a deprecation you
have deliberately accepted -- it has to be waived once per layer, in `filterwarnings` for the
plugin (see [Allowlisting a specific warning](#allowlisting-a-specific-warning)) and in ruff's
own config for the linter.

### The config to start with

Removal tier as errors, suggestion tier off until the cutover. `extend-select`, not `select` --
`select` replaces the list rather than adding to it, and would silently drop the rest of your
rules:

```toml
[tool.ruff.lint]
extend-select = ["AIR301", "AIR302"]
```

`AIR301`, `AIR302`, `AIR311`, and `AIR312` are all stable as of ruff 0.16.3 -- none of them
needs `preview = true`. Six other `AIR` rules are still preview-gated (`AIR003`, `AIR004`,
`AIR201`, `AIR202`, `AIR304`, `AIR321`), so a bare `select = ["AIR"]` quietly enables the seven
stable rules only.

### AIR311/AIR312 autofixes break a dual-family suite

The suggestion tier rewrites imports to Airflow 3 spellings, and those spellings do not exist on
2.x. `AIR311`'s `airflow.Dataset` -> `airflow.sdk.Asset` fix is classified *safe*, so a bare
`ruff check --fix` applies it with no `--unsafe-fixes` opt-in:

```console
ruff check --select AIR --fix --diff example.py
--- example.py
+++ example.py
@@ -1,2 +1,3 @@
 from airflow import Dataset
-ds = Dataset("s3://bucket/key")
+from airflow.sdk import Asset
+ds = Asset("s3://bucket/key")

Would fix 1 error.
```

There is no `airflow.sdk` on Airflow 2.x, and the blast radius is wider than the tests you
marked [`requires_airflow2`](../reference/markers.md). A rewritten Dag file fails its own
Dag-file import item and poisons `dag_bag` for every test that parses the whole corpus; a
rewrite that lands in a test module or a shared helper is a plain pytest collection error. The
family markers gate *execution*, not import -- they cannot rescue a file that no longer parses.

Note also that ruff adds the new import rather than rewriting the old one; the now-unused
`from airflow import Dataset` is left for `F401` to clean up, so the file is briefly wrong on
*both* families in between. `AIR302`/`AIR312` fixes are unsafe-only and so need an explicit
opt-in, but setting `unsafe-fixes = true` in `[tool.ruff]` is enough of an opt-in to make them
apply too.

So: `AIR301`/`AIR302` as errors immediately, since a removed symbol is broken on 3.x no matter
when you look at it, and `AIR311`/`AIR312` deferred until the 3.x cutover -- that's the last
thing left. The autofix hazard is the loud reason, but not the real one -- the real one is that
the suggestion tier is *unactionable* on a dual-family codebase. Any compliant rewrite breaks 2.x,
by hand exactly as it does by autofix, so gating on those two rules means a permanently red
build with no legal way to turn it green. Come the cutover both facts invert at once and the
autofixes become the fastest way to land the rewrite.

If you want the suggestion tier *visible* in the meantime without the rewrite risk, select it
and mark it unfixable rather than deselecting it -- the diagnostic still reports, and no `--fix`
run (safe or unsafe) will touch it:

```toml
[tool.ruff.lint]
extend-select = ["AIR301", "AIR302", "AIR311", "AIR312"]
unfixable = ["AIR311", "AIR312"]
```

Keep that one out of the gating job, though; it is an inventory of pending work, not a pass/fail
signal.

### Dynamic imports blind AIR entirely

This repo's own Dag corpus works around exactly the dual-family problem above. Every
family-divergent symbol in `tests/dags/` is imported through a dynamic
[`_resolve()`](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/tests/dags/_family.py)
helper that tries the 3.x module path and falls back to the 2.x one, so the same files parse
under both families. The cost is that `ruff check --select AIR tests/dags/` reports nothing at
all -- not because the corpus is clean, but because no `AIR` rule can see through
`import_module`. That blind spot is precisely what `--airflow-migration-strict` covers: the
dynamic import resolves at runtime, and whatever deprecation the resolved symbol emits gets
promoted like any other.

## Diffing outcomes across the upgrade

"Which tests pass on 2.11 but fail on 3.x" cannot be an in-run switch -- one environment is one
Airflow, and the `airflow2`/`airflow3` extras are declared mutually exclusive (see
[Certification](../internals/certification.md)). It is a two-run workflow the
plugin owns: record outcomes in one environment, compare them against a second.

This is the last and most expensive layer of the migration funnel. Run
[ruff's `AIR3xx` rules](#pairing-migration-strict-with-ruffs-air-rules) and
[`--airflow-migration-strict`](#migration-strict-mode) first and let this catch the *executed*
breakage neither of them can see. It does not subsume them: this layer only ever sees what your
tests exercise.

### Record, then compare

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
[`airflow-migration-diff`](#driving-both-families-in-one-run) is for.

### Selecting by baseline outcome

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

### Non-strict xfail during migration

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

### A same-family comparison warns, it does not fail

Recording the baseline on the same `airflow_family` as the live run (3.1 -> 3.3, say) logs a
warning and prints a visible terminal-summary line, never an error -- checking for regressions
across a same-family upgrade is a legitimate use of these flags.

The recorded JSON's schema, the seven-category algorithm, and how a raw pytest report folds into
one outcome are all in the [baseline artifact contract](#baseline-artifact-contract).

## Driving both families in one run

[Diffing outcomes across the upgrade](#diffing-outcomes-across-the-upgrade) needs two
environments, and standing those up correctly is the hard part of the workflow, not the diff.
Three traps, all of which this command already handles:

- **The extras are mutually exclusive.** One environment is one Airflow. You need two venvs, and
  the plugin's `airflow2` and `airflow3` extras are declared conflicting so `uv` will not let you
  merge them
- **Airflow's constraints pin pytest below this plugin's floor.** Airflow 2.x published
  constraints pin pytest as low as 7.4.4; the plugin needs `pytest>=8`. A single constrained
  install cannot satisfy both. Provisioning is two passes per family: Airflow itself under
  Airflow's published constraints file first, then the plugin's family extra unconstrained (with
  an explicit `pytest>=8,<9` on the 2.x side). A plugin-install failure therefore stays
  attributable to `--plugin-spec`, never to the Airflow core pin
- **The Python ceiling is per-release, not per-family.** Airflow 2.7.3 and 2.8.4 cap at 3.11 and
  publish no 3.12 constraints file, while 2.9+ reach 3.12. `--python-airflow2` clamps to the
  bounds of the *exact* release you asked for, so a family-wide clamp cannot hand a 3.12
  interpreter through and fail deep inside provisioning

`airflow-migration-diff` is a console script (not a pytest option) that does all of that, records
outcomes on each environment with `--airflow-record`/`--airflow-baseline`, and prints the
categorized diff:

```console
uv tool install pytest-airflow-in-a-box
airflow-migration-diff --project-dir . -- -k "not slow"
```

Everything after a literal `--` forwards verbatim to both `pytest` invocations.

Family-marked tests are safe across the two runs by construction: `gated` beats the pass/fail
projection in `compute_categories`, so a test carrying `requires_airflow2` or `requires_airflow3`
never lands in `regression` just because the other family skipped it.

Reach for this after the cheaper layers have stopped finding things --
[ruff's `AIR3xx` rules](#pairing-migration-strict-with-ruffs-air-rules) statically, then
[`--airflow-migration-strict`](#migration-strict-mode) on the 2.x environment already in CI. A
regression only this command finds is one neither promoted warning category announced.

### Exit codes

| Code | Meaning |
| --- | --- |
| `0` | No regressions. |
| `1` | At least one regression was found. |
| `2` | The orchestrator itself failed: `uv` missing, a provisioning failure, a record run that crashed before writing its artifact, or an absent/malformed/unsupported-schema-version/incomplete artifact. |

Only `1` describes the migration itself -- `0` and `2` both mean "not a migration
regression," for different reasons. Ordinary test failures inside either record run are *not*
fatal; only a missing artifact afterward is.

### Options

| Option | Default |
| --- | --- |
| `--project-dir` | The current directory. Installed editable (`--no-deps`) into both environments when it has a `pyproject.toml`/`setup.py`, from `requirements.txt` when only that is present, or skipped. |
| `--work-dir` | A fresh temporary directory. A unique run subdirectory is created inside it either way. |
| `--keep-work-dir` | Off -- the run's scratch directory (venvs, constraints files, artifacts) is removed on exit. |
| `--airflow2-version` / `--airflow3-version` | The newest certified release per family. |
| `--python-airflow2` / `--python-airflow3` | The current interpreter, clamped into the supported Python range. For 2.x that is the range of the exact `--airflow2-version` requested. |
| `--plugin-spec` | This checkout's installed plugin version. Pass explicitly for an unreleased version -- an unreleased version will not resolve from an index, and the orchestrator names `--plugin-spec` as the fix when the default fails to resolve. |
| `--uv-path` | Resolved from `$PATH`. `uv` is required; there is no bundled fallback. |
| `--baseline-artifact` / `--live-artifact` | `baseline.json`/`live.json` under the run's work directory. |
| `--allow-incomplete-baseline` / `--allow-incomplete-live` | Off -- an artifact recorded `complete: false` fails the run. |

The `--plugin-spec` trap is the one that bites in practice: the default pins the version
installed alongside the console script, so running an unreleased build (a dev checkout, a
just-bumped version not yet on PyPI) fails at the plugin-install step of the *first* venv. Pass a
released version, a VCS ref, or a local path.

### Real environments, not your test process

Every `uv venv`, package install, and `pytest` invocation described above runs for real -- no dry
run. Provisioning two fresh Airflow installs from Airflow's published constraints files takes
real time and a real network connection; run this command in
[CI](#running-both-families-in-ci) or a scratch environment, not inside a tight edit-test loop.

The category definitions and the artifact schema are not redefined here -- this command drives
the same `compute_categories` and the same
[baseline artifact contract](#baseline-artifact-contract) a hand-run pair of environments does.

## Running both families in CI

A CI job installs one Airflow. The migration diff needs two. The resolution is that
[`airflow-migration-diff`](#driving-both-families-in-one-run) provisions its own pair of scratch
venvs, so the job itself only has to supply the console script and a `uv` on `$PATH`.

That script is the package's only console script, and `pytest --help` will never list it -- it is
not a pytest option. If it is missing from a job, the fix is an install, not a flag.

### With the GitHub Action

[The action](ci/github-action.md) already installs `uv` onto `$PATH` and exposes the
provisioned venv as `venv-path`, which is where the console script lands:

```yaml
- uses: nredd/pytest-airflow-in-a-box/action@v0
  id: airflow-env
  with:
    airflow-version: "3.3.0"
    python-version: "3.12"

# Exit 0 = no regressions, 1 = regressions found, 2 = the orchestrator itself failed.
- run: ${{ steps.airflow-env.outputs.venv-path }}/bin/airflow-migration-diff --project-dir .
```

The `airflow-version`/`extra` you give the action does not decide what gets diffed. That
environment only hosts the script; the two families under comparison come from
`--airflow2-version`/`--airflow3-version`, defaulting to the newest certified release of each.

### Without the action

Any job with `uv` on `$PATH` works:

```yaml
- run: uv tool install pytest-airflow-in-a-box
- run: airflow-migration-diff --project-dir . -- -k "not slow"
```

### Reading the job's result

The exit code is the gate, and `2` is the one that needs a distinct response:

- `0` -- no regressions. Green
- `1` -- at least one regression. The rendered report lists every `regression` nodeid, plus
  `fixed`, `new`, and `missing`
- `2` -- the orchestrator failed, not your Dags: no `uv`, a provisioning failure, a record run
  that died before writing its artifact, or a malformed artifact. Do not treat this as a clean
  run, and do not treat it as a migration finding either

The report goes to stdout as plain text with no ANSI, so it survives a redirect into a job
summary or an uploaded file.

### Budget for it

Two fresh Airflow installs from Airflow's published constraints files, per invocation. This is a
nightly or on-demand job, not something to bolt onto every push. The cheaper layers --
[ruff's `AIR3xx` rules](#pairing-migration-strict-with-ruffs-air-rules) and
[`--airflow-migration-strict`](#migration-strict-mode) -- are the ones that belong in the
per-push gate.

## Baseline artifact contract

Building your own tooling on a recorded run -- a dashboard, a bot that files one issue per
regression, a merge gate stricter than the exit code -- means depending on the artifact's shape
and on how an outcome was derived. Both are public and both are specified here. If you are only
running the workflow, [Diffing outcomes across the upgrade](#diffing-outcomes-across-the-upgrade)
is the section you want.

Two importable modules:

- [`pytest_airflow_in_a_box.artifact`](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/src/pytest_airflow_in_a_box/artifact.py)
  -- `Outcome`, `OutcomeEntry`, `Artifact`, `ARTIFACT_SCHEMA_VERSION`, `load_artifact`,
  `write_artifact`
- [`pytest_airflow_in_a_box.baseline`](https://github.com/nredd/pytest-airflow-in-a-box/blob/main/src/pytest_airflow_in_a_box/baseline.py)
  -- `compute_categories(baseline, live) -> CategoryBuckets`, the pure function behind the
  terminal summary and behind `airflow-migration-diff`. Nothing re-derives a category anywhere
  else

### The seven categories

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

### Artifact schema (v1)

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

### Outcome derivation

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
