# Airflow 2->3 Migration

You are on Airflow 2, you have to be on 3, and the question is *what breaks*. Answer it in four
layers, cheapest first:

1. Ruff finds removed or moved symbols in every line it can parse.
2. Migration-strict turns executed Airflow deprecations into test failures on 2.x.
3. Outcome recording compares the same suite across real 2.x and 3.x environments.
4. `airflow-migration-diff` provisions both environments and runs that comparison for you.

Each layer is blind on a different axis; run all four during the migration, then delete the
bridge after cutover.

## Start with ruff

Enable hard Airflow 3 removals immediately:

```toml
[tool.ruff.lint]
extend-select = ["AIR301", "AIR302"]
```

Ruff sees unexecuted source that a test run cannot. It does not see provider-issued runtime
deprecations or symbols hidden behind dynamic imports.

Delay `AIR311` and `AIR312` until the 3.x cutover. Their suggested imports do not exist on 2.x,
and `AIR311` includes safe autofixes that a normal `ruff check --fix` will apply. If you want an
inventory before cutover, make the suggestions visible but unfixable in a non-gating job:

```toml
[tool.ruff.lint]
extend-select = ["AIR301", "AIR302", "AIR311", "AIR312"]
unfixable = ["AIR311", "AIR312"]
```

Family markers gate test execution, not Python import. They cannot rescue a Dag or test module
whose import was rewritten to a 3.x-only path.

## Migration-strict mode

On the Airflow 2.x leg:

```console
pytest --airflow-migration-strict
```

The flag promotes `RemovedInAirflow3Warning` and `AirflowProviderDeprecationWarning` during the
runtest phase only. Collection remains nonfatal because Airflow itself emits those categories
while importing on 2.x. Plain `DeprecationWarning` remains excluded.

Allowlist a known warning with pytest's normal later-wins filter precedence:

```ini
[pytest]
filterwarnings =
    ignore:known message:airflow.exceptions.RemovedInAirflow3Warning
```

On Airflow 3 or without Airflow, the flag is a visible no-op: configure emits
`MigrationStrictNoOpWarning`, and `--airflow-doctor` reports the state.

## Diffing outcomes across the upgrade

Record the current 2.x behavior:

```console
pytest --airflow-record=baseline.json
```

Then compare in the 3.x environment and preserve the live side:

```console
pytest --airflow-baseline=baseline.json --airflow-record=live.json
```

The summary sorts node IDs into `still-passing`, `broken-on-both`, `gated`, `regression`,
`fixed`, `new`, and `missing`. Exact matching is intentional; pin parametrized `ids=` when a
version-dependent ID would otherwise appear as a new/missing pair.

A same-family comparison, such as Airflow 3.1 against 3.3, is valid too. It emits a warning so
the report cannot be mistaken for the 2-to-3 workflow, but it does not fail merely because the
families match. The categories and exit behavior stay identical.

Use `--airflow-baseline-select=passing` for the migration iteration loop: every possible
regression starts from a passing baseline. `failing` selects fix candidates and `new` selects
node IDs absent from the baseline.

To keep migration CI green while regressions are worked, pass a prior live recording through
`--airflow-baseline-xfail`. Known regressions become non-strict xfails; fixes surface as XPASS
until the artifact is recorded again. A test's own xfail marker always wins.

The [migration artifact reference](../reference/migration.md) defines categories, schema,
outcome folding, and incomplete-artifact behavior.

## Driving both families in one run

One environment cannot contain both Airflow families. The console script provisions two
constraints-correct scratch venvs and drives the record/compare workflow:

```console
uv tool install pytest-airflow-in-a-box
airflow-migration-diff --project-dir . -- -k "not slow"
```

Arguments after `--` reach both pytest runs. Family-gated tests never become false regressions.

The script handles three traps that hand-built jobs commonly miss:

- The `airflow2` and `airflow3` extras are mutually exclusive.
- Airflow 2 constraints pin pytest below this plugin's floor, so installation needs a second,
  unconstrained pass with `pytest>=8,<9`.
- Each Airflow 2 release has its own Python ceiling; the requested interpreter is clamped to
  that exact release's supported range.

Exit codes are `0` for no regressions, `1` for regressions, and `2` when orchestration itself
failed. Ordinary test failures are data for the comparison, not an orchestration error.
Provisioning is real and requires a network connection; this is an on-demand or nightly job,
not an edit-test loop.

The default `--plugin-spec` is the installed plugin version. For an unreleased checkout, pass a
released version, VCS reference, or local path explicitly. All options are in the
[migration artifact reference](../reference/migration.md#orchestrator-options).

## Running both families in CI

The bundled action installs `uv` and exposes the console script through `venv-path`:

```yaml
- uses: nredd/pytest-airflow-in-a-box/action@v0
  id: airflow-env
  with:
    airflow-version: "3.3.0"
    python-version: "3.12"

- run: ${{ steps.airflow-env.outputs.venv-path }}/bin/airflow-migration-diff --project-dir .
```

The action's Airflow version only hosts the script; the compared releases come from
`--airflow2-version` and `--airflow3-version`. Without the action, install the tool with `uv`
and run the same command.

Budget two fresh Airflow installations per invocation. Ruff and migration-strict belong in the
per-push gate; the real two-family diff belongs in a nightly or on-demand job. See
[GitHub Actions and reports](ci/github-action.md) for the environment and artifact setup.
