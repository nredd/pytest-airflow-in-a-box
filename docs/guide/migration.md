# Airflow 2->3 Migration

Move the suite in four passes, from static checks to a real cross-family comparison:

1. Find removed imports and symbols with Ruff.
2. Turn executed Airflow 2 deprecations into failures.
3. Record the passing 2.x contract and compare it on 3.x.
4. Automate both environments with `airflow-migration-diff`.

Keep both families green while you migrate. Remove the family gates, baseline artifacts, and
migration-only assertions after cutover.

## Start with ruff

Enable rules for APIs removed from Airflow 3:

```toml
[tool.ruff.lint]
extend-select = ["AIR301", "AIR302"]
```

Ruff finds removed APIs in source that tests never execute. It cannot find runtime deprecations
from Airflow or providers.

Delay `AIR311` and `AIR312` until cutover: their replacements may not import on Airflow 2, and
`AIR311` has safe fixes that `ruff check --fix` will apply. To inventory those findings without
rewriting the dual-family branch, run them in a non-gating job and disable their fixes:

```toml
[tool.ruff.lint]
extend-select = ["AIR301", "AIR302", "AIR311", "AIR312"]
unfixable = ["AIR311", "AIR312"]
```

Family markers control whether a test runs; they cannot protect a module whose import already
uses a 3.x-only path.

## Migration-strict mode

Run the Airflow 2 leg with:

```console
pytest --airflow-migration-strict
```

This turns `RemovedInAirflow3Warning` and `AirflowProviderDeprecationWarning` raised during tests
into failures. Collection warnings remain nonfatal because Airflow 2 emits the same categories
while importing. Plain `DeprecationWarning` is not promoted.

Use pytest's normal warning filters for a migration you have deliberately deferred:

```ini
[pytest]
filterwarnings =
    ignore:known message:airflow.exceptions.RemovedInAirflow3Warning
```

The option emits `MigrationStrictNoOpWarning` when it is a no-op on Airflow 3 or when Airflow is
absent. Prefer Airflow 2.11 for this pass because it contains the final 2.x deprecation signals.

## Diffing outcomes across the upgrade

First record the current 2.x suite:

```console
pytest --airflow-record=baseline.json
```

Then run the same suite on 3.x, compare it with the baseline, and retain the new recording:

```console
pytest --airflow-baseline=baseline.json --airflow-record=live.json
```

The comparison distinguishes regressions from failures that already existed, fixes, family-
gated tests, and added or missing node IDs. Node IDs match exactly, so give parametrized tests
stable `ids=` values.

Use `--airflow-baseline-select=passing` for the normal repair loop: it runs only tests that
passed in the baseline, where every failure is a possible regression. If migration CI must stay
green temporarily, add `--airflow-baseline-xfail=prior-live.json`; known regressions become
non-strict xfails and repaired tests surface as XPASS. A test's own xfail marker still wins.

Commit or retain the baseline where both environments can read it. Do not trust an interrupted
recording by default: incomplete artifacts are rejected unless you opt in explicitly. The
[migration artifact reference](../reference/migration.md) defines the file schema, comparison
categories, selectors, and incomplete-artifact controls.

## Driving both families in one run

One environment cannot install both Airflow families. Use the console script to provision two
disposable, constraints-correct environments and run the record/compare sequence:

```console
uv tool install pytest-airflow-in-a-box
airflow-migration-diff --project-dir . -- -k "not slow"
```

Arguments after `--` reach both pytest runs. Tests marked `requires_airflow2` or
`requires_airflow3` are classified as gated rather than regressions.

Provisioning requires `uv`, network access, and two fresh Airflow installations. The script
handles the mutually exclusive family extras, Airflow 2's two-pass pytest installation, and
each 2.x release's Python ceiling. It exits `0` when no regressions remain, `1` when regressions
exist, and `2` when provisioning, execution, or artifact loading fails. Ordinary test failures
are comparison data, not orchestration failures.

The default `--plugin-spec` is the plugin version that installed the script. For an unreleased
checkout, pass a released version, VCS reference, or local path explicitly. See
[orchestrator options](../reference/migration.md#orchestrator-options) to pin releases,
interpreters, artifact paths, or the scratch directory.

## Running both families in CI

The bundled action provides `uv` and exposes `airflow-migration-diff` through `venv-path`:

```yaml
- uses: actions/checkout@v7
- uses: nredd/pytest-airflow-in-a-box/action@v0
  id: airflow-env
  with:
    airflow-version: "3.3.1"
    python-version: "3.13"

- run: ${{ steps.airflow-env.outputs.venv-path }}/bin/airflow-migration-diff --project-dir .
```

The action's Airflow version hosts the script; `--airflow2-version` and
`--airflow3-version` select the two releases being compared. Without the action, install the
tool with `uv` and run the same command.

Run Ruff and migration-strict on every push. Schedule the two-family diff nightly or on demand,
because it creates both environments from scratch. See
[GitHub Actions and reports](ci/github-action.md) for action inputs and report artifacts.
