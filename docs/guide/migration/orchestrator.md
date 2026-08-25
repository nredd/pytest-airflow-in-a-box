# Driving both families in one run

[Diffing outcomes across the upgrade](outcome-diff.md) needs two environments, and standing
those up correctly is the hard part of the workflow, not the diff. Three traps, all of which
this command already handles:

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

Reach for this after the cheaper layers have stopped finding things -- [ruff's `AIR3xx`
rules](ruff-air-rules.md) statically, then [`--airflow-migration-strict`](strict.md) on the 2.x
environment already in CI. A regression only this command finds is one neither promoted warning
category announced.

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | No regressions. |
| `1` | At least one regression was found. |
| `2` | The orchestrator itself failed: `uv` missing, a provisioning failure, a record run that crashed before writing its artifact, or an absent/malformed/unsupported-schema-version/incomplete artifact. |

Only `1` describes the migration itself -- `0` and `2` both mean "not a migration
regression," for different reasons. Ordinary test failures inside either record run are *not*
fatal; only a missing artifact afterward is.

## Options

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

## Real environments, not your test process

Every `uv venv`, package install, and `pytest` invocation described above runs for real -- no dry
run. Provisioning two fresh Airflow installs from Airflow's published constraints files takes
real time and a real network connection; run this command in
[CI](orchestrator-in-ci.md) or a scratch environment, not inside a tight edit-test loop.

The category definitions and the artifact schema are not redefined here -- this command drives
the same `compute_categories` and the same
[baseline artifact contract](baseline-artifact.md) a hand-run pair of environments does.
