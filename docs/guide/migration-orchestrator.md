# Migration diff orchestrator

`airflow-migration-diff` is a console script (not a pytest option) that runs the full
2.x -> 3.x migration diff workflow in one command: it `uv`-provisions a disposable
Airflow 2.x environment and a disposable Airflow 3.x environment, records outcomes on
each with the plugin's [`--airflow-record`/`--airflow-baseline`](migration-diff.md)
flags, and prints the categorized diff.

```console
uv tool install pytest-airflow-in-a-box
airflow-migration-diff --project-dir . -- -k "not slow"
```

Everything after a literal `--` forwards verbatim to both `pytest` invocations.

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | No regressions. |
| `1` | At least one regression was found. |
| `2` | The orchestrator itself failed: `uv` missing, a provisioning failure, a record run that crashed before writing its artifact, or an absent/malformed/unsupported-schema-version/incomplete artifact. |

Only `1` describes the migration itself -- `0` and `2` both mean "not a migration
regression," for different reasons.

## Options

| Option | Default |
| --- | --- |
| `--project-dir` | The current directory. Installed editable (`--no-deps`) into both environments when it has a `pyproject.toml`/`setup.py`, from `requirements.txt` when only that is present, or skipped. |
| `--work-dir` | A fresh temporary directory. A unique run subdirectory is created inside it either way. |
| `--keep-work-dir` | Off -- the run's scratch directory (venvs, constraints files, artifacts) is removed on exit. |
| `--airflow2-version` / `--airflow3-version` | The newest certified release per family. |
| `--python-airflow2` / `--python-airflow3` | The current interpreter, clamped into each family's supported Python range. |
| `--plugin-spec` | This checkout's installed plugin version. Pass explicitly for an unreleased version -- an unreleased version will not resolve from an index, and the orchestrator names `--plugin-spec` as the fix when the default fails to resolve. |
| `--uv-path` | Resolved from `$PATH`. `uv` is required; there is no bundled fallback. |
| `--baseline-artifact` / `--live-artifact` | `baseline.json`/`live.json` under the run's work directory. |
| `--allow-incomplete-baseline` / `--allow-incomplete-live` | Off -- an artifact recorded `complete: false` fails the run. |

## What it does not do

`airflow-migration-diff` never reimplements the seven-category diff algorithm -- it
drives the same `pytest_airflow_in_a_box.baseline.compute_categories` [Migration
outcome diff](migration-diff.md) contract exposes directly, the same code whether run
by hand across two manually-managed environments or through this one command. The
category definitions (`regression`, `fixed`, `broken-on-both`, `still-passing`,
`gated`, `new`, `missing`) and the artifact schema live with that contract, not with
this orchestrator.

## Real environments, not your test process

Every `uv venv`, package install, and `pytest` invocation described above runs for
real -- no dry run. Provisioning two fresh Airflow installs from Airflow's published
constraints files takes real time and a real network connection; run this command in
CI or a scratch environment, not inside a tight edit-test loop.
