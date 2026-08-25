# Running both families in CI

A CI job installs one Airflow. The migration diff needs two. The resolution is that
[`airflow-migration-diff`](orchestrator.md) provisions its own pair of scratch venvs, so the job
itself only has to supply the console script and a `uv` on `$PATH`.

That script is the package's only console script, and `pytest --help` will never list it -- it is
not a pytest option. If it is missing from a job, the fix is an install, not a flag.

## With the GitHub Action

[The action](../ci/github-action.md) already installs `uv` onto `$PATH` and exposes the
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

## Without the action

Any job with `uv` on `$PATH` works:

```yaml
- run: uv tool install pytest-airflow-in-a-box
- run: airflow-migration-diff --project-dir . -- -k "not slow"
```

## Reading the job's result

The exit code is the gate, and `2` is the one that needs a distinct response:

- `0` -- no regressions. Green
- `1` -- at least one regression. The rendered report lists every `regression` nodeid, plus
  `fixed`, `new`, and `missing`
- `2` -- the orchestrator failed, not your Dags: no `uv`, a provisioning failure, a record run
  that died before writing its artifact, or a malformed artifact. Do not treat this as a clean
  run, and do not treat it as a migration finding either

The report goes to stdout as plain text with no ANSI, so it survives a redirect into a job
summary or an uploaded file.

## Budget for it

Two fresh Airflow installs from Airflow's published constraints files, per invocation. This is a
nightly or on-demand job, not something to bolt onto every push. The cheaper layers --
[ruff's `AIR3xx` rules](ruff-air-rules.md) and [`--airflow-migration-strict`](strict.md) -- are
the ones that belong in the per-push gate.
