# Developing this plugin

## Set up the repository

```console
git submodule update --init
make install
```

`make install` syncs the locked development environment and installs the pre-commit and
pre-push hooks. The default `dev` dependency group carries Airflow 3.x. To experiment against
an Airflow 2.x resolution instead, omit the conflicting default group:

```console
uv sync --no-default-groups --extra airflow2
```

Use `make install-postgres` to add the disposable Postgres dependencies, then select the
backend with `--airflow-db-backend=postgres`.

## Run the gate

Before opening a pull request:

```console
make all
uv run prek run --all-files
```

`make all` formats and lints the code, type-checks it, runs the coverage-gated test suite,
verifies the lockfile, builds the distributions, and builds the documentation in strict mode.
The repository hooks are a separate required check.

For iteration, run a focused test with `uv run pytest`; use `make test` for the real 100%
branch-coverage gate because it measures pytester subprocesses too. Use `make test-xdist` to
reproduce CI's `-n auto --dist loadgroup` execution. That parallel target deliberately omits
the coverage gate because serial-only tests skip on workers. The network-backed migration
orchestrator test is also separate; run it with `make test-migration-e2e`.

Run the Linux GitHub Actions workflow locally with [act](https://nektosact.com/):

```console
act pull_request
```

`act` cannot reproduce the native macOS, ARM Linux, or Alpine/musl compatibility legs.

## Compatibility suite

`tests/enduser/` is a sanitized consumer-style catalog run on every certified matrix leg
-- the consumer contract, exercised the way a user's own suite would. It covers custom
operators, TaskFlow and mapping, hooks and connections, SQLite provider SQL, sensors,
deferral, callbacks and retries, assets, provider packages, `DagBag`/collection,
logging, `xdist`, and REST API CRUD.

Provider discovery gets two angles: `tests/enduser/test_isolated_discovery.py` (marked
`airflow_isolated`) registers a provider's `apache_airflow_provider` entry point through a
synthetic distribution in a one-shot child process and resolves it through a live
`ProvidersManager`, and `check_component`'s `ComponentKind.PROVIDER` checks catch the same
discovery failure modes statically -- see
[provider checks](guide/custom-components.md#distribution-components).

## Concurrent local runs

Running two suites from this repository at once -- two worktrees, or a gate alongside a scratch
run -- can rarely end a session with a nonzero exit code even though every test passed:

```
FileNotFoundError: .../pytest-current
```

The race is pytest's cleanup of the dangling `pytest-current` symlink in the shared
`pytest-of-<user>/` root. This plugin's zero-ini
`tmp_path_retention_policy = "failed"` default -- pytest's own is `"all"` -- makes the race more
likely than in a bare pytest installation. The isolated `AIRFLOW_HOME` is not involved; it is
a per-session `mkdtemp` outside `pytest-of-<user>/`.

Two ways to avoid it, cheapest first:

- `pytest -o tmp_path_retention_policy=all` restores pytest's own default for that run and
  removes the precondition at no storage cost.
- `PYTEST_DEBUG_TEMPROOT=<dir>` relocates pytest's own `pytest-of-<user>/` root without
  touching `TMPDIR`, so it does not affect where `AIRFLOW_HOME` lands.

Setting `TMPDIR` per checkout or passing `--basetemp` also works, but both select the
`caller-temp` rung of the `AIRFLOW_HOME` storage ladder and trade away the faster rungs. See
[The isolated AIRFLOW_HOME](internals/test-environments.md#the-isolated-airflow_home).
