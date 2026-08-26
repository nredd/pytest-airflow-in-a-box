# Developing the plugin

```console
uv sync
uv run prek install
make all
```

The `dev` dependency group carries Airflow 3.x, so a plain `uv sync` is a working 3.x
environment. To experiment against an Airflow 2.x resolution instead (the `airflow2` extra
conflicts with the default `dev` group by design):

```console
uv sync --no-default-groups --extra airflow2
```

Run the GitHub Actions workflow locally on Linux with [act](https://nektosact.com/):

```console
act pull_request
```

`act` cannot reproduce native macOS or Windows behavior.

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
[Providers, if you are shipping one](guide/custom-components.md#providers-if-you-are-shipping-one).

## Concurrent local runs

Running two suites from this repo at once on one machine -- two worktrees, or a gate alongside a
scratch run -- can rarely end a session with a non-zero exit code even though every test passed:

```
FileNotFoundError: .../pytest-current
```

The race is pytest's own dangling-`pytest-current` symlink cleanup in the shared
`pytest-of-<user>/` root, but this plugin's zero-ini `tmp_path_retention_policy = "failed"`
default (pytest's own is `"all"`) makes it far likelier to trip than a bare pytest install.
The isolated `AIRFLOW_HOME` is not involved -- that is a per-session `mkdtemp` outside
`pytest-of-<user>/`.

Two ways to avoid it, cheapest first:

- `pytest -o tmp_path_retention_policy=all` restores pytest's own default for that run,
  removing the precondition at no storage cost.
- `PYTEST_DEBUG_TEMPROOT=<dir>` relocates pytest's own `pytest-of-<user>/` root without
  touching `TMPDIR`, so it does not affect where `AIRFLOW_HOME` lands.

`TMPDIR` per checkout (or `--basetemp`) also works, but both feed the `caller-temp` rung
of the `AIRFLOW_HOME` storage ladder and so trade away the faster rungs -- see
[The isolated AIRFLOW_HOME](internals/test-environments.md#the-isolated-airflow_home).
