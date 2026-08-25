# Who owns AIRFLOW__\*

You are probably here because an xdist run died before executing a single test:

```console
ERROR: Inherited Airflow environment for the local xdist worker disagrees with state:
`AIRFLOW__API_AUTH__JWT_SECRET`, `AIRFLOW__CORE__AUTH_MANAGER` -- another pytest plugin or
conftest likely mutated `AIRFLOW__*` after bootstrap exported it, and a module-scope
`os.environ` write is the usual suspect. Set the `airflow_worker_env_drift = repair` ini option
to re-install this run's values and continue.
```

Every worker fails identically, so the run ends as `xdist: maximum crashed workers reached`
with nothing run. The check is doing its job: the JWT secret, auth manager, Dag folder and
Fernet key really are no longer this run's. What follows is the ownership model behind it.

## The ownership window

The plugin owns `AIRFLOW__*` **through bootstrap** and makes no claim over it afterwards.

Ordering, on every process:

1. pytest parses the command line and the ini file, folding `-o` overrides into the ini table.
2. `plugin.pytest_load_initial_conftests` runs. Bootstrap installs the environment, then the
   [`airflow_config` ini option](../guide/configuration.md) is applied on top.
3. pytest's own conftest-collecting hookimpl runs. It is `trylast`, so it is strictly after
   step 2. This is the moat: a consumer `conftest.py` structurally cannot precede bootstrap.
4. Anything a conftest imports is free to write over the whole surface.

Step 4 is not hypothetical. An `import` with a module-scope `os.environ` assignment is a common
harness shape, and Airflow's own `tests_common/pytest_plugin.py` is one of them.

Bootstrap also refuses to start at all when Airflow is already imported:

```console
ERROR: Airflow was imported before pytest-airflow-in-a-box could configure it. Remove the early
import or disable the importing pytest plugin.
```

## What bootstrap installs, and what it scrubs

`_environment(state)` builds the minimum pre-import environment: `AIRFLOW_HOME`,
`AIRFLOW_CONFIG`, the Dag and plugins folders, `core.unit_test_mode`, `core.load_examples`,
`scheduler.catchup_by_default`, the SQLAlchemy connection, the log folder, the Fernet key, plus
a family branch (`core.auth_manager` + SimpleAuthManager users, passwords file, and
`api_auth.jwt_secret` on 3.x; `webserver.secret_key` and a `SequentialExecutor` default on 2.x)
and the optional `core.xcom_backend` / `secrets.backend` / `secrets.backend_kwargs` /
`core.executor` values an ini configured.

Installing is `os.environ.update`, and then a scrub: every name in `_environment_names()` that
this run's state did *not* set is popped. That is why a prior run's `airflow_xcom_backend`
cannot leak into a run whose ini no longer configures one, and why the other family's auth
variables cannot survive a family switch.

Two names sit outside that model on purpose:

- `AIRFLOW__CORE__EXECUTOR` is not in `_environment_names()`, so an ambient executor choice is
  legitimate consumer configuration and survives the scrub. Its pre-bootstrap value is still
  snapshotted, because an ini-configured executor does overwrite it and teardown has to restore
  it.
- `PYTEST_AIRFLOW_IN_A_BOX_BOOTSTRAP_STATE` is the handoff variable: the run's whole
  `BootstrapState` as compact JSON, exported at the end of `_install_environment`. It is how a
  child process learns which run it belongs to.

The `airflow_config` ini option's denylist is *derived* from `_environment_names()`, not copied,
so a name bootstrap starts owning is denied on the same commit.

## Why a worker cross-checks

Two process kinds inherit state rather than bootstrapping their own: a local xdist worker
(`PYTEST_XDIST_WORKER`) and the one-shot child an
[`airflow_isolated`](../guide/isolated-tests.md) batch spawns
(`PYTEST_AIRFLOW_IN_A_BOX_ISOLATED_WORKER`) -- structurally an xdist worker of one, over the
identical environment channel.

Each is spawned with a copy of the parent's environment, which by then may already have been
rewritten by step 4. So the child rebuilds `_environment(state)` from the inherited state and
compares it name by name against what it actually inherited. A mismatch means something outside
this plugin now owns variables the run depends on, and the default policy is fatal.

Serially the same rewrite usually goes unnoticed, which is exactly why the parallel failure
looks so abrupt.

## The repair policy, and what it does not promise

The first thing to try is stopping the foreign write, because then every guarantee holds. When
that is not yours to change:

```ini
# pytest.ini
[pytest]
airflow_worker_env_drift = repair
```

A child that finds drift re-installs this run's variables and continues, warning once at
configure time with the names it repaired (a `WorkerEnvironmentDriftWarning`, relayed to the
controller's summary through `pytest_warning_recorded`).

What that buys, precisely: the process is put back into the state its parent was in at the
*same* point in its own lifecycle -- bootstrap done, no conftest imported yet. It does **not**
promise the repair sticks. Whatever rewrote the environment on the controller runs again in
this process and rewrites it again, exactly as it does serially. The mode makes a parallel run
behave like the serial run that already worked; it does not restore isolation the foreign write
took away. Past bootstrap, what Airflow reads is your harness's business.

The value is validated only when drift is actually found, so a typo in this option cannot abort
a healthy run. Anything other than `error` or `repair` is a usage error.

`repair` never overwrites an override you asked for: an `airflow_isolated` `environment=`
payload naming a bootstrap-owned variable is rejected outright when the marker is parsed, long
before any child starts.

## Restoring

The `airflow_config` ini overrides are held open on an `ExitStack` closed from
`Config.add_cleanup`, and that callback stack unwinds last-in-first-out, so the override
restore runs *ahead* of bootstrap's own environment restore. It still runs when a later step of
the initial parse aborts the session. Bootstrap then restores every owned name (plus
`AIRFLOW__CORE__EXECUTOR`) to its pre-run value, deleting the ones that were absent.

## Reading the state back

[`--airflow-doctor`](../reference/diagnostics.md) prints the resolved paths, the declared
`airflow_config` overrides (credential-looking values redacted), and the configured
`airflow_worker_env_drift` policy, so a bug report shows which mode produced the run.
