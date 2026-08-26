# Keeping your own airflow_local_settings.py

Your repo has an `airflow_local_settings.py` at its root, holding [cluster
policies](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/cluster-policies.html),
and the session aborts before collection:

```console
ERROR: `airflow_local_settings` resolves to '/repo/airflow_local_settings.py', not this run's
generated '/tmp/.../config/airflow_local_settings.py'; move the foreign module aside, or set
the `airflow_local_settings` ini option to its dotted module path instead
```

Airflow supports exactly one `airflow_local_settings` module process-wide, and the plugin's
[isolated `AIRFLOW_HOME`](airflow-home.md) already generates one at
`config/airflow_local_settings.py` to install the SQLite engine tuning documented in
[Database](database.md). Without the guard, yours would win silently: Airflow's own
`prepare_syspath_for_config_and_plugins` *appends* `AIRFLOW_HOME/config` to the end of
`sys.path`, while pytest's default import mode inserts your project root at the front. Your
module wins the plain `import airflow_local_settings`, and the tuning is dropped with no
message at all.

## The fix

Name your module, do not let it collide:

```ini
[pytest]
airflow_local_settings = myproject.cluster_policies
```

The value is a **dotted module path**, not a file path: no `/`, no `\`, no `.py` suffix, and
every segment a valid ASCII identifier. A file path is rejected, because it could name the very
file pytest's import order is already fighting over -- a module has to be importable anyway,
which sidesteps the shadowing question entirely.

Move the root-level `airflow_local_settings.py` into the package (`myproject/cluster_policies.py`)
and point the option at it. It is also fine to leave your deployment's real
`airflow_local_settings.py` where it is and give this option a *different* module holding the
policies you want under test.

## What gets composed

The generated `airflow_local_settings.py` imports your module explicitly and copies its public
names into its own namespace, the same way Airflow's own
`settings.import_local_settings` composes ours:

- If your module defines `__all__`, only those names are copied.
- Otherwise, every attribute not starting with `__` is copied.

Composition is a union and the plugin's own names win a tie: a name already in the generated
module's `__all__` (`create_metadata_engine`) is filtered out of your set before
`globals().update`. Nothing uses `from ... import *` -- the generated source does an explicit
`import_module` plus `globals().update` -- so the SQLite engine tuning survives regardless of
what your module exports.

You never edit `config/airflow_local_settings.py`. It is regenerated deterministically every
run, same as `airflow.cfg`.

## When it fails loudly

A `pytest.UsageError` aborts the session before any test runs when:

- A foreign `airflow_local_settings` module -- one this run did not generate -- would resolve
  ahead of the generated file on `sys.path`. The message names both paths.
- `airflow_local_settings` does not resolve at all after generation, or resolves to a namespace
  package with no single origin file, so the two cannot be told apart.
- The configured value looks like a file path rather than a module path. This one is checked
  during bootstrap, on shape alone.
- The configured module cannot be imported, or resolves to a namespace package with no single
  origin file.

Resolving a dotted path executes every parent package's module-level code, which is your code;
any exception from it is caught broadly and reported as one actionable usage error rather than a
raw traceback out of `pytest_configure`.

## Timing, and the case it misses

The collision guard and the module resolution both run from `pytest_configure` -- after pytest's
own conftest loading has put your project on `sys.path`, and still before collection starts.
They cannot run during bootstrap: at that point no conftest has loaded, your project is not
importable yet, and a legitimate module would be rejected as missing.

The consequence is one uncaught case. A foreign `airflow_local_settings` module introduced only
by a test file pytest has not collected yet is not seen. A module sitting at the project root
next to a `conftest.py` -- the shape that actually causes this -- is already visible at that
point and is caught.

## Per-test policies instead

For a policy scoped to one test, register it directly rather than through a settings module:
`airflow_components.policy(task_policy=...)` builds a policy plugin from hookspec-named
callables and registers it with Airflow's policy plugin manager. It never writes an
`airflow_local_settings.py`, so it is fully decoupled from everything on this page. See
[Custom components](custom-components-wiring.md#runtime-component-registration).
