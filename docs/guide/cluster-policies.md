# Cluster policies

Airflow supports exactly one `airflow_local_settings` module process-wide, and it is also
where projects conventionally put [cluster
policies](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/cluster-policies.html).
`config/airflow_local_settings.py` inside the plugin's own
[isolated `AIRFLOW_HOME`](airflow-home.md) already owns that name, applying the SQLite engine
tuning documented in [Database](database.md). A project-root `airflow_local_settings.py` would
otherwise collide with it silently: Airflow's own `sys.path.append(AIRFLOW_HOME/config)` puts
the generated module *behind* whatever pytest's own import machinery already put ahead of it on
`sys.path`, so the foreign module wins the import race and the SQLite tuning is silently
dropped.

## Why this needs a dedicated ini option

Composing a project's real policies into the generated file -- rather than letting the two
modules race for the same name -- needs an unambiguous handle on "the project's real module."
A file path could point at the very same file pytest's own import order is already fighting
over, so the `airflow_local_settings` ini option takes a **dotted module path** instead. It has
to be importable anyway, and requiring a module sidesteps the shadowing question entirely.

## Configuring your policy module

```ini
[pytest]
airflow_local_settings = myproject.cluster_policies
```

The value must be a plain dotted module path: no `/`, no `\`, no `.py` suffix, and every
segment a valid Python identifier. Its shape is checked immediately during bootstrap; the
module itself is resolved (not imported) once pytest's own conftest loading has made the
project importable, before any test runs. Either way a typo or a missing module fails with a
`pytest.UsageError` naming the bad value before collection starts -- not a stack trace deep
inside Airflow's own import machinery partway through the first test.

## What gets composed

The generated `airflow_local_settings.py` imports your module explicitly and copies its public
names into its own namespace, the same way Airflow's own `airflow.settings.import_local_settings`
composes ours:

- If your module defines `__all__`, only those names are copied.
- Otherwise, every attribute not starting with `__` is copied.

Composition is a union: your module's names are added alongside the plugin's own
`create_metadata_engine` export, never in place of it. Nothing here uses `from ... import *` --
the generated source does an explicit `import_module` plus `globals().update`, so the SQLite
engine tuning always survives regardless of what your module exports.

## When it fails loudly

`pytest.UsageError` aborts the session before any test runs when:

- A foreign `airflow_local_settings` module -- one this run did not generate -- would resolve
  ahead of the generated file on `sys.path`. The message names both paths; move the foreign
  module aside, or point the `airflow_local_settings` ini option at its dotted module path
  instead of leaving it to collide by name.
- The configured `airflow_local_settings` value looks like a file path rather than a module
  path.
- The configured module cannot be imported, or resolves to an implicit namespace package with
  no single origin file.

## Relationship to the generated file

You never edit `config/airflow_local_settings.py` directly -- it is regenerated deterministically
every run, same as `airflow.cfg`. Your own policies live in your own module, referenced only by
dotted path through the ini option.

## Limitations

The collision guard and module resolution both run once, from `pytest_configure` -- after
pytest's own conftest loading has put the project on `sys.path`, but still before collection
starts. A foreign `airflow_local_settings` module introduced only by a test file pytest has not
collected yet (as opposed to one sitting at the project root next to a `conftest.py`, which
pytest's own import setup already makes visible at that point) will not be caught by this
check.
