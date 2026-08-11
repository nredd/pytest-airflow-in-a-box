# Airflow configuration

`airflow_config` overrides Airflow configuration options and plain environment variables through
one code path, as a context manager or a decorator. Options are applied as `AIRFLOW__SECTION__KEY`
environment variables -- the same pre-import-safe mechanism bootstrap uses -- so they reach every
Airflow configuration parser in the process, including the Task SDK parser added in Airflow 3.2:

```python
from pytest_airflow_in_a_box.config import airflow_config


def test_with_overrides():
    with airflow_config({("core", "unit_test_mode"): "False"}, env={"MY_FLAG": "1"}):
        ...


@airflow_config({("core", "dagbag_import_timeout"): "120"})
def test_decorated(): ...
```

Every name is restored exactly on exit, and a name that was absent beforehand is deleted rather
than emptied. Nesting restores last-in-first-out. A `None` value makes a name absent for the
duration of the context, so Airflow falls back to `airflow.cfg` and then to its own default:

```python
with airflow_config({("core", "dagbag_import_timeout"): None}):
    ...  # conf.get returns Airflow's default
```

Both mappings are validated before anything is assigned, so a malformed argument cannot leave the
environment partly modified. Validation runs on context entry, so a bad argument to the decorator
form surfaces as a test failure rather than a collection error. `env` names may not start with
`AIRFLOW__` -- pass configuration options through `overrides` instead -- but `AIRFLOW_HOME` and
other single-underscore names are fine.

Airflow resolves `SQL_ALCHEMY_CONN`, `DAGS_FOLDER`, and `PLUGINS_FOLDER` into `airflow.settings`
globals once at import, and those do not follow an environment assignment. Pass
`refresh_settings=True` for options read through `settings` rather than through the config parser:

```python
with airflow_config({("core", "plugins_folder"): str(tmp_path)}, refresh_settings=True):
    ...  # airflow.settings.PLUGINS_FOLDER now agrees
```

It defaults to off because it imports Airflow and rewrites process-global state bootstrap owns,
and it is a partial remedy: a module that re-exported a settings value *by value* froze that
binding at import and no refresh can update it.

Three things worth knowing:

- **Values are expanded when Airflow reads them.** `conf.get` runs the raw variable through
  `expandvars` then `expanduser`, so a value containing `~` or `$` does not round-trip --
  `os.environ` holds the literal while `conf.get` returns the expansion.
- **A `None` override does not hide a `_CMD`/`_SECRET` sibling.** Setting a plain value always
  wins, but `None` means "fall back to whatever Airflow would otherwise do", and an already-set
  sibling variable is one of those things.
- **Do not wrap the first use of `api_client`/`api_server_url`.** Those fixtures launch a
  session-scoped subprocess that inherits the environment live at startup, so an override would
  outlive the context inside that server.

`conf_vars` ships as a deprecated alias under the name public Airflow docs teach. It emits a
`DeprecationWarning` and carries this plugin's semantics, so it does not recompute the settings
globals the way upstream's does -- use `airflow_config(..., refresh_settings=True)` for that.
