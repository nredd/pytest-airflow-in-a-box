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

## Repo-wide defaults

`airflow_config` is also an ini option. Declare `section.key = value` lines and the plugin
applies them as `AIRFLOW__SECTION__KEY` variables during pytest's initial parse -- before your
`conftest.py` is imported, and therefore before anything can build a `DagBag`:

```ini
# pytest.ini
[pytest]
airflow_config =
    core.dag_ignore_file_syntax = glob
    core.dagbag_import_timeout = 120
```

In `pyproject.toml` the same option is an array of strings, and in `tox.ini` / `setup.cfg` the
section is `[tool:pytest]`:

```toml
[tool.pytest.ini_options]
airflow_config = [
    "core.dag_ignore_file_syntax = glob",
    "core.dagbag_import_timeout = 120",
]
```

That ordering is the point. A `with airflow_config(...)` block in a test runs long after
`full_dag_bag` has parsed your Dag folder once for the whole worker process, and a
`.__enter__()` at conftest module scope both runs too late and never restores.

The grammar is one option per line. The value is split on the *first* `=`, so a value may
contain more (`airflow.some_url = https://h/?a=1`); the section and key are split on the *last*
`.`, so a section may contain dots while a key may not. Section, key, and value are each
stripped, and an empty value means the empty string -- there is no line syntax for "make this
option absent", which the programmatic form spells `None`. Two lines resolving to one variable
are an error, including `a.b.k` and `a_b.k`, which Airflow cannot address separately.

Options this plugin's bootstrap owns are rejected rather than silently fought:

```console
ERROR: Ini option `airflow_config` may not set `database.sql_alchemy_conn`;
pytest-airflow-in-a-box owns `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` -- select the backend with
the `airflow_db_backend` ini option or `--airflow-db-backend`
```

That covers the metadata database URL and pool flag, `core.dags_folder`, `core.unit_test_mode`,
`core.load_examples`, `scheduler.catchup_by_default`, the log folder, the Fernet key, and the
auth-manager and JWT surface. Each message names the supported knob. `core.executor` is *not*
on the list -- bootstrap does not own it, and `--airflow-doctor` already tells you to set it.

One option is rejected only *conditionally*: `core.dagbag_import_timeout` is fine on an ordinary
run, but with [the smoke catalog](smoke-tests.md) enabled it is an error, because the catalog
pins that same variable from `airflow_dag_parse_timeout` -- which also scales the per-file parse
watchdog and the slowpoke budget. Set `airflow_dag_parse_timeout` instead; it is the one knob
driving all three.

Nothing is written into the generated `airflow.cfg`. On Airflow 3 the environment already
outranks every file on each `conf.get()`, and on 2.x `core.unit_test_mode` sends the parser to
`unit_tests.cfg` and never reads `AIRFLOW_CONFIG` at all, so the environment is the only channel
that works on both families. One consequence: a *retained* `AIRFLOW_HOME` inspected with
`airflow config list` outside the pytest process will not show these overrides. Use
[`--airflow-doctor`](../reference/diagnostics.md), which echoes them back -- redacting any value
whose option name reads as a credential, since that report is meant to be pasted into bug
reports.

Under `xdist` the overrides reach every worker: workers inherit the controller's environment and
re-apply the identical values.

## Session-scoped overrides

For a value a config file cannot hold -- a path from `tmp_path_factory`, a port, a credential
minted for the run -- request the `airflow_configure` fixture and apply a batch from your own
session fixture:

```python
import pytest


@pytest.fixture(scope="session", autouse=True)
def _repo_defaults(airflow_configure, tmp_path_factory):
    airflow_configure(
        {("core", "plugins_folder"): str(tmp_path_factory.mktemp("plugins"))},
        refresh_settings=True,
    )
```

`refresh_settings=True` is there for the same reason it is above: `core.plugins_folder` is one
of the options Airflow reads through `airflow.settings` rather than through the configuration
parser, so an environment assignment alone does not move it.

Arguments are `airflow_config`'s, unchanged: `overrides`, `env=`, `refresh_settings=`, and a
`None` value to make a name absent. Batches compose and unwind last-in-first-out at session
teardown, restoring every name exactly.

Ordering is the caveat. pytest instantiates autouse fixtures ahead of requested ones at the same
scope, so a `scope="session", autouse=True` wrapper like the one above *is* applied before a
session-scoped `full_dag_bag` that the same test requests. But that guarantee is per test:

- A *function*-scoped wrapper is instantiated after every session-scoped fixture, so it cannot
  precede a Dag parse.
- `full_dag_bag` parses once per worker process and caches. A test collected earlier and outside
  your wrapper's conftest scope can win the parse, after which no override reaches it.

So use the ini option for anything that must precede the first parse unconditionally, and this
fixture for the rest. And do not reconfigure anything the API server already inherited -- the
warning about `api_client`/`api_server_url` above applies here too, for the whole session.

## Where the run lives

`airflow_home_path` and `airflow_dags_folder_path` return this run's isolated `AIRFLOW_HOME` and
the Dag directory `full_dag_bag` parses, as `pathlib.Path`:

```python
def test_paths(airflow_home_path, airflow_dags_folder_path):
    assert (airflow_home_path / "airflow.cfg").is_file()
    assert (airflow_dags_folder_path / "my_dag.py").is_file()
```

Both are session-scoped and touch neither Airflow nor the metadata database, so asking where a
directory is never triggers an import or a migration. The `_path` suffix is deliberate: plain
`airflow_home` and `airflow_dags_folder` are the ini options these fixtures resolve.

`airflow_dags_folder_path` follows the same ladder `full_dag_bag` does -- `--dag-folder`, then
the `airflow_dags_folder` ini option, then the empty `dags/` directory inside the run's
`AIRFLOW_HOME`. That last one is a disposable scratch directory meaning "no Dag folder was
configured", not a corpus.
