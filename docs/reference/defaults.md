# Defaults

The plugin needs zero ini configuration. It applies `--tb=short`, `-ra`, `--durations=20`, and
failed-only `tmp_path` retention, but only where the user has not chosen a value -- explicit flags
and ini settings always win. Warning filters silence traced third-party deprecation noise
(`flask_appbuilder`, `flask_sqlalchemy`, `starlette`) while keeping Airflow's own deprecation
warnings visible, and promote pytest's collection and unraisable warnings to errors. User-supplied
`filterwarnings` lines take precedence.

Warnings sourced from the plugin's *own* bootstrap stack live in a separate knob, the
`airflow_default_filterwarnings` ini option. Its default currently suppresses alembic's
`path_separator` deprecation warning, emitted during the metadata-database initialization the
plugin itself performs. That initialization runs inside its own reset warnings context, which
plain `filterwarnings` ini lines cannot reach -- this option is the knob that does. Lines use
pytest's `filterwarnings` syntax (`action:message:category:module:lineno`, message and module
matched as regexes from the start), are prepended below user-supplied `filterwarnings` lines,
and are additionally replayed inside the bootstrap's warnings context. Redefining the option
replaces the default wholesale, so a warnings-as-errors suite can empty it:

```ini
[pytest]
airflow_default_filterwarnings =
```

The isolated `AIRFLOW_HOME` carries the same failed-only default under its own
`airflow_home_retention_policy` ini option and `--airflow-home-retention` flag; see
[the isolated `AIRFLOW_HOME`](../guide/airflow-home.md).

No default here writes a file. Report artifacts (`pytest.log`, `pytest.xml`) are opt-in and
live outside this set -- see [Report artifacts](../guide/reports.md).
