# Defaults

The plugin needs zero ini configuration. It applies `--tb=short`, `-ra`, `--durations=20`, and
failed-only `tmp_path` retention, but only where the user has not chosen a value -- explicit flags
and ini settings always win. Warning filters silence traced third-party deprecation noise
(`flask_appbuilder`, `flask_sqlalchemy`, `starlette`) while keeping Airflow's own deprecation
warnings visible, and promote pytest's collection and unraisable warnings to errors. User-supplied
`filterwarnings` lines take precedence.

The isolated `AIRFLOW_HOME` carries the same failed-only default under its own
`airflow_home_retention_policy` ini option and `--airflow-home-retention` flag; see
[the isolated `AIRFLOW_HOME`](../guide/airflow-home.md).

No default here writes a file. Report artifacts (`pytest.log`, `pytest.xml`) are opt-in and
live outside this set -- see [Report artifacts](../guide/reports.md).
