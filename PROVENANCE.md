# Provenance

`pytest-airflow-in-a-box` is an independent project informed by public Apache Airflow source,
documentation, issue discussions, and mailing-list discussions.

`src/pytest_airflow_in_a_box/_compat/taskrun.py::run_task_instance` is adapted from Apache Airflow
`devel-common/src/tests_common/test_utils/taskinstance.py` at commit
`2d374f71bc81202204ac0208df07b07c280668fa`, introduced by merge
`960973bfd8341040150ac312302cd795bf72bc20`. The local version defers Airflow imports, resolves an
optional task, preserves dependency flags on both execution paths, implements Airflow 3.2+
`mark_success`, commits before Execution API access, and refreshes the original ORM task instance.
The ASF license header is retained in that module.

`src/pytest_airflow_in_a_box/_compat/asset_schedule.py::_evaluate_v3_dag` is adapted from the
DagRun-creation body of Apache Airflow's `SchedulerJobRunner._create_dag_runs_asset_triggered`
and the readiness evaluation in `DagModel.dags_needing_dagruns`
(`airflow-core/src/airflow/jobs/scheduler_job_runner.py` and
`airflow-core/src/airflow/models/dag.py`) at commit `1438ea3587031417cc85d74323235cf087a058fb`
(tag `3.3.0`). `_evaluate_v2_dag` is adapted analogously from
`SchedulerJobRunner._create_dag_runs_dataset_triggered` (`airflow/jobs/scheduler_job_runner.py`)
at commit `b93c3db6b1641b0840bd15ac7d05bc58ff2cccbf` (tag `2.10.5`). Both drop row locking,
`max_active_runs` throttling, paused/stale/import-error Dag filters, and batching -- scheduler-
operational concerns that do not apply to one evaluated test Dag in an isolated single-process
test database -- and simplify the attached-events query to omit the real scheduler's lower bound
at the previous asset/dataset-triggered run of the same consumer. Neither file's license header
is copied verbatim; both are reimplementations against the same public/private model surface.

`ordered_task_instances`, all DagMaker extensions, and `evaluate_asset_schedules` (the family
dispatcher in `asset_schedule.py`) are independently authored for this project.

No proprietary source code, credentials, hostnames, internal paths, or private repository history
may be included in this project.

## References

- Apache Airflow: https://github.com/apache/airflow
- Apache Airflow license: https://github.com/apache/airflow/blob/main/LICENSE
- Adapted task-instance helper: https://github.com/apache/airflow/blob/2d374f71bc81202204ac0208df07b07c280668fa/devel-common/src/tests_common/test_utils/taskinstance.py
- Adapted asset-triggered scheduling (3.x): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/jobs/scheduler_job_runner.py
- Adapted asset-triggered readiness evaluation (3.x): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/models/dag.py
- Adapted dataset-triggered scheduling (2.x): https://github.com/apache/airflow/blob/b93c3db6b1641b0840bd15ac7d05bc58ff2cccbf/airflow/jobs/scheduler_job_runner.py
- pytest plugin documentation: https://docs.pytest.org/en/stable/how-to/writing_plugins.html
