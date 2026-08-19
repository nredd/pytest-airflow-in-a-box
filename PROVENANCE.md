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

`src/pytest_airflow_in_a_box/_compat/components.py`'s timetable, listener, and executor
conformance checks, plus `_compat/capabilities.py::_probe_executor_contract` and
`_probe_sdk_listener_manager_available`, are independently authored -- no Apache Airflow function
body is copied or adapted. Their embedded facts (which `BaseExecutor` methods and attributes exist
under which names and types on which release, which `Timetable` methods lack a usable default, and
which hookspec modules each listener manager registers) were transcribed by reading Apache Airflow
source directly, not derived from documentation. Verified against
`airflow-core/src/airflow/timetables/base.py`, `airflow-core/src/airflow/executors/base_executor.py`,
`airflow-core/src/airflow/listeners/listener.py`,
`airflow-core/src/airflow/listeners/spec/{dagrun,asset,importerrors}.py`,
`shared/listeners/src/airflow_shared/listeners/spec/{lifecycle,taskinstance}.py` (symlinked
unchanged into both `airflow-core/src/airflow/_shared/listeners` and
`task-sdk/src/airflow/sdk/_shared/listeners`), and `task-sdk/src/airflow/sdk/listener.py`, all at
commit `1438ea3587031417cc85d74323235cf087a058fb` (tag `3.3.0`). The executor's sentry-flag rename
and `execute_async` removal were additionally verified against `base_executor.py` at commit
`54bd5d8cd9f6f477cc83445737614dec81c4323c` (tag `3.1.0`) and commit
`3bc3ccfacc3dec9f359a3b153bfd4fc706c661ba` (tag `3.2.0`). `task-sdk/src/airflow/sdk/listener.py`
does not exist at all at the `3.1.0` commit -- confirmed both by installing the real `3.1.0` wheel
and by the absence of the path in the `3.1.0` tag's own source tree -- so
`sdk_listener_manager_available` certifies `False` for every 3.1.x release and `True` from `3.2`
onward, alongside the rest of that release's Task SDK listener-architecture changes.

No proprietary source code, credentials, hostnames, internal paths, or private repository history
may be included in this project.

## References

- Apache Airflow: https://github.com/apache/airflow
- Apache Airflow license: https://github.com/apache/airflow/blob/main/LICENSE
- Adapted task-instance helper: https://github.com/apache/airflow/blob/2d374f71bc81202204ac0208df07b07c280668fa/devel-common/src/tests_common/test_utils/taskinstance.py
- Adapted asset-triggered scheduling (3.x): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/jobs/scheduler_job_runner.py
- Adapted asset-triggered readiness evaluation (3.x): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/models/dag.py
- Adapted dataset-triggered scheduling (2.x): https://github.com/apache/airflow/blob/b93c3db6b1641b0840bd15ac7d05bc58ff2cccbf/airflow/jobs/scheduler_job_runner.py
- Timetable Protocol (3.3.0): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/timetables/base.py
- BaseExecutor (3.3.0): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/executors/base_executor.py
- BaseExecutor (3.2.0): https://github.com/apache/airflow/blob/3bc3ccfacc3dec9f359a3b153bfd4fc706c661ba/airflow-core/src/airflow/executors/base_executor.py
- BaseExecutor (3.1.0): https://github.com/apache/airflow/blob/54bd5d8cd9f6f477cc83445737614dec81c4323c/airflow-core/src/airflow/executors/base_executor.py
- Core listener manager (3.3.0): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/listeners/listener.py
- Core-only listener hookspecs (3.3.0): https://github.com/apache/airflow/tree/1438ea3587031417cc85d74323235cf087a058fb/airflow-core/src/airflow/listeners/spec
- Shared lifecycle/taskinstance listener hookspecs (3.3.0): https://github.com/apache/airflow/tree/1438ea3587031417cc85d74323235cf087a058fb/shared/listeners/src/airflow_shared/listeners/spec
- Task SDK listener manager (3.3.0): https://github.com/apache/airflow/blob/1438ea3587031417cc85d74323235cf087a058fb/task-sdk/src/airflow/sdk/listener.py
- pytest plugin documentation: https://docs.pytest.org/en/stable/how-to/writing_plugins.html
