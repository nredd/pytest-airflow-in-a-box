# dag_maker adopts upstream's deterministic run defaults

Issue #259 (the #227 round-3 scan) measured the residual `dag_maker` drift at ~200+
upstream-test failures per certified version, on both families, and traced it to three
defaults this plugin chose differently from upstream `tests_common`:

- No default Dag `start_date`: upstream injects one (explicit kwarg >
  `default_args["start_date"]` > the consumer test module's `DEFAULT_DATE` attribute >
  `2016-01-01` UTC), and upstream suites assert on `task.start_date` and build run dates
  from it -- a flat 49-50 failures/version were exactly
  `assert task.start_date is not None`.
- `logical_date` defaulted to `utcnow()`: a run dated today falls outside a Dag whose
  `start_date`/`end_date` window is upstream's 2016-vintage `DEFAULT_DATE`, so
  `verify_integrity` created ZERO task instances -- the empty `dr.task_instances` class.
- Derived run ids spelled `manual__pytest-airflow-in-a-box-<dag_id>-<hash>`: upstream
  defaults to `test` (no `run_type` passed) or the timetable-generated id (explicit
  `run_type`), and upstream tests look runs up by those.

ADR 0002 kept the hashed run id on purpose, calling it "the xdist collision mitigation
for same-`dag_id` contention". That claim does not hold up: `_default_run_id(dag_id,
invocation)` never encoded worker identity, and DagRun uniqueness is per `(dag_id,
run_id)` anyway -- collision safety always came entirely from the derived per-test
`dag_id`, never from the run-id spelling. A fixed `test` id under a unique `dag_id`
collides with nothing; under a caller-chosen repeated `dag_id` the contention exists
regardless of spelling and remains the documented `xdist_group` caveat.

We decided to adopt upstream's defaults wholesale in `dag_maker`:

- `dag_maker(...)` injects a default `start_date` via upstream's ladder, exposed as
  `dag_maker.start_date`. One deliberate deviation: an EXPLICIT `start_date=None` opts
  out of injection entirely (upstream silently replaces it with `DEFAULT_DATE`); only an
  absent key climbs the ladder. A module `DEFAULT_DATE` that is not a `datetime` is
  ignored rather than handed to Airflow's constructor to blow up far from its source.
- `create_dagrun()` without `logical_date` uses the Dag's resolved `start_date` for
  manual runs and the timetable's next-run info for explicit non-manual `run_type`s
  (`next_dagrun_info` -- keyword `last_automated_run_info` on 3.2+,
  `last_automated_dagrun` before), degrading to `start_date` then `utcnow()` when the
  timetable schedules nothing (`schedule=None`) or the Dag opted out of `start_date` --
  where upstream would crash on the `None` run info.
- `create_dagrun()` without `run_id` uses `test` when no `run_type` was passed, and
  `timetable.generate_run_id(...)` when one was -- keyed on the KEYWORD's presence, so
  an explicit `run_type=MANUAL` also gets a generated id, exactly as upstream.
- `data_interval` inference follows the run type: manual runs keep
  `infer_manual_data_interval`, non-manual runs use `infer_automated_data_interval`
  (module-level in `airflow.models.dag` on 3.1+, the instance method on 2.x). The run's
  default `start_date` and `run_after` also take upstream's shapes (the Dag's
  `start_date`, and the resolved interval's end).

`run_dag` keeps its derived `manual__pytest-airflow-in-a-box-...` ids and `utcnow()`
dating: it has no upstream analogue and adopts externally-authored Dags whose
`start_date` this plugin does not control.

Consequences: this supersedes ADR 0002's "run-id conventions stay divergent" clause --
the rest of 0002 (the authoring yield, the opt-in scheduler handles) stands. A second
bare `create_dagrun()` on one Dag now collides loudly on `(dag_id, "test")` exactly as
upstream does, where the invocation-counter ids used to make it silently unique -- pass
explicit `run_id`s for multi-run tests. And every Dag `dag_maker` builds now carries a
`start_date` unless the test explicitly passes `start_date=None`.
