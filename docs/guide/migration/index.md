# Start here

You are on Airflow 2.x, you have to be on 3.x, and the question you cannot answer is *what
breaks*. These four pages answer it in one order, cheapest first:

1. [Migration-strict mode](strict.md) -- promote Airflow's own 2->3 deprecation warnings to
   errors on the 2.x environment already in your CI. No second Airflow install, no second run
2. [Diffing outcomes across the upgrade](outcome-diff.md) -- record pass/fail on 2.x, compare
   it against a real 3.x run. Two runs, two environments, real outcomes
3. [Driving both families in one run](orchestrator.md) -- the `airflow-migration-diff` console
   script provisions both environments and runs step 2 for you, in one command
4. [Running both families in CI](orchestrator-in-ci.md) -- wiring that command into a workflow

Two supporting pages hang off that spine:

- [Pairing migration-strict with ruff's AIR rules](ruff-air-rules.md) -- the static layer below
  step 1, and the ruff config a dual-family repo actually wants
- [Baseline artifact contract](baseline-artifact.md) -- the recorded JSON's schema and the
  seven-category algorithm, for tooling built on `pytest_airflow_in_a_box.artifact`

Each layer is blind along a *different* axis, so the last one does not subsume the first.
ruff sees symbols no test executes; migration-strict sees warnings on executed paths; the diff
sees real pass/fail on code your tests exercise. Run all of them.

This whole subtree has an expiry date. It is a
[deliberate, temporary artifact](../testing-scope.md#the-one-exception-a-pre-upgrade-regression-suite)
-- delete it once the cutover lands.
