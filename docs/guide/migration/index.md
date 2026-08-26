# Airflow 2->3 Migration

You are on Airflow 2.x, you have to be on 3.x, and the question you cannot answer is *what
breaks*. This tier answers it in layers, cheapest first -- each layer is blind along a
*different* axis, so the last one does not subsume the first. Run all of them.

The workflow spine, in order:

1. [Migration-strict mode](strict.md) -- promote Airflow's own 2->3 deprecation warnings to
   errors on the 2.x environment already in your CI
2. [Diffing outcomes across the upgrade](outcome-diff.md) -- record pass/fail on 2.x, compare
   against a real 3.x run
3. [Driving both families in one run](orchestrator.md) -- the `airflow-migration-diff` console
   script provisions both environments and runs the diff in one command
4. [Running both families in CI](orchestrator-in-ci.md) -- wiring that command into a workflow

Two supporting pages:

- [Pairing migration-strict with ruff's AIR rules](ruff-air-rules.md) -- the static layer below
  step 1: lint sees symbols no test executes
- [Baseline artifact contract](baseline-artifact.md) -- the recorded JSON's schema and the
  seven-category algorithm, for tooling built on `pytest_airflow_in_a_box.artifact`

2.x support is a temporary bridge -- see [Certification](../../internals/certification.md#airflow-2x-is-a-migration-bridge-not-a-second-home) -- so delete
this subtree once the cutover lands.
