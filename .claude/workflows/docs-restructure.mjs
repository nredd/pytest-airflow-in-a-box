export const meta = {
  name: 'docs-restructure',
  description: 'Write every page of the agreed docs tree from the vision audit verdicts',
  phases: [
    { title: 'Write', detail: 'one agent per nav section, disjoint output files' },
    { title: 'Crosslink', detail: 'verify every internal link resolves against the final tree' },
  ],
}

const TREE = `Final agreed tree (docs/ root). EVERY path below must exist when this is done:

Getting started:  index.md, quickstart.md, install.md, compatibility.md
Why test:         why/index.md, why/dagbag-callable-gap.md, why/why-not.md
What to test:     guide/testing-scope.md, guide/cookbook.md
Rung by rung:     guide/ladder.md, guide/db-free-execution.md, guide/task-execution.md,
                  guide/deferrable-operators.md, guide/rest-api.md
Environment:      guide/airflow-home.md, guide/database.md, guide/configuration.md,
                  guide/seeding.md, guide/structlog.md, guide/cluster-policies.md
Corpus:           guide/smoke-tests.md, guide/dag-collection.md, guide/dag-coverage.md
CI:               guide/ci/github-action.md, guide/reports.md
Components:       guide/custom-components.md, guide/custom-components-wiring.md,
                  guide/custom-timetables.md, guide/isolated-tests.md
In the box:       reference/fixtures.md, reference/markers.md, reference/diagnostics.md
Migration:        guide/migration/index.md, guide/migration/strict.md,
                  guide/migration/ruff-air-rules.md, guide/migration/outcome-diff.md,
                  guide/migration/baseline-artifact.md, guide/migration/orchestrator.md,
                  guide/migration/orchestrator-in-ci.md
Under the hood:   internals/compat-layer.md, internals/bootstrap-env-ownership.md,
                  internals/tests-common-parity.md, internals/parse-time-secrets.md,
                  internals/dag-corpus.md, development.md

DELETED: reference/defaults.md (merged into guide/configuration.md)
ALREADY MOVED for you via git mv (edit in place, do not re-create):
  guide/migration-strict.md      -> guide/migration/strict.md
  guide/migration-diff.md        -> guide/migration/outcome-diff.md
  guide/migration-orchestrator.md-> guide/migration/orchestrator.md`

const RULES = `HOUSE RULES -- non-negotiable:

- Read docs/vision.md FIRST. It is the agreed spec: thesis, ICP, the fidelity ladder,
  feature tiers, and the cut/merge/split table naming your unit's destination
- Read the GitHub issue named in your task with \`gh issue view N\`. It is the per-page spec
- VERIFY EVERY TECHNICAL CLAIM against src/pytest_airflow_in_a_box/ and tests/ before you
  write it. Do not copy a claim forward from the old page without checking it -- the audit
  found several live false claims. If you cannot verify something, cut it rather than
  restate it
- DOCS ONLY. Do not touch src/, tests/, mkdocs.yml, or README.md. Another stage owns those
- Cross-links are relative .md paths from YOUR file's directory and are validated by
  \`mkdocs build --strict\`. A broken link fails the build. Only link to paths in the tree

VOICE -- this is Redd's repo, match it:
- Terse. If one line does it, write one line. Never pad to look thorough
- Bullets over prose. Colon-terminated lead-ins ("Changes:", "Cases:", "Need:")
- NEVER em-dashes or en-dashes. Use "--" or a comma. ASCII arrows "->" only
- Backtick every identifier, path, flag, and dotted module path
- "*asterisk italics*" on the one load-bearing word, never underscores
- No hedging, no filler adjectives, no closing summary paragraph, no emoji, no checkboxes
- Markdown headers ARE fine (these are docs pages)
- Lead with the reader's job or the failure prevented, never with mechanism or a flag
- Keep every code example runnable and honest about what it proves`

const TASKS = [
  { label: 'getting-started', issues: '#288, #287',
    files: 'index.md, quickstart.md, install.md, compatibility.md',
    brief: `Split docs/index.md per issue #288. index.md keeps the pitch and gains a link to
why/why-not.md; extract install mechanics to install.md and the whole version story to
compatibility.md. quickstart.md becomes the CANONICAL quickstart (README will include this
same fence later via pymdownx.snippets -- so write the fence to be includable, and put the
runnable example in one contiguous block).
compatibility.md is the SOLE owner of the Airflow 2.x tier question. The audit found that
claim contradicts itself across docs/index.md, CLAUDE.md and CONTEXT.md -- read all three,
read compat.yml, and state what is ACTUALLY tested. Do not restate the contradiction.
install.md precedes compatibility.md: you install, then \`--airflow-doctor\` tells you if your
pin works. Name \`--airflow-doctor\` explicitly -- it currently has zero mentions in index.md.` },

  { label: 'why', issues: '#309, #286',
    files: 'why/index.md, why/why-not.md',
    brief: `why/index.md comes from the README Manifesto (#309): keep the 500+ tasks framing,
cut employer-identifying detail. This is the "why do we test at all" on-ramp.
why/why-not.md (#286) is the competitive frame. Per docs/vision.md there are exactly THREE
claims: dag.test() (clears task instances and swallows task exceptions, so \`assert
result.success\` is not an assertion; \`use_executor=True\` queues workloads nothing serves --
apache/airflow#59074), DebugExecutor (does not exist on Airflow 3, and a reader searching
"DebugExecutor Airflow 3" must land somewhere), and a hand-rolled conftest.py (the real
competitor: bootstrap runs from \`pytest_load_initial_conftests\` and pytest's conftest
collector is \`trylast\`, so a consumer conftest structurally cannot win the race).
Drop the Flowminder and airflow-pytest-plugin bullets. The tests_common claim moves to
guide/testing-scope.md, so do NOT keep it here.
Do NOT write why/dagbag-callable-gap.md -- another agent owns it.` },

  { label: 'scope-cookbook', issues: '#289',
    files: 'guide/testing-scope.md, guide/cookbook.md, why/dagbag-callable-gap.md',
    brief: `testing-scope.md is verdict KEEP -- do not rewrite it. Two surgical edits only:
(1) cite the smoke-catalog "stock" carve-out so \`test_dag_serialization_roundtrip\` /
\`test_schedule_sanity\` running by default stops contradicting the out-of-scope list; read
smoke.py to get the carve-out right. (2) absorb the provider/tests_common boundary claim
from the README's "Why not" section.
Split cookbook.md per #289: the ARGUMENT (what a dagbag test and a callable test miss)
becomes why/dagbag-callable-gap.md, promoted to the on-ramp; the RECIPES stay in
cookbook.md. Promote \`evaluate_asset_schedules\` out of being recipe 7 of 7.
Also fix: testing-scope.md currently routes "custom components" at guide/custom-components.md
-- that page is being split, so point the link at the right half (the how-to, not the wiring).` },

  { label: 'ladder-core', issues: '#290, #291',
    files: 'guide/ladder.md, guide/db-free-execution.md, guide/task-execution.md, internals/tests-common-parity.md',
    brief: `guide/ladder.md is NEW and is the section landing page. Write the fidelity ladder
exactly as docs/vision.md §4 states it: rung 0 render_task, 1 run_task/task_context, 2
dag_maker+run_ti, 3 dag_maker.run()/run_dag, 4 executor=. For each rung: what it proves, what
it costs, what it CANNOT prove. State the climbing rule: stand on the lowest rung that can
still fail for the reason you care about. Note run_trigger and the REST API sit off the
ladder, and that corpus checking is a different axis entirely.
db-free-execution.md (#291): retitle to "One operator, no database". Lead with the THREE
silent breakages of a hand-rolled \`op.execute(mock_context)\` -- template fields never
rendered, no active \`get_current_context()\`, a MagicMock \`ti\` making \`ti.try_number > 1\`
truthy. All three are pinned in tests/fixtures/test_task_context.py -- read it. Move the 2.x
gate out of the opener. Do NOT split it.
task-execution.md (#290): split. Practitioner half stays (run_dag, executor=, dag_maker.run(),
DagRunResult, matchers, run_ti). The upstream tests_common parity contract (currently ~lines
247-406: upstream harness keywords, scheduler-side handles, ADR 0002 migration table, the
nine deviations) moves to internals/tests-common-parity.md.
Keep \`Executor-driven runs\` HIGH on the practitioner page, right after run_dag -- it is the
headline differentiator. State the two honest caveats ONCE, not twice: every instance is
attempted exactly once so \`retries\` never re-run, and the same-\`dag_id\` xdist race whose only
mitigation \`pytest.mark.xdist_group\` is inert outside \`--dist loadgroup\`.
FIX A LIVE BUG: task-execution.md cites \`--airflow-home-keep\`, which does not exist. The real
flag is \`--airflow-home-retention\` -- verify in plugin.py.` },

  { label: 'ladder-edges', issues: '#314, #315',
    files: 'guide/deferrable-operators.md, guide/rest-api.md',
    brief: `deferrable-operators.md (#314): lead with the renamed TriggerEvent payload key
mismatch -- the actual failure. State the single-shot limit plainly: run_trigger models the
first event and one resume, a poll-loop trigger is not modeled. Add the
\`run_triggerer=\`/\`executor=\` exclusion. Verify against taskinstance.py.
rest-api.md (#315): lead with base-url publication -- the job is testing YOUR code that
resolves \`conf.get("api", "base_url")\` or calls /api/v2, not asserting a stock endpoint
works. Compress the executor paragraph to a cross-link to guide/task-execution.md.` },

  { label: 'env-home-db', issues: '#302, #294',
    files: 'guide/airflow-home.md, guide/database.md',
    brief: `airflow-home.md (#302): lead with the isolation promise -- your real ~/airflow is
never touched. Fix the duplicated table header row and the dead \`--airflow-home-keep\` link
(real flag is \`--airflow-home-retention\`; verify in plugin.py).
database.md (#294): lead with isolation, demote backends (SQLite/Postgres) to LAST -- the
audit found the Postgres tier over-serves the ICP. Give \`clear_db\` real prose, and warn
plainly that it is SERIAL-ONLY: under \`-n auto\`, which is what CI runs, it is a footgun.
Verify against db.py and storage/.` },

  { label: 'env-config', issues: '#293, #306, #317',
    files: 'guide/configuration.md, internals/bootstrap-env-ownership.md, guide/cluster-policies.md',
    brief: `configuration.md (#293): LEAD WITH THE INI OPTION, not the context manager. The
audit's finding: \`airflow_config\` as a context manager is a thin wrapper over
\`monkeypatch.setenv\` (env is first in \`_lookup_sequence\`, no cache, no invalidation hook);
only the ini option is irreducible, because only it lands before the first DagBag parse.
Leading with the wrapper is dishonest about the core claim.
MERGE reference/defaults.md into this page (#306) and DELETE reference/defaults.md with
\`git rm\`. Two beats must survive intact or the merge is a regression: (1)
\`airflow_default_filterwarnings\` -- the bootstrap's \`catch_warnings()\` + \`simplefilter("default")\`
wipes the ini filter list, so a strict repo provably cannot silence alembic's deprecation
without it; (2) the plugin silently rewrites \`tbstyle\`, \`reportchars\`, and \`durations\`.
Verify both in defaults.py. Also give \`conf_vars\` exactly ONE sentence as a deprecated alias.
The cluster-policy ini option lands HERE, not in the deleted page.
internals/bootstrap-env-ownership.md is NEW: who owns AIRFLOW__*, bootstrap ordering, env
drift. This is the mechanism behind the moat, so write it properly and link it from
configuration.md.
cluster-policies.md (#317): demote into the environment cluster. Lead with the UsageError,
cut the ini-option apologia. Fix custom-components.md's dangling "above" reference if it
points here.` },

  { label: 'env-seed-log', issues: '#311, #313',
    files: 'guide/seeding.md, internals/parse-time-secrets.md, guide/structlog.md',
    brief: `seeding.md (#311): split. The honest framing the audit found: for the ordinary
case the fixtures LOSE to \`monkeypatch.setenv("AIRFLOW_CONN_DB", ...)\`. Say so. The part with
no substitute is the parse-time shim, which moves to internals/parse-time-secrets.md.
structlog.md (#313) is 13 lines today and badly undersells itself. Lead with the real
finding: on Airflow 3 \`caplog\` returns EMPTY, so a log assertion passes forever -- including
after you invert the branch. That is a silent-false-green, and it is currently one clause.
Name \`structlog.testing.capture_logs\`. Verify against fixtures/.
NOTE: \`StructlogCapture\` is missing from types.py while reference/fixtures.md promises every
return type lives there. That is a CODE fix owned by a later PR -- do NOT edit types.py.
Write the page against the intended contract and do not promise the type is exported.` },

  { label: 'corpus', issues: '#292, #312, #316',
    files: 'guide/smoke-tests.md, internals/dag-corpus.md, guide/dag-collection.md, guide/dag-coverage.md',
    brief: `smoke-tests.md (#292): split. The catalog stays; corpus parsing, parallelism, the
flock-guarded JSON artifact and \`dag_corpus\` internals move to internals/dag-corpus.md. Cite
the "stock" carve-out so the default checks stop contradicting guide/testing-scope.md.
Read CONTEXT.md -- it already defines SmokeCheck, SmokeContext, DagCorpus precisely. Match it.
dag-collection.md (#312): lead with per-file items, not the flag. State the honest overlap
with \`--airflow-smoke\`: identical messages, both parse, enabling both parses twice.
dag-coverage.md (#316): demote under the corpus section. FIX A FALSE CLAIM: it says there is
no subprocess. \`executor=\` runs supervised workers. Verify before writing.` },

  { label: 'ci', issues: '#298, #301',
    files: 'guide/ci/github-action.md, guide/reports.md',
    brief: `guide/ci/github-action.md is NEW (#298) and is the HIGHEST-VALUE page in this batch:
the GitHub Action is a published, v0-tagged, consumer-facing interface, and the README section
being moved here is currently its only complete documentation anywhere in the repo. Read
action.yml and action/ and document every input and output faithfully. Nothing may be lost.
FIX: reports.md points at \`action@main\`; the documented tag is \`@v0\`.
reports.md (#301): invert it. Lead with the two failure modes -- the xdist log race and the
isolated-child XML clobber -- then the flag that fixes them.` },

  { label: 'components', issues: '#295, #303',
    files: 'guide/custom-components.md, guide/custom-components-wiring.md, guide/custom-timetables.md, guide/isolated-tests.md',
    brief: `custom-components.md (#295) is 503 lines doing two jobs: split into the how-to
(checking a custom component -- \`check_component\`, conformance) and
guide/custom-components-wiring.md (wiring components into the run). Retitle away from the bare
phrase "Custom components". Verify against components.py.
custom-timetables.md is verdict KEEP -- leave it alone except for fixing links to paths that
moved.
isolated-tests.md (#303): demote to the last leaf here. Name \`uv pip install -e .\` as the more
faithful test.` },

  { label: 'reference', issues: '#304, #305',
    files: 'reference/markers.md, reference/diagnostics.md',
    brief: `markers.md (#304): lead with the GATING JOB each marker does, order by gate
precedence, and document the \`environment\` ini grammar (currently undocumented). Verify
against plugin.py MARKER_DESCRIPTIONS.
diagnostics.md (#305): lead with the false-green that \`--airflow-doctor\`'s \`--cov\` containment
check catches. Paste a REAL report -- run \`uv run pytest --airflow-doctor\` and use its actual
output, do not invent one. Verify against doctor.py.
NOTE: reference/fixtures.md is verdict KEEP -- do not rewrite it. Only fix links to moved paths.` },

  { label: 'migration', issues: '#318, #319, #320, #310',
    files: 'guide/migration/index.md, guide/migration/strict.md, guide/migration/ruff-air-rules.md, guide/migration/outcome-diff.md, guide/migration/baseline-artifact.md, guide/migration/orchestrator.md, guide/migration/orchestrator-in-ci.md',
    brief: `The whole Airflow 2->3 subtree. strict.md, outcome-diff.md and orchestrator.md are
ALREADY at these paths via git mv -- edit in place, do not create them.
guide/migration/index.md is NEW and is the section landing page. It states the funnel order
ONCE: strict -> outcome-diff -> orchestrator -> orchestrator-in-ci. mkdocs section labels hold
no prose, which is exactly why this page exists.
strict.md (#318): split -- pairing migration-strict with ruff's AIR rules moves to
ruff-air-rules.md.
outcome-diff.md (#319): split -- the baseline artifact CONTRACT moves to baseline-artifact.md.
orchestrator.md (#320): lead with the provisioning problem it solves. Cut "What it does not do"
to one sentence.
orchestrator-in-ci.md (#310) is NEW, from the README's migration-orchestrator section: running
both Airflow families in one CI job. Name the \`airflow-migration-diff\` console script -- it is
the only console script and \`pytest --help\` will never show it.
Verify everything against scripts/ and the migration modules in src/.` },

  { label: 'compat-layer', issues: '#284',
    files: 'internals/compat-layer.md',
    brief: `internals/compat-layer.md is NEW. The audit's finding: \`_compat/\` version shielding
is claimed as one of the five reasons anyone installs this plugin, and it has ZERO
documentation surface today. Fix that.
Read src/pytest_airflow_in_a_box/_compat/ in full, especially capabilities.py. Document: what
_compat/ absorbs (how many private Airflow modules, across which versions), how capability
probes work, why the boundary exists, the rule that any use of Airflow internals goes behind a
probe, and how tests/enduser/ is the consumer contract. Read PROVENANCE.md and CLAUDE.md.
This page is linked from why/why-not.md as the mechanism behind the moat, so it must be
readable by someone evaluating the plugin, not only by a contributor.` },
]

phase('Write')
log(`Writing ${TASKS.length} sections across the agreed tree`)

const written = await parallel(
  TASKS.map((t) => () =>
    agent(
      `You are writing part of an agreed docs restructure for pytest-airflow-in-a-box.

${RULES}

${TREE}

YOUR FILES (you own these and ONLY these -- another agent owns every other path):
${t.files}

YOUR ISSUES: ${t.issues}

YOUR BRIEF:
${t.brief}

Write the files now with the Write/Edit tools. When done, return a short plain-text report:
which paths you created, which you edited, which you deleted, and any claim from the old page
you dropped because you could not verify it.`,
      { label: `write:${t.label}`, phase: 'Write' },
    ),
  ),
)

phase('Crosslink')

const reports = TASKS.map((t, i) => `[${t.label}] ${written[i] || 'FAILED'}`).join('\n\n')

const linkFix = await agent(
  `The docs restructure below has just been written by ${TASKS.length} parallel agents, each
blind to the others. Your job is to make \`mkdocs build --strict\` pass and the tree cohere.

${TREE}

Agent reports:
${reports}

Do this:
1. \`ls -R docs/\` and confirm every path in the tree exists and nothing extra was created.
   Report anything missing or unexpected
2. Find EVERY internal markdown link in docs/ and verify its target resolves relative to the
   linking file. The three migration pages and the two new subdirectories moved, so inbound
   links from anywhere in docs/ are the likely breakage. Fix them
3. Confirm reference/defaults.md is deleted and nothing still links to it
4. Fix any link pointing at guide/migration-strict.md, guide/migration-diff.md, or
   guide/migration-orchestrator.md -- those paths no longer exist
5. Check for duplicated content across the split pairs -- if two pages now both explain the
   same mechanism at length, leave the better one and cut the other to a cross-link
6. Do NOT touch mkdocs.yml or README.md

Return a plain-text list of every link you fixed and every problem you could not fix.`,
  { label: 'crosslink:verify', phase: 'Crosslink' },
)

return { written: TASKS.map((t, i) => ({ section: t.label, ok: Boolean(written[i]) })), linkFix }
