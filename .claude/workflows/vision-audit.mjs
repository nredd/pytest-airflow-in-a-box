export const meta = {
  name: 'vision-audit',
  description: 'Adversarially audit every docs page and README section, then synthesize a vision doc + nav tree',
  whenToUse: 'When sharpening the plugin thesis, ICP, or docs information architecture',
  phases: [
    { title: 'Ground', detail: 'establish the currently-stated thesis and ICP baseline' },
    { title: 'Debate', detail: '2 defend + 1 prosecute + 1 affinity agent per unit' },
    { title: 'Adjudicate', detail: 'one verdict per unit from the four briefs' },
    { title: 'Synthesize', detail: 'positioning, architecture, ladder, methodology' },
    { title: 'Critic', detail: 'coherence and completeness pass over the syntheses' },
    { title: 'Author', detail: 'write docs/vision.md and the proposed nav block' },
  ],
}

// ---------------------------------------------------------------------------
// Units under trial: 27 docs pages (mkdocs nav) + 11 README sections.
// ---------------------------------------------------------------------------

const GUIDE = [
  ['testing-scope', 'What to test'],
  ['task-execution', 'Task execution'],
  ['deferrable-operators', 'Deferrable operators'],
  ['db-free-execution', 'DB-free task execution'],
  ['seeding', 'Seeding Variables and Connections'],
  ['structlog', 'Structlog capture'],
  ['dag-collection', 'Dag-file collection'],
  ['dag-coverage', 'Dag coverage'],
  ['configuration', 'Airflow configuration'],
  ['smoke-tests', 'Smoke tests'],
  ['custom-components', 'Custom components'],
  ['custom-timetables', 'Custom timetables'],
  ['isolated-tests', 'Isolated entry-point tests'],
  ['reports', 'Report artifacts'],
  ['migration-strict', 'Migration-strict mode'],
  ['database', 'Database'],
  ['airflow-home', 'The isolated AIRFLOW_HOME'],
  ['cluster-policies', 'Cluster policies'],
  ['rest-api', 'Live REST API'],
  ['migration-diff', 'Migration outcome diff'],
  ['migration-orchestrator', 'Migration diff orchestrator'],
  ['cookbook', 'Cookbook'],
]

const REFERENCE = [
  ['fixtures', 'Fixtures'],
  ['markers', 'Markers'],
  ['defaults', 'Defaults'],
  ['diagnostics', 'Diagnostics'],
]

const README_SECTIONS = [
  ['readme-quickstart', 'Quickstart', 39, 129],
  ['readme-fixtures', 'Fixtures', 130, 158],
  ['readme-markers', 'Markers', 159, 175],
  ['readme-why-not', 'Why not...', 176, 192],
  ['readme-requirements', 'Requirements', 193, 217],
  ['readme-installation', 'Installation', 218, 276],
  ['readme-github-action', 'GitHub Action', 277, 342],
  ['readme-migration-orchestrator', 'Migration diff orchestrator', 343, 357],
  ['readme-documentation', 'Documentation', 358, 365],
  ['readme-development', 'Development', 366, 384],
  ['readme-manifesto', 'Manifesto', 389, 400],
]

const UNITS = [
  { slug: 'index', path: 'docs/index.md', title: 'Home', kind: 'page', navSection: 'Home' },
  ...GUIDE.map(([s, t]) => ({ slug: s, path: `docs/guide/${s}.md`, title: t, kind: 'page', navSection: 'Guide' })),
  ...REFERENCE.map(([s, t]) => ({ slug: `reference-${s}`, path: `docs/reference/${s}.md`, title: t, kind: 'page', navSection: 'Reference' })),
  ...README_SECTIONS.map(([s, t, from, to]) => ({ slug: s, path: 'README.md', title: t, kind: 'readme-section', lines: [from, to] })),
]

const TITLE_LIST = UNITS.map((u) => `- ${u.slug} (${u.kind}): "${u.title}" -- ${u.path}`).join('\n')

const LAYOUT_HINT = `Source layout (src/pytest_airflow_in_a_box/): plugin.py, bootstrap.py, airflow_cfg.py,
config.py, db.py, components.py, defaults.py, ini_config.py, collection.py, smoke.py, doctor.py,
dagcorpus.py, taskinstance.py, types.py, fixtures/, storage/, _compat/. tests/ mirrors src/.
Use grep/rg to find the module(s) and tests backing this unit -- there is no hardcoded map.`

function unitRef(u) {
  return u.kind === 'readme-section'
    ? `README.md section "## ${u.title}" (lines ${u.lines[0]}-${u.lines[1]}; read those lines plus ~30 lines of surrounding context)`
    : `the docs page ${u.path} ("${u.title}", nav section: ${u.navSection})`
}

// ---------------------------------------------------------------------------
// Schemas
// ---------------------------------------------------------------------------

const BRIEF_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['claim', 'evidence', 'strength'],
  properties: {
    claim: { type: 'string', description: 'One-sentence thesis of this brief' },
    evidence: {
      type: 'array',
      minItems: 1,
      items: {
        type: 'object',
        additionalProperties: false,
        required: ['point', 'source'],
        properties: {
          point: { type: 'string' },
          source: { type: 'string', description: 'file path, path:line, or named alternative tool' },
        },
      },
    },
    strength: { type: 'string', enum: ['weak', 'moderate', 'strong'] },
    concession: { type: 'string', description: 'The strongest point for the opposing side' },
  },
}

const NARRATIVE_STAGES = ['why-test', 'how-to-test', 'what-to-test', 'in-the-box', 'how-deep-the-box-goes']
const AUDIENCES = ['newcomer', 'practitioner', 'extender', 'contributor']

const AFFINITY_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['slug', 'narrativeStage', 'audience', 'proposedParent', 'siblings', 'depth', 'rationale'],
  properties: {
    slug: { type: 'string' },
    narrativeStage: { type: 'string', enum: NARRATIVE_STAGES },
    audience: { type: 'string', enum: AUDIENCES },
    proposedParent: { type: 'string', description: 'Proposed nav parent node title, invented if needed' },
    siblings: { type: 'array', items: { type: 'string' }, description: 'slugs that belong under the same parent' },
    depth: { type: 'integer', minimum: 1, maximum: 4 },
    splitInto: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        required: ['title', 'stage', 'what'],
        properties: { title: { type: 'string' }, stage: { type: 'string', enum: NARRATIVE_STAGES }, what: { type: 'string' } },
      },
      description: 'Only when the unit does two jobs at two depths and should become two nodes',
    },
    rationale: { type: 'string' },
  },
}

const VERDICT_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['slug', 'kind', 'verdict', 'confidence', 'rationale', 'jobSentence', 'tier', 'narrativeStage', 'audience', 'proposedParent'],
  properties: {
    slug: { type: 'string' },
    kind: { type: 'string', enum: ['page', 'readme-section'] },
    verdict: { type: 'string', enum: ['keep', 'rewrite', 'merge', 'demote', 'move-to-docs', 'split', 'cut'] },
    mergeInto: { type: 'string', description: 'target slug when verdict is merge' },
    splitInto: { type: 'array', items: { type: 'string' }, description: 'proposed node titles when verdict is split' },
    confidence: { type: 'string', enum: ['low', 'medium', 'high'] },
    rationale: { type: 'string' },
    jobSentence: { type: 'string', description: 'The one-line job-to-be-done this unit should lead with' },
    tier: { type: 'string', enum: ['core', 'supporting', 'peripheral'] },
    narrativeStage: { type: 'string', enum: NARRATIVE_STAGES },
    audience: { type: 'string', enum: AUDIENCES },
    proposedParent: { type: 'string' },
  },
}

const ARCH_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['navBlock', 'placedSlugs', 'readingOrder', 'deepEndPolicy', 'notes'],
  properties: {
    navBlock: { type: 'string', description: 'Paste-ready mkdocs.yml nav: block, valid YAML' },
    placedSlugs: { type: 'array', items: { type: 'string' }, description: 'every slug placed in navBlock, exactly once each' },
    readingOrder: { type: 'array', items: { type: 'string' }, description: 'slugs in the order a newcomer reads top to bottom' },
    deepEndPolicy: { type: 'string', description: 'where deep design reference, ADRs and internals live' },
    notes: { type: 'string' },
  },
}

const PROSE_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['markdown'],
  properties: { markdown: { type: 'string' } },
}

const ISSUES_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['issues'],
  properties: {
    issues: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        required: ['title', 'body', 'labels'],
        properties: {
          title: { type: 'string' },
          body: { type: 'string' },
          labels: { type: 'array', items: { type: 'string' } },
        },
      },
    },
  },
}

// ---------------------------------------------------------------------------
// Phase 1 -- Ground
// ---------------------------------------------------------------------------

phase('Ground')
log(`${UNITS.length} units under trial (${UNITS.filter((u) => u.kind === 'page').length} docs pages, ${README_SECTIONS.length} README sections)`)
log('Excluded on purpose: README "## Contents" (generated TOC) and "## License" (legally required, not a product decision)')

const ground = await agent(
  `You are establishing the baseline for an adversarial audit of the pytest-airflow-in-a-box docs.

Read, completely: README.md, docs/index.md, CONTEXT.md, docs/guide/testing-scope.md, mkdocs.yml,
and CLAUDE.md.

Return a dense factual brief -- no recommendations, no opinions -- covering:
- The thesis the project currently states about itself, quoted
- The ICP it claims or implies: who is the reader, what is their job, what scale of Airflow repo
- The scope boundary it asserts ("test the Airflow code you wrote") and exactly where it draws it
- Every alternative or competitor the docs name or allude to (Airflow's dag.test(), DebugExecutor,
  hand-rolled conftest, other plugins), and the stated reason each is insufficient
- The current mkdocs nav ordering, verbatim
- Any place where README.md and docs/index.md disagree with each other or with testing-scope.md

Be specific and cite file:line. This brief is injected into ~150 downstream agents, so it must be
accurate and self-contained. Aim for 800-1500 words.`,
  { label: 'ground:baseline', phase: 'Ground' },
)

if (!ground) throw new Error('Ground brief failed -- cannot run the audit without a shared baseline')

const BASE = `--- SHARED BASELINE (current stated positioning) ---\n${ground}\n--- END BASELINE ---`

// ---------------------------------------------------------------------------
// Phase 2 -- Debate (2 defend + 1 prosecute + 1 affinity) then Adjudicate
// ---------------------------------------------------------------------------

phase('Debate')

function defendJob(u) {
  return agent(
    `${BASE}

Read ${unitRef(u)}. Then grep-locate and read the src/ module(s) and tests backing it.
${LAYOUT_HINT}

Your job: DEFEND this unit on the JOB-TO-BE-DONE lens. Argue it earns its place because a real
user has a real job it does. Be concrete:
- Name the user and the job. Not "developers" -- a specific person with a specific Airflow repo
- Say exactly what breaks, silently or loudly, for that user if this did not exist
- Point at the code that delivers the job, by path
- Say how often that job comes up: every test run, once per repo, once per migration, never

Do NOT argue about docs quality or wording. Argue about whether the underlying capability serves
a job. Concede the strongest point against you honestly.`,
    { label: `defend:job:${u.slug}`, phase: 'Debate', schema: BRIEF_SCHEMA },
  )
}

function defendMoat(u) {
  return agent(
    `${BASE}

Read ${unitRef(u)}. Then grep-locate and read the src/ module(s) and tests backing it.
${LAYOUT_HINT}

Your job: DEFEND this unit on the DIFFERENTIATION lens. You must name the alternative and beat it.
- What would the reader hand-roll in their own conftest.py to get this? Sketch it. How many lines,
  and which Airflow internals would they have to touch?
- Which existing tool already covers this -- Airflow's own dag.test(), DebugExecutor, the
  airflow CLI, pytest-mock, a homegrown fixture, another plugin? Name it
- Why does this plugin's version win: version-compat shielding, typing, isolation, speed,
  determinism, or nothing at all
- If the honest answer is "the alternative is fine and this adds little", SAY SO and mark
  strength "weak". A defense that cannot name a beaten alternative is a weak defense

Concede the strongest point against you honestly.`,
    { label: `defend:moat:${u.slug}`, phase: 'Debate', schema: BRIEF_SCHEMA },
  )
}

function prosecute(u) {
  const readmeAngle = `This is a README section. README real estate is the scarcest surface the project has --
it is the first screen, and every section pushes the next one below the fold. The charge here is
as much "this belongs in docs/, not here" as it is "delete it". Argue the attention budget.`
  const pageAngle = `This is a docs page. The site is currently a flat blob of 22 sibling Guide pages -- every
page you spare is a page competing with onboarding for a newcomer's attention. Argue the tree budget.`

  return agent(
    `${BASE}

Read ${unitRef(u)}. Then grep-locate and read the src/ module(s) and tests backing it.
${LAYOUT_HINT}

Your job: PROSECUTE. Argue this unit should be cut, merged into another unit, demoted out of the
primary path, or moved. You are not a neutral reviewer -- you are the case for removal.

${u.kind === 'readme-section' ? readmeAngle : pageAngle}

Build the case from evidence, not vibes:
- Maintenance cost: measure it. Lines in the implementing module, lines of test, how much
  _compat/ surface it drags in, how many CI legs it touches
- Overlap: name the other unit(s) from the list below that already cover this ground
- ICP fit: does the baseline's user actually have this problem, or is this a capability that
  exists because it was buildable rather than because it was needed?
- Fidelity: is this a feature or a footnote? Would a reader ever land here on purpose?

Default to "cut" when the case for keeping is thin. Only decline to prosecute -- strength "weak"
-- when the unit is genuinely load-bearing, and then say precisely why.

Full unit list for overlap analysis:
${TITLE_LIST}

Concede the strongest point against you honestly.`,
    { label: `prosecute:${u.slug}`, phase: 'Debate', schema: BRIEF_SCHEMA },
  )
}

function place(u) {
  return agent(
    `${BASE}

Read ${unitRef(u)}. Skim -- you do not need the source, you need the SHAPE of what this covers.

Your job: IGNORE whether this unit should exist. Answer only: WHERE DOES IT BELONG IN THE TREE?

The docs site today is a flat blob -- one "Guide" section with 22 peers, mixing first-hour
onboarding with deep design reference at the same depth. We are rebuilding it as a tree that
tells one end-user story, in five stages:

  why-test           -- why bother testing Airflow code at all; what goes wrong without it
  how-to-test        -- the mechanics and the fidelity ladder (run_task -> dag_maker + run_ti ->
                        run_dag -> live REST API); how you actually drive a test
  what-to-test       -- scope: which of your code earns a test, and where the line falls
  in-the-box         -- what ships: fixtures, markers, smoke checks, options, reports
  how-deep-the-box-goes -- the basement: design reference, compat internals, migration tooling,
                        extension points for people bending the plugin

Return, for THIS unit:
- narrativeStage: which of the five it belongs to. Pick one. If it straddles two, that is a
  signal to use splitInto
- audience: newcomer / practitioner / extender / contributor
- proposedParent: the nav parent node it should sit under. INVENT the parent title if the right
  one does not exist today -- do not force it into "Guide"
- siblings: slugs from the list below that belong under that same parent
- depth: 1-4, how deep in the tree this node sits
- splitInto: fill this ONLY when the unit visibly does two jobs at two depths -- a shallow
  how-to and a deep reference welded together. Large pages are prime suspects but size alone is
  not the test; two audiences is the test

Full unit list, to reason about neighbors:
${TITLE_LIST}`,
    { label: `place:${u.slug}`, phase: 'Debate', schema: AFFINITY_SCHEMA },
  )
}

const results = await pipeline(
  UNITS,
  (u) => parallel([() => defendJob(u), () => defendMoat(u), () => prosecute(u), () => place(u)]),
  (briefs, u) => {
    const [job, moat, pros, aff] = briefs
    return agent(
      `${BASE}

You are the ADJUDICATOR for ${unitRef(u)}.

Four agents examined it independently. Weigh them. They are advocates, not referees -- discount
a brief whose evidence is thin, and do not split the difference just to be fair.

DEFENSE (job-to-be-done):
${JSON.stringify(job, null, 2)}

DEFENSE (differentiation):
${JSON.stringify(moat, null, 2)}

PROSECUTION:
${JSON.stringify(pros, null, 2)}

PLACEMENT:
${JSON.stringify(aff, null, 2)}

Read ${unitRef(u)} yourself before deciding.

Return one verdict:
- keep: earns its place as-is
- rewrite: capability is sound, the page does not sell it -- lead with the wrong thing
- merge: fold into another unit (set mergeInto to that slug)
- demote: keep the capability, move it out of the primary reading path
- move-to-docs: README section that belongs in docs/ (valid ONLY for kind readme-section)
- split: one unit doing two jobs at two depths (set splitInto)
- cut: the capability or the page should go away

Also carry the placement fields forward. You MAY overrule the placement agent -- if you do, say
so explicitly in rationale.

jobSentence is the single sentence this unit should lead with, phrased as the reader's job, not
the feature's name. Write it in Redd's voice: terse, concrete, no marketing.`,
      { label: `adjudicate:${u.slug}`, phase: 'Adjudicate', schema: VERDICT_SCHEMA, effort: 'low' },
    )
  },
)

const verdicts = results.filter(Boolean)

const missing = UNITS.filter((u) => !verdicts.some((v) => v.slug === u.slug)).map((u) => u.slug)
if (missing.length) log(`WARNING: ${missing.length} unit(s) produced no verdict: ${missing.join(', ')}`)
const noJob = verdicts.filter((v) => !v.jobSentence || !v.narrativeStage).map((v) => v.slug)
if (noJob.length) log(`WARNING: verdicts missing jobSentence/narrativeStage: ${noJob.join(', ')}`)

const tally = verdicts.reduce((acc, v) => ({ ...acc, [v.verdict]: (acc[v.verdict] || 0) + 1 }), {})
log(`Verdicts: ${Object.entries(tally).map(([k, n]) => `${k}=${n}`).join(' ')}`)

const TABLE = JSON.stringify(verdicts, null, 1)
const SURVIVORS = verdicts.filter((v) => v.verdict !== 'cut' && v.verdict !== 'merge')

// ---------------------------------------------------------------------------
// Phase 3 -- Synthesize
// ---------------------------------------------------------------------------

phase('Synthesize')

const synthBase = `${BASE}

--- ADJUDICATED VERDICTS (${verdicts.length} units) ---
${TABLE}
--- END VERDICTS ---`

const [positioning, architectureRaw, ladder, methodology] = await parallel([
  () =>
    agent(
      `${synthBase}

You own POSITIONING. Using the verdict table, answer, hard and unhedged:
- Who is this plugin for? One ICP, named concretely -- team shape, repo size, Airflow version,
  what their test suite looks like today
- Who is it explicitly NOT for? Say the names out loud
- The thesis, in one sentence. Not a feature list
- The competitive frame: against Airflow's own dag.test()/DebugExecutor and against a hand-rolled
  conftest, what is the actual claim?
- The README's first screen: what must the reader see in the first 30 lines, and in what order do
  the surviving README sections run? Use the readme-* verdicts

Where the verdicts contradict the current stated positioning, say so plainly. If the honest
conclusion is that the project is two products wedged into one box, say THAT.

Write in Redd's voice: terse, bullets over prose, "--" never em-dashes, backtick identifiers, no
headers-as-template. 600-1200 words.`,
      { label: 'synth:positioning', phase: 'Synthesize' },
    ),
  () =>
    agent(
      `${synthBase}

You own INFORMATION ARCHITECTURE. Build the tree.

The site today is a flat blob: one "Guide" with 22 siblings. Replace it with a tree that walks the
reader through five stages -- why-test, how-to-test, what-to-test, in-the-box, how-deep-the-box-goes
-- using the narrativeStage / audience / proposedParent fields every verdict carries.

Rules, non-negotiable:
- Every surviving slug (verdict is not "cut" and not "merge") appears EXACTLY ONCE in navBlock
- No cut or merged slug appears at all
- Every "split" verdict expands into its two nodes
- README sections are NOT nav entries -- but a "move-to-docs" README section becomes a new page,
  so place it and name the file it should become
- Section titles are reader-facing job names, not internal feature names
- navBlock must be valid YAML, paste-ready into mkdocs.yml, using real docs/ paths. For split
  nodes and new pages, use the path they should get

Also return:
- placedSlugs: every slug you placed, so it can be checked mechanically
- readingOrder: the slugs a newcomer reads top to bottom on their first day
- deepEndPolicy: where the basement goes -- deep design reference, docs/adr/, _compat internals,
  migration tooling -- so it stops competing with onboarding

Surviving slugs you must place (${SURVIVORS.length}):
${SURVIVORS.map((v) => `${v.slug} [${v.narrativeStage}/${v.audience}/${v.tier}] parent=${v.proposedParent}`).join('\n')}`,
      { label: 'synth:architecture', phase: 'Synthesize', schema: ARCH_SCHEMA },
    ),
  () =>
    agent(
      `${synthBase}

You own the FEATURE LADDER. Using the tier and verdict fields:
- The irreducible core: what must exist for this to be worth installing at all? Be brutal -- if
  the core is more than 5 things it is not a core
- Supporting: earns its keep, but nobody installs the plugin for it
- Peripheral: exists, costs maintenance, serves few. Say what should happen to each
- The progressive disclosure the docs tree must enforce: what does a reader meet in hour one,
  week one, month one?
- The merge/cut list, with a concrete target for every merge
- The README-to-docs/ split line: what stays on the front page and what moves

Where a "core" feature has a weak defense in the verdicts, flag it -- that is the most important
finding you can produce.

Redd's voice: terse, bullets, "--" not em-dashes, backticked identifiers. 600-1200 words.`,
      { label: 'synth:ladder', phase: 'Synthesize' },
    ),
  () =>
    agent(
      `${synthBase}

You own METHODOLOGY -- the "why do we test / what can we test" spine.

Read docs/guide/testing-scope.md, docs/guide/db-free-execution.md, docs/guide/task-execution.md
and docs/reference/fixtures.md before answering.

Articulate the test philosophy this plugin actually asserts, as a doctrine a reader could follow:
- Why test Airflow code at all: what class of bug this catches that a live deployment catches
  late and expensively
- The FIDELITY LADDER, as rungs: run_task (DB-free) -> dag_maker + run_ti (persisted) ->
  run_dag (full run) -> live REST API. For each rung: what it proves, what it costs, what it
  cannot prove, and the rule for climbing to the next one
- What can be tested vs what cannot, and the "if it fails, is the bug yours?" boundary -- is that
  test actually the right test?
- Where corpus-level checking (smoke checks, dag_corpus, dag coverage) sits: is it a rung on the
  same ladder or a different axis entirely? Decide
- Where TODAY'S docs contradict this doctrine -- pages that push a reader up the ladder when a
  lower rung would do, or that never mention the ladder exists

This is the spine the nav tree hangs off, so be structural, not decorative.

Redd's voice: terse, bullets, "--" not em-dashes, backticked identifiers. 800-1400 words.`,
      { label: 'synth:methodology', phase: 'Synthesize' },
    ),
])

// Mechanical check on the nav tree, with one re-prompt.
let architecture = architectureRaw
function navProblems(arch) {
  if (!arch) return ['architecture agent returned nothing']
  const placed = arch.placedSlugs || []
  const want = new Set(SURVIVORS.map((v) => v.slug))
  const problems = []
  const dupes = placed.filter((s, i) => placed.indexOf(s) !== i)
  if (dupes.length) problems.push(`placed more than once: ${[...new Set(dupes)].join(', ')}`)
  const orphaned = [...want].filter((s) => !placed.includes(s))
  if (orphaned.length) problems.push(`surviving but unplaced: ${orphaned.join(', ')}`)
  const ghosts = placed.filter((s) => !want.has(s))
  if (ghosts.length) problems.push(`placed but cut/merged or unknown: ${ghosts.join(', ')}`)
  return problems
}

const problems = navProblems(architecture)
if (problems.length) {
  log(`Nav tree failed the placement check, re-prompting once: ${problems.join(' | ')}`)
  const retry = await agent(
    `Your proposed mkdocs nav tree failed a mechanical placement check.

Problems:
${problems.map((p) => `- ${p}`).join('\n')}

Your previous answer:
${JSON.stringify(architecture, null, 1)}

Surviving slugs that must each appear EXACTLY ONCE (${SURVIVORS.length}):
${SURVIVORS.map((v) => `${v.slug} [${v.narrativeStage}/${v.audience}/${v.tier}]`).join('\n')}

Return a corrected tree. Same rules as before. Fix the placement, keep the structure you had
wherever it was already sound.`,
    { label: 'synth:architecture:retry', phase: 'Synthesize', schema: ARCH_SCHEMA },
  )
  if (retry && navProblems(retry).length <= problems.length) architecture = retry
  const still = navProblems(architecture)
  if (still.length) log(`Nav tree STILL imperfect after retry -- flagged for human review: ${still.join(' | ')}`)
}

// ---------------------------------------------------------------------------
// Phase 4 -- Critic
// ---------------------------------------------------------------------------

phase('Critic')

const SYNTH = `POSITIONING:
${positioning}

ARCHITECTURE:
${JSON.stringify(architecture, null, 1)}

LADDER:
${ladder}

METHODOLOGY:
${methodology}`

const criticNotes = await agent(
  `${synthBase}

Four synthesis agents worked from the same verdict table without seeing each other's output.
You are the completeness and coherence critic. Be adversarial -- your value is what they missed.

${SYNTH}

Do the MECHANICAL pass first, and report failures concretely:
- Does every surviving slug appear exactly once in the nav block? Name orphans and duplicates
- Does any cut or merged slug still appear in the tree?
- Do POSITIONING's README shape and ARCHITECTURE's nav disagree about what is core?
- Is METHODOLOGY's fidelity ladder the same ladder as ARCHITECTURE's how-to-test branch, rung for
  rung? Name any mismatch
- Does LADDER's "core" match the tier fields in the verdict table?

Then the JUDGMENT pass:
- Where do the four syntheses contradict each other? Quote both sides
- Which verdicts did no synthesis act on?
- What can a newcomer still not find? Walk the proposed tree as a first-day reader and name the
  question that has no answer
- What is the single weakest claim across all four, and why

Do not summarize them back. Only what is wrong, missing, or contradictory.
Redd's voice: terse, bullets, "--" not em-dashes.`,
  { label: 'critic:coherence', phase: 'Critic' },
)

// ---------------------------------------------------------------------------
// Phase 5 -- Author
// ---------------------------------------------------------------------------

phase('Author')

const [visionDoc, issueSet] = await parallel([
  () =>
    agent(
      `${synthBase}

${SYNTH}

CRITIC NOTES (resolve these -- do not paper over them):
${criticNotes}

Write docs/vision.md. Do NOT create the file -- return its full markdown as the "markdown" field.

Structure, mirroring the end-user story:
1. The thesis -- one sentence, then a short paragraph
2. Who it's for / who it's not for
3. Why test -- the doctrine, from METHODOLOGY
4. How to test -- the fidelity ladder, rung by rung, with the rule for climbing
5. What you can test -- the scope boundary
6. What's in the box -- the feature tiers: core, supporting, peripheral
7. How deep the box goes -- the design-reference floor and who it is for
8. The proposed docs tree -- the nav block in a fenced yaml block, plus the newcomer reading order
9. Cut / merge / split list -- a table of every non-keep verdict with its target

Voice, load-bearing -- this is Redd's repo and the doc must read like Redd wrote it:
- Terse. If one line does it, one line
- Bullets over prose. Colon-terminated lead-ins ("Changes:", "Cases:", "Need:")
- NEVER em-dashes or en-dashes. Use "--" or a comma. ASCII arrows "->"
- Backtick every identifier, path, and dotted module path
- "*asterisk italics*" on the one load-bearing word, never underscores
- No hedging, no filler adjectives, no closing summary paragraph, no emoji
- Markdown headers ARE fine here (this is a docs page, not a commit message)
- Start with an H1 "# Vision"

State conclusions, do not survey options. Where the audit found the project is doing two things,
say so in the doc.`,
      { label: 'author:vision-doc', phase: 'Author', schema: PROSE_SCHEMA },
    ),
  () =>
    agent(
      `${synthBase}

${SYNTH}

CRITIC NOTES:
${criticNotes}

Draft the GitHub issues this audit implies. Do NOT file them -- return them as data.

One issue per non-keep verdict (rewrite / merge / demote / move-to-docs / split / cut), plus
exactly one umbrella issue for the nav restructure that references the proposed tree.

Each issue:
- title: capitalized imperative, no trailing period, no conventional-commit prefix. e.g.
  "Merge \`dag-collection\` into \`dag-coverage\`", "Split \`custom-components\` into a how-to and a reference"
- body: Redd's voice. NO markdown headers. NO checkbox lists. A bare colon-terminated lead-in
  ("Changes:", "Need:", "Cases:") then "-" bullets, one concrete change each, phrased "Verb the
  \`thing\`". Backtick every path and identifier. "--" never em-dashes. Include the verdict's
  rationale as the why, and name the source page path. Keep it under ~15 lines
- labels: from this repo's vocabulary -- "needs-triage", "needs-info", "ready-for-agent",
  "ready-for-human", "wontfix". Use "ready-for-human" for positioning-level rewrites and anything
  that deletes user-facing capability; "ready-for-agent" for mechanical merges, moves and splits.
  Add "documentation" where it fits

Order them: umbrella first, then highest-confidence/highest-impact first.`,
      { label: 'author:issues', phase: 'Author', schema: ISSUES_SCHEMA },
    ),
])

log(`Done. ${verdicts.length} verdicts, ${(issueSet?.issues || []).length} issues drafted.`)

return {
  verdicts,
  tally,
  positioning,
  architecture,
  ladder,
  methodology,
  criticNotes,
  navBlock: architecture?.navBlock,
  navProblems: navProblems(architecture),
  visionDoc: visionDoc?.markdown,
  issues: issueSet?.issues || [],
  excluded: ['README "## Contents" (generated TOC)', 'README "## License" (legally required)'],
}
