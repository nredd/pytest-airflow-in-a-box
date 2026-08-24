---
name: ship-issue
description: Address a GitHub issue end-to-end in a dedicated git worktree -- implement, run the repo's full gate, adversarially review, open a PR, and queue an auto-merge. Use when the user names an issue URL or number and wants it shipped.
argument-hint: <issue-url-or-number> [extra instructions]
---

# ship-issue

Take a GitHub issue from "here is the URL" to "PR is queued to merge on green", in an isolated
worktree, with a real review round in between.

The repo's own `CLAUDE.md` / `AGENTS.md` OVERRIDES everything below. Read it first (step 3) and
let it win on gate commands, coverage bars, layout, and style. This file is the procedure, not
the house rules.

Anything you emit -- commit messages, PR bodies, review comments, the final report -- follows
the user's writing style from their global `CLAUDE.md`: bare colon-terminated lead-ins plus `-`
bullets, NO markdown headers, no checkbox lists, `--` never em-dashes, backtick every
identifier and path. The headers in *this* file are instructions to you, not a template.

## 1. Resolve the issue

Accept a full URL, `#128`, or a bare `128`. From a URL, pull owner/repo too -- it may not be the
cwd repo.

- `gh issue view <N> --json number,title,body,labels,state,comments`
- Read the comments, not just the body. Prior design context and scope cuts usually live there
- STOP and report if: the issue is closed, or `gh pr list --search "<N>"` shows an open PR
  already addressing it. Do not double-ship
- If two readings of the issue produce materially different work, ask before implementing. If
  it is merely underspecified, make the call a careful colleague would make and say which
  assumption you took

## 2. Worktree

Never work in the user's checkout. One issue, one worktree.

- `git -C <repo> fetch origin` then `git -C <repo> worktree prune` (stale `prunable` entries
  accumulate)
- Branch: `redd/issue-<N>`. Append a short kebab slug from the title when the bare number is
  ambiguous or the repo has several open issues in the same area -- e.g.
  `redd/issue-103-flaky-port-race`
- Path: a SIBLING of the repo root, `<parent-of-repo>/issue-<N>`. For
  `/Users/redd/code/PLUGIN/pytest-airflow-in-a-box` that is `/Users/redd/code/PLUGIN/issue-<N>`
- `git -C <repo> worktree add -b <branch> <path> origin/<default-branch>`
- Do NOT use the `EnterWorktree` tool. It does not honor this layout
- Every subsequent command runs with `<path>` as cwd. Pass it explicitly (`git -C`, `make -C`,
  absolute paths) rather than relying on shell state

## 3. Read the repo's rules

- Root `CLAUDE.md` / `AGENTS.md`, plus any directory-scoped ones near the files you will touch
- `Makefile` targets -- that is where the real gate is defined
- `CONTRIBUTING.md`, `CHANGELOG.md` (does the repo keep one? Keep a Changelog format?)

Note the hard rules explicitly before writing code. They are the ones the reviewers will check
against, and the ones that make a PR bounce. Typical shapes: no inline waivers (`noqa`,
`type: ignore`), a coverage floor, modules that must stay import-light, version strings that
live in more than one file, a required changelog entry.

## 4. Implement

Ordinary work, scoped to the issue. Tests by default -- the repo's existing test layout tells
you where they go and what style they are in.

- Match the surrounding code's idiom, naming, comment density, and docstring form
- Update the changelog if the repo keeps one
- Update docs if the change is user-facing and the repo has a docs tree
- Do not widen scope. If you find an adjacent bug, note it for the report; fix it only if the
  change is unshippable without it, and say so in the PR body

## 5. Gate -- before review, not after

Run the repo's real gate and capture the ACTUAL output. It becomes the `Verification:` bullets.

- `make all` when the `Makefile` defines it. Otherwise the closest equivalent: format, lint,
  type check, tests
- `uv run prek run --all-files` (or `pre-commit run --all-files`) when the repo has a
  `.pre-commit-config.yaml`. Local hooks gate commits; a manual run catches what you would
  otherwise discover at push time
- Lockfile check and build if the repo has them (`uv lock --check`, `uv build`)

Red gate means do not proceed. Fix it, or stop and report why it cannot be made green. Never
ship on a gate you did not actually run -- "the imports resolved" is not evidence.

## 6. Commit and push

- Subject: capitalized imperative, no trailing period, no conventional-commit prefix (no
  `feat:` / `fix:` / `chore:`)
- Body only when the subject cannot carry it. Then `Changes:` plus `-` bullets, one concrete
  change each, phrased "Verb the `thing`"
- Keep the `Co-Authored-By: Claude ...` footer. Attribution is a footer, not a writing style --
  the body above it reads like the user
- `git push -u origin <branch>`
- Never `git push --force` to the default branch. Force-push only your own issue branch, and
  only with `--force-with-lease`

## 7. Review round -- three passes

Two adversarial subagents plus one structured pass. Spawn the two subagents in a SINGLE message
so they run concurrently, then do the third pass yourself while they work.

Subagent 1: `Agent(subagent_type: "general-purpose", model: "sonnet")`
Subagent 2: `Agent(subagent_type: "general-purpose", model: "opus")`

Give both the same prompt shape, filled in with real values:

```
Review the diff `origin/<default>...HEAD` in the git worktree at `<path>`.

Context: this branch addresses <owner>/<repo> issue #<N>: "<title>".
The repo's rules are in `<path>/CLAUDE.md` -- read it first and treat violations as findings.

Hunt for: correctness bugs, broken edge cases, race conditions, error paths that swallow or
mislabel failures, tests that pass for the wrong reason, and violations of the repo's stated
rules.

Before you report ANY finding, try hard to refute it yourself -- read the surrounding code,
check whether a guard elsewhere already handles it, and consider whether you have misread the
control flow. Report only findings that survive your own refutation. An empty report is a
perfectly good result.

For each surviving finding give: `file:line`, a concrete failure scenario (inputs/state ->
wrong output or crash), and the fix you propose.

Do NOT report style nits, naming preferences, "consider adding a comment", or speculative
future-proofing. Your final message is the return value -- raw findings, no preamble.
```

Third pass: invoke `/code-review high` yourself against the branch.

Then triage all three:

- Fix everything real. Push a follow-up commit and RE-RUN the gate
- Reject the rest explicitly, with the reason. This is the most valuable part of the PR body --
  a reader learns more from "reviewer flagged X, refuted because Y" than from a clean report
- When two reviewers converge on the same bug from different angles, say so. That is signal

## 8. Open the PR

`gh pr create --title "<commit subject>" --body-file <tmpfile>`. Write the body to a file in the
scratchpad; do not fight shell quoting.

Body, in order:

- `Closes #<N>.` (or `Fixes #<N>.`)
- `Changes:` plus bullets -- one concrete change each, backtick every identifier, path, and
  dotted module path
- `Verification:` plus prose bullets carrying REAL command output. Fence it when long. Test
  counts, coverage numbers, and actual failure transcripts, not "tests pass"
- The review round: what each reviewer found, what was fixed, what was rejected and why
- The `🤖 Generated with [Claude Code](https://claude.com/claude-code)` footer

Never: `## Summary`, `## Test plan`, `## Root cause`, checkbox lists, or an invented body for a
change whose title already says everything. A title-only PR is a fine default for something
genuinely routine -- but a shipped issue almost always earns a body.

Mark it draft (`--draft`) only when the branch genuinely depends on unmerged work, and say what
it is waiting on.

## 9. Merge and clean up

- `gh pr merge --squash --auto --delete-branch`
- Do NOT poll CI to completion. Queue the auto-merge and hand back -- a big matrix takes long
  enough that holding the session is waste
- Do NOT remove the worktree. Its PR has not landed yet. Tell the user the follow-up is
  `git worktree remove <path> && git -C <repo> worktree prune` once it merges
- If auto-merge is not enabled on the repo, `gh pr merge --squash --delete-branch` fails
  loudly -- say so and leave the PR open rather than merging past red checks

## 10. Report

Short. Issue -> branch -> PR URL -> gate result -> review outcome -> the worktree cleanup
follow-up. No closing summary paragraph, no restating the plan back.

## Guardrails

- Gate cannot be made green: stop and report. Do not ship it
- Never force-push the default branch; `--force-with-lease` on your own branch only
- Never merge past failing checks by hand
- The repo's `CLAUDE.md` outranks this file on every conflict
