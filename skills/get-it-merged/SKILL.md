---
name: get-it-merged
description: Land a set of open PRs as a serialized merge train -- order by dependency and conflict, run CI only on the head, cancel stale runs, arm auto-merge, advance on green. Use when multiple PRs are open in one repo and the user wants them merged, or when parallel agent work has produced PRs that will invalidate each other's CI.
argument-hint: [pr-numbers-or-repo] [extra instructions]
---

# get-it-merged

Land every open PR with the fewest CI runs. The enemy is the stale run: under a ruleset with
strict required status checks, every merge to the default branch invalidates every other open
PR's checks, so a run started on a branch that is not next to merge is minutes burned twice.
The cure is a merge train: one PR runs CI at a time, everything else waits cold.

The repo's own `CLAUDE.md` / `AGENTS.md` OVERRIDES everything below. Anything you emit --
comments, reports -- follows the user's writing style from their global `CLAUDE.md`.

## 1. Survey

One pass, before touching anything:

- Open PRs: `gh pr list --json number,title,headRefName,baseRefName,autoMergeRequest,mergeStateStatus,mergeable`
- The ruleset: `gh api repos/{owner}/{repo}/rulesets` -- confirm `strict_required_status_checks_policy`
  (strict true is what makes this skill worth running; if strict is false, PRs only need
  update-branch on real conflicts, and the train relaxes to "arm auto-merge everywhere,
  cancel nothing"), `required_linear_history` (decides squash/rebase vs merge commit), and
  the required-check count (the per-run cost you are saving)
- Merge method: match the repo's convention from `git log` on the default branch (single
  squashed commits referencing PR numbers means `--squash`)
- Live CI: `gh run list` -- note every queued or in-progress run and which branch it serves

## 2. Order the train

- A PR whose branch another PR builds on goes before its children; children rebase after the
  parent lands (branch deletion auto-retargets their base)
- PRs touching the same files ride adjacent, so the conflict surfaces once, at one rebase
- A PR with CI already green or in progress outranks a cold one -- never throw away a run
  that can still merge
- Ties break by arrival

## 3. Run the train

Loop until the list is empty:

- **Head**: update-branch if behind (`gh pr update-branch`), let CI run ONCE, arm
  auto-merge (`gh pr merge --auto` with the repo's method). Head is the only branch with a
  live run
- **Everyone else**: cancel their queued and in-progress runs (`gh run cancel`) -- under
  strict checks those runs cannot produce a mergeable state, only heat. New PRs joining
  mid-train get their fresh runs canceled the same way, then take a seat
- **On merge**: advance. The new head update-branches, runs, arms
- **Head goes red**: pull it out (disable auto-merge), advance the next PR immediately, and
  triage the failure in parallel. A legitimately broken PR re-queues at the tail after the
  fix; a flaky failure re-runs only the failed jobs (`gh run rerun --failed`) and keeps its
  seat

Watch with a polling loop on `mergeStateStatus` / `gh run watch`, not by re-running full
surveys.

## 4. Report

- The order chosen and why (dependencies, shared files)
- Runs canceled and roughly what they would have cost (jobs x legs)
- Final state per PR: merged SHA, or where it stalled and what it needs
