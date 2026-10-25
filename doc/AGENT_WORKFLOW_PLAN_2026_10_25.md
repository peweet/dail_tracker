---
tier: PLAN
status: LIVE
domain: infra
updated: 2026-10-25
supersedes: []
read_when: changing how agent sessions work in this repo - rules, hooks, the two-tier wiki, review subagents, compaction, issue/PR flow
key: PLAN|LIVE|infra
---

# Agent workflow plan - 2026-10-25

## Why this document exists

On 2026-10-25 the owner shared a personal "AgentWorkflow.md" of practices gathered from other
projects and asked for an assessment against this repository's harness, then for the result
to be consolidated into one repeatable package. This page records the assessment, the package
that was built, and the one decision that remains open. The durable harness design stays in
[AGENT_HARNESS.md](AGENT_HARNESS.md); this page is the delta.

## What the assessment found

The harness already implemented most of the practices, often in a stronger form. The
repository front-loads conventions in `CLAUDE.md` and seven path-scoped rule pages, runs
twenty hook scripts across six events, defines seven subagent roles with a fresh-context
verifier, keeps a persistent task-contract store, and gates pushes with a thirty-five-task
runner and ten CI jobs. Knowledge was already held in small files: rule pages, tracked memory
cards, an indexed `doc/` tree, a trigger-keyed discovery index and a per-user auto-memory
store.

Three gaps were real. There was no global tier: `~/.claude/CLAUDE.md` and `~/.claude/rules/`
did not exist, so personal cross-project rules lived only inside this repository's files.
Nothing made a compaction survivable: the compaction rule in `CLAUDE.md` asked the summariser
to keep the modified-file list, and the PreCompact and PostCompact hook events were unwired.
Nothing detected a weakened test: the gold-quality guards protect data baselines and the
expected-failures ledger requires a reason, but a deleted assertion or an added skip marker in
a test file reached CI unremarked.

Two practices conflicted with earlier decisions in this repository and were adapted rather
than adopted. "Kill any session that compacts" conflicts with the owner's 2026-08-21 steer that
long sessions on one thread are valuable; the package makes compaction survivable instead.
"Always spawn a review subagent" was rejected on cost in AGENT_HARNESS.md; the package nudges
for the verifier once a session has edited three or more code files.

Two practices were set aside. The mex memory tool duplicates the MCP code graph, the BM25
project search and the memory cards, and adds a Node runtime;
[memory_recall_policy_2026_10_18.md](../memory/memory_recall_policy_2026_10_18.md) already
decided against a graph server without an operational need. The issue-to-merge-queue pipeline
is unavailable: GitHub offers merge queues only to organisation-owned repositories, both
repositories here are owned by a personal account, and the private repository cannot enable
branch protection on its current plan. Today every public issue is bot-authored and work lands
as direct commits to `main`.

## The package

| Piece | Path | What it does |
|---|---|---|
| Engine | `tools/wiki.py` | `status` measures both tiers and the upkeep backlog; `setup-global` installs the personal tier; `upkeep` lists the fixes with commands; `page` writes a page skeleton |
| Global tier templates | `tools/wiki_templates/global/*.md`, `global_CLAUDE.md` | Windows shell traps, git safety across parallel sessions, working style; installed to `~/.claude/rules/` by `setup-global` |
| Project tier templates | `tools/wiki_templates/project/{rules,skills}/`, `hooks.json` | Tracked source of truth for the two rule pages, the skill and the hook wiring; installed into the local `.claude/` tree by `setup-project --hooks`, which merges hook commands idempotently and never overwrites a page without `--force` |
| Skill | `.claude/skills/wiki/SKILL.md` (installed copy) | The working loop and the "where a fact belongs" table |
| Rules | `.claude/rules/work-queue.md`, `.claude/rules/tests.md` (installed copies) | Read list first; queue discipline; mistake names its instruction gap; wide change gets the verifier; assertions and baselines off limits |
| Hooks | `tools/hooks/precompact_resume.py`, `resume_context.py` | Write a resume file before compaction; inject it on the SessionStart that follows |
| Hooks | `tools/hooks/assertion_guard.py`, `verifier_nudge.py` | Warn on a removed assertion or added skip; nudge for the verifier at three code files |
| Task runner | `dev.py wiki` (in `preflight`) | `wiki status --strict`: fails on page size and a missing global tier, reports backlog, no-ops on CI |
| Issue template | `.github/ISSUE_TEMPLATE/agent-task.md` | The five-section task packet as a GitHub issue |
| Tests | `test/hooks/`, `test/tools/test_wiki.py` | Deterministic coverage for the hooks and the engine |

All hook additions are advisory. They inject context and never block, in line with the
determinism tiers: a mechanical observation is made deterministic, the judgement about it
stays with the model and the owner.

The whole `.claude/` tree is ignored by the root repository on purpose (`.gitignore`, the
note under "Claude Code & AI tooling"), as is every `*.json` file not re-included by name.
A rule page or a hook entry written only there exists on one machine. The package therefore
keeps its project-tier pages and its hook fragment under `tools/wiki_templates/project/`,
re-includes `hooks.json` in `.gitignore` next to the other admitted JSON files, and treats
the installed `.claude/` copies as derived. `wiki status` warns when an installed copy
differs from its template, so an edit made in place is noticed and copied back rather than
lost at the next clone.

## What gets harder

Every rule page added is read on every matching session, and the Claude Code documentation
notes that longer instruction files reduce adherence, so the two new rule pages are the budget
for this change and a later rule should replace rather than add. The global tier is personal
configuration outside version control; `setup-global` reinstalls it from the tracked
templates on a new machine, but an edit made directly in `~/.claude/rules/` is not tracked
until it is copied back to the template. The assertion guard will fire on legitimate test
refactors; it is advisory for that reason, and the report should say when the refactor keeps
the seam asserted. The resume file reads the transcript tail at compaction time, which adds a
bounded file read to each compaction.

## Open decision: commit flow on the public repository

The public repository can take a free ruleset on `main` requiring the lint, firewall and test
checks, with agent-produced changes arriving as pull requests. This changes how the owner
commits: every change then needs a push the owner runs, and a round trip through CI before it
lands. It is a two-way door. If adopted, the issue template becomes the entry point for
agent-sized work and `preflight` becomes the local mirror of the required checks. Nothing in
the package depends on this decision.

## Verification

`python tools/wiki.py status` reports both tiers. On a fresh clone, `python tools/wiki.py
setup-global` and `python tools/wiki.py setup-project --hooks` reinstall the personal tier and
the local `.claude/` pieces from the tracked templates. `uv run --locked --group dev --extra
pipeline --extra api --extra mcp python tools/dev.py verify` runs the hook and engine tests
with the rest of the fast lane. The validation ledger lists every customization touched by this change
as UNVALIDATED until `tools/evals/validate_customizations.py --run` is executed from a plain
terminal.
