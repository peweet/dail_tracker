---
name: wiki
description: Keep the two-tier mini-wiki (personal ~/.claude/rules + project rules/memory/doc) small and current - status, upkeep, new page, where a fact belongs. Use at session start when the status line shows a backlog, before preflight, and whenever a lesson or rule is worth keeping.
version: 1.0.0
user-invocable: true
argument-hint: "[status | upkeep | page <global|rule|card> <slug> | where <topic>]"
---

# Wiki

Knowledge lives in small files, each one fact, each under 120 lines. `python tools/wiki.py`
measures both tiers; this skill is the working loop around it. No new store: the wiki is the
files that already exist, plus the personal tier.

## Where a fact belongs

| Fact | Page | Loads |
|---|---|---|
| Personal preference or trap that holds in every project (shell, git habits, register) | `~/.claude/rules/<topic>.md` | every session, every project |
| Rule that must hold for one part of this repo | `.claude/rules/<topic>.md` with tight `paths:` | when a matching file is touched |
| Rule that must hold repo-wide | `.claude/rules/<topic>.md`, `paths: "**"` | every session (keep rare) |
| A lesson with evidence and a date | `memory/<slug>.md` + one row in `tools/discoveries.jsonl` | on trigger words, two rows max |
| Reference that a reader opens on purpose | `doc/<NAME>.md` with front matter | never automatically; via `doc/INDEX.md` |
| A correction to how I work, not yet proven general | auto-memory card (`feedback_*`) | first 200 lines of MEMORY.md |

A fact goes in exactly one place. If it is already in CLAUDE.md or AGENTS.md, do not add a
second copy; link to it.

## Loop

1. `python tools/wiki.py status`. FAIL lines are size or missing-tier problems and are fixed
   now. WARN lines are backlog, each with the command that clears it.
2. `python tools/wiki.py upkeep` lists only the fixes. Run them in order. Never mark a
   customization validated without running the gate; never archive a memory card that the HOT
   index links or that carries safety language (memory_gc refuses both).
3. New page: `python tools/wiki.py page <tier> <slug> --title "..."` writes the skeleton with
   front matter. Fill it with one fact, its evidence and its date. For a card, add the
   discoveries row with the trigger words a future task would contain.
4. First time on a machine or a fresh clone: `python tools/wiki.py setup-global` installs the
   personal tier from `tools/wiki_templates/global/`, and `python tools/wiki.py setup-project
   --hooks` installs this repo's rule pages, this skill and the hook wiring from
   `tools/wiki_templates/project/`. The whole `.claude/` tree is local by repo convention
   (`.gitignore`), so the tracked templates are the source of truth: edit the template, then
   `setup-project --force`; `status` warns when an installed copy drifts from its template.
   Neither command overwrites an existing page without `--force`.

## Page rules

One topic per file. A `description:` line in the front matter that a session can route on.
Under 120 lines; split rather than grow. Dates absolute. Evidence or the word unverified on
every claim. Plain words. Delete a page when it is wrong rather than appending a correction
under it; Git keeps the history.

## What this is not

Not a graph index, not an automatic rewriter, not a replacement for CLAUDE.md in one step
(doc/AGENT_HARNESS.md defers that until a parity check). The upkeep is manual by design; the
tool makes the backlog visible and the commands exact.
