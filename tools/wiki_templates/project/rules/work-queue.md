---
description: Read list first, then edits, then one check; corrections append to the queue; a mistake names its instruction gap
paths:
  - "**"
---

# Work queue

Four rules for how a task proceeds. They are repo-wide because the failure they stop is
repo-wide: single-file reads interleaved with single-line edits, a correction that derails
the item in progress, and a mistake explained instead of fixed at its source.

1. **Read list first.** Before changing anything, name every file the change touches and read
   them in one pass. Then make all edits. Run one targeted check per edit batch and the full
   gate (`dev.py check`, or `preflight` before a push) once at the end. The tdd skill is the
   exception: its red-green loop runs the one failing test as often as the loop needs.
2. **The task is a queue.** Keep the todo list explicit and visible. A correction or a new
   request from the user goes to the end of the queue unless it invalidates work in flight. Say
   when that is the case. The task ends when the queue is empty or when an honest stop is
   recorded per `task-outcomes.md`, never in between.
3. **A mistake is an instruction gap.** Do not apologise. Name the rule page, hook, skill or
   test that would have caught it and propose the smallest fix there, preferring a
   deterministic check over prose. Edits to `.claude/`, `CLAUDE.md` or `tools/hooks/` are
   marked UNVALIDATED by `customization_edit_marker.py` until the eval gate runs.
4. **Wide change, fresh eyes.** When a change touches three or more code files, spawn the
   `verifier` agent before claiming done. Brief it as the five-section packet, each heading
   on its own line ending in a colon: Objective, Scope, Invariants, Acceptance, Result
   contract (`doc/AGENT_HARNESS.md`). One well-briefed spawn, not several narrow ones.

Where things live: personal cross-project rules in `~/.claude/rules/` (the global tier);
project rules here; lessons in `memory/` with a row in `tools/discoveries.jsonl`; reference in
`doc/`. `python tools/wiki.py status` reports both tiers and the upkeep backlog.
