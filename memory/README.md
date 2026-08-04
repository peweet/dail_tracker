# Durable project lessons

This directory is the shared, provider-neutral evidence layer for lessons that should
survive individual Claude Code, Codex, ChatGPT, or API sessions.

## Where knowledge belongs

| Knowledge | Canonical location |
| --- | --- |
| Rule that must apply to every task | Root or nearest nested `AGENTS.md` |
| Short lesson worth recalling by topic | One row in `tools/discoveries.jsonl` |
| Evidence, history, caveats, or reproduction detail | `memory/<discovery-slug>.md` |
| Private Siting decision history | `planning/product/claude/memory/` (legacy directory name, provider-neutral content) |
| Personal preferences or recent working context | Local Codex Memories; never the only copy of a team rule |

Run `uv run python tools/discoveries.py <keywords>` before re-deriving a known trap.
Codex also runs the same compact index through the project `UserPromptSubmit` hook,
after the hook is reviewed and trusted with `/hooks`. The hook injects at most two
matching one-line lessons and never loads an entire memory corpus into context.
After a substantive session, the project `Stop` hook asks once for an explicit
`promoted`, `already-captured`, or `no-durable-delta` closeout record. It does not
decide what is worth keeping and it does not write a memory card automatically.

Some older discovery rows still have only a workstation-local Claude detail card. Their
tracked one-line lesson remains portable, but the hook labels the supporting card as an
external memory slug rather than inventing a repository path. Curate those cards into
the correct public or private Git boundary after reviewing currency, duplication,
secrets, and personal data; do not bulk-copy the local memory directory.

## Promotion checklist

1. Record one concrete lesson, not a transcript summary.
2. Include the evidence and date; mark assumptions and point-in-time measurements.
3. Add trigger words a future task would naturally contain.
4. Remove secrets, credentials, unnecessary personal data, and raw private evidence.
5. If the lesson is a mandatory invariant, also promote its shortest enforceable form
   to the applicable `AGENTS.md` or a deterministic check.
6. Re-run the relevant source or test before relying on an old path, flag, count, or
   external fact.

Local Codex memory files under `~/.codex/memories/` are generated state. Manage them
with `/memories`; do not edit or commit them as the project's knowledge base. Existing
Claude Code project memories can be imported from desktop settings or Codex `/import`
where that feature is available, but portable lessons should still be curated into this
directory and the discovery index.
