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

Some older discovery rows still have only a workstation-local Claude detail card.
Their tracked one-line lesson remains an unverified lead, but automatic detail
resolution searches only the admitted checkout roots. Request `external_memory`
explicitly for workstation notes. Curate those cards into the correct public or
private Git boundary after reviewing currency, duplication, secrets, and personal
data; do not bulk-copy the local memory directory.

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

## Recall validity and correction

The discovery CLI and `UserPromptSubmit` hint hook share `tools/memory_policy.py`.
The default scope is public. Use `python tools/discoveries.py --scope private
<keywords>` explicitly for private work from the public checkout. The hook includes
private findings only when its supplied cwd is inside `planning/product/`.
Existing `domain: siting` and private `repo_path` values also imply private scope;
unscoped legacy rows are not automatically claimed to be public or current.
Detail-card lookup follows each record's namespace even inside a private context.
An explicit public row never falls back to a private card, and a private row never
uses a same-named public card. Ambiguous unscoped slugs omit the detail pointer.

Recall labels have deliberately narrow meanings:

- **unverified**: a compatible legacy lead without a recorded source snapshot;
- **source-matched**: the declared source bytes still match their SHA-256 snapshot;
- **recheck**: evidence changed, is missing/oversized, or an explicit expiry passed.
  The cached claim is replaced with a source-reverification lead.

An explicit `superseded_by`, non-active `status`, future `valid_from`, malformed
metadata, conflicting duplicate ID, or wrong scope excludes a record from recall.
No inference engine discovers contradictions or decides that newer text is true.
Source matching never promotes a claim to the project's Verified data-trust tier.

After reviewing a lesson, add `scope`, a positive integer `revision`,
`verified_at` (timezone-qualified ISO 8601), and one to four `source_refs`, each
with a checkout-relative `path` and lowercase hex `sha256`. Optional `expires_at`
and `valid_from` use the same timestamp format. Sources must be bounded text files
inside the checkout; parquet, PDFs, raw JSONL and whole transcripts are ineligible.
Update a corrected record in place and increase its revision. To retire a separate
old record, set `superseded_by` to the replacement ID; history remains in Git.

Repeated unchanged hints remain suppressed. A changed record or observed source
state can appear again in the same session. Old ID-only markers receive one fresh
emission under this policy. The two-row/1,000-character prompt budget still applies.

The policy currently covers the discovery CLI and hint hook. Direct memory-file
reads, personal Codex Memories and MCP FTS `kind="memory"` remain separate surfaces;
none inherit this policy automatically. See the
[implementation plan, research and verification record](memory_recall_policy_2026_10_18.md).
