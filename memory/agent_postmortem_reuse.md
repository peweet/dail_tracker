# Post-mortem capture and Codex hook reuse

Verified against repository code and current Codex hook documentation, 2026-10-04.

## Reusable lesson

Codex merges adjacent `hooks.json` and inline TOML hooks; the JSON file is not a
fallback merely because the repository calls it a compatibility matrix. Register
each handler once, preserve unrelated protection hooks when removing duplicates,
and verify changed definitions through `/hooks`. Trust metadata and registration
alone do not establish execution.

The local migration removed duplicate session_context, discovery_hint, and
closeout_gate entries from the ignored JSON after backing it up. Fifteen other
handlers were retained. A fresh checkout uses the tracked TOML; local hook trust
and client execution remain separate checks.

Capture one evidenced lesson at a meaningful milestone and record its outcome.
Do not mistake an appended review record for promotion into reusable knowledge:
only the discovery index and evidence card support later retrieval. The pending
ledger is Claude-specific; the Stop counter measures hook invocations. Neither
proves all Codex work received a post-mortem.

The regression suites cover deduplicated pending sessions, valid review notes,
full/legacy ID ambiguity, idempotent recording, bounded discovery output, and
portable TOML registration. Live client hook invocation was not established by
those tests. See [the procedure](../doc/AGENT_HARNESS.md#post-mortem-and-reuse) and
[Codex's hook contract](https://learn.chatgpt.com/docs/hooks).
