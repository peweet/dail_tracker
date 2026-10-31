---
tier: REFERENCE
status: LIVE
domain: infra
updated: 2026-10-31
supersedes: []
read_when: changing reusable agent prompts, hooks, subagent roles, or the coding-agent evaluation harness
key: REFERENCE|LIVE|infra
---

# Agent harness decisions and prompt contract

Latest measured result: [Agent harness measurement — 2026-08-05](AGENT_HARNESS_MEASUREMENT_2026_08_05.md).

This is the repository decision record for the harness-guide audit completed on 2026-08-05.
A recommendation is adopted only when it closes an observed gap, can be enforced or measured,
does not weaken a data or product boundary, and has regression coverage. The guide and its
GitHub repository are inputs, not an instruction to copy every mechanism.

## Reusable task packet

Shared entry prompts use five small sections:

1. **Objective** — one observable outcome.
2. **Scope** — allowed files, tools, and systems.
3. **Invariants** — safety, provenance, data-grain, and ownership boundaries.
4. **Acceptance** — binary checks that can produce evidence.
5. **Result contract** — changed files, commands and observed results, unresolved questions,
   and residual risk.

Review prompts return `Verdict: PASS | FAIL`. Findings carry severity, `path:line` or screenshot
evidence, consequence, and the smallest required action. An unrun check is `NOT RUN`; mocked or
focused verification is never described as a full integration pass. Numeric aesthetic scores
are not acceptance evidence.

Repository `AGENTS.md` plus the nearest nested `AGENTS.md` are the provider-neutral guidance
entry point. `CLAUDE.md` remains a compatibility fallback and may contain provider-specific
instructions, but shared prompt packs do not route to it directly.

## Persistent task contract pilot

The observed failure was objective drift: a request to improve the coding-agent harness
became a product-validation task. More reference papers or larger prompts do not prevent
that by themselves. `tools/task_contracts.py` now records the captain's explicit interpretation
of user intent separately from the conversation. This is a bounded pilot for public harness
work, not automatic task extraction or a new orchestration service.

Each record contains the original objective, current objective, scope, mode, allowed relative
paths, acceptance checks, progress, and unresolved questions. It is keyed by resolved checkout
and exact session id. SQLite transactions append immutable revisions; updates require the
recorded owner and expected revision. Owner names are coordination metadata, not authentication.
The default database is ignored local state at `logs/agent_task_contracts.sqlite3`; never copy
real session state or private holdout answers into tracked fixtures or the public cleanroom.

Create a UTF-8 JSON file outside tracked source, containing these six fields:

```json
{
  "objective": "Improve the coding-agent harness architecture",
  "scope": "Task persistence and objective drift evaluation",
  "mode": "implementation",
  "allowed_paths": ["tools", "test/tools", "doc/AGENT_HARNESS.md"],
  "acceptance": ["Resume restores the objective", "Scope amendments are explicit"],
  "unresolved": ["Live client compaction and model quality are unverified"]
}
```

```powershell
python tools/task_contracts.py start --session-id <exact-id> --owner captain --input <task.json>
python tools/task_contracts.py show --session-id <exact-id>
python tools/task_contracts.py checkpoint --session-id <exact-id> --owner captain --revision 1 --input <progress.json>
python tools/task_contracts.py check-action --session-id <exact-id> --action edit --path tools/task_contracts.py
```

`--checkout` selects the exact worktree; its default is the tool's checkout. `CODEX_THREAD_ID`
can supply the session id. `checkpoint` accepts only `progress` and `unresolved`; it cannot
expand scope. `amend` accepts changes to the six task fields and requires
`--authorization-reference` naming the user correction or established authorization.
It preserves the original objective. No new approval round is required when the session
already establishes authorization. The reference is an audit claim supplied by the captain;
the tool cannot prove the semantics of the user's words. `history` shows prior revisions;
`complete --revision N --owner captain --evidence "..."` retires the active task. A later
`start` creates a new task id. Completed state is omitted from continuation context.

The separately bounded `task_context.py` hook reads this state on `SessionStart`
(startup/resume/clear/compact) and `UserPromptSubmit`. It does not interpret prompts,
rewrite the contract, or guess another session's latest task. Missing state is silent;
unreadable state produces an explicit recovery note. Its JSON context is capped at 6,000
characters by write-time validation. Codex registers it only in TOML because the adjacent
hooks JSON can also load; Claude Code registers it in `.claude/settings.json` (see
[Claude Code parity](#claude-code-parity)). Existing project status has its separate
1,600-character budget.
[Codex documents compact-triggered SessionStart restoration](https://learn.chatgpt.com/docs/hooks#sessionstart),
but registration and subprocess tests do not prove activation in a running client.

`check-action` validates a declared action against mode and resolved file/directory boundaries,
including symlink escape. Research and verification modes reject declared edits. Callers must
enforce its result; it is not attached to every tool or arbitrary shell command. Host read-only
sandboxing remains the actual write boundary. This pilot does not claim to eliminate semantic
drift, enforce all workflow transitions, or make a locally editable record a security policy.

### Continuity measurement

The benchmark accepts external held-out `continuation-replay` scenarios with `prompt`,
`task_contract` (the six fields above), and evaluator-only `expected` JSON. Use opaque case ids;
case-normalization collisions are rejected. `--continuity-pair` compares `context-off` and
`context-on`: both use the same cleanroom, read-only sandbox, disabled project settings/hooks,
and no MCP. The sole intervention is adding the persisted and reloaded task context to the
continuation prompt. The existing balanced order alternates the treatment order by repeat.

```powershell
# No model calls. Validates external task shape, state, and cleanroom wiring.
.venv\Scripts\python tools/evals/harness_bench.py --tasks-file <external-holdout.json> --continuity-pair --preflight
# Paid evaluation; run only when model calls are authorized.
.venv\Scripts\python tools/evals/harness_bench.py --tasks-file <external-holdout.json> --continuity-pair --repeat 4
```

The external file shape is `{"tasks":{"opaque-id":{"scenario":"continuation-replay",
"prompt":"...","task_contract":{...},"expected":{...}}}}`. Expected answers never enter
provider requests or cleanroom copies. The cleanroom also excludes `logs/` even when a runtime
file is explicitly listed, preventing real local task state from contaminating a trial.
Manifest/attempt rows label this `synthetic-replay`.
This intentionally compares restored context with omitted context, not a full native chat
or general harness quality. Final JSON leaf matches measure stated decisions, not actual
tool permission enforcement. The one-shot provider adapter cannot resume a native session;
`native_required` and `native-compaction` scenarios fail before model calls. Native compaction,
live hook trust/activation, and production quality uplift remain **NOT RUN** until separately
verified. Keep held-out content outside this repository and use host isolation for strict secrecy.

Implementation evidence: `test/tools/test_task_contracts.py` was first observed failing at
collection because the task store did not exist. The focused benchmark tests then failed on
missing restoration, unsupported native-task acceptance, and case-id overwrite. The green
checks exercise real SQLite and subprocess hook I/O; model requests are faked. Public test
fixtures are regression examples, not held-out model-quality evidence.

## Claude Code parity

Added 2026-10-20. The recall-validity change (`discovery_hint.py`, `memory_policy.py`) and the
bounded `session_context.py` already ran under Claude Code, because both clients register the
same scripts. Two controls were Codex-only; the table records what carried over and why not the rest.

| Control | Claude Code | Reason |
|---|---|---|
| Task context restore | `task_context.py` on `SessionStart` only | Claude saves `UserPromptSubmit` context in the transcript and replays it, and `SessionStart` fires again with source `resume` or `compact`. A per-prompt copy would re-add the same record every turn. Codex keeps both events. |
| Session id for `task_contracts.py` | Read from `CLAUDE_CODE_SESSION_ID` when `--session-id` is absent | Hook payloads carry `session_id`. Both ids set and different (a shell started inside the other client) resolves to no id, never a guess. |
| Subagent spawn contract | `spawn_packet_hint.py` on `Agent`, advisory | `guard_subagent_spawn.py` rejects every Claude spawn: Claude's input is `prompt`/`description`/`subagent_type`/`model`, with no `fork_turns` and different role names. Only the five-section packet carries over. |
| Role and model pinning | Not carried over | Claude roles are `.claude/agents/*.md` frontmatter that the client enforces; rejecting `model` overrides would also reject an override the main thread passes deliberately. |

The hint adds context when a prompt has no line for Objective, Scope, Invariants, Acceptance or
Result contract, and never changes the permission decision. Set `DAIL_SPAWN_PACKET_MODE=block`
(for example under `env` in `.claude/settings.json`) to deny with exit code 2 as the Codex guard does.
Advisory is the default because nothing yet shows how often Claude spawns lack a packet, and a
blocking hook that is wrong stops all delegation. Reading the first weeks of hints is the evidence
for switching it.

`.claude/` is gitignored, so the registration is per workstation and a fresh checkout needs both
entries added to `.claude/settings.json` (the two tests that read it skip when the file is absent).
`$CLAUDE_PROJECT_DIR/.venv/Scripts/python` is the interpreter the other hooks there already use:

```json
"SessionStart": [{"hooks": [
  {"type": "command", "command": "\"$CLAUDE_PROJECT_DIR/.venv/Scripts/python\" \"$CLAUDE_PROJECT_DIR/tools/hooks/task_context.py\"", "timeout": 10}]}],
"PreToolUse": [{"matcher": "Agent", "hooks": [
  {"type": "command", "command": "\"$CLAUDE_PROJECT_DIR/.venv/Scripts/python\" \"$CLAUDE_PROJECT_DIR/tools/hooks/spawn_packet_hint.py\"", "timeout": 5}]}]
```

Add each entry to the existing `SessionStart` group and `PreToolUse` list rather than replacing them.

Evidence: the Claude payload fields and the `compact` and `PreToolUse` `additionalContext`
behaviour come from the [hooks reference](https://code.claude.com/docs/en/hooks). Regression
tests are `test/tools/test_spawn_packet_hint.py` and the Claude cases in
`test/tools/test_task_contracts.py`; they use real subprocess hook I/O and no model calls.
**NOT RUN:** activation in a live Claude client (a new session must start for `SessionStart` to
fire), the continuity benchmark against Claude, and any measure of drift or token change.

## Cross-session sidecars

Read this procedure before dispatching a cross-session sidecar. The original session
remains captain, integration owner, and sole writer; sidecars are read-only scouts or
reviewers with exact read paths. They never mark their own work integrated or verified.

- Before dispatch, run `python tools/dev.py roots`, choose one stable task key from objective, scope, source snapshot, and role, and check that the same task is not already active or complete.
- Create the packet in a temporary file outside the source worktree with `python tools/dev.py sidecar-handoff template`; bind it with `sidecar-handoff snapshot --root <worktree> --read-path <bounded-relative-path>` (repeat read paths), then validate or queue it with the same `--source-root`.
- A queue receipt means `accepted_unconsumed`, not delivered. Run `sidecar-handoff status` to confirm `delivered`; the target alone owns `integrated`, `verified`, and `closed`.
- An ambiguous queue or receipt-write outcome keeps an exact recovery claim and reports `recovery_required`. Inspect the target first, then use `sidecar-handoff recover --resolution accepted|failed`; never delete or bypass a claim blindly.
- Never resend blindly. Use `supersedes: <handoff-id>` only for a corrected packet with the same task key; otherwise create a genuinely different bounded task.

## Codex context and usage

Run `python tools/codex_token_usage.py --days 7` from the checkout to inspect its
recent Codex usage. `--format json` includes cache writes, reasoning output, model,
provider, event time bounds, and diagnostics; `--scope root` or `subagent` narrows the
report. The default scan caps are 200 files and 64 MiB; `--max-files` and `--max-mb`
can raise them deliberately. A partial scan is labeled. The report is read-only and
prints aggregates, never transcript content. Session files under `CODEX_HOME/sessions`
are the default; `--sessions-dir` can select an archive explicitly.

Only modern per-response `token_usage_record.payload.usage` contributes to totals.
Cumulative snapshots are counted as ignored coverage, and conflicting duplicate
response identities are excluded. Input already includes cached input; reasoning is
a subset of output. Neither cache percentage nor raw token counts estimate plan
consumption or billing. Existing `token_ledger.py`, `token_trend.py`, and
`token_week_review.py` remain Claude-specific and have different transcript scopes.

In the IDE, use `/status` to inspect context, `/ide-context` to control automatic
editor context, and `/compact` when continuing a long task. Keep useful same-task
history; use fresh chats for unrelated objectives. Cache reuse does not make
irrelevant context free. Do not issue synthetic keep-warm prompts.

SessionStart output stays capped at 1,600 characters. Its `.mcp.json` dail-tracker
handshake is cached for 60 seconds after success or 15 seconds after failure. Cache
hits include their age and are advisory, not proof of a live client connection.
Config/source/launcher changes, corrupt entries, and future timestamps invalidate
the cache. `DAIL_SKIP_MCP_PROBE=1` bypasses both probe and cache.

The tracked Codex TOML registers SessionStart, agent-spawn validation, bounded
UserPromptSubmit discovery hints, and Stop. Codex also loads `hooks.json` beside
active config layers: it is not merely a fallback. Keep each handler in one place.
When migrating a local ignored matrix, back it up and remove only the duplicated
`session_context.py`, `discovery_hint.py`, and `closeout_gate.py` handlers; retain
other protection hooks. Registration and old trust metadata are not proof of live
execution. Review changed definitions with `/hooks` and confirm an invocation.

## Efficient tool use

Plan retrieval around the decision it must support. Batch independent read-only lookups
when their arguments are already known; keep dependent discovery, mutations and approval
steps sequential. In a code orchestration tool, inspect every result and emit the selected
fields, relevant source spans and errors. Printing a complete tool catalogue or every
intermediate response defeats the context benefit of batching.

Use `search_project` to locate a topic, then `code_outline(..., response_format='concise')`
for symbol spans and a bounded read for the needed implementation. Directory outlines now
honour `limit` as a module count, capped at 80; file outlines count definitions, capped at
200. Directory responses expose `module_count`, `returned_modules` and an omission marker.
Only returned modules are parsed. Search responses expose `metadata_total`,
`metadata_returned` and `metadata_truncated`: these count lexical metadata matches, while
`content_spans` remain a separate FTS surface. Narrow the query or kind when the result is
limited. If an index call stalls or is unavailable and the path is known, inspect that
source directly rather than repeating the same call.

Reuse verified findings while inputs remain current. After an edit, source refresh or
scope change, revalidate the affected evidence. Existing verification receipts already
invalidate on the Git/worktree fingerprint; do not introduce a cache keyed only by commit
or elapsed time. For long-running work, use completion notifications and incremental log
output, with waits sized to expected work and timely user updates. Repeated successful
calls can still be necessary, so an identical signature alone must not block execution.

Common techniques and their status here:

Pilot A (2026-10-06) repairs source freshness and evaluation before adding retrieval
machinery. The metadata index is bound to the resolved checkout and fact-card source.
A lock serializes cache transitions; every 120 seconds, admitted Python/SQL paths,
`doc/INDEX.md`, and fact-card modification times and sizes are rechecked. Unchanged
fingerprints reuse the built index. Source identity changes invalidate it immediately.
This is a stat fingerprint, not content hashing: edits preserving both size and timestamp
can escape detection. Ordinary query tokens now preserve Unicode letters, numbers and
underscores; existing FTS AND/OR fallback and name normalizers remain in place.

The project-recall evaluator scores exact, case-preserving normalized paths on metadata
and content spans separately. Content hits require a valid line span, and optional gold
lines must be covered. Invalid expected spans cannot earn content credit. Ineligible
golds are disclosed and excluded from denominators before any private-tree probe.
The JSON report records each query, gold, eligibility and surface rank. A metadata-first
deduplicated union is a labelled baseline; either-surface top-K coverage measures two
independent lists and is not a fused top-K ranking. The authored gold set is a smoke
check, not a held-out test of recall, answer quality, token use or useful discoveries.

Run only the public-source suite without starting a local MCP server:
`uv run --locked --group dev --extra pipeline --extra api --extra mcp python tools/evals/app_retrieval_recall.py --suite project --transport direct --json artifacts/project-recall.json`.
Do not use the default `all` suite for a source-only check. Next, freeze held-out tasks
from historical usage, including unfamiliar vocabulary, accented terms and identifiers.
Measure uncached input, tool rounds, median/tail latency, correctness and source coverage.
Try bounded aliases or result selection only if those tasks expose a gap; then separately
trial zero-to-two sourced reference leads on research tasks, judging usefulness and
adoption as well as cost. These later trials are proposed, not implemented by Pilot A.

The portable [`repo-navigation` skill](../.agents/skills/repo-navigation/SKILL.md)
routes public repository questions to specific MCP tools and bounded source reads.
Its description distinguishes discovery from already-known source spans; the body
loads only when used. `agents/openai.yaml` declares the existing `dail-tracker`
dependency without provisioning a server or granting permissions. Codex discovers
`.agents/skills` natively and VS Code supports that project location. Other existing
`.agents` and `.claude` customizations remain local. Confirm discovery and invocation
in the actual client before attributing any token or latency benefit to the skill.
Keep future routing skills tied to repeated tasks, with short descriptions and
conditional references/scripts rather than copying large manuals into every body.

The Codex benchmark's OFF condition explicitly disables immediate skill folders
under the copied checkout's `.agents/skills` through `skills.config`. AGENTS byte
limits and `--ignore-rules` do not disable skills. ON retains normal discovery;
strict read-only Claude already supplies `skills=[]`. This controls automatic
project-skill loading, not manual file reads or user/admin/system skills. The
shared cleanroom remains a cwd boundary rather than complete host isolation.

| Technique | Use here |
|---|---|
| Programmatic tool orchestration | Batch predictable read-only work and filter intermediate results in code. Adaptive search still needs judgment after each relevant result. |
| Progressive tool and skill loading | Six navigation tools carry always-load metadata; domain tools remain eligible for deferral. Actual client loading must be observed, not inferred from server annotations. Load detailed skill instructions only for the current task. |
| Compaction and small handoffs | At completed phases retain the goal, decisions, exact files, verification and unresolved questions; check that evidence survives the checkpoint. |
| Bounded delegation | Batch related scouting questions, reuse the returned evidence and preserve independent review where needed. Every child has its own context cost. |
| Deterministic acceptance and recovery | Keep correctness gates outside model prose. After a repeated failure, inspect the cause before retrying; change inputs only with evidence. |
| Task-specific model/effort routing | Evaluate narrow tasks separately before adopting a lower-effort route. Maintain required reviewer roles and quality checks. |

Evaluate one intervention at a time on repeated, counterbalanced tasks. Compare correctness,
evidence completeness, total/uncached input, output/reasoning tokens, elapsed time and tool
calls per successful task. The benchmark records tool sequences, but does not yet distinguish
avoidable repeated work, internal calls inside code batches or model round trips. Add those
measurements before claiming a reduction in polling or reasoning overhead. Local payload and
parse-count regressions prove the navigation boundary, not general model-call savings.

Skill loading references: [Codex skill discovery and progressive disclosure](https://learn.chatgpt.com/docs/build-skills),
[VS Code skill locations and invocation](https://code.visualstudio.com/docs/agent-customization/agent-skills).

Sources: [OpenAI programmatic tool calling](https://developers.openai.com/api/docs/guides/tools-programmatic-tool-calling),
[OpenAI tool search](https://developers.openai.com/api/docs/guides/tools-tool-search),
[OpenAI cache measurement](https://developers.openai.com/api/docs/guides/agents-api/observability#prompt-caching),
and [Anthropic context engineering](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents).

## Post-mortem and reuse

After a confirmed failure/repair, recurring correction, or expensive investigation:

1. Assess one lesson that could prevent the next repeated investigation.
2. If useful, save a short trigger-keyed discovery and an evidence card under
   `memory/`; include the cause, remedy, verification, and remaining limits.
3. Record `promoted`, `already-captured`, or `no-durable-delta` using
   `python tools/session_closeout.py --record <full-session-id> <outcome> --note "..."`.
   Notes name the evidence or existing lesson. Identical records are idempotent;
   later distinct lessons in the same session remain recordable.
4. Reuse matching discoveries on later prompts: at most two, once per session,
   within a script-enforced 1,000-character total. TOML additionally sets a
   500-token spill threshold; `additionalContextLimit` measures approximate tokens,
   not characters. Full cards are retrieved only when needed.

This is an agent closeout duty, not automatic model-generated reflection. The Stop
hook is a once-per-session backstop after 500 Stop invocations and stops updating
its counter after firing. It validates the review structure, not its insight.
The pending list separately deduplicates the historical **Claude** ledger using
500 assistant messages. Codex has no equivalent pending-ledger coverage here;
an empty list must not be presented as evidence that every Codex task was reviewed.
Legacy 12-character session IDs match full IDs only when unambiguous.

Regression evidence covers stale Windows architecture caching, malformed reviews,
duplicate ledger rows, full/legacy identity, repeated records, and bounded hints.
The concrete Windows lesson is
[`memory/polars_windows_platform_cache.md`](../memory/polars_windows_platform_cache.md).

Sources: [Codex IDE commands](https://learn.chatgpt.com/docs/developer-commands?surface=ide),
[prompt caching](https://developers.openai.com/api/docs/guides/prompt-caching), and
[skill configuration](https://learn.chatgpt.com/docs/config-file/config-reference), and
[Codex hooks](https://learn.chatgpt.com/docs/hooks).

## Implemented controls

| Guide-derived proposal | Validation | Repository implementation |
|---|---|---|
| Canonical layered guidance | Already effective; shared UI prompts bypassed it | UI prompts now load root and nested `AGENTS.md`; `CLAUDE.md` remains a fallback |
| Fixed task and result contracts | Existing entry prompts varied and reviewers returned unstructured prose | `.github/prompts/` uses the five-section packet; reviewers use evidence-bearing verdicts |
| Persistent task contract pilot | The architecture objective drifted into a product-validation task | Checkout/session-bound SQLite revisions, explicit amendments, bounded resume hooks, and an isolated synthetic replay experiment; live model benefit remains unmeasured |
| Provider-neutral task packets | The UI pack named one provider | Shared prompts no longer require Claude-specific guidance |
| Bounded subagent roles | Generic or inherited-context spawns could bypass ownership and result contracts | Tracked scout, reviewer, and worker roles plus a fail-closed `PreToolUse` hook require fresh five-part task packets |
| Binary review rubrics | A 1–5 design score was subjective and not regression-testable | Review and critique prompts use PASS/FAIL/NOT APPLICABLE plus evidence and severity |
| Phase separation | Already implemented by navigator/explorer, builder, and fresh verifier roles | Retained; no duplicate orchestration layer added |
| Bounded injected context | Discovery hints were capped, but SessionStart aggregation had no hard ceiling | SessionStart is capped at 1,600 characters and reports omitted lower-priority notes |
| Generated prompt inventory and budget | The old 2,500-word warning could not catch realistic prompt bloat | `tools/check_agent_context.py` discovers prompts, enforces 600 words, and emits `--catalog` JSON |
| Critical controls outside prose | Already implemented by firewalls, read guards, atomic writers, and merge gates | Retained and linked from acceptance checks |
| Hidden eval answer key | The ON benchmark could read its own scorer and Git history | ON and OFFCLEAN use the same ephemeral cwd without `.git`, `tools/evals`, scorer tests, or private product overlay; strict secrecy still requires host isolation |
| Mutable ground truth | The awards row count was frozen in scorer source | The scorer reads the current fact card at evaluation time |
| Repeated/versioned evaluation | The benchmark documented `n=1` and omitted a run manifest | `--repeat N`, aggregate rows, commit/dirty state, harness/task hashes, model/provider settings, platform, and an infrastructure label are emitted |
| Measured long-job status | NLC derivative builds, SCP transfers, and remote verification were reported from different tasks without one owner or measured observation | `tools/job_status.py` keeps separate append-only jobs with owner, artifact, snapshot, phase, current/total, ETA or unknown, evidence, and terminal state |
| Checkout-safe Git closeout | An action command could select the primary checkout from a Codex worktree and stage every dirty path | `tools/roots_status.py` actions now require one repo, exact checkout, and explicit paths; unrelated staged paths fail closed and push remains separate |
| Private holdouts | Public smoke tasks alone are gameable | `--tasks-file` accepts structured holdouts only from outside the repository; expected answers are never copied into agent cwd |
| Cross-provider result normalization | Already implemented by `provider_adapter.py` | Retained; attempt rows now include normalized usage, tools, provider, model, and errors |
| Source/data tiering | Already encoded in dataset fact cards, money grains, documentation status, and memory currency bands | Retained; no second taxonomy added |
| Explicit interfaces and loud failure | Already enforced by data contracts, MCP schemas, dev tasks, and `TODO_PIPELINE_VIEW_REQUIRED` | Retained; result contracts expose unresolved work |

## Local document and shared-skill checks

`python tools/check_no_private_ip.py --content --ref HEAD` reads committed Markdown,
using distinctive Python module stems from the authoritative nested private repository's
local `main` revision, minus stems already public in the inspected commit. It never fetches
or switches branches. A linked public worktree resolves the primary checkout's nested repo;
`--private-repo` and `--private-ref` make another reviewed source explicit.

The pre-push hook checks each non-deletion ref's tip SHA. The `private-doc-content`
preflight task checks HEAD; neither checks uncommitted edits. A missing private checkout/ref
is `NOT_RUN` (exit 2), findings are `FAIL` (exit 1), and both block the hook. Public CI retains
the path guard because it lacks the private source. The local hook must be installed for
local enforcement. This is a partial name-disclosure check, not copied-source, architecture
or outgoing-history detection. Short, numeric and otherwise nonmatching module names are
outside its deliberately bounded stem pattern; a pass is not a general secrecy guarantee.

Diagnostics publish document locations and source SHAs, never matched private tokens.
Reviewed public vocabulary collisions live in `CONTENT_EXCEPTIONS`, with a reason and
SHA-256 of the entire committed document. Any edit invalidates that exception. Review every
finding in a document before approving its exact bytes, and triage new matches in the same
change. Token-source changes also require inspection of the exception documents.

`python tools/wiki.py status --strict` compares shared `.claude/skills/*/SKILL.md` and
`.agents/skills/*/SKILL.md` bytes locally. Drift or a missing copy of a known pair is FAIL;
a missing client tree is SKIP. New client-only skills do not require a mirror.
The `shape` and `skill-creator` variants are explicit WARN exceptions pending a contract
decision. Canonical local skills remain under `.claude`; no renderer or public skill-copy
publication is introduced. This check does not prove that a client actually loaded a skill.

## Decision evidence

Consequential recommendations to adopt **or reject** an owner's approach receive a fresh,
read-only independent review. The captain supplies the actual goal, strongest case on each
side, current source evidence and a disconfirming check. Assess fit, benefits, costs and
alternatives separately from popularity or existence. Uncertainty stays unresolved; do not
default to rejection or use majority vote as proof. Reconcile reviewer disagreements against
sources, and label review unavailable when it could not run. Routine mechanical edits do not
need a research fan-out. The portable rule is in `AGENTS.md`; local detailed guidance remains
in the evidence rule. Prompt changes remain UNVALIDATED until their separate evaluation.

## Deliberately not implemented

| Proposal | Decision |
|---|---|
| Automatic task extraction and unrestricted workflow controller | Deferred. The explicit public harness task-contract pilot does not infer authorization, replace host permissions, or provide native continuation evaluation. The job registry remains separate measured external-work state. |
| Managed async-agent service | Rejected for now. Existing bounded local subagents cover observed work; no latency, recovery, or throughput evidence justifies service infrastructure. |
| Broad tool result-envelope rewrite | Deferred. MCP tools already have typed schemas and catalog checks; changing every result would be a breaking client migration without a measured failure. |
| Blanket terse-output truncation | Rejected. Existing flood/read guards target the real context risks; unconditional truncation would hide diagnostic and provenance evidence. |
| New scheduled-job/reconciliation framework | Deferred. It must be designed against a specific unattended workflow, owner, idempotency key, and failure mode rather than added generically. |
| Publishing generated agent content | Not applicable. The project has no automatic agent-to-publication path; human review and existing data contracts remain required. |
| Always-on abuse-hunter agent | Rejected as a default. Independent review is useful for high-risk changes, but mandatory extra adversarial calls would add cost without a failure-triggered scope. |
| Grant/refuse scores, probabilities, rankings, or objection drafting for private Siting | Rejected. These conflict with the evidence-only, professionally reviewed product boundary. Missing land, control, or exact-site evidence remains unresolved, not inferred. |
| Replacing `CLAUDE.md` with a one-line import | Deferred. It currently contains compatibility guidance not represented elsewhere; deleting it before a parity migration could reduce behavior. |

## Operating the benchmark

The default `--order balanced` runs variants together for each selected task and
rotates the first variant by task position and repeat. With two variants, each
task reverses its order on the next repeat. Use an even repeat count for a paired
comparison; with more variants, use a multiple of the variant count. Task order
follows the task dictionary, not selector argument order. `--order fixed` restores
the historical repeat, variant, task nesting for reproduction.

Manifests record the selected tasks and variants, schedule policy, and
`cache_policy: provider-managed-uncontrolled`. Attempts record execution order,
UTC observation time, resolved model/reasoning, original usage-field presence,
and `cache_observation_state`: `unknown`, `no_reuse_reported`, or `reuse_reported`.
Missing cache counters remain unknown; explicit zero is an observation. Codex
cache ratios use total input before normalization, while Claude input, cache reads,
and cache creation are disjoint. Missing denominator components leave the ratio
null. Summaries include observation coverage and nullable observed totals; legacy
zero-filled usage totals are retained for compatibility and are not coverage evidence.

These are attempt-wide usage observations. Starting a new process or chat does not
prove a cold provider cache, and alternating order does not isolate every source of
latency. Do not flush unrelated local caches or send synthetic warming requests.

Validate the tracked prompt, role, and hook contracts without bootstrapping the full dependency
profile:

```powershell
python tools/dev.py agent-context
```

No-cost wiring and isolation check:

```powershell
.venv\Scripts\python tools/evals/harness_bench.py --preflight
```

Public smoke comparison:

```powershell
.venv\Scripts\python tools/evals/harness_bench.py --repeat 4 offclean on
```

The ON arm marks only its validated ephemeral cleanroom as trusted and uses Codex's
automation-only hook-trust bypass so tracked project hooks actually execute. The paid path reruns
the no-cost preflight before the first provider call; OFFCLEAN continues to disable project
settings and hooks.

Private holdout schema, stored outside the checkout:

```json
{
  "tasks": {
    "opaque-id": {
      "prompt": "Return only JSON with the requested fields.",
      "expected": {"decision": false, "owner": "services.example"}
    }
  }
}
```

Set `DAIL_EVAL_INFRA_LABEL` to the stable runner or machine class. Pin provider/model/reasoning
with the existing `DAIL_EVAL_PROVIDER`, `DAIL_EVAL_MODEL`, and
`DAIL_EVAL_REASONING_EFFORT` variables. Set `DAIL_EVAL_HOLDOUT_VERSION` to an opaque private
suite version. Compare multiple attempts; do not present a single run or a public smoke suite
as proof of general harness quality. Attempt rows record elapsed time, score, errors, tool and
MCP calls, usage, and provider-reported cost; summaries aggregate those measures per task and
variant.

The local cleanroom removes the direct cwd and Git-history route to the scorer; it is not an OS
sandbox. A provider with arbitrary host-file read access could still escape that boundary. Run
strict secret holdouts in a container or VM that mounts only the cleanroom and provider runtime,
with expected answers available only to the evaluator process.

Codex loads the bounded SessionStart hook from tracked `.codex/config.toml`. After cloning or
changing the hook, review and trust its exact definition once with `/hooks`; until that trust
step, Codex deliberately skips the project command hook. Claude's `.claude/` configuration
remains workstation-local by repository policy.

## Local editor telemetry

Keep client measurements separate: Copilot OTel, Codex response records, and the
Claude SessionEnd ledger have different scopes. Codex's bounded report above is
content-free; the older Claude `token_ledger.py` and `token_week_review.py` also print
prompt snippets. The Claude ledger contains completed-session snapshots, combines
fresh input and cache creation, and is not an event-window or billing ledger.

Copilot OTel settings have application scope in the installed extension. Configure
them in local VS Code User Settings, with an absolute local file destination:

```json
{
  "github.copilot.chat.otel.enabled": true,
  "github.copilot.chat.otel.exporterType": "file",
  "github.copilot.chat.otel.outfile": "<absolute-local-path>/copilot.jsonl",
  "github.copilot.chat.otel.captureContent": false,
  "github.copilot.chat.agentDebugLog.fileLogging.enabled": true,
  "github.copilot.chat.agentDebugLog.fileLogging.maxRetainedSessionLogs": 5,
  "github.copilot.chat.agentDebugLog.fileLogging.maxSessionLogSizeMB": 20
}
```

Back up the original settings and exclude these keys from Settings Sync for a local
pilot. Reload the window, perform a representative task, then open **Developer:
Open Agent Debug Logs** and its Summary view. Use Cache Explorer when reported
reuse and latency warrant investigating changes between requests. Configuration
on disk is not proof of activation: confirm new telemetry records after reload.
Managed policy and environment overrides can change the effective settings.

OTel content capture off and debug logging are separate controls. Debug logs can
contain prompts and source; keep them local and disable file logging after the
diagnostic sample. The five-session/20-MB bounds apply to debug logs, not the OTel
outfile. Archive that append-only outfile after the sample and disable OTel when
the pilot ends. Do not commit or upload either raw log stream.

The installed Copilot file exporter interleaves raw SDK spans, logs, and metrics.
They are not three independent sources of token spend: `invoke_agent` spans may
aggregate child `chat` calls, and logs/metrics can repeat those measurements.
Validate a captured schema before building a parser, deduplicate request/span
identities, and avoid adding parent and child usage together. Retain provider
semantics and missing-field coverage when computing cache ratios.

Independent Codex and Claude extensions are not configured by Copilot's OTel
settings. Codex OTel routing belongs in user-level `~/.codex/config.toml`; do not
add a collector endpoint without a running consumer. Existing response records
provide a local Codex token baseline without enabling a new export service.

Compare one setting at a time with fixed tasks, model, reasoning, tools, and
acceptance checks. Record correctness, elapsed time, input/cache/output tokens,
tool calls, errors, and rework. Keep cache-key, breakpoint, search/execution
subagent, and reasoning-effort experiments separate from instrumentation. A higher
cache ratio alone is not evidence of a better harness.

Sources: [VS Code OTel](https://code.visualstudio.com/docs/agents/guides/monitoring-agents),
[Cache Explorer](https://code.visualstudio.com/docs/agents/agent-troubleshooting/cache-explorer),
and [Codex configuration](https://developers.openai.com/codex/config-reference).
