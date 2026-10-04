---
tier: REFERENCE
status: LIVE
domain: infra
updated: 2026-10-04
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

The Codex TOML currently registers SessionStart, agent-spawn validation, and Stop.
The broader `.codex/hooks.json` compatibility matrix does not establish live Codex
hook activation. In particular, use `tools/discoveries.py` directly unless `/hooks`
confirms discovery-hint injection.

Sources: [Codex IDE commands](https://learn.chatgpt.com/docs/developer-commands?surface=ide),
[prompt caching](https://developers.openai.com/api/docs/guides/prompt-caching), and
[skill configuration](https://learn.chatgpt.com/docs/config-file/config-reference).

## Implemented controls

| Guide-derived proposal | Validation | Repository implementation |
|---|---|---|
| Canonical layered guidance | Already effective; shared UI prompts bypassed it | UI prompts now load root and nested `AGENTS.md`; `CLAUDE.md` remains a fallback |
| Fixed task and result contracts | Existing entry prompts varied and reviewers returned unstructured prose | `.github/prompts/` uses the five-section packet; reviewers use evidence-bearing verdicts |
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

## Deliberately not implemented

| Proposal | Decision |
|---|---|
| General persistent active-task state file | Still deferred. The adopted job registry is narrower: it records measured external work that can outlive a turn, not plans, agent tasks, or product decisions. |
| Managed async-agent service | Rejected for now. Existing bounded local subagents cover observed work; no latency, recovery, or throughput evidence justifies service infrastructure. |
| Broad tool result-envelope rewrite | Deferred. MCP tools already have typed schemas and catalog checks; changing every result would be a breaking client migration without a measured failure. |
| Blanket terse-output truncation | Rejected. Existing flood/read guards target the real context risks; unconditional truncation would hide diagnostic and provenance evidence. |
| New scheduled-job/reconciliation framework | Deferred. It must be designed against a specific unattended workflow, owner, idempotency key, and failure mode rather than added generically. |
| Publishing generated agent content | Not applicable. The project has no automatic agent-to-publication path; human review and existing data contracts remain required. |
| Always-on abuse-hunter agent | Rejected as a default. Independent review is useful for high-risk changes, but mandatory extra adversarial calls would add cost without a failure-triggered scope. |
| Grant/refuse scores, probabilities, rankings, or objection drafting for private Siting | Rejected. These conflict with the evidence-only, professionally reviewed product boundary. Missing land, control, or exact-site evidence remains unresolved, not inferred. |
| Replacing `CLAUDE.md` with a one-line import | Deferred. It currently contains compatibility guidance not represented elsewhere; deleting it before a parity migration could reduce behavior. |

## Operating the benchmark

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
.venv\Scripts\python tools/evals/harness_bench.py --repeat 3 offclean on
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
