# Cheap Codex cross-session sidecar handoffs - 2026-08-23

## Trigger

Use this when one Codex session is large and another session could answer a genuinely
independent, bounded question or perform a read-only review. A large context by itself
is not the trigger: compact the original session when the remaining work is one tightly
coupled evidence chain. Never create work merely to fill concurrency slots.

## Durable lesson

The original session remains captain, integration owner, and sole writer. Sidecars use
the `scout` or `reviewer` role and name exact read paths. Before dispatch, choose a
human-readable task key from objective, scope, source snapshot, and role. Reusing that
key lets the handoff helper block duplicate active work even when the wording changes.

Bind every result to the canonical worktree root, `HEAD`, the actual contents of changed
tracked and untracked files, and every declared read path. This is important because the
public root, private product root, and other worktrees can have different ownership and
state. Run `python tools/dev.py roots` first and pass the exact source worktree and the
same bounded read paths to the snapshot, validate, and queue commands.

## Minimal workflow

```powershell
python tools/dev.py roots
python tools/dev.py sidecar-handoff snapshot --root . --read-path tools/example.py
python tools/dev.py sidecar-handoff template
python tools/dev.py sidecar-handoff validate --thread <target-uuid> --source-root . --file <temp-packet.md>
python tools/dev.py sidecar-handoff queue --thread <target-uuid> --source-root . --file <temp-packet.md>
python tools/dev.py sidecar-handoff status --thread <target-uuid> --handoff-id <sc-id>
python tools/dev.py sidecar-handoff recover --thread <target-uuid> --task-key <key> --handoff-id <sc-id> --resolution <accepted|failed>
```

The Markdown packet is the only work item. Keep it in a temporary file outside the
source worktree so the snapshot field is not self-referential. It records the task key,
snapshot, read-only role, bounded relative read paths, target write ownership, sidecar
verification status, findings, checks run, checks not run, and integration advice. The
ignored local JSONL receipt stores metadata but not the evidence body. There is no
service, scheduler, or global task ledger to operate.

## State and authority boundary

The lifecycle is `draft -> accepted_unconsumed -> delivered -> integrated -> verified
-> closed`, with canceled, expired, or unresolved as explicit alternatives. The helper
only observes through `delivered`:

- A successful `codex queue` receipt means `accepted_unconsumed`.
- A matching receipt-backed, complete immutable user message in the target transcript
  means `delivered`; quoted markers and untracked or failed handoffs do not.
- A malformed receipt, unexpected runner interruption, or receipt-ledger failure keeps
  the atomic claim and reports `recovery_required`. Inspect the target, then release the
  exact claim as `accepted` or confirmed `failed`; never retry by deleting it blindly.
- Only the target session may decide `integrated`, `verified`, or `closed`.
- The sidecar may report checks as `not_run` or `reported`; it cannot declare target
  verification.
- Do not resend blindly. A corrected packet must name the active handoff in
  `supersedes`; unrelated work gets a different task key.
- Symlinked declared, changed, or untracked paths fail closed because an external target
  cannot be freshness-bound to the named worktree cheaply.

## Evidence from the first use

The useful parallel lanes were independent repository mapping, integration-point
inspection, and read-only process review. Keeping them read-only avoided collision with
the original writer. The independent review caught a material semantic defect: queue
acceptance had initially been treated as delivery. Focused tests now cover snapshot
staleness, task-key deduplication, explicit supersession, queue failure retry, receipt
storage, and transcript-confirmed delivery.

The goal is shorter critical-path time with trustworthy evidence, not maximum worker
utilization. If a sidecar cannot state a non-overlapping objective and exact read scope,
leave it idle.
