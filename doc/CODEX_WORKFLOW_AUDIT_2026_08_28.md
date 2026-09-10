---
tier: REFERENCE
status: LIVE
domain: infra
updated: 2026-08-28
supersedes: []
read_when: auditing Codex task efficiency, planning-product ownership, long-running work, or git closeout
key: REFERENCE|LIVE|infra
---

# Codex and planning-product workflow audit - 2026-08-28

## Verdict

The recurring waste is not long context by itself. It is loss of operational identity: agents
reinspect implemented Siting work, poll a moving shared checkout, confuse related artifacts, and
reach closeout without one explicit Git root, checkout, writer, and commit/push owner.

The repository already had good durable mechanisms: layered `AGENTS.md`, the trigger-keyed
discovery index, evidence notes, a session closeout ledger, fingerprint-bound verification, and a
careful sidecar protocol. This audit did not create substitutes. It closed two concrete gaps:

- long builds/transfers now have a measured append-only status registry; and
- multi-root commit/push actions are checkout-explicit and path-explicit.

## Evidence reviewed

The audit listed the 50 most recent Codex tasks, selected project-relevant summaries from the
approximately seven-day request window, and then read the actual turns and outputs for the
following high-signal tasks. Titles were used only to select candidates.

| Task | Thread | Evidence from turns |
|---|---|---|
| Locate NLC prechunk processing | `01a046f5-272f-7e32-a055-4e06fc0cd8c3` | Distinguished the 31.4 GB canonical NLC parquet, an experimental reviewer-only partition lane, and the missing runtime sibling; replaced an eager 10.1m-geometry build with bounded DuckDB spill. Ways and Water siblings completed while NLC remained running. |
| Define SCP layer sync | `01a04528-efa9-77a2-a6a9-ad154605d32f` | Two sessions briefly owned the same archive; one transfer was interrupted after 13 seconds and left indeterminate. Later turns delivered strong CI repairs, but local transfer, remote staging, CI, and image-build state were reported in one conversational stream. |
| Audit planning product learnings | `01a04063-f29a-7463-822f-fa58c15de018` | More than 160 status messages repeatedly sampled runners and quiet windows while other tasks changed the private tree. The audit never obtained a durable terminal snapshot during the reviewed turns. |
| Run tests and Docker validation | `01a04061-0d2e-7a82-8f0c-75aa264bef10` | Correctly kept public and private commits separate, but spent many updates rediscovering whether concurrent commands and writes had stopped. |
| Commit planning logic | `01a0405e-54f2-7773-b5a6-af1334b7ccb4` | Positive closeout: isolated the nested private root, preserved unrelated public changes, ran focused tests when the RAM guard blocked the full suite, and committed a clean private snapshot as `8d99ee4`. |
| Inspect ArcGIS Ways layer | `01a03a2b-ffda-7111-acd2-e9f8638e76a4` | A useful implementation audit found real index/dependency gaps. Later planning repeatedly proposed a new service/workbench/node/output before the user forced a current-state inventory that found existing ingest, evidence, appendix, GIS, and DXF paths. |
| Check cadastre integration status | `01a03eee-4833-70d3-a7be-adcd3fe17cac` | Positive evidence boundaries were preserved, but product direction expanded through several artifact concepts before converging on an existing reviewer-only seam. |

Pi Firstmate was invoked for the required Public Signal and SpecPlan advisory preflight. It timed
out after four minutes and produced no evidence, so no finding in this audit relies on it.

## Highest-value positive patterns

1. **Exact failure to durable contract.** The SCP/CI task converted platform-dependent deep-JSON
   behaviour into an explicit structural limit, added an in-image import smoke test after a Docker
   allowlist omission, and reproduced a report-completion race deterministically. These are the
   best examples of spending tokens once and leaving a guard.
2. **Evidence-bounded performance work.** The NLC task stopped an unsafe eager materialisation,
   built a bounded DuckDB external-sort path, preserved the canonical dataset, and kept final
   manifest authorization behind completion. It did not treat the earlier reviewer experiment as
   a production derivative.
3. **Correct nested-root closeout when ownership was clear.** The planning-logic task separated the
   private nested commit from unrelated public work and reported exact focused verification and the
   full-suite RAM blocker.
4. **Existing harness controls are generally sound.** Sidecar packets bind read scope and Git
   snapshots; discoveries preserve expensive lessons; verification receipts are fingerprint-bound;
   release gates distinguish local, CI, container, and live-source evidence.

## Main waste patterns and root causes

| Priority | Pattern | Root cause | Consequence |
|---:|---|---|---|
| 1 | Repeated quiet-window/process polling | Several writers shared the primary private checkout; there was no job owner or authoritative terminal event | Dozens of near-identical updates, tests invalidated by later edits, no clear handoff |
| 2 | Work left for manual add/commit/push | Three Git roots, extra worktrees, private repos absent from public Codex worktrees, concurrent dirty state, and no explicit closeout owner | Correct changes exist locally but responsibility is rediscovered at the end |
| 3 | NLC, Ways, Water, Buildings, local builds, and SCP reported as one evolving state | Dataset name was used as identity instead of operation plus artifact plus owner | A canonical 31.4 GB file, point-scoped sibling, experimental partitions, and remote copy were conflated |
| 4 | New Siting architecture proposed before current-state inventory | Planning/design workflows began before reading implemented probes, staged work, tests, and output paths | User repeatedly had to say the proposal ignored existing work or was over-engineered |
| 5 | Valuable findings not promoted consistently | Discoveries exist, but the 500-turn closeout threshold and voluntary promotion do not select every expensive medium-length task | Index compatibility, output reuse, and operating traps can be rediscovered |
| 6 | Advisory integrations add latency when unhealthy | Pi Firstmate can run for four minutes without evidence | Long tasks spend time waiting on a non-authoritative input |

Safety rules were not the main cause of uncommitted planning work. They correctly prevented broad
staging and commits during concurrent edits. The failure was missing orchestration around those
rules: the writer and exact checkout were not established early, and worktrees did not include the
private nested repository. Worktree isolation and nested-root absence are therefore real blockers;
"Codex refused to commit" is usually an incomplete diagnosis.

## Changes implemented

### 1. Measured long-job registry

`tools/job_status.py` records one append-only event per observation with:

- stable job id, owner, kind, exact artifact, and source snapshot;
- phase, current/total/unit, ETA or `unknown`, observation time, and evidence;
- an atomic ownership claim, monotonic progress, immutable scope, owner-only updates, and explicit
  terminal states.

A local NLC derivative and an SCP destination must use different job ids. Process existence alone
is not accepted as progress. `python tools/dev.py job-status show --active` is the shared status
view.

### 2. Checkout-safe multi-root closeout

`tools/roots_status.py` still reports all roots and worktrees, but action mode now requires:

- one named repo;
- one exact checkout (defaulting only to that repo path in the invoking checkout);
- explicit intended paths for commit;
- no unrelated staged paths.

It no longer uses `git add -A`, no longer silently chooses the primary checkout from a Codex
worktree, and never implies push from commit. `roots` and `job-status` are stdlib tasks, so they do
not bootstrap or mutate the development environment merely to report status.

### 3. Durable routing rules

Root `AGENTS.md` now assigns writer closeout, exact-root/path staging, separate push authority, and
measured long-job reporting. The compact triggers live in `tools/discoveries.jsonl`; supporting
evidence lives in `memory/project_codex_workflow_audit_2026_08_28.md`.

## Prioritized remaining work

1. **Apply the job registry in the private product's actual NLC/SCP/release entry points.** This
   public worktree does not contain `planning/product`, so this audit could not safely patch those
   scripts. Add start/update/finish calls or wrapper steps in the real private checkout.
2. **Mirror the closeout rule in `planning/product/AGENTS.md`.** Its repository is authoritative for
   private commits and release gates. Do this in that checkout, not by creating a public shadow.
3. **Calibrate expensive-session closeout using current telemetry.** Consider a dual threshold
   based on tokens/tool activity as well as turns. Do not simply lower 500 and recreate the
   unreviewable backlog that caused the increase.
4. **Add a bounded owner/terminal-event check to unattended release scripts.** A second session
   should be able to read status but not start a second writer for the same artifact.
5. **Re-run the harness ON/OFF measurement after enough real jobs exist.** The 2026-08-05 benchmark
   showed equal correctness, fewer tokens/tool calls, and higher elapsed time. Do not claim product
   quality or token savings from these changes until measured on implementation and release tasks.

## Verification boundary

Focused tests cover registry ownership/progress/terminal behaviour, build-versus-transfer
separation, exact-path commits, refusal of unrelated staged paths, worktree reporting, and stdlib
command routing. The full locked profile could not bootstrap in this detached worktree because
network access to missing packages was unavailable; this is an environment limitation, not a green
full gate. The private Siting repository and its release gates were not present and were not run.
