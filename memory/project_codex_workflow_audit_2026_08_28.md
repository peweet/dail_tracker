# Codex workflow closeout and long-job status - 2026-08-28

## Trigger

Use this note when planning or Siting work is left uncommitted, a Codex worktree cannot see
`planning/product`, or several tasks are reporting one long-running spatial build or transfer.

## Durable lessons

1. The public repository, private `planning/product`, and private Public Signal product are
   independent Git roots. A Codex worktree for the public repo does not automatically contain the
   private nested checkouts. Confirm the exact root and checkout before editing, staging, committing,
   or asking the user to push.
2. Git's worktree list puts the primary checkout first. An action launched from another worktree
   must never infer that the primary checkout is its target. Commit one explicit checkout and only
   the declared paths; refuse an index already holding unrelated work.
3. A local derivative build, an SCP transfer, and remote verification are separate jobs even when
   they concern the same dataset. Register distinct job ids and artifacts in `tools/job_status.py`.
   Only the owner updates a job, and every status needs an observation time and evidence. ETA stays
   `unknown` unless measured.
4. Before proposing a new SpecPlan/Siting subsystem or artifact, inventory implemented code, staged
   work, tests, probes, and output seams. Recent Ways work repeatedly proposed a new service,
   workbench, node, and PDF before rediscovering the existing ingest, evidence contract, appendix,
   GIS, and DXF paths.
5. Preserve long, coherent sessions when valuable. The waste to remove is repeated rediscovery,
   moving-target polling, and ambiguous ownership, not context length by itself.

## Commands

```powershell
python tools/dev.py roots
python tools/dev.py job-status show --active
python tools/dev.py roots --repo public --checkout . --commit --path <file> -m "<message>"
```

Full evidence and remediation priorities are in
`doc/CODEX_WORKFLOW_AUDIT_2026_08_28.md`.
