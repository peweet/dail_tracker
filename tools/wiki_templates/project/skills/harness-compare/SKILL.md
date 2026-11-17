---
name: harness-compare
description: Compare a proposed skill, tool, model or harness change using comparable tasks, correctness controls and cost per successful task. Use for claims of AI token or latency savings; not for ordinary code fixes or a single timing observation.
---

# Harness comparison

Read `doc/AGENT_HARNESS.md` and the selected evaluator's `--help`. Reuse its controls and
receipt format. Keep deterministic plumbing checks separate from agent-quality evidence.

1. Specify one intervention and a quality acceptance rule before measuring savings. Freeze
   representative held-out tasks and independent expected results outside the agent workspace.
2. Bind source, task and answer-key digests, provider/model/effort, tool policy, runtime and
   usage schema. Refuse incomparable arms. Missing usage is unknown, never observed zero.
3. Run scorer, permission, error and missing-evidence controls without model calls first.
   Check that every enabled tool reads the intended evaluation root. Host filesystem
   access can extend beyond a cwd cleanroom. Secret holdouts require an isolated runtime.
4. For authorized model runs, use repeated paired, counterbalanced attempts with a bounded
   task/time budget. Native resume requires native-session evidence; replay is replay.
   Do not start paid/live evaluation without the authorization required by the repository.
5. Retain failed attempts and their spent usage. Report eligible/completed/successful counts,
   failures, correctness, evidence coverage, uncached input, output and latency per success.
   Require accepted quality before recommending a cheaper arm. Use observed billing data.
6. Use `tools/evals/bench_compare.py` for compatible recorded comparisons and
   `tools/codex_token_usage.py` for bounded usage summaries. Disclose partial scans.

Return the identities, quality result, paired changes, within-arm variation, missing metrics,
and adopt/reject/unresolved recommendation. Single-run differences remain indicative.
