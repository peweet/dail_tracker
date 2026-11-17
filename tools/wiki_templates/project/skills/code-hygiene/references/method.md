---
description: Conditional cleanup, dependency and performance checks for code hygiene.
---

# Cleanup method

The owner prioritizes private engine, spatial and report maintenance before civic cleanup.
Follow current repository guidance and recorded decisions; dated worktree or timing figures
are evidence to recheck, not current authority.

Prune unused imports/variables, commented-out code and unreachable branches. Before deleting
a function, inspect references plus string names, dynamic imports/getattr and patch targets.
An empty static-reference result is evidence, not proof.

Before extracting a helper, look for an existing owner in the selected root's shared services.
Prefer Extract Function, Decompose Conditional, Guard Clauses or Split Phase as appropriate.
Change a parameter interface only when its callers and compatibility are covered.

Performance applies to cleanup as well as restructuring. State whether the path runs once per
job or once per element. Additional per-element calls, allocations or lookups require a matched
before/after comparison using that repository's canonical performance harness, with alternating
order and parity checks. Read a performance-ledger row to its end: it may record a repair after
the old measurement. Estimate absolute whole-job benefit before building. Missing evidence
leaves a candidate; it does not authorize a slower refactor.

Name any golden file, snapshot, digest or byte comparison that pins the output. Ordering and
hashed bytes are behavior. A helper test cannot prove issued-artifact parity.

Stop on an unrelated red baseline, a behavior-changing step, or three unsuccessful attempts
at the same step. Do not weaken tests, baselines or gates. Record the exact limitation.
