# Harness evidence integrity

## Trigger

Use this when changing review hooks, evaluation cleanrooms, usage comparators,
mutation selection or focused verification policy.

## Confirmed failure modes

Review-completion events can credit files the reviewer never inspected. Retain an
explicit reviewer result with checkout, session, reviewed paths, content digests
and acceptance evidence. Invalidate coverage after a content change. Use
`tools/review_receipt.py` after the independent review returns its actual result.

Provider completion and task acceptance need separate evidence. Keep provider
failures and their observed spending in the denominator. Require independent
quality acceptance before recommending an arm. Preserve missing usage as unknown,
retain explicit zeroes and reject inconsistent counters. Require comparable
provider, model, effort, harness, tool policy, inputs and native usage schema.

Bind evaluation provenance through the normal entry point. A digest computed by
the cleanroom copier must survive preflight and reach the run manifest. Hash the
actual copied bytes, including visible untracked source, and ensure retrieval
tools use the copied evaluation root. A directory copy still permits host access
under the selected runtime's permissions.

An empty mutation session provides no assertion-sensitivity evidence. Keep
hand-authored mutation targets and harness provenance visible while excluding
known generated session state. Mark upstream-source checks as fresh observations
so a local fingerprint cannot reuse an old external result.

## Verification and reuse

The 2026-11-17 repairs have focused regressions in
`test/hooks/test_code_hygiene_gate.py`, `test/hooks/test_verifier_nudge.py`,
`test/tools/test_review_receipt.py`, `test/tools/test_run_mutation_pilot.py`,
`test/tools/test_verify_changed.py`, `test/tools/test_bench_compare.py` and
`test/tools/evals/test_{cleanroom,provider_adapter,harness_bench}.py`.
These tests cover missing evidence, stale content, aliases, error attempts,
untracked inputs and unknown or malformed counters. Their results establish
deterministic plumbing. Live hook execution and model-quality or savings claims
require the corresponding observations.

Reuse the `adversarial-regression` and `harness-compare` skill templates for future
repairs and comparisons. Run a minimal reproducer first, then the relevant
existing lanes. Preserve the selected checkout's final gates.
