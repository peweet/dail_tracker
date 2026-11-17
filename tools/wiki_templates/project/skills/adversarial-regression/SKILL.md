---
name: adversarial-regression
description: Turn a confirmed failure or a fragile change into a minimal reproducer and adversarial regression coverage. Use for missing evidence, stale state, retries, input boundaries and incomplete test results; keep review-only requests read-only.
---

# Adversarial regression

Read the selected checkout's `AGENTS.md`. Preserve unrelated work and keep public and
private verification separate. A review returns findings; an authorized repair follows TDD.

1. Name the observable failure, invariant and smallest real or synthetic input. Reuse an
   existing incident or case registry. Never turn customer data into a public fixture.
2. Run one focused regression red before changing production code. Record the node and
   isolated failure. Include the neighbouring valid case and the expected unavailable state.
3. Attack the evidence as well as the input: remove or duplicate a required item, change
   identity, omit one assertion, return an unknown state, retry after failure, and mutate
   content after verification. Missing, stale or mismatched required evidence cannot PASS.
4. Make the smallest repair, then record green. Add a Hypothesis property/state machine
   when sequences or input combinations matter; retain minimized failures as regressions.
5. Run the relevant existing lanes. Use narrow mutation checks for assertion sensitivity;
   zero active mutants is not evidence. API fuzzing needs a controlled target. Artifact,
   browser and live-source obligations follow the selected repository's guidance.
6. Request one bounded independent reviewer for consequential changes. Retain the actual
   outcome and reviewed content identity; a completion event alone proves nothing.

Return changed files, red/green commands, required versus observed coverage, receipt location,
and checks not run. Use `verify --plan` to inspect selection; never weaken final gates to
make iteration cheaper. This skill introduces no scheduler or paid evaluation.
