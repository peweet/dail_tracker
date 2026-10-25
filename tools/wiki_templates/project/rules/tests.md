---
description: Assertions and baselines are off limits - never weaken a test or a recorded baseline to make a change pass
paths:
  - "test/**"
  - "**/test_*.py"
  - "**/conftest.py"
  - "data/_meta/*quality*"
  - "data/_meta/*baseline*"
  - "test/expected_failures.yaml"
---

# Tests and baselines

- Never delete, loosen or comment out an existing assertion so a change passes. Fix the code
  under test. If the assertion is wrong, say so, cite why, and stop for the user's decision.
- Never add `skip`, `xfail` or a broader `except` to get green. An expected failure goes
  through `test/expected_failures.yaml` with a reason and an owner, which
  `check_expected_failures.py` validates in CI.
- A recorded baseline (gold quality, output regression, extraction match-rate) changes only
  with data-anchored evidence and the user's agreement. A re-baseline is a separate commit
  that says what moved and why.
- A test asserts the seam (returned columns, grain, row floor, error state), not a private
  helper, and never recomputes the expected value with the logic under test.
- `tools/hooks/assertion_guard.py` warns when an edit removes an assertion or adds a skip
  marker. The warning is the prompt to re-read this page, not a gate to argue with.
