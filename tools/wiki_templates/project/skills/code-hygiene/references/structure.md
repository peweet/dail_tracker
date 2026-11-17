---
description: Conditional checks before splitting files or adding interfaces during code hygiene.
---

# Structural checks

1. Name the catalogue refactoring and intended common change it will localize.
2. Map imports, callers, string/dynamic references, persisted names and file-relative paths.
   Use the existing navigation tools when explicitly enabled; otherwise bounded shell reads.
3. Deduplicate first. Apply the measured split gate in `tools/check_conventions.py` and the
   selected repository's refactoring runbook. A gate measured in another root is indicative.
   Reopen a recorded exclusion only with new evidence.
4. Keep the named interface fixed. A compatibility facade must be pure re-exports with explicit
   `__all__`; preserve ordering, errors and serialized forms as well as names.
5. Add an interface/protocol only with two real implementations. Production plus a test stand-in
   counts. One implementation does not justify an abstraction by itself.
6. Apply the deletion test: if removing a layer removes its complexity, it was a pass-through;
   if the complexity would recur at each caller, the layer may earn its place.
7. Preserve the root's data, evidence, atomic-write and UI/business-logic boundaries.
8. Apply the no-slowdown and exact-output checks in `method.md` before retaining the step.

Keep substantial structural steps independently reviewable. Record accepted, rejected and
measure-first candidates in the selected root's existing code-context ledger with evidence.

Vocabulary: an interface includes ordering and errors; a deep module hides substantial
behavior behind a small interface; a seam allows behavior to vary without editing its caller;
locality keeps a change, its bugs and the necessary knowledge together.
