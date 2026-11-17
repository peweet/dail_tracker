---
name: code-hygiene
description: Measure and simplify code without changing behavior or making the system slower. Use for cleanup, deduplication, pruning, modularization and structural review; behavior fixes follow TDD and review-only requests remain read-only.
---

# Code hygiene

The selected repository's `AGENTS.md` owns boundaries, permissions and verification.
Lower complexity never justifies a regression or a slower system. Long, flat validation
or phase sequences may deserve `leave`.

1. Identify the exact checkout and preserve concurrent edits. For private work, use the
   current main revision and check other worktrees for overlapping unmerged files.
2. Reuse decisions before reading source:
   `python tools/code_context.py show <files> --root <repo>`.
   Add `--rev origin/main` when inspecting private main. Revisit fresh `leave`/`rejected`
   decisions only with new evidence; recheck changed callers and runtime conditions.
3. Measure with `tools/check_code_hygiene.py report --root <repo> --top 20`. Name the
   unchanged observable interface and run its green test before editing.
4. Apply one named refactoring at a time: prune, deduplicate, then simplify. Re-run the
   focused test after each step. Read [the method](references/method.md) for deletion,
   dynamic-reference and performance checks.
5. Before a split or new abstraction, apply [the structural checks](references/structure.md).
   Use [the smell catalogue](references/smells.md) only when a judgement needs a name.
6. Run the explicit hygiene gate on intended files and the repository's required broader
   checks. An incomplete check is not clean. Only the owner adds a justified waiver.
7. For consequential or broad changes, use the allowed `reviewer` role and retain a successful
   receipt bound to reviewed content and acceptance. A reviewer event is not a receipt.

In audit mode, measure and evaluate candidates without editing implementation. Record
verdicts with `tools/code_context.py add`, including `leave` and `rejected`. Report measured
before/after figures, tests, what became harder and any missing performance or artifact proof.

This tracked template is the source for local `.claude` and `.agents` copies. The hook is
advisory and has bounded retries; the explicit repository gate supplies the final result.
