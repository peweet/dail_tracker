"""The planning lane.

Two subpackages, split by who may see them:

- ``planning.civic`` — PUBLIC. The planning-statistics lane that belongs to the civic tool:
  appeal outcomes, decision profiles, the applications register, CPO, the LA overturn view.
- ``planning.product`` — PRIVATE. The siting constraint engine, the council rulebook corpus
  and everything that serves them. Gitignored as the single prefix ``/planning/product/``.

The split is the whole point of this package: the public/private boundary is one directory
prefix rather than a list of globs. See doc/private/PLAN_PLANNING_CONSOLIDATION.md.
"""
