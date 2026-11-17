---
description: Optional smell vocabulary and sources for a named code-hygiene judgement.
---

# Smells and possible refactorings

These are judgement aids, not gate violations or automatic reasons to split a module.

| Smell | Candidate step |
|---|---|
| Mysterious name | Rename Variable or Change Function Declaration |
| Duplicated code | Extract Function or Slide Statements |
| Long function | Extract Function, Decompose Conditional, Split Phase |
| Long parameter list | Introduce Parameter Object or Preserve Whole Object |
| Global data | Encapsulate Variable |
| Divergent change | Split Phase, Extract Class, Move Function |
| Shotgun surgery | Move Function/Field or Combine Functions into Class |
| Feature envy | Move Function |
| Data clumps | Extract Class or Introduce Parameter Object |
| Primitive obsession | Replace Primitive with Object |
| Repeated switches | Replace Conditional with Polymorphism when real variants justify it |
| Speculative generality | Inline Function/Class or Remove Dead Code |
| Middle man | Remove Middle Man or Inline Function |
| Message chains | Hide Delegate |

The existing skill's method combined the repository's measured conventions and
`doc/REFACTORING_TOKEN_ECONOMICS.md` with the
[Fowler catalogue](https://refactoring.com/catalog/),
[Matt Pocock engineering skills](https://github.com/mattpocock/skills/tree/main/skills/engineering),
and [Anthropic's official plugins](https://github.com/anthropics/claude-plugins-official/tree/main/plugins).
This migration preserves that attribution; it does not claim those external sources were
revalidated during the harness repair. Confirm a catalogue step fits the actual code.
