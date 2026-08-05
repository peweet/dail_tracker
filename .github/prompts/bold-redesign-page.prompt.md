---
name: bold-redesign-page
description: Boldly redesign one Streamlit page without changing backend logic
argument-hint: "[page_id]"
agent: agent
---

## Objective

Redesign page `${input:page_id}` so its information hierarchy and interaction are materially
better while its data behavior remains unchanged.

## Scope

Use `dail_tracker_bold_ui_contract_pack_v5/page_runbooks/${input:page_id}.md` and read only the
files it lists.

## Invariants

This is a UI redesign, not a safe refactor. The existing page is a functional reference, not a
design reference. Preserve all data semantics and the logic firewall. Produce a redesign plan
before coding.

## Acceptance

- The page is materially different and answers its primary user question above the fold.
- The contract, shared UI patterns, accessibility rules, and current-view export are preserved.
- The logic-firewall checker and focused page tests pass.

## Result contract

Return implemented design decisions, changed files, check evidence, and unresolved items.
Separate automated checks from visual/manual inspection and do not claim unrun verification.
