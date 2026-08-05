---
name: review-page
description: Review one Streamlit page for logic firewall and UI boldness
argument-hint: "[page_id]"
agent: agent
---

## Objective

Decide whether page `${input:page_id}` is safe and complete enough to hand off.

## Scope

Check the logic firewall, registered-view assumption, contract compliance, current-view export,
source links, temporal controls, `TODO_PIPELINE_VIEW_REQUIRED` wiring, and material UI quality.
Run `tools/check_streamlit_logic_firewall.py`. Do not rewrite unrelated code.

## Invariants

Review against the page runbook and contract. Do not invent requirements, data semantics, or
backend work that the contract does not require.

## Acceptance

Run focused tests when available. Treat an unrun check as `NOT RUN`, not a pass. The verdict is
`PASS` only when no blocker or major finding remains.

## Result contract

Return:

- `Verdict: PASS | FAIL`
- each finding as `Severity: blocker | major | minor`, `Evidence: path:line`, consequence, and
  the smallest required action
- verification commands and observed results
- residual risk or `None`
