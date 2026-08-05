---
name: build-page
description: Build one Streamlit page from its YAML contract with bold UI
argument-hint: "[page_id]"
agent: agent
---

## Objective

Build page `${input:page_id}` from its runbook and YAML contract.

## Scope

Read only:

- `AGENTS.md` and `utility/pages_code/AGENTS.md`
- `dail_tracker_bold_ui_contract_pack_v5/page_runbooks/${input:page_id}.md`
- `dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/${input:page_id}.yaml`
- shared policy/pattern files, the target page file, shared CSS, and relevant `utility/ui` helpers

Do not inspect unrelated pages or generated data folders. Use `search_project` or the
dail-tracker MCP to place data; never scan a parquet.

## Invariants

Preserve data semantics and the logic firewall: queries and transforms belong in
`utility/data_access/`, not the page. Emit `TODO_PIPELINE_VIEW_REQUIRED` for missing data
instead of inlining a query.

## Acceptance

- The runbook's allowed-file boundary is respected.
- Contract fields, temporal behavior, current-view export, source links, and empty states work.
- Focused page tests and the logic-firewall checker pass.

## Result contract

Return changed files, acceptance checks with command evidence, and unresolved items. Do not
claim a check passed unless it ran; distinguish automated, mocked, and manual verification.
