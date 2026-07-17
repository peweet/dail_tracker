---
name: review-page
description: Review one Streamlit page for logic firewall and UI boldness
argument-hint: "[page_id]"
agent: agent
---

Review page: ${input:page_id}

Check:
- logic firewall (no business logic in the page — queries/transforms belong in
  `utility/data_access/`; run `tools/check_streamlit_logic_firewall.py`)
- registered-view assumption (page reads a registered contract, not raw data)
- UI difference from the old page + visual polish (dataframes are secondary)
- contract compliance + current-view export + source links + temporal controls
- `TODO_PIPELINE_VIEW_REQUIRED` wiring

Report findings; don't rewrite unrelated code.
