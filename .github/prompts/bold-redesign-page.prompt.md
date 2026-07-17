---
name: bold-redesign-page
description: Boldly redesign one Streamlit page without changing backend logic
argument-hint: "[page_id]"
agent: agent
---

Redesign page: ${input:page_id}

Use `page_runbooks/${input:page_id}.md` and read only the files it lists.

This is a UI redesign, not a safe refactor. The existing page is a **functional** reference,
not a design reference. Preserve all data semantics and the logic firewall (no queries in the
page). Produce a redesign plan first — do not code until the plan is clear.
