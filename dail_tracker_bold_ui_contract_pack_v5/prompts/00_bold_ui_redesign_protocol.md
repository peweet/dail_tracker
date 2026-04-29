# Bold UI Redesign Protocol

Use this at the start of any UI redesign task.

```text
This is a UI redesign, not a safe refactor.

The existing page is a functional reference, not a design reference.

Preserve backend behaviour and data semantics.
Rethink layout, hierarchy, interaction, controls, chart/table presentation, source links, empty states, and visual polish.

The final UI must be materially different from the current page.
If it looks basically the same, the task has failed.

Do not add backend logic or business logic to Streamlit.
If missing data is needed, write:
TODO_PIPELINE_VIEW_REQUIRED: <specific missing item>
```
