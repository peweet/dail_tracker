You are working in the Dáil Tracker repo (Python + Streamlit). The procurement page lives at `utility/pages_code/procurement.py`; it routes drill-down profiles via `st.query_params` and renders through `_render_*` functions. Pages contain no business logic: all data access goes through `fetch_*_result` functions imported from `utility/data_access/`.

Add a new drill-down profile for a "framework agreement" entity, following the existing drill-down patterns exactly:

1. A `_framework_href(name)` URL builder following the existing `_*_href` builder pattern.
2. A `_render_framework_profile(name)` renderer that shows a hero/header with the framework name and, for now, an empty-state body using the page's existing empty-state component — following the structure of an existing profile renderer.
3. Wire `?framework=<name>` into the query-param dispatch in `procurement_page()` alongside the existing profile branches.
4. Add a stub `fetch_framework_result(name)` to the procurement data-access module that returns the module's standard "ok but empty" result shape, following existing fetch functions exactly, and import it in the page the same way the other fetchers are imported.

At the very end of your response, output exactly this JSON block (fill in real values):

```json
{
  "files_read": [
    {"path": "utility/pages_code/procurement.py", "chars": 123456}
  ],
  "response_chars": 7890
}
```

`files_read` must list every file you read, with the total characters you actually received from reads of that file. `response_chars` is the total characters of code and text you produced.

Do NOT commit the change. Do NOT run any tests or the app. Stop after writing the code.
