# SQL view guidance

- A view's declared grain is a contract. Preserve keys and provenance columns, and never combine the three money grains or sum TED notice values.
- Before renaming a view, changing registration order, or changing a column, inspect `view_deps` or `column_deps`; regex-only lineage hits require a direct source read.
- Keep transformations in registered views rather than Streamlit pages.
- Add or update a contract under `test/sql_views/`. With committed gold data present, run `DAIL_INTEGRATION_TESTS=1 uv run pytest -m sql -q`.
