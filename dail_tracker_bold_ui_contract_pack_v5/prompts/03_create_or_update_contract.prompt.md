Create or update the YAML contract for page: `<PAGE_ID>`.

The contract must use:

```yaml
data_access:
  mode: duckdb_in_process_registered_analytical_views
  persistent_duckdb_file: null
```

Include:
- approved registered views
- retrieval SQL policy
- temporal mode
- interaction patterns
- UI creativity budget
- exports
- source links
- member drilldown if relevant
- acceptance tests

Do not point at a persistent DuckDB file.
Do not add Streamlit-side modelling.
Missing data becomes:
`TODO_PIPELINE_VIEW_REQUIRED`.
