# Registered Analytical Views

This project should treat the frontend data model as:

```text
Parquet/gold outputs
→ in-process DuckDB connection
→ registered analytical SQL views
→ Streamlit retrieval SQL
```

The contracts do not point to a persistent database file.

## Streamlit may query

Approved registered views declared in the page contract.

## Streamlit may not do

- create or register views
- call `read_parquet`
- call `parquet_scan`
- join raw files
- calculate business metrics
- perform fuzzy matching
- use a persistent database path as a shortcut

## Missing data

Use:

```text
TODO_PIPELINE_VIEW_REQUIRED: <specific missing view/column/filter/metric/source_url>
```

Then implement the missing data shape in the pipeline/view layer.
