# Contract Authoring Guide

A page contract is a narrow agreement between the pipeline-owned analytical layer and Streamlit.

It says:

> The pipeline provides this already-computed shape; the UI may retrieve and present it.

It does not define business meaning.

## Required sections

- `page`
- `intent`
- `design`
- `data_access`
- `sql_policy`
- `filters`
- `metrics`
- `visuals`
- `tables`
- `exports`
- `provenance`
- `empty_states`
- `out_of_scope`
- `logic_firewall`
- `acceptance_tests`

## SQL policy

Allowed:
- SELECT
- explicit columns
- FROM approved views
- WHERE approved filter columns/operators
- ORDER BY approved columns
- LIMIT

Forbidden:
- JOIN
- GROUP BY
- HAVING
- window functions
- CREATE VIEW
- read_parquet
- parquet_scan

## Metrics

Prefer:
- precomputed columns
- precomputed summary views
- UI row count only

Avoid:
- Python formulas
- dataframe groupby
- hidden score calculations
- “conflict score” or “risk score” unless pipeline-owned

## Filters

Good filters are pushdown-friendly:
- equality
- IN
- BETWEEN
- >= / <=
- ILIKE only for approved text search columns

Bad filters require:
- fuzzy matching
- joins
- calculated labels
- Python-side dataframe transformation

## Missing capability

Use:

```yaml
TODO_PIPELINE_VIEW_REQUIRED:
  - "Add v_member_interests_category_summary for category bar chart."
```
