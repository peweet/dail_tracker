# Contract SQL Notes

Streamlit should generate only retrieval SQL.

Allowed:

```sql
SELECT approved_column_1, approved_column_2
FROM approved_relation
WHERE approved_column = ?
ORDER BY approved_sort_column DESC
LIMIT ?
```

Forbidden in Streamlit:
- JOIN
- GROUP BY
- HAVING
- window functions
- CREATE VIEW
- read_parquet
- parquet_scan

Raw Parquet access belongs in pipeline-owned DuckDB views.

Missing capability pattern:

```text
TODO_PIPELINE_VIEW_REQUIRED: add <view_or_column> for <page/use_case>
```
