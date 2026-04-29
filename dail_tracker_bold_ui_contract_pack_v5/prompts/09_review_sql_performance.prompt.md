Review page `<PAGE_ID>` retrieval SQL.

Allowed:
- SELECT
- FROM approved registered view
- WHERE approved filter columns
- ORDER BY approved sort columns
- LIMIT

Check:
- parameter binding
- no string interpolation of user input
- limits exist
- selects only needed columns
- no modelling SQL in Streamlit
- no persistent DB file assumption
