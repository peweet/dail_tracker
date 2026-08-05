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

Result contract:
- `Verdict: PASS | FAIL` (`PASS` requires no blocker or major finding)
- each finding with `Severity`, `Evidence: path:line`, consequence, and required action
- query/test evidence and any checks that were `NOT RUN`
