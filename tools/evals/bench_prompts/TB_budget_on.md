You are working in the Dáil Tracker repo (Python). SQL view tests live in `test/sql_views/test_sql_views.py` — one test per view, each following the file's standard skeleton (skip-if-fixture-missing, connect, load the view's SQL file with template substitution, materialise the view, assert expected columns, assert non-empty).

Add a test for a planned new view `v_payments_quarterly` (SQL file `payments_quarterly.sql`), following the existing per-view test pattern exactly:

1. `test_v_payments_quarterly_executes`, marked consistently with the file's other tests.
2. Use the same helpers the other tests use (skip helper, connection helper, SQL loader, result helper).
3. Assert the columns `year`, `quarter`, `total_eur` are present and the result is non-empty.
4. Place it in the appropriate section of the file per its ordering conventions.

The SQL file does not exist yet — that is fine; the test will skip until it lands. Do not create the SQL file.

Keep all prose in your responses under 300 tokens total — the code edit itself and the JSON block below are excluded from this budget. Do not pad or recap.

At the very end of your response, output exactly this JSON block (fill in real values):

```json
{
  "files_read": [
    {"path": "test/sql_views/test_sql_views.py", "chars": 123456}
  ],
  "response_chars": 7890
}
```

`files_read` must list every file you read, with the total characters you actually received from reads of that file. `response_chars` is the total characters of code and text you produced.

Do NOT commit the change. Do NOT run any tests. Stop after writing the code.
