You are working in a private planning-constraint engine (Python). The site-report rendering code starts at `planning/product/core/report.py` — it builds one `SiteReport` object via `build_report()` and renders it into HTML, Markdown, docx and a machine-readable JSON evidence chain (`render_json`). Other code imports what it needs with `from planning.product.core.report import <name>` — that is the module external callers always use, regardless of how the rendering code is internally organised.

Add a new function `render_summary_json(report)` that returns a condensed JSON summary of a `SiteReport`: just four keys — `"headline"` (the brief's headline string), `"excluded"` (bool, whether any exclusion fired), `"hard_constraint_count"` (int), `"shaping_constraint_count"` (int). Reuse the existing full JSON renderer's brief/data access patterns — do not duplicate its whole banding/evidence-chain machinery, this is a condensed view for a caller that only wants the topline counts.

It must be importable exactly as `from planning.product.core.report import render_summary_json` — find wherever that needs to be wired for that import to work, following whatever pattern the existing exports (`render_json`, `render_html`, etc.) already use.

At the very end of your response, output exactly this JSON block (fill in real values):

```json
{
  "files_read": [
    {"path": "planning/product/core/report.py", "chars": 123456}
  ],
  "response_chars": 7890
}
```

`files_read` must list every file you read, with the total characters you actually received from reads of that file. `response_chars` is the total characters of code and text you produced.

Do NOT commit the change. Do NOT run any tests or the app. Stop after writing the code.
