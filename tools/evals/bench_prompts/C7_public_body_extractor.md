You are working in the Dáil Tracker repo (Python, Polars for ETL). The multi-publisher payments extractor lives at `extractors/procurement_public_body_extract.py` — a configuration-driven harvest over tiered publishers, with one `read_*` reader per publisher format and a shared fetch → read → emit → classify flow.

Add a new publisher "National Gallery of Ireland" following the existing tier and reader patterns exactly:

1. A config entry in the appropriate tier for a publisher whose quarterly payments PDF lists one table per page with columns supplier / description / amount, publishing at a placeholder URL `https://example.invalid/ngi/payments.pdf` (a real URL will be substituted later).
2. A `read_ngi(b: bytes, max_pages)` reader following the structure of an existing simple PDF table reader in the file, normalising to the extractor's standard row shape.
3. Wire it into the harvest/classifier dispatch the same way the existing publishers are wired.

Preserve the file's invariants: Polars only, the privacy quarantine untouched, `save_parquet` untouched.

At the very end of your response, output exactly this JSON block (fill in real values):

```json
{
  "files_read": [
    {"path": "extractors/procurement_public_body_extract.py", "chars": 123456}
  ],
  "response_chars": 7890
}
```

`files_read` must list every file you read, with the total characters you actually received from reads of that file. `response_chars` is the total characters of code and text you produced.

Do NOT commit the change. Do NOT run any tests or the pipeline. Stop after writing the code.
