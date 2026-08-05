Critique the implemented page `<PAGE_ID>` against observable acceptance criteria.

For each criterion, record `PASS`, `FAIL`, or `NOT APPLICABLE` with concrete evidence:
- primary question and information hierarchy are clear above the fold
- tables and filters are usable with the contract's data shape
- temporal controls match the available dates or years
- each chart answers a stated question
- source/provenance display matches the contract, including deliberate omission
- current-view CSV export is understandable
- mobile content order preserves the primary flow
- the page has the civic/editorial character and is materially different from the old layout

Do not add new data semantics.

Result contract:
- `Verdict: PASS | FAIL` (`PASS` requires every applicable acceptance criterion to pass)
- failures with `Severity`, `Evidence: path:line` or screenshot, consequence, and minimal fix
- verification performed and residual risk
