# Frontend Quality Prompt Cookbook

Run these after UI changes.

## Contract compliance

Use `prompts/04_review_logic_firewall.prompt.md`.

Good result:
- pass/fail
- no forbidden logic
- no invented features
- no unsafe SQL

## UX/accessibility

Use `prompts/05_review_frontend_ux_accessibility.prompt.md`.

Good result:
- friction points by impact
- exact fixes
- no new data features

## Design drift

Use `prompts/06_review_design_drift.prompt.md`.

Good result:
- catches generic dashboard aesthetics
- preserves Direct/Civic/Accountable identity
- keeps CSS consistent

## SQL performance

Use `prompts/07_review_sql_performance.prompt.md`.

Good result:
- explicit projection
- pushdown-friendly filters
- row limits
- no Python-side large-frame filtering

## Release checklist

- [ ] Contract exists
- [ ] Required views exist or TODO_PIPELINE_VIEW_REQUIRED is recorded
- [ ] Shared CSS reused
- [ ] Required columns validated
- [ ] Empty states exist
- [ ] Provenance visible
- [ ] CSV export works if enabled
- [ ] Logic firewall checker passes
- [ ] UX review has no critical issues
