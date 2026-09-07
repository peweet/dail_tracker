# C:\tmp "generic reusable" candidates all duplicate shipped code

**Verified 2026-08-29.** A three-cluster survey of `C:\tmp` (204 top-level dirs, ~2,050 files)
flagged three directories as domain-agnostic candidates worth promoting into `tools/`:
`cso_enrich_probe/audit_all_money.py`, `ia_probe/` (probe.py, ptext.py, rhythm.py, toc.py), and
`lsprobe/detail.py`. A follow-up verification workflow read each in full, checked it against the
live repo, and rejected all three — the standard applied was "prove non-duplicate before
integrating," and none of them cleared it.

## audit_all_money.py — skip_duplicate

Its deflation-safety checks restate `test/contracts/test_cpi_deflator_contract.py:66`
(`test_factor_bounded`, asserts `deflator_to_base` in `(0, 8.0)`) and `:109`
(`test_value_plausible_flag_present_and_consistent`, asserts the same plausibility/coverage
behaviour over `procurement_awards/value_eur` and `procurement_payments_fact/amount_eur`). Its
`<=0` scan duplicates the `nonneg` helper in `test/contracts/_invariants.py:62`, already invoked
on money columns across `test_gold_invariants.py` (lines 47, 66, 140, 148, 155, 164, 185, 215) and
`test_supplier_entity_xref.py:99`. Its remaining descriptive per-column null%/min/max output is
diagnostic-only and is exactly `tools/check_gold_quality.py`'s territory (`measure_table()`,
lines 58-87 — the repo's existing metric-repository/baseline-regression convention for gold
content quality). It does **not** duplicate `tools/check_money_grain_sums.py` — that file is a
static sqlglot AST lint over `sql_views/` for cross-grain SUM/UNION and never reads parquet data
(lines 61-131) — but `audit_all_money.py` also never checks cross-grain summing, so it doesn't
fill that gap either. The other 13 files in the same directory were skimmed and confirmed
CSO/council-specific one-offs (hardcoded council names, CSO PxStat API codes, one-shot period
investigations) — none are generic.

## ia_probe/* — skip_not_reusable

All four are narrow, undocumented investigation scratch scripts (6-13 lines, no docstring, no
tests) written for one document review.

- `probe.py` — its NEEDLES list (pharma_chemical, seveso_comah_adjacent, epa_ie_licence, ...) is
  hardcoded to one specific site's planning/environmental review.
- `ptext.py` — a 3-line page-range text dump; the repo already inlines per-page `get_text()`
  wherever needed (e.g. `pipeline_sandbox/new_sources/fetch_national_standards.py:57`).
- `rhythm.py` — its "thin page" scanned/native heuristic (chars-per-page <400 outside a margin
  band) duplicates three existing implementations: `shared/pdf_classification.py`
  (`legacy_has_text_layer`/`decide_text_layer`, with its own test suite at
  `test/shared/test_pdf_classification.py`), `tools/paper_extract.py:126-129` (per-page
  `is_scanned` classification), and
  `pipeline_sandbox/historic_members/probe_register_textlayer_census.py:80-98` (identical
  400-char threshold).
- `toc.py` — duplicates two existing call sites verbatim
  (`pipeline_sandbox/new_sources/fetch_national_standards.py:63-67` and
  `fetch_hiqa_and_strategy.py:33-37`) and carries a latent Windows bug:
  `p.split('/')[-1]` on a backslash path prints the whole path, not the basename.

## lsprobe/detail.py — skip_duplicate (private-repo duplicate)

Fully superseded by mature, tested private siting-engine code:
`planning/product/ingest/planning_layers_freshness.py:346` (`layer_metadata`), `:354`
(`metadata_last_edit` — same `editingInfo.lastEditDate` extraction), `:382` (`live_count`,
filter-aware unlike `detail.py`'s unfiltered count); and
`planning/product/ingest/layer_source_audit.py:80-88` (`_get` helper), `:138-166` (`probe()`,
URL-liveness sweep), `:168-207` (`census()`, the national per-org/service/layer ArcGIS sweep that
built `planning/product/rules/org_census.csv` and `council_org_registry.csv` — see
[[siting-council-own-gis]]). Public `tools/` (root) has zero ArcGIS/FeatureServer/`f=pjson` hits,
confirming there's no *public*-side gap — but the private equivalent means this was never a gap
for the public repo to fill. The other 5 files in the directory (pip.py, sweep.py, sweep2.py,
vals.py, vals2.py) are confirmed one-offs: siting-specific org lists, a landscape-character-area
regex filter, hardcoded Galway coordinates.

## Cost and re-check guidance

This verification cost ~629k subagent output tokens across two workflow runs (the first run's
aggregation was lost to a `pipeline()` return-shape bug in the orchestrating script — it returns
only the final stage's result per item, not a per-stage array — fixed and re-run cheaply from
cache). Before re-running a similar promotion pass on these same three directories, read this
card first: the duplication findings above cite exact file:line matches to re-verify rather than
re-derive from scratch.
