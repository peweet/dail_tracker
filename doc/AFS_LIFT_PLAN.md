# AFS multi-year lift — handoff (own context window)

**Status 2026-06-08:** the multi-year extractor change is **written and in place**, recoverability
is **proven**, but the deep run came back as a 1-year fact — an open bug to resolve in isolation
(suspected concurrent-overwrite; see §4). This doc is self-contained so a fresh context can finish it.

## 1. Goal
Turn the per-LA **Annual Financial Statements (AFS)** fact from a 1-year snapshot into a
**by-division spend time series (~2016–2024)**, then surface it as the **complete-spend
enrichment** on the public-body (council) dossier — the *denominator* the named-supplier PO/payment
data sits against. **BUDGET grain: a sibling fact, NEVER summed** with PO/award euros.

Why it matters: AFS is the only source that gives a council's **total** spend, **by function**
(Housing/Roads/Environment…), **comparable across all 31 LAs**. The PO data (just deep-lifted to
2016–2026) gives *who got paid* within the traceable over-€20k slice. Together: AFS = the whole;
POs = the traceable part. Concrete: Cork City 2024 = **€337m total (AFS)** vs **€174m named (POs)** →
~52% traceable.

## 2. What's done (data + code)
- **PO/payment deep lift COMPLETE** (separate, already shipped): councils 2016–2026, gold
  `procurement_payments_fact` 165,848 rows / 54 publishers; the council dossier already has a
  spend-over-time chart (`payments_by_year`). See [[project_la_payments_fact]].
- **AFS extractor multi-year change IN PLACE** in `extractors/la_afs_extract.py`:
  - `MIN_AFS_YEAR = 2016` (pre-2016 used programme-group division names the parser can't read).
  - `select_afs_years(urls)` — one AFS per title-year (audited preferred), years ≥ 2016, newest
    first (replaces `select_afs` which took only the latest — that was the cap).
  - `_parse_one_afs(cf, picked)` — factored-out download+parse+**reconcile gate** for ONE PDF.
  - `ingest_council` now **loops every post-2016 statement**, accumulates rows; each year passes
    its OWN reconcile gate (a drifted old year is skipped, clean years still land).
- Backup of the pre-change fact: was at `c:/tmp/la_afs.bak.parquet` (identical to current — no
  regression). Current `data/silver/parquet/la_afs_divisions.parquet` = 168 rows / 21 councils / 1 yr.

## 3. Recoverability is PROVEN (don't re-litigate this)
- **Multi-year PDFs harvest + download fine.** Bronze cache `data/bronze/pdfs/la_afs/<slug>/`
  already holds many years (cork_county 2017–2024, **south_dublin 2016–2025**, galway_county 2014–24,
  meath/westmeath ~8 each). Councils file AFS annually and keep the archive.
- **The parser RECONCILES most older years** (tested directly on the cached PDFs via `best_ie_page`):
  - south_dublin: **all 10 years 2016–2025 reconcile** (8/8 divisions, gross == printed total).
  - galway_county: 7 yrs (2017,19,20,21,22,23,24); cork_county: 4 (2021–24); meath: 6; westmeath: 8.
  - Failures are isolated old years (no-IE-page / no-printed-total), correctly skipped by the gate.
- So the data IS there and parseable. Expected result of a clean run: ~**1,000–1,300 rows**, 2016–2024.

## 4. THE OPEN BUG (start here)
Despite §2+§3, the deep run (`python extractors/la_afs_extract.py`) produced **168 rows / 1 year**.
`main` does **not** dedup (`pl.DataFrame(all_rows).with_columns(...).sort(...)` — no `.unique()`), so a
multi-year `all_rows` would have been written. Yet the fact is single-year.

**Most likely cause:** a **concurrent process re-ran the same extractor with single-year code and
overwrote the fact** (the other session edits/re-runs `extractors/*` and `procurement_payments_consolidate.py`
every few seconds — it has been churning `la_afs_*`, TED, charity, etc. all session). The multi-year
code is on disk *now*, but the run that wrote the fact may not have been mine.

**Debug steps (in isolation — no other extractor runs):**
1. Confirm no concurrent process is running `la_afs_extract.py` (the churn must be paused/scoped).
2. Re-run on a couple of proven councils: `la_afs_extract.py --only south_dublin,galway_county`.
   - South Dublin alone should yield ~80 rows (10 yrs × 8 div). If it does → it was the overwrite;
     run the full set and verify per-council year counts.
   - If it STILL yields 8 → add row-level logging in `ingest_council` (print `picks`, then per
     `picked`: `_parse_one_afs` status + len(rows)); the loop or `download()` year-keying is the bug.
3. Backup → run full → verify (councils × years × rows) → **merge if the extractor `--only`-overwrites**
   (it overwrites the fact with only the run's councils — same trap as the PO extractor: always
   back up + merge, never `--only` blind).
4. Re-run `la_afs_capital_extract.py` too (capital twin; reuses the revenue cache/years).

## 5. Phase B — dossier enrichment (after the data lands)
Additive to the public-body dossier in `utility/pages_code/procurement.py`
(`_render_payments_publisher_profile`, only when `publisher_type='local_authority'`):
1. **Total spend per year** — a second, clearly-labelled chart **"Council accounts — all spending
   (by function)"**, BUDGET grain, separate from the PO chart, **never merged**.
2. **By-division breakdown** (Housing/Roads/…) for the latest year — cards or a compact bar.
3. **Traceability denominator** — *"€X spent (accounts) · €Y traceable to named suppliers (POs) — Z%"*.
   Computed in the **view/core**, not the page.

New pipeline-owned queries (firewall: no aggregation in the page):
`afs_total_by_year(council)`, `afs_by_division(council, year)`, `afs_vs_po_coverage(council, year)`.
New view(s) `sql_views/procurement_afs_*.sql` over `la_afs_divisions.parquet`.

## 6. Honesty rails / grain rules (non-negotiable)
- AFS = `realisation_tier=SPENT`/`value_kind=net_expenditure_actual` (it's audited actuals by
  division) — but a **different grain** from the per-transaction PO/payment fact. **NEVER summed**
  with PO or award euros; it's a denominator/context, not a sibling list.
- Cleanest comparison: revenue-AFS ↔ revenue-POs; don't loosely mix capital across them.
- Verb on every figure ("spent (accounts)"); the traceable-% is *indicative* (grain/threshold
  caveat). Per-council AFS year coverage varies — say so, don't over-claim.
- Reconcile gate stays load-bearing: only Σ-gross == printed-total years enter the fact.

## 7. Files
- `extractors/la_afs_extract.py` (multi-year change DONE) + `la_afs_capital_extract.py` (reuses it).
- Fact: `data/silver/parquet/la_afs_divisions.parquet` (+ capital) ; coverage
  `data/_meta/la_afs_coverage.json`.
- Enrichment (Phase B): `dail_tracker_core/queries/procurement.py`, `utility/data_access/procurement_data.py`,
  `sql_views/procurement_afs_*.sql`, `utility/pages_code/procurement.py`, `utility/shared_css.py`.
- Context: [[project_la_afs_fact]], [[project_la_payments_fact]], [[project_procurement_phase_taxonomy]]
  (the value taxonomy: AFS = BUDGET/accounts grain, sibling, never unioned).
