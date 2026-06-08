# TED Expansion — Build Plan (pipeline + app)

**Date:** 2026-06-08
**Context:** The live API probe (report §8) showed we ingest **~8.6k of a ~55.9k** Irish TED footprint, using **9 of 1,830** available fields. This plan expands that *without* breaking the money-grain firewall or the no-inference rule.

## 0. Current state (verified, not assumed)
- **Wired:** `ted` chain in `pipeline.py` (line 85) → `extractors/ted_ireland_extract.py` → `data/silver/parquet/ted_ie_awards.parquet` (13,126 rows / 8,614 award notices, 2024+, 9 fields). *(The extractor docstring's "NOT wired into pipeline.py" is stale — fix it.)*
- **Surfaced:** `sql_views/procurement_ted_awards.sql` + `procurement_ted_supplier_summary.sql` read the **silver parquet directly** (silver→view→page, no gold) → Procurement page **"EU-level awards (TED)"** tab (`_render_ted`).
- **Rails already in place:** award grain never summed with eTenders; `is_pan_eu_outlier` excludes the €586bn GÉANT-type frameworks; `value_safe_to_sum`; winner→CRO match.
- **API:** `POST https://api.ted.europa.eu/v3/notices/search`, zero-auth, body `{query, fields, limit≤250, page, paginationMode:"PAGE_NUMBER"}`. Full field vocabulary (1,830 ids) saved at `c:/tmp/ted_fields.txt`.

## Design rules (non-negotiable, carried from PROCUREMENT_MASTER.md)
1. **Three grains never unioned:** award (eTenders + TED CAN), competition/tender (TED CN — *new*, pre-award), payment (public_payments_fact). A TED tender ceiling is **not** an award and **not** a payment.
2. **`value_safe_to_sum` everywhere.** Tender estimated-values and framework ceilings are `FALSE`.
3. **No inference in the app.** "1 tender received" is a *fact* shown neutrally — never labelled "rigged"/"uncompetitive" as a conclusion.
4. **Silver→view→page** stays the pattern (consistent with current TED + lobbying-overlap). No gold layer unless a downstream rollup needs it.

---

## Phase 1 — Curated field expansion (LOW effort, high value, NO new grain)
*Enrich the existing 2024+ award pull; backward-compatible (new columns only).*

**Pipeline**
- In `ted_ireland_extract.py`, grow `FIELDS` from 9 to ~20. Map these to exact eForms ids from `c:/tmp/ted_fields.txt`:
  - **procedure type** (open / restricted / negotiated / direct) — `BT-105-Procedure`
  - **number of tenders received** per lot → *competition intensity* (1 = single-bid)
  - **SME participation / winner size** (`sme-part` family)
  - **place-of-performance NUTS** (region) — the constituency-linkage hook (Phase 3)
  - **contract duration**, **framework flag**, **award-criteria type** (price-only vs quality)
- Add the derived columns to silver: `procedure_type`, `n_tenders_received`, `is_single_bid` (n_tenders_received = 1), `winner_is_sme`, `place_nuts`, `place_label`, `contract_duration_months`, `award_criteria_kind`.
- Bump `OUT_COV` coverage json with per-field fill rates (some eForms fields are sparsely populated — measure, don't assume).

**App** (extend `_render_ted` + the supplier panel)
- Add neutral chips/meta: procedure type, "N tenders received", an SME chip.
- New honest analytical angle: a **single-bid filter** ("awards that received only one tender") — a factual competition signal, captioned as disclosure not judgement.

**Tests**
- Extend `test/extractors/test_procurement_gold_quality.py` (or the TED test) for the new columns + fill-rate floors.
- Extend `sql_views` test for `v_procurement_ted_awards` new columns.

**Effort:** ~0.5–1 day. **Risk:** low (additive). **Gotcha:** several eForms BTs are sparsely filled — gate on measured fill rate, show "—" gracefully.

---

## Phase 2 — Competition/tender notices (`cn-standard`, ~28k) as a NEW grain (MED effort)
*The pre-award pipeline: what's being procured, by whom, under what procedure — a forward-looking view eTenders+awards can't give.*

**Pipeline**
- New extractor `extractors/ted_ireland_tenders_extract.py` (or a `--tenders` mode on the existing one) querying `buyer-country=IRL AND notice-type=cn-standard`.
- Output `data/silver/parquet/ted_ie_tenders.parquet`. Grain: **one row per notice × lot**. Fields: buyer, title, CPV, procedure-type, **submission deadline**, **estimated value** (`value_safe_to_sum = FALSE` — it's a pre-award ceiling), place-of-performance, framework flag.
- New chain `ted_tenders` in `pipeline.py` (or fold into `ted`).

**App**
- New view `sql_views/procurement_ted_tenders.sql` (`v_procurement_ted_tenders`, reads silver).
- New Procurement page tab **"Tenders (recent & open)"** — distinct from awards: "what's being procured", procedure-type mix, deadlines. Hard label: *opportunities/ceilings, not awards — never summed with award or payment figures*.

**Tests:** new sql_view contract test (grain = one-row-per notice×lot; estimated value never `value_safe_to_sum`).

**Effort:** ~2–3 days. **Risk:** medium (new grain — the firewall discipline is the main thing to get right). **Value:** high + unique (no Irish civic tool surfaces the live tender pipeline legibly).

---

## Phase 3 — Place-of-performance → constituency linkage (BLOCKED — depends on crosswalk)
- The Phase-1 `place_nuts`/`place_label` field unlocks "public contracts by area".
- **Blocked on the missing LA→constituency crosswalk** (see the linkage deep-dive in `INGESTION_SOURCES_REPORT_REVIEW_2026_06_07.md` §5, linkage #8). Build the crosswalk first (it also unblocks SSHA + LA-AFS-by-area). Defer until then; flag the dependency.

---

## Phase 4 — Historical backfill, pre-2024 (OPTIONAL, LOW priority)
- `can-standard` all-time = 19,352 → **+10,668** pre-eForms award notices (2014–2023).
- **Caveat:** pre-eForms TED uses legacy standard forms — structured award value/winner are weaker/absent. Backfilling doubles award *coverage* at lower *field quality*.
- If pursued: use the **TED Open Data Service (SPARQL / bulk)** rather than paging the Search API; tag legacy rows with a `schema_era = 'pre_eforms'` provenance flag and default their `value_safe_to_sum = FALSE`.
- **Recommendation:** defer unless a long-run award trend becomes a product requirement.

---

## Cross-cutting
- **Source health:** add a TED API canary to `tools/build_source_health.py` (zero-auth endpoint, easy to ping).
- **Docstring fix:** correct `ted_ireland_extract.py` ("not wired" → wired as `ted` chain; gold = silver-direct-view).
- **DATA_MAP / README:** update TED from "silver only / no page" to "surfaced on Procurement page; award grain"; the README/DATA_MAP both currently understate it.
- **Reconciliation:** keep the existing pan-EU-outlier exclusion and the eTenders-overlap cross-reference (66% of TED winners also appear in eTenders — cross-link, never sum).

## Recommended sequence
1. **Phase 1** (cheap, high value, additive) — do first.
2. **Phase 2** (new tender-pipeline grain + tab) — the distinctive new capability.
3. **Phase 3** — after the LA→constituency crosswalk exists.
4. **Phase 4** — optional, only if historical trend is needed.

*Provenance: live TED v3 API probe 2026-06-08 (counts are live `totalNoticeCount`); current-state facts verified against `pipeline.py`, `extractors/ted_ireland_extract.py`, `sql_views/procurement_ted_*.sql`, `utility/pages_code/procurement.py`.*
