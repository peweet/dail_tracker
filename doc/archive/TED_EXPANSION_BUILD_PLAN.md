# TED Expansion — Build Plan (pipeline + app)

> **STATUS 2026-06-08: Phases 1 & 2 SHIPPED.** Phase 1 (competition-intensity fields + single-bid lens)
> and Phase 2 (`cn-standard` tender-pipeline grain + tab) are built, wired into `pipeline.py`
> (`ted`, `ted_tenders`), surfaced on the Procurement page (TED tab competition strip + new "Tender
> pipeline (TED)" tab), and tested (`test_v_procurement_ted_awards_competition_columns`,
> `test_v_procurement_ted_tenders_pre_award_grain`). Phase 3 (place→constituency) stays BLOCKED on the
> LA→constituency crosswalk; Phase 4 (pre-2024 backfill) confirmed **not viable via the API** — pre-2024
> notices are winner-less (0% winner fields), so it needs the bulk legacy TED_EXPORT XML lane.

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

## Data exploration (live API, 2026-06-08) — what's actually populated
Measured fill rates on **250 real Irish `can-standard` award notices (2025)**, so the field set is evidence-based, not guessed. (Vocabulary saved at `c:/tmp/ted_fields.txt`.)

| Candidate field (friendly id) | Fill % | Verdict |
|---|---:|---|
| `place-of-performance` (NUTS/country) | **100%** | ADD — constituency hook (Phase 3) |
| `procedure-type` | **99.6%** | ADD — open/restricted/negotiated mix |
| `received-submissions-type-val` (+ `-code`) | **99.2%** | **ADD — the competition-intensity signal** |
| `award-criterion-type-lot` (price/cost/quality) | **95.6%** | ADD — price-vs-quality dimension |
| `estimated-value-lot` (+ `-cur`) | 51.6% | ADD (partial) — show when present |
| `green-procurement-criteria-lot` | 12.8% | SKIP — too sparse |
| `contract-duration-period-lot` | 2.4% | SKIP — effectively empty |
| `winner-size` (SME) | **0.4%** | **DROP — the "SME chip" idea is dead** |

**Headline finding (the analytical payoff):** `received-submissions-type-val` is a true count (e.g. 3, 9, 4) → **29% of Irish award notices received only ONE tender on ≥1 lot** (72/248). Combined with `procedure-type` (8/250 were *negotiated-without-prior-call*), this is a real, factual **competition-intensity** layer. `award-criterion-type` splits quality 272 / cost 202 / price-only 56 (lot-level).

**Implementation notes from the raw payloads:**
- Fields return as **per-lot arrays** (`place-of-performance: ["IRL","IRL"]`, `received-submissions-type-val: ["3"]`). Each new column needs an explicit **aggregation rule** (e.g. `n_tenders_min` = min across lots for the single-bid flag; explode/first for CPV; first non-IRL-default NUTS for place).
- `received-submissions-type-code` is a **taxonomy** (`tenders`, `t-esubm`, requests-to-participate…) — the count is only comparable within a code; filter to tender-count codes before deriving single-bid.
- `buyer-name` (needed in Phase 2) is a **multilingual dict** `{"eng": [...], "gle": [...]}` — extract `eng` (fall back `gle`).

## Phase 1 — Curated field expansion (LOW effort, high value, NO new grain)
*Enrich the existing 2024+ award pull; backward-compatible (new columns only). Field set is the measured one above — not the original guess.*

**Pipeline** — in `ted_ireland_extract.py`, grow `FIELDS` from 9 to ~14 (the ADD rows above), and derive notice-level columns with explicit aggregation:
- `procedure_type` (99.6%), `is_uncompetitive_procedure` (procedure ∈ {neg-wo-call, oth-single})
- `n_tenders_received` (from `received-submissions-type-val`, tender-codes only) + `is_single_bid` (min across lots = 1) — **the headline new signal**
- `award_criteria_kind` (price-only / cost / quality — from `award-criterion-type-lot`)
- `place_nuts` + `place_label` (100%) — constituency hook for Phase 3
- `estimated_value_eur` + `estimated_value_safe` (partial; `value_safe_to_sum` semantics — a pre-award estimate, never summed with the awarded value)
- **DROPPED from the original plan:** SME/`winner-size` (0.4%), contract-duration (2.4%), green-procurement (12.8%).
- Bump `OUT_COV` with measured per-field fill rates (already gathered above — bake the floors into the coverage gate).

**App** (extend `_render_ted` + supplier panel) — neutral, no-inference framing:
- Card meta: procedure type, "N tenders received".
- **A "single-bid awards" filter/lens** ("awards that received only one tender") — captioned as factual disclosure, never "uncompetitive"/"rigged". This is the distinctive new capability and it's ~universally populated.
- A small price-vs-quality breakdown (award-criteria mix).

**Tests:** extend the TED gold-quality + `sql_views` tests for the new columns; **fill-rate floors** (procedure ≥95%, tenders-received ≥95%, criteria ≥90%) so a future eForms layout change that drops a field fails loudly.

**Effort:** ~0.5–1 day. **Risk:** low (additive). **Gotcha (now confirmed, not feared):** per-lot arrays + the submissions-code taxonomy — handle both in the aggregation step.

---

## Phase 2 — Competition/tender notices (`cn-standard`, ~28k) as a NEW grain (MED effort)
*The pre-award pipeline: what's being procured, by whom, under what procedure — a forward-looking view eTenders+awards can't give.*

**Pipeline**
- New extractor `extractors/ted_ireland_tenders_extract.py` (or a `--tenders` mode on the existing one) querying `buyer-country=IRL AND notice-type=cn-standard`.
- Output `data/silver/parquet/ted_ie_tenders.parquet`. Grain: **one row per notice × lot**. **Fields confirmed present in the probe:** `buyer-name` (multilingual dict → take `eng`), `classification-cpv`, `procedure-type`, `deadline-receipt-tender-date-lot` (submission deadline ✓), `estimated-value-lot` + `estimated-value-cur-lot` (pre-award ceiling → **`value_safe_to_sum = FALSE`**, present on a subset), `place-of-performance`.
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
