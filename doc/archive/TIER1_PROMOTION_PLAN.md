# Promotion plan — NOAC-extended & disclosed-PO (data-validated + UI impact)

**Created:** 2026-06-26. **Status:** plan only — no gold writes, no git run.
**Companion:** `doc/DISCLOSED_PO_INTEGRATION_PLAN.md` (the code-verified *backend* hooks for disclosed-PO;
this doc does **not** duplicate it — it adds the data-state validation and the UI impact that doc omits).

> ⛔ **CORRECTION (2026-06-27): §A (NOAC-extended) is ALREADY FULLY DONE — do not build it.** The
> audit's "sandbox graduation gap" was wrong: it compared the sandbox only to gold's housing
> `noac_*_wide` files and missed that **`noac_indicators_long.parquet` (3,875 rows, all 41 indicator
> codes incl. F1/F2/F3 fire, W1 water, M1/M4 finance, R1 roads, P4 planning, E3 litter, C2 sickness)
> and `noac_scorecard_wide.parquet` (31 LAs)** already carry the full extended set, promoted via
> `extractors/noac_indicators_long_extract.py` + `noac_scorecard_extract.py`. It is surfaced on the
> **local_government page** (two scorecard cards: "How the council is run" + "Services to residents",
> each vs the national median, with history) via `v_la_noac_scorecard`, and **unit-tested**
> (`test/dail_tracker_core/test_core_local_government_queries.py` — incl. the Sligo −10.6% assertion
> and the fire-service-NULL edge case; 11 tests green). The `pipeline_sandbox/noac_accountability/`
> dir is an **obsolete prototype**. §A below is retained only as a record of the wrong call.
>
> **This means the audit's "sandbox graduation gap" signal (agent 3) is unreliable too** — like the
> orphan-parquet signal. Verify §B (disclosed-PO) the same way (grep gold for what it claims is
> missing) BEFORE building.

---

## A. NOAC-extended council accountability  ❌ ALREADY DONE — see correction above

### A.1 What exists (validated 2026-06-26)
`pipeline_sandbox/noac_accountability/`:
- **12 indicator-family wide parquets** (`noac_{m1_revenue_balance, m4_overheads, f1_fire_cost_per_capita,
  f2_fire_mobilisation, f3_fire_attendance, w1_water_compliance, r1_pavement_condition,
  p4_planning_cost_per_capita, e3_litter_pollution, c2_sickness_absence, insurance_claims}_wide.parquet`).
- **`noac_council_scorecard.parquet`** — the curated cross-family snapshot. **Validated:** 155 rows =
  **31 local authorities × 5 headline metrics × 2024** (`revenue_balance_pct`, `fire_within_10min_pct`,
  `litter_problem_pct`, `sickness_absence_pct`, `roads_poor_pct`). Long format (`local_authority,
  metric_key, value, year`).
- **`validate_and_build.py`** + `extend_coverage.json` — the build/validation harness.

**Ground-truth spot check (revenue balance, 2024):** Sligo **−10.59%** (worst), Donegal −1.18%,
Mayo −0.53%, Leitrim −0.42%, Offaly −0.36%, Kildare 0.00% — 5 councils in deficit. Matches the recorded
finding; figures are NOAC's own published indicators, not derived.

### A.2 Gap
Gold has only the **H-housing** and **M2-collection** NOAC families (`noac_*_wide` in `data/gold/parquet/`).
None of these 12 families nor the scorecard reached gold, a view, or a page. The existing local-government
page (`pages_code/local_government.py`, "Who runs your county") is the natural home and already renders
NOAC-style indicators — so the UI pattern exists; only the data and a view are missing.

### A.3 Promotion path (sandbox → vet → promote, per the project's data-anchored rule)
1. **Extractor** — graduate `noac_extend_extract.py` → `extractors/noac_extend_extract.py`; write the 12
   families + scorecard to `data/silver/parquet/` (atomic `save_parquet`, mirror the existing NOAC extractor).
2. **Gold** — add the scorecard (and the family wides that back the drill-downs) to the gold build; register
   in `pipeline.py --list` so the refresh routine carries it.
3. **View** — one new `sql_views/` view, e.g. `v_noac_council_scorecard` (long→served rows; the page reads,
   never pivots). A per-family `v_noac_family_*` for drill-downs as needed. Keep all aggregation in the view.
4. **data_access + core query** — `local_government_data.py` `fetch_noac_scorecard_result()` → a new
   `dail_tracker_core/queries/local_government.py` retrieval fn (`SELECT … WHERE local_authority = ? / metric_key = ?`).
5. **Tests** — reconciliation guard (scorecard value == family-wide value for the same LA/metric/year) +
   a row-floor on the gold scorecard.

### A.4 UI impact
**Page:** `local_government.py` ("Who runs your county", nav *Your Area → local-government*).
**New section — "How your council performs" scorecard:**
- A 5-metric **per-council scorecard** card on each LA profile: revenue balance, fire response,
  litter, staff sickness, road condition — each as a labelled bar vs the 31-council distribution
  (best/median/worst markers), reusing the `pr-afsbar` bar pattern already on the procurement council dossier.
- A **national "who's slacking" league** strip on the index: e.g. *"5 councils ran a revenue deficit in 2024;
  Sligo −10.6% is the largest"* with click-through to each council.
- **Honesty rails:** each metric is NOAC's published indicator for one year (2024); not a ranking of
  "good vs bad councils" — label as published indicators, link the NOAC source. Deficit ≠ mismanagement
  (one-year timing effects); state it.

**Effort:** Medium (1 view + 1 fetch + 1 page section; data is clean and small). **Risk:** Low —
additive section on a page that already renders this shape; no change to existing surfaces.

---

## B. Disclosed national PO/payments dataset  ✅ VERIFIED A GENUINE GAP (2026-06-27)

> **Verification (the one §A taught us to do):** grepped gold `procurement_payments_fact` (72 publishers,
> 318,532 rows, 2012–2026) against the disclosed source's claims.
> - **"Recovers HSE 2017–2020" — ALREADY DONE.** Gold HSE = 78,869 rows spanning 2017–2026. Moot.
> - **"141 new bodies" — REAL.** ~130 of the 141 `candidate_new.csv` bodies are genuinely absent from
>   gold, including **Dublin City Council** (the largest council, not in gold — gold has only Fingal &
>   South Dublin), **An Garda Síochána**, **Irish Water**, **EirGrid**, **Gas Networks Ireland**,
>   **Central Bank**, **IDA**, **NAMA**, **RSA**, and ~6 county councils (Kerry/Louth/Tipperary/…).
>
> **Verdict: disclosed-PO is the ONE genuine remaining gap** (not a false-positive like §A/AFS/Seanad/
> SIPO). It is, however, a LARGE central-fact build (full 216-body assemble + publisher identity for
> ~130 bodies + regime + HSE dedup + manifest/baseline updates) — a deliberate staged promotion per
> `doc/DISCLOSED_PO_INTEGRATION_PLAN.md`, not a quick wire. The €34.76bn is GROSS — coverage (the new
> bodies) is the value, never the summed figure.


### B.1 What exists (validated 2026-06-26)
`pipeline_sandbox/disclosed_po_spend/` + `doc/DISCLOSED_PO_INTEGRATION_PLAN.md` (44KB, code-verified hooks).
- **Source:** `data/raw_bq/bq-results-20260619-…csv` — **582,119 rows · 216 bodies · 2011-q1→2026-q1**.
- **`build/disclosed_bq_po_payments_fact.parquet`** — **schema is gold-conformant** (29 columns identical to
  `procurement_payments_fact`: publisher_id, amount_eur, amount_semantics, value_safe_to_sum, public_display,
  supplier_class, regime fields, source provenance). This is the big promotion advantage — it already speaks
  the gold dialect, so the consolidator can union it with no reshape.

> ⚠ **Data-state caveat (the one thing the existing plan doesn't flag):** the *built* parquet currently
> contains **only the HSE history-recovery slice** — **61,897 rows, 1 body (HSE), 2017-Q3→2026-Q1, all
> `payment_actual`**. The full 216-body assemble has **not** been run into a fact yet. So "582k/216 bodies"
> is the **validated source**, not the built artifact. Promotion is **not one-step**: the assemble step
> (`assemble.py` / `build_xref.py`) must run over all bodies before merge. The HSE slice proves the
> schema/pipeline end-to-end.

### B.2 Trust verdict (from `FINDINGS.md`, confirmed)
Trustworthy as a faithful copy of bodies' published €20k returns. HSE reconciles **cent-for-cent** across
16 overlapping quarters — but that is **shared source lineage, not independent corroboration**. BQ is *more*
complete than our parse (carries 2017-q3..2020-q2, 2025-q4, 2026-q1, and 12 lines we dropped). **Upshot:**
trust the numbers as an accurate transcription; never present the match as two-source confirmation of *actual*
spend. The ~€117bn gross is **meaningless** — payment-list, PO-commitment and utility roll-up bodies must
never be summed (the INDEX's one governing rule).

### B.3 Promotion path
Follow `doc/DISCLOSED_PO_INTEGRATION_PLAN.md` §6 (Hooks A–D), §11 (layer/registration), §12 (dedup/write-safety).
Sequence: run the **full assemble** (216 bodies) → silver fact → publisher-identity for 141 new bodies +
53-rename crosswalk (Hook D maps to existing `publisher_id`, no duplicates) → per-body regime → HSE dedup
(§7/§12e) → `--only … --merge` whole-publisher replace → row-floor + baseline guards (§11g). **No gold writes
until vetted.**

### B.4 UI impact (where 141 new bodies + €34.76bn of coverage land)
The disclosed fact flows into the **same gold `procurement_payments_fact`** that already powers two pages, so
**no new page is needed** — existing surfaces simply get materially more complete:
- **Public-Body Payments** (`public_payments.py`): publisher count jumps **~72 → ~210**; the HSE series
  back-fills **2017–2020** (currently a hole); the new "What the money buys" lens (just shipped) and the
  Suppliers/Publishers tabs all widen automatically.
- **Follow the money** & **Company dossier**: 141 new bodies as payment counterparties → more complete
  body⇄supplier trails.
- **Coverage/provenance copy must update** (honesty): new bodies, the regime split (99 payment-list / 89
  PO-commitment / 28 mixed), and the "never sum across regimes" rule surfaced in the page caveat.

**Effort:** High (assemble + identity/regime/dedup across 216 bodies; the backend plan is large but written).
**Risk:** Medium — touches the central payment fact; mitigated by the existing dedup/row-floor/baseline guards
and whole-publisher merge. Highest single coverage gain available to the project.

---

## C. Sequencing recommendation
1. **NOAC-extended first** — low risk, clean small data, a page already shaped for it, fast citizen value.
2. **Disclosed-PO second** — bigger payoff but a real backend project (assemble + identity + dedup); the
   plan is written, the schema is conformant, the HSE slice de-risks it. Do it as its own checkpoint.
