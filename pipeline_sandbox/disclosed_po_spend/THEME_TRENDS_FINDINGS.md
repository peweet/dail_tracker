# Disclosed national PO / payments-over-EUR-20k — CATEGORY / SPEND THEMES OVER TIME

_Sandbox-only. Source: `data/raw_bq/bq-results-20260619-122315-1781871808837.csv` (read-only). Per-body
regime anchored against `data/gold/parquet/procurement_payments_fact.parquet`. Build script:
`pipeline_sandbox/disclosed_po_spend/theme_trends_build.py`. Nothing promoted to silver/gold._

## Method

- Each of the 582,119 rows assigned to **at most one theme** by priority-ordered keyword regex over
  `Description` (Supplier unused — descriptions carry the category signal). First match wins so the
  theme x year matrix never double-counts a euro line. ~50% of gross lands in `other` (48,020 distinct
  descriptions; many are generic e.g. "Capital Contracts Expenditure", "CONTRACT PAYMENTS").
- **THE non-negotiable rule (from the prior workflow):** payment-lists and PO-commitment-lists are
  different units and must never be summed as one "spend". Every theme x year cell is split into:
  - `payment_actual` — money actually paid (HSE, OPW, Education, DCEDIY, DECC, Revenue, TII, Meath…).
  - `po_committed` — purchase orders raised, a commitment that may never fully pay (Justice, Defence,
    NTA, Tusla, **every Local Authority**…).
  - `aggregated_rollup` — utility/regulator per-category quarterly buckets, **NOT EUR-20k line items**
    (Irish Water, EirGrid, Gas Networks, Central Bank, ESB, RTE). MEMO ONLY — excluded from spend.
  Regime per body = our fact's `amount_semantics` where anchored (`fact_anchored`); else the blank-PO
  heuristic (>=95% blank -> payment, else PO) flagged `heuristic_uncertain` in `body_regime_crosswalk.csv`.
- **The `ALL_gross_line_value_NOT_spend` rows in `theme_trends.csv` are GROSS LINE VALUE, not spend.**
  Cite them only with that caveat. For real trend, use the regime-specific rows.
- **Cross-year confound:** body coverage GROWS every year (2012: 7,420 rows; 2018: 43,024; 2024: 73,987;
  2026 partial). So a rising theme partly reflects more bodies joining the EUR-20k regime, not only more
  spend. 2018-vs-2024 is the cleanest comparable full-year pair; 2011 is near-empty and 2026 is partial.

## Outputs
- `theme_trends.csv` — PRIMARY: regime x theme x year (2011..2026) gross matrix (4 regime blocks).
- `theme_year_regime_long.csv` — long-form theme x Year x regime (gross + rows).
- `theme_trend_early_late.csv` — 2018 vs 2024 delta/%/series-total/peak-year per regime x theme.
- `body_regime_crosswalk.csv` — every body's assigned regime + source + blank-PO fraction.

---

## Headline themes (regime-honest)

### 1. ASYLUM / IP / UKRAINE ACCOMMODATION — the standout political theme
The single most dramatic grower, and it **crosses regimes because the government function moved bodies**:

| Year | gross EUR (combined, see caveat) | who is publishing it |
|---|---:|---|
| 2015 | 54.3m | Dept of Justice (po) — "Asylum Seeker Accommodation, S39" |
| 2018 | 77.6m | Dept of Justice (po) |
| 2020 | 47.0m | DCEDIY (payment) — function moved to Children dept |
| 2022 | 829.7m | DCEDIY (payment) — Ukraine war surge begins |
| 2023 | 1,792.9m | DCEDIY (payment) |
| 2024 | 1,853.8m | DCEDIY (payment) — peak |
| 2025 | 1,630.9m | **SPLIT: DCEDIY 533m (payment) + Dept of Justice 1,097m (po)** |
| 2026* | 300.6m | Dept of Justice (po) — partial year |

- **~24x from 2018 to 2024** (78m -> 1.85bn). The Ukraine war + IP arrivals are the driver.
- **The 2025 "drop" in DCEDIY is NOT a real fall** — migration/IPAS moved to the Department of Justice
  in the 2025 machinery-of-government reshuffle, so the spend re-appears under Justice as `po_committed`.
  Because DCEDIY=payment and Justice=PO, the combined line straddles two regimes from 2025 — report the
  two streams separately, do not read the DCEDIY 2024->2025 fall as a policy/spend reduction.
- Top bodies inside the theme: DCEDIY EUR 5.19bn (payment, 2020-2025), Dept of Justice EUR 1.95bn (po,
  2015-2018 + 2025-2026). Council/Tusla shares are small.
- **Decontamination done:** council HOMELESSNESS ("Emergency Accommodation", DCC EUR 238m) was initially
  mis-caught by the asylum regex; it is now a separate `homeless_emergency_accommodation` theme.

### 2. CONSTRUCTION / BUILDING — biggest by absolute euro
- `payment_actual`: 658m (2018) -> 3.16bn (2024), **+380%**; series total EUR 17.2bn. Driven by OPW,
  Education school building, DCEDIY (modular/construction). The 2021 jump (2.0bn) coincides with the
  national capital ramp.
- `po_committed`: 476m -> 665m, **+40%** (LA + Justice/Defence capital).
- Big chunk of construction lives in `aggregated_rollup` (TII, Irish Water capital) — EUR 5.6bn series,
  NOT EUR-20k line items.

### 3. IT / SOFTWARE / ICT — broad, fast growth in both regimes
- `po_committed`: 128m -> 495m, **+286%** (series EUR 3.23bn) — the largest single ICT block.
- `payment_actual`: 126m -> 285m, **+125%** (series EUR 1.98bn).
- Consistent across departments + LAs; a genuine sector-wide digitalisation trend, not one body.

### 4. MANAGEMENT CONSULTANCY — politically salient, strong growth both regimes
- `po_committed`: 32m -> 144m, **+346%** (series EUR 821m).
- `payment_actual`: 60m -> 163m, **+171%** (series EUR 885m).
- Combined consultancy line roughly **3x 2018->2024** (~92m -> ~307m gross). Worth a standalone civic
  story given recurring PAC interest; note "Non-clinical Management Consultancy" (HSE) is a big payment
  contributor.

### 5. ENERGY / ELECTRICITY / GAS — energy-crisis signature
- `po_committed`: 96m -> 313m, **+225%** (LA + state-body utility bills through the 2022-23 price spike).
- `payment_actual`: 33m -> 45m, +36%. The price-crisis impact is concentrated in PO-bodies.
- Plus EUR 690m in `aggregated_rollup` (utilities' own category buckets) — excluded from spend.

### 6. AGENCY / LOCUM / TEMPORARY STAFF — growth but smaller than expected
- `payment_actual`: 49m -> 70m, +44% (series EUR 655m) — note this UNDERSTATES true agency-staff cost:
  HSE agency/locum pay mostly sits in PAYROLL, not the EUR-20k PO/payments returns, so this theme is a
  floor, not the full bill.
- `po_committed`: 2.2m -> 19.9m, +784% (off a tiny base; LA temp/contract staff appearing late).

### 7. LEGAL / SOLICITOR / BARRISTER
- `po_committed`: 14m -> 52m, **+258%** (series EUR 374m).
- `payment_actual`: 52m -> 72m, +39% (series EUR 641m; spikes 2022 EUR 159m via large one-off legal payments).

### 8. Other themes (regime-honest, brief)
- cleaning/facilities: payment 53m->100m (+89%), po 19m->35m (+80%); + EUR 978m utilities rollup.
- rent/property: payment 59m->93m (+59%), po 44m->73m (+65%).
- transport/fleet: payment 16m->43m (+171%), po 105m->147m (+40%); incl. SAR helicopter contracts.
- advertising/PR/media: payment 10m->15m, po 14m->34m (+135%); + utilities rollup.
- security: payment 5m->9m (+74%); po actually DOWN 8.3m->4.3m (-49%).
- medical/drugs/pharma: payment FLAT 206m->210m (HSE-dominated; 2020 EUR 815m = Covid PPE/vaccine spike).
- homeless emergency accommodation (NEW split): payment 1m->5.8m then EUR 219m in 2025 (DCC homelessness
  surge late-published); a real and rising LA-funded theme distinct from asylum.

## Standout GROWERS (2018->2024, within a single comparable regime)
1. Construction/building (payment) +380% — EUR 658m -> 3.16bn
2. Management consultancy (po) +346%
3. IT/ICT (po) +286%
4. Legal (po) +258%
5. Energy/electricity/gas (po) +225%
6. Management consultancy (payment) +171% ; Transport/fleet (payment) +171%
7. IT/ICT (payment) +125%
8. Asylum/IP/Ukraine (payment) — from ~0 (2018 under Justice) to EUR 1.85bn (2024) ≈ 24x vs the 2018
   Justice figure; largest absolute swing of any theme.

## Standout DECLINERS / flat
- Asylum/IP/Ukraine (po) 2018->2024 -99% — but **artefact of the body move** (left Justice for DCEDIY);
  returns to Justice in 2025. Do NOT read as a real decline.
- Security (po) -49% (8.3m -> 4.3m).
- Medical/drugs/pharma (payment) effectively FLAT +2% (HSE EUR-20k pharma stable; 2020 Covid was a one-off).
- Medical/drugs (po) -34%.

## Caveats to carry into any write-up
1. `ALL_gross_line_value_NOT_spend` rows are NOT spend — payment + PO + rollup mixed. Per-body or
   per-regime only.
2. Theme growth is partly **coverage growth** (more bodies each year). 2018-vs-2024 is the cleanest pair.
3. ~50% of gross is unclassified `other` — themes are a floor on each category, not the whole of it.
4. Agency-staff and (mostly) payroll-funded categories are systematically understated (payroll is not in
   the EUR-20k PO/payments regime).
5. The asylum theme's apparent 2024->2025 "fall" is a machinery-of-government body transfer
   (DCEDIY -> Dept of Justice), not a spend cut.
6. No per-record source URL, no VAT/PII flags in the extract (unchanged from prior findings).
