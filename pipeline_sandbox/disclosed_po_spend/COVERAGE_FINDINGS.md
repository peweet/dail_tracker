# Disclosed PO/payments extract — TEMPORAL COVERAGE profile

Source: `data/raw_bq/bq-results-20260619-122315-1781871808837.csv` (582,119 rows, 216 bodies; READ-ONLY).
Compared against parsed corpus `data/gold/parquet/procurement_payments_fact.parquet` (247,457 rows, 70 publishers with non-null year).

Scripts (sandbox-only):
- `coverage_profile.py` — overall timeline, per-body coverage, longest runs, sparse bodies, fact comparison.
- Per-body table written to `per_body_coverage.csv` (216 rows).
- Earlier-than-ours table written to `earlier_than_ours.csv` (32 rows).

SEMANTICS CAVEAT (carried from prior workflow): bodies with ~100% blank PO are PAYMENT lists;
~0% blank PO are PURCHASE-ORDER commitments. 99 payment bodies / 89 PO bodies / 28 mixed.
Every euro total below is **gross line value within a single body** — NEVER a cross-body sum.

## 1. How far back: 2011 → 2026-q1

Earliest quarter in the whole dataset is **2011-q1**. Only 9 rows fall in 2011 (8 = Crawford Art
Gallery, 1 = Health Insurance Authority q4) — 2011 is a thin partial tail. The dense floor is
**2012-q1**: most departments/agencies start there; most county councils start 2012-q4. Latest
quarter is **2026-q1** (partial). So the usable spine is **2012 → 2025 full, 2011 + 2026 partial**.

### Overall timeline (rows, gross line value per Year — mixed semantics, do not treat sum as spend)
| Year | rows | gross_eur |
|---|---|---|
| 2011 | 9 | 253,982 |
| 2012 | 7,420 | 765,447,910 |
| 2013 | 13,150 | 1,318,688,966 |
| 2014 | 14,863 | 1,512,216,169 |
| 2015 | 18,537 | 1,932,688,502 |
| 2016 | 21,787 | 2,428,424,271 |
| 2017 | 31,020 | 4,143,011,081 |
| 2018 | 43,024 | 6,010,493,001 |
| 2019 | 53,732 | 7,004,010,073 |
| 2020 | 47,915 | 7,288,374,832 |
| 2021 | 43,352 | 8,677,294,931 |
| 2022 | 55,518 | 12,383,608,394 |
| 2023 | 66,839 | 15,711,089,149 |
| 2024 | 73,987 | 15,692,681,505 |
| 2025 | 75,937 | 15,892,929,711 |
| 2026 | 15,029 | 2,160,213,312 |

Row volume rises ~8x from 2012 to 2025 — coverage *broadens over time* (more bodies onboarded),
it is not a fixed panel back-filled. The early years are real but thin in body count.

## 2. Longest continuous runs (top 15 by distinct-quarters present)
Many bodies are 100% continuous across their span. Standout 14-year unbroken runs:
- Crawford Art Gallery: 2011-q1..2025-q4, 60q, 100% [PAYMENT] — the single deepest run, back to 2011.
- Dept of Agriculture, Food & Marine: 2012-q1..2026-q1, 57q, 100% [PO] (€1.50bn).
- Dept of Public Expenditure & Reform: 2012-q1..2026-q1, 57q, 100% [PO].
- Enterprise Ireland: 2012-q1..2026-q1, 57q, 100% [PO] (€305m).
- IDA: 2012-q1..2025-q4, 56q, 100% [PO] (€664m).
- Dept of Foreign Affairs: 2012-q1..2025-q4, 56q, 100% [PAYMENT] (€424m).
- State Laboratory, Dept of Social Protection, Cork ETB, RSA, Companies Registration Office,
  Cork/Clare/Wicklow/Monaghan County Councils, An Garda Síochána, Dept Environment, Dept Health,
  Dept Taoiseach, Kilkenny CoCo — all 54–55q, ~98–100% continuous, starting 2012.

## 3. Sparse / one-off bodies (≤2 quarters present) — 11 total
- Houses of the Oireachtas Service — 1q (2026-q1 only), €7.87m
- National Concert Hall — 1q (2025-q4)
- Judicial Council — 1q (2022-q3)
- Section 38 : Beaumont Hospital — 2q (2024-q1, 2025-q1), €255m
- Section 38 : The National Maternity Hospital — 2q (2023-q4, 2024-q4)
- An Coimisiún Toghcháin — 2q (2024)
- Quality and Qualifications Authority of Ireland — 2q (2024)
- The National Cancer Registry — 2q
- NTMA Future Ireland Fund — 2q (2025); NTMA Infrastructure, Climate & Nature Fund — 2q (2025)
- Irish Film Classification Office — 2q
These are recent onboarders or single-snapshot publishers, not historical gaps.

## 4. Earlier history than OUR parsed fact — 32 bodies (see earlier_than_ours.csv)
Per-body min-year comparison (loose name match handling council short-names). 21 councils + 11
depts/agencies in the disclosed extract reach years BEFORE our corpus. Highlights:
- **HSE: disclosed 2017-q3, ours 2021 → +4 years recovered.** Confirmed quarters present:
  2017-q3,q4; 2018 q1-q4; 2019 q1-q4; 2020 q1,q2; then a GAP (2020-q3..2021-q3 absent);
  2021-q4; 2022..2026-q1 continuous. So the recovery is **2017-q3..2020-q2** (11 quarters)
  plus the 2025-q4 + 2026-q1 tail noted by the prior workflow. (30 distinct HSE quarters total.)
- Dept of Agriculture: disclosed 2012 vs ours 2024 → +12y.
- Dept of Finance: 2012 vs 2024 → +12y.
- Enterprise Ireland: 2012 vs 2023 → +11y.
- Irish Prison Service: 2013 vs 2024 → +11y.
- National Transport Authority: 2013 vs 2024 → +11y.
- Revenue: 2012 vs 2020 → +8y.
- Dept of Transport: 2013 vs 2018 → +5y.
- Councils (back to 2012-q4 for most): Clare +6, Cork County +5, Kildare/Kilkenny/Meath/Monaghan/
  South Dublin/Westmeath +4, Mayo +3, Donegal +6, Fingal +9, Longford +9, etc.

NOTE on Galway: the conservative dedup mis-paired "Galway County Council" with our short
publisher "Galway City". Galway COUNTY genuinely matches our "Galway County" (2016) → +4y;
Galway CITY Council matches "Galway City" (2025) → +2y. Both still reach earlier than ours; the
"+13y" figure in the auto CSV row for Galway County is a label artefact, real gap is +4y.

## 5. Depth verdict
The extract is DEEP and BROADENING. Spine runs **2011-q1 → 2026-q1**, dense and reliable from
**2012**, with dozens of bodies on unbroken 13–14-year quarterly runs (Crawford Art Gallery alone
reaches 2011). It does not just extend HSE: it back-fills **32 bodies** before our corpus —
notably +11–12 years on core departments (Agriculture, Finance, Enterprise Ireland, NTA, Prison
Service) and +4–9 years across ~21 county councils. The deepest single recovery for an
already-held body is HSE (+4y, 2017-q3..2020-q2). Coverage grows from 9 rows (2011) to ~76k
rows/yr (2025) as bodies are progressively onboarded — early years are genuine but thin.
