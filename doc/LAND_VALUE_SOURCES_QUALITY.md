---
tier: REPORT
status: LIVE
domain: land-value
updated: 2026-09-03
supersedes: []
read_when: using ipav_farming_report / land_value_index or any pair of land-price sources together; refreshing from a new IPAV edition
key: REPORT|LIVE|land-value
---

# Land value sources — IPAV ingest and index quality report

Companion to [SCSI_AGRI_LAND_INGEST_QUALITY.md](SCSI_AGRI_LAND_INGEST_QUALITY.md). Covers the
IPAV Farming Report ingest and the derived `land_value_index`.

## Sources held (the four that must never blend)

| table | method | grain | years |
|---|---|---|---|
| `scsi_agri_land_values` | SCSI/Teagasc agent survey | county × size band × quality | 2025 |
| `ipav_farming_report` | IPAV member-auctioneer survey | national + province × measure | 2016–2025 |
| `cso_ara02` | Revenue stamp-duty returns | NUTS3 × land type | 2013–2024 |
| `cso_rzlpa01/02/03/04` | Revenue zoned-land data | NUTS3 (01, 2018–2024) · county (02, 2024) · RZLT status (03) · participant (04) | see each |

The Property Price Register (residential sales only — no land) is deliberately not in the
index. A fifth source, `fj_land_price_report`, holds the **Irish Farmers Journal Land Price
Report** figures verifiable from FREE coverage only, each row citing its URL: national
averages 2022 (€12,288) and 2023 (€11,925), plus the published 2023 county extremes (Dublin
€38,023 highest, Mayo €6,284 lowest) `[Reported — RTÉ/thejournal republication, fetched this
session]`. Known but NOT entered for want of a citable free source: the 2024 figures
circulating in search summaries (≈€12,515 national) and the 2025 level (free coverage gives
only "+3% / €361/ac"; deriving a level from a percentage is forbidden). The county TABLES —
the part that would matter most, since the Journal compiles actual auctioneer-input sales at
county grain — are paywalled and © the Journal: filling 2023–2025 by county needs a
subscription plus reuse permission, an owner decision. The site's AMP pages fetch without a
browser, so ingest is mechanical once licensed. Distinct compiler to keep separate: the REA
Land Price Survey (2025: €13,232 national over 91 sales — too thin to ingest)
`[Reported — Anglo Celt 2026-02-24]`. No statutory per-transaction land price register
exists `[Reported — search sweep 2026-09-03]`.

## Completeness — IPAV ingest

Denominator = the chart-labelled figures in the two editions read this session (2024 edition
pub. 2025-03; 2025 edition pub. 2026-03, both primary PDFs): the 2016–2025 national sale
back-series (10), provincial sale 2023–2025 (12), forestry national 2023–2025 (3) and
provincial 2024–2025 (8), national rents 2024–2025 (6), provincial rents as charted (15) —
**54 rows, all transcribed** `[Verified — validator + row floor]`. Not ingested: unlabelled
chart bars (axis-read values would be guesses), agent commentary price RANGES (e.g. "€12,000
to €20,000" — ranges, not averages), and the 2025 edition's county purchaser-mix percentage
table (not a price; add on request).

## Recall — IPAV ingest

Single transcription pass, cross-checked against an independent secondary report of the same
edition (Westmeath Examiner, 2026-03-14): national 2025 €14,442 (+3.5%), forestry €6,602,
all four 2024 and 2025 provincial values, con-acre €287, long-term lease €313, tillage €292
— **all match** `[Verified — WebFetch of the article this session]`. Known source
inconsistency: the 2024 edition's prose gives Leinster "€16,259" and Munster "€17,262" where
its charts print €16,529 and €17,162; the chart values are taken (the 2025 edition's
year-on-year comparatives use the chart values, confirming that choice).

## The index (`land_value_index`)

A UNION for side-by-side display — 1,575 rows, one per (source, year, geo, land_class,
measure, unit) `[Verified — builder log 2026-09-03]`. Rules, enforced by tests
(`test_land_value_sources.py`):

- every row carries `source` + `method`; no blended or cross-source aggregate row exists;
- units stay as published (per-acre and per-hectare rows travel separately; surveys are
  per-acre only — a converted figure would be invented);
- the one within-source derivation is the SCSI county mean across its three size bands,
  which reproduces the report's own headline method exactly (Wexford good €19,226);
- CSO "mean" is value-weighted (Value ÷ Volume), so mean < median happens legitimately
  where large cheap parcels dominate — not a transcription error.

**Usage rule**: comparing rows across sources is reading, not arithmetic. The
agricultural-vs-zoned gap (e.g. West NUTS3 agricultural ≈ €15,140/ha mean vs zoned Galway
county ≈ €754,097/ha median, both 2024 `[Verified — index spot query]`) may be *shown*;
subtracting them to state "what zoning is worth" is forbidden — different markets, different
weightings, different grains.

## Licence

CSO tables are CC BY 4.0. SCSI/Teagasc and IPAV publications state no open licence —
internal analysis only until reuse is cleared with the owner.

## Refresh

CSO: re-run `cso_pxstat_extract --tables ARA02 RZLPA01 RZLPA02 RZLPA03 RZLPA04` (annual
releases, autumn). IPAV: each spring, append the new edition's rows to
`data/_meta/ipav_farming_report.csv`, extend `NATIONAL_SALE_YEARS`, bump the row floor,
re-run. Then re-run `land_value_index_build` and `tools/build_fact_cards.py`.
