---
tier: REPORT
status: LIVE
domain: land-value
updated: 2026-09-03
supersedes: []
read_when: using scsi_agri_land_values / scsi_agri_land_rental, or refreshing them from a new SCSI/Teagasc edition
key: REPORT|LIVE|land-value
---

# SCSI/Teagasc land review ingest — quality report

Source: SCSI/Teagasc Annual Agricultural Land Market Review & Outlook 2026 (published April
2026, covering the 2025 survey year; 13th edition), retrieved 2026-09-03 from
<https://scsi.ie/wp-content/uploads/2026/04/SCSI-Teagasc-Annual-Agricultural-Land-Market-Review-Outlook-2026.pdf>.
There is no machine-readable release, so the tables were transcribed by hand into curated
CSVs (`data/_meta/scsi_agri_land_values.csv`, `data/_meta/scsi_agri_land_rental.csv`) and
landed by `extractors/scsi_agri_land_extract.py` with validation tripwires and row floors.

## Completeness

The denominator comes from the report's own table structure, checked against the PDF:

- **Sales values**: Tables 6–8 publish 25 counties (26 minus Dublin, excluded by the source)
  × 3 plot-size bands × 2 quality grades = **150 values**; all 150 are transcribed, none
  null. `[Verified — validator counts + the committed CSV]`
- **Rental values**: Tables 10–12 publish 3 provinces × 5 land uses × 2 years (2024, 2025)
  = 30 cells, of which 6 are published as *n/a* (Connacht/Ulster tillage uses). All 30 rows
  are present with the n/a cells kept as nulls. `[Verified — validator]`
- **Not ingested, deliberately**: Table 5 (provincial averages + 12-month change — derivable
  context, different grain), Table 9 (3-year rolling changes), the sector-performance
  narrative, the risk special feature, and the statistical annex. Add on request; each is a
  new grain and must not be mixed into the county table.

## Recall

Hand transcription has no classifier, so recall here means "were any cells miscopied":

- The <50-acre slice (50 of 150 values) was independently cross-checked against the report's
  county map (p4), which restates it: **25/25 counties match both quality grades**.
  `[Verified — page-image comparison, this ingest session]`
- The report's own headline county averages reproduce exactly from the transcribed table:
  Wexford good €19,226, Kildare good €19,200, Leitrim poor €3,772 (Key highlights, p5).
  These means span all three size bands, so they jointly constrain the 50–100 and >100
  columns for those counties. `[Verified — test_scsi_agri_land_extract.py]`
- The remaining ~90 cells are covered by the transposition tripwire (poor ≥ good fails
  validation) and by the two arithmetic checks above, not by a second independent read:
  residual risk is a same-column typo in a county the headlines don't name.
  `[Extracted — single transcription pass + partial cross-checks]`

## Grain and usage rules

Survey opinion of typical values from 169 agents, not transaction records. Three land-value
sources now coexist and **must never be averaged, differenced, or summed across**:
`scsi_agri_land_values` (survey, county × band × quality), `cso_ara02` (stamp-duty medians,
NUTS3 × land type, 2013–2024), and the PPR (recorded residential sales — no land at all).
RZLPA02 (zoned residential land, county, 2024-only) is a fourth, different market again.

## Licence

© SCSI/Teagasc; the report states no open licence. Cleared for internal analysis only —
customer-facing use of these figures needs owner sign-off on reuse terms (attribution quote
or permission), tracked with the other licence questions.

## Refresh procedure

Each April: download the new edition, append the new survey year's rows to both CSVs (keep
prior years), re-run the map/headline cross-checks above for the new year, bump the row
floors in the extractor, then `python -m extractors.scsi_agri_land_extract`.
