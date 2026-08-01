# nphdb_bam_disclosures_extract.py — quality report

Static, hand-verified loader (not a scraper). See the module docstring for the
two source PQ debate-record URLs; every figure was checked against the raw
Oireachtas XML the session this extractor was written (2026-08-01).

## Completeness

**Slippage ledger**: 14/14 update rows (plus the baseline zero-point row = 15
total), cross-checked against BOTH the 2025-06-17 (dbsect_691) and 2025-09-17
(dbsect_1150) Dáil written-answer tables — the two PQ answers carry the
identical table, so completeness is checked against two independent
publications of the same disclosure, not one. The one discrepancy found (the
final row's delay footnote: "+3 months" in the June answer vs "+2 months" in
the September answer) is recorded in that row's `notes` column, not silently
resolved.

**Cost table**: 5/5 rows from the single 2021-07-27 PQ 2598 answer (the
Capital Build sub-total, the "other programme costs" sub-total, the grand
total, and the two sub-items — Gross Construction Costs and Main NCH — nested
within Capital Build). The expected universe (denominator) is the table as
published in that one PQ answer; there is no larger series to be incomplete
against.

## Recall

N/A in the scraping sense — this is a hand-verified static disclosure (20 rows,
typed from source, not classified/extracted by a rule or model), so there is no
recall to measure against a labeled set. The applicable check is transcription
fidelity, not recall: every value was checked against the raw XML rather than
copied from a secondary summary.
