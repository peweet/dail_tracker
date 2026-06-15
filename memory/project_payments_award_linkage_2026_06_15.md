---
name: project_payments_award_linkage_2026_06_15
description: payments↔tender linkage fix (added TED winner_history) + unlinked-payee 3-bucket sandbox probe; why linkage caps ~35%
metadata:
  type: project
---

2026-06-15 investigation: can rankings-public-payments payees be linked to "old tenders"?

**Answer:** only at the SUPPLIER spine, never contract-level — `public_payments_fact` carries NO
tender/contract ref (only `supplier_normalised`); eTenders has `Tender ID` but payments never cite it.

**Bug found + FIXED in [[project_procurement_drilldowns_2026_06_13]] family:**
`extractors/procurement_award_spend_link.py` `load_ted()` joined only `ted_ie_awards.parquet`
(2023+, API) — the API returns NO winner for pre-2024 notices. Added the
`ted_ie_winner_history.parquet` layer (2016–2023, ~23k rows, the real "old tenders"; see
[[project_ted_layers]]). Money-linkage 30%→~35% (€4.85bn realised of €14.26bn clean spend),
award+spend entities 902→1,663. Union deduped on (publication_number, winner_name_norm); the two
TED layers overlap on ~1 notice. `ted_ie_buyer_history` is buyer-side only (no winner) → not used.

**AFS is NOT a supplier source** — `la_afs_divisions`/`_capital`/`amalgamated` are council×service-
division×year budget rows, `entity`=council, NO payee column. Cannot add payment↔tender links.

**Sandbox probe** `pipeline_sandbox/procurement_unlinked_payees_probe.py` decomposes the ~€9.4bn
unlinked into: B3 STRONG public-body transfers (53 ents/€343m — HEA, EFSF, C&AG, NTMA — reclassify
to supplier_class=public_body) + WEAK watchlist (263); B2 fuzzy variant-split REVIEW candidates
(~1,970/€1.49bn, high=€319m — PJ Hegarty, Ganson, Cubic, General Dynamics); residual €7.5bn =
genuinely un-tendered PPP/concession SPVs (NBI Infrastructure DAC €1.34bn, BAM school bundles, road
concessions) + abbreviation splits (DUGGAN BROS↔Brothers). Outputs in data/sandbox (gitignored).

**Fuzzy-match lesson:** single shared token (even rare) = garbage (NBI~Coir, BAM-Schools~Three
Ireland, NTMA~PC Agency). What works = ≥1 distinctive (rare+non-generic) shared token AND TWO-SIDED
overlap ≥0.6 (both names mostly explained). Still REVIEW-only — never auto-join (schema join gate).
Real B2 fix = CRO-number anchoring of unlinked payee names, not string matching.
