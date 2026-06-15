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

**CRO-anchor prototype** `pipeline_sandbox/procurement_payee_cro_anchor_probe.py` (the principled B2
fix): match unlinked payees against canonical CRO names of TENDER-WINNING companies only (~2,832-row
dict), count only when the matched company INDEPENDENTLY won a tender (double corroboration). Gate:
distinctive token + two-sided overlap ≥0.7. Recovers 314 payees/€269m → money-linkage 34.1%→36.0%
(124 high-conf overlap≥0.85/€54m clean; medium has Charter~Alliance-Medical-type errors → review).
GOTCHA fixed: `company_num.is_in().fill_null(False)` — null is_in + `~` silently drops payees.
NEGATIVE RESULT: anchoring CANNOT touch the big unlinked money (NBI €1.34bn, PPP/SPV) — no winning
CRO entity exists; that residual needs a NEW source (NDFA PPP contracts), not better matching.
**PROMOTED 2026-06-15:** 106 high-conf CRO anchors (overlap≥0.85 AND ≥2 norm tokens; stubs like
FLOOD/PARK/MEATS excluded) → curated `data/_meta/procurement_payee_cro_overrides.csv` (gitignore
`!data/_meta/*.csv` negation already covers it). `procurement_award_spend_link.py` `load_spend()`
now coalesces a company_num from it keyed on name_norm_expr(supplier_raw). Linkage 1,663→1,702
entities / realised €4.85bn→€4.88bn (net +39 not +106 — variants merge into already-linked firms).
Caveat: a few overrides are UK↔IE group entities (Accenture UK→Accenture Ltd) — same group, fine
for footprint linkage. Extend the CSV by review to add more.

**Bucket 3 NOT auto-patchable** — the 53-entity STRONG list (€343m) is ~top-dozen clean (HEA €207m,
EFSF €119m, Pobal, Teagasc, Comptroller, Oireachtas, NTMA, Research Councils) BUT the AUTHORITY/
COUNCIL tokens catch a false tail: charities (NCBI "Council for the Blind", NCEF "Council for
Exercise & Fitness"), professional bodies (Law Society "...NTMA PC 2025" — embedded-token hit),
commercial semi-states (daa plc, IAA, Shannon Airport), and parser bleed (amounts/dates in the
name field: "Airport Authority PLC 40,416.40"). So a token rule would mis-flag — needs a CURATED
allowlist CSV (like the overrides), seeded from the clean top, NOT a blanket supplier_class patch.
**DONE 2026-06-15:** curated `data/_meta/procurement_public_body_payees.csv` (27 rows / €334m,
hand-verified; charities/semi-states/bleed EXCLUDED) + wired into `load_spend()` exclusion → these
drop from the link spend side (spend-only €9.38bn→€9.04bn, honest denominator). NOTE: this only
fixes the LINK extractor; removing them from the actual rankings page needs the same CSV applied in
the payments-consolidation `supplier_class` step (still TODO, page-facing change).

**NDFA/PPP source SCOPED:** `doc/NDFA_PPP_SCOPING.md` — the €9bn unlinked residual (NBI €1.34bn,
BAM/MPFI/Inspired school bundles, M50/road concessions) needs a CURATED PPP project registry, not
a scraper: ~30-40 projects, sources = DPER gov.ie PPP projects (spine) + C&AG PPP chapters (values,
PDF) + NDFA project pages (SPV→consortium map, the join key) + TII (15 road PPPs). Deliverable =
`data/_meta/ppp_project_registry.csv` (spv_payee_norm→project/consortium/parent_cro/capital/term).
Unitary payment = own value_kind (25-yr availability commitment), NEVER sum with award OR realised
spend. NBI = separate (NBP/DECC, not NDFA). Verdict: HIGH value, LOW-MED effort, curation. PII-free.

**BAM/PPP answer (user asked re HSE/Tusla):** BAM school bundles are paid by DEPT OF EDUCATION (not
HSE/Tusla — 0 BAM rows there; those are health/child-welfare). They ARE in our payment data. They
don't link because the payee is a ring-fenced PPP SPV ("BAM SCHOOLS BUNDLE THREE LTD" #distinct
CRO) — a different legal entity from "BAM Contractors Ltd"/"BAM Building Ltd" which DO win eTenders
(€268m+€190m+€149m awards, linkable). PPP availability payments → SPV; the PPP itself awarded via
NDFA, not in eTenders/TED. Same pattern: Glasgiven Contracts links, "BAM Glasgiven JV Ltd" SPV
doesn't. This is the structural residual — needs an NDFA PPP-contract source, not better matching.
