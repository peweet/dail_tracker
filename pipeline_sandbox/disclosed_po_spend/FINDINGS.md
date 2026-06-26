# Disclosed national PO / payments-over-EUR-20k extract — sandbox findings

_Sandbox-only assessment. Source file: `data/raw_bq/bq-results-20260619-122315-1781871808837.csv` (do not modify). Cross-referenced against our parsed fact `data/gold/parquet/procurement_payments_fact.parquet`. READ-ONLY: nothing here is promoted to silver/gold._

---

## 1. What this is

A national "Purchase Orders / payments over EUR 20,000" dataset **disclosed to the project owner** (not scraped or parsed by us). It is a single harmonised BigQuery export: **582,119 rows, 216 bodies, 2011-2026**, with an 8-column schema (`PO, Supplier, Total, Description, QTR, Year, entity, year_quarter`). The breadth is the tell: all central departments (no prefix), all 31 local authorities (no prefix), 161 agencies (prefixed `Agency : `), 4 Section 38 voluntary health bodies (prefixed `Section 38 : `), **plus commercial semi-states normally absent from central PO sets** (Irish Water, EirGrid, Gas Networks, ESB, RTE, Central Bank). The likely source is **a third-party cross-sector aggregator's BigQuery warehouse** that harvests the official Circular 07/2012 / Public Service Reform Plan (2011) EUR 20k transparency files across the whole public sector and exported via the `bq` CLI (hence the `bq-results-...csv` filename). It is **not** a single official portal; opengov.ie is the closest conceptual analogue but is ruled out as the direct source (departments-only, UPPERCASE suppliers, no `Agency :` prefix), and publicinformation.ie is ruled out (FOI-log repos only, no spend/PO pipeline).

---

## 2. Trust verdict

**Verdict: trustworthy as a faithful rendering of HSE's published EUR 20k returns, but the agreement is _shared source lineage_, not independent corroboration.**

Our HSE rows and the BQ HSE slice are **siblings off one parent**: both transcribe HSE's own FOI Model Publication "PO payments above EUR 20,000" file. Our 16,972 HSE rows came from a **single PDF** (`HSE_FOI_Model_Publication_..._above_20k.pdf`, one source hash `52a8b4e9...`, `disclosure_basis=foi_s8_model_scheme`, EUR 20,000 threshold) per `data/_meta/hse_tusla_payments_coverage.json`. BQ's HSE slice carries the identical regime fingerprint: every one of its 78,869 HSE rows is **>= EUR 20,000 (min exactly 20000.0, zero below)**, and 532 of our 561 distinct descriptions appear **verbatim** in BQ.

Quarter-grain reconciliation (16 overlapping quarters, 2021-q4 -> 2025-q3):

- **8 of 16 quarters agree to exactly 0.0000%** (2021-q4, 2022-q1/q2/q3, 2023-q1/q3, 2024-q1/q2).
- **Worst delta: -0.34% (2025-q2)**, and that is simply the euro value (~EUR 856k) of 2 PO lines our PDF parser dropped, not a coverage disagreement.
- Across all 16 shared quarters: **BQ EUR 6,385,474,105 vs ours EUR 6,382,796,915 = -0.042% overall**. BQ has exactly **12 more rows** (16,972 vs 16,960); we have **zero rows BQ lacks**. The 12-row gap is 0-2 dropped lines/quarter from our PyMuPDF x-coordinate parser (row-clustering / continuation-line loss), concentrated in 8 quarters. NOTE: our 190 privacy-suppressed rows remain in the parquet with amount intact and do not move counts/totals.

**Independence = `shared_source_lineage`.** Cent-for-cent agreement at this scale is the signature of one upstream source read twice, not two independent measurements converging. Direction rules out circularity: BQ is **more complete than us** (it carries 2017-q3..2020-q2 history, 2025-q4, 2026-q1, and the 12 lines we dropped), so it cannot be derived from our parse: it is upstream/parallel to us. **Practical upshot:** trust the numbers as an accurate copy of HSE's official return; do **not** present the cross-source match as two-source confirmation of HSE's _actual_ spend (it validates transcription fidelity only).

---

## 3. What it ADDS vs our 247k-row parsed fact

### (a) HSE history we lack, and recovery of the "only surviving public copy"

Our memo recorded the HSE EUR 20k file as **the only surviving public copy** (HSE deleted the source in its 2026 site rebuild; Wayback never archived it). The disclosed BQ extract **back-fills the parts we never held and recovers the parts we lost**:

- **2017-q3 -> 2020-q2** pre-PDF HSE history (entirely absent from our fact).
- **2025-q4 (10,238 rows / EUR 1.01bn)** and **2026-q1 (7,918 rows / EUR 694m)**: our source PDF stopped at 2025-q3, so these were out-of-window for us, not a disagreement.
- The 12 individual PO lines our parser dropped inside the overlap window.

So the headline "2025 divergence" in the brief is **not** a disagreement: it is purely the 2025-q4 + 2026-q1 quarters that exist only in BQ.

### (b) The TRUE list of genuinely-new bodies

After resolving every one of our 72 publishers to its BQ counterpart and collapsing 53 rename/abbreviation false-positives, the corrected figure is **141 genuinely-new bodies, EUR 34.76bn gross** (the naive exact-string diff of 194 new / EUR 71.0bn, and the brief's "175 / EUR 61.24bn", were both partial normalisations: do not use them).

Largest genuinely-new bodies (confirmed absent under any name):

| Body | Rows | Gross EUR |
|---|---:|---:|
| Irish Water | 401 | 11.05bn |
| Dublin City Council | 40,432 | 4.10bn |
| EirGrid | 305 | 2.97bn |
| Gas Networks Ireland | 374 | 2.15bn |
| An Garda Siochana | 8,714 | 1.76bn |
| Central Bank of Ireland | 3,730 | 1.18bn |
| Louth County Council | 3,933 | 0.99bn |
| Dun Laoghaire-Rathdown CC | 8,151 | 0.87bn |
| IDA | 4,571 | 0.66bn |
| NAMA | 592 | 0.57bn |
| Kerry County Council | 2,526 | 0.55bn |
| Road Safety Authority (RSA) | 5,782 | 0.53bn |

Plus **8 new councils** (Dublin City, DLR, Kerry, Tipperary, Roscommon, Cavan, Carlow, Louth: we hold only Fingal/South Dublin among Dublin LAs, **no DCC, no DLR**), **14 new ETBs**, and **6 new universities/HEIs** (NUIG, UCC, Trinity, DkIT, IADT, DIAS, NUI: we hold only the 4 TUs).

### WARNING: Rename false-positives, do NOT double-count

53 BQ names are the **same body** as one of our publishers under a different label and must be collapsed, not counted as new. Categories:

- **23 councils**: BQ "X County/City [and County] Council" -> our short "X" (e.g. `Clare County Council`->`Clare`, `Limerick City and County Council`->`Limerick`).
- **8 renamed departments** (machinery-of-government churn): Justice->`...Home Affairs and Migration`; Children EDIY->`...Disability and Equality`; Education->`...and Youth`; Enterprise->`...Tourism and Employment`; Foreign Affairs->`...and Trade`; Environment/Climate/Comms->`Dept of Climate, Energy and the Environment`; DPER->`Dept of Public Expenditure, Infrastructure, PSR and Digitalisation`; Tourism/Culture/.../Media->`Culture, Communications and Sport`.
- **Abbreviations**: ATU/MTU/TUD/TUS, HEA->Higher Education Authority, SEAI, HPRA, CCPC, Revenue->Revenue Commissioners, Courts, BIM, ESB->ESB Networks DAC, TUSLA, `Section 38 : Beaumont Hospital`->Beaumont Hospital.
- **NTMA family**: BQ splits NTMA into **6 statutory sub-funds** (Administration, National Debt, ISIF, NDFA, Future Ireland Fund, Infrastructure-Climate-Nature Fund) -> all collapse to our single `National Treasury Management Agency (NTMA)`.

Note: TII (EUR 8.41bn in BQ) matched verbatim and is **not** new. Our fact holds 2 publishers with no BQ counterpart at all (CHI, Sport Ireland). The Section 38 prefix in BQ uses a narrow no-break space (U+202F), not ASCII.

---

## 4. Value semantics caveat: the gross is NOT a spend total

**The ~EUR 103bn (approx EUR 117bn variant) cross-body gross of `Total` is NOT a meaningful spend figure and is NOT safe to sum.** `gross_is_safe_to_sum = false`. It conflates at least three incompatible measures:

1. **Payments mixed with commitments.** `Total` mixes `payment_actual` bodies (HSE, OPW, Education, DCEDIY, DECC, Revenue, TII) with `po_committed` bodies: purchase orders raised, a commitment that may never be fully paid (Justice, Defence, NTA, Tusla, Agriculture, Transport, **and every Local Authority**). Even within just the ~31% of euros (EUR 31.9bn) confidently mapped to our fact's semantics, the split is **EUR 23.0bn payment_actual + EUR 8.9bn po_committed**: different units that must never be added as one "spend" number.
2. **Aggregated category roll-ups, not line items.** ~EUR 16.5bn (~16%) comes from utility/regulator bodies publishing a handful of per-category quarterly buckets with **no PO and no supplier**: Irish Water EUR 11.05bn over just **401 rows** (avg EUR 27.6m/row, max EUR 506m), EirGrid EUR 2.97bn/305 rows, Gas Networks EUR 2.15bn/374 rows, plus RTE, ESB. These are not EUR 20k purchase-order disclosures at all and are absent from our 72-publisher fact.
3. **The blank-PO heuristic is unreliable.** Blank PO sometimes means a payment list (Education: 100% blank, `payment_actual`) and sometimes a commitment list with the PO column merely omitted (Tusla, Cork City, Limerick, DCC: 100% blank, `po_committed`). Conversely a **fully populated PO does NOT imply payment**: Justice, Defence, NTA, Mayo, South Dublin all carry clean PO numbers yet are `po_committed`. Semantics must be arbitrated by our fact's per-publisher tags; where a body has no fact anchor (Dublin City Council, An Garda Siochana, Louth, Central Bank, the utilities), semantics **cannot be asserted from the CSV alone**.

**The extract carries none of our regime metadata**: no `amount_semantics`, no `value_safe_to_sum`, no `vat_status`, no privacy/PII flags. Our regime logic would have to be **re-derived per body** before any aggregation or display. Cite `Total` only per-body, labelled payment vs commitment vs aggregated-rollup. (Scratch: `pipeline_sandbox/disclosed_po_spend/top25_classification.csv`.)

---

## 5. Provenance & handling

**Likely source.** A third-party **aggregator's BigQuery warehouse** harvesting the entire-public-sector EUR 20k transparency regime, then `bq`-exported. Evidence: one harmonised 582k-row/216-body table 2011-2026 with a single 8-col schema; cross-sector breadth (departments + 31 LAs + 161 prefixed agencies + Section 38 health bodies + commercial semi-states); curatorial choices (NTMA split into 6 statutory sub-funds; suppliers lowercased, 41,988/41,990 distinct); EUR 20k threshold enforced (only 0.4% of rows < EUR 20k abs; credit notes present, min -176,319); the `bq-results-...csv` filename. **Convergence proving the same underlying official source as our parse:** HSE 2023 BQ 4,052 rows/EUR 1,410.06m vs ours 4,049/EUR 1,409.47m; NPHDB 2022-24 identical; OPW/Revenue near-identical. **opengov.ie ruled out** (departments-only, UPPERCASE, no `Agency :`); **publicinformation.ie ruled out** (FOI-log repos only).

**Licensing, two-layer, unresolved.** The underlying per-body EUR 20k reports are **public** (Circular 07/2012, Public Service Reform Plan 2011; CC-BY-4.0 / PSI re-use when from gov.ie / data.gov.ie), so the **facts** are re-usable with attribution to the originating bodies. **But this artefact is not automatically clear to re-publish:** (1) the aggregator's harmonisation work (the `Agency :` taxonomy, NTMA sub-fund split, supplier normalisation, the schema) may carry **its own licence / database rights**, and the aggregator is **unidentified**; (2) "disclosed to me" implies the transfer may carry **private disclosure terms** (confidentiality / attribution / no-redistribution) that override the underlying public status; (3) **no per-record provenance**, so we cannot re-derive each figure from a public URL to re-anchor it. `handling_recommendation = needs_owner_decision`; `gold_eligible = FALSE`. **Per-record `source_file_url` is ABSENT** in this extract: unlike our parsed fact, no row can be traced to its originating published file.

**Open questions for the owner (before anything touches gold or the live app):**

1. **Who disclosed this and under what terms?** Any written/verbal condition (confidentiality, attribution, no-redistribution, sandbox-only)?
2. **Who is the aggregator** behind the BigQuery warehouse, and what is its compilation/database licence?
3. **Re-publish or private-use only?** If re-publish, is attribution to the aggregator and/or originating bodies required, and in what form?
4. **Per-record provenance is absent**: should we re-derive each surfaced figure from the originating body's own published EUR 20k file (fixes citation and side-steps the disclosure-terms risk)?
5. **No privacy/PII or VAT-basis flags**: accept our own PII/privacy and value-safe-to-sum gating being applied before any display?
6. **Keep strictly under `pipeline_sandbox/`** (read-only, never promoted) until licensing/disclosure questions are answered?

---

## 6. Recommended next steps (all sandbox / scoping, NO gold promotion)

1. **Resolve the owner questions in section 5 first.** Licensing + disclosure terms are blocking; nothing public-facing or promoted until answered.
2. **Use it now for cross-validation and lead-generation only.** It already confirmed the HSE = payments finding to the cent and recovered HSE 2017-2020 + 2025-q4 + 2026-q1; treat those as a validation/back-fill _candidate_, not a fact source, until provenance is re-anchored.
3. **Build a per-body semantics crosswalk in sandbox** mapping each of the 216 BQ bodies -> our `amount_semantics`/`value_safe_to_sum`, flagging the utility category-rollups (Irish Water, EirGrid, Gas Networks, Central Bank) as non-line-level. Reuse `top25_classification.csv` as the seed.
4. **Scope a provenance re-derivation path** for the genuinely-new bodies of greatest public interest (Dublin City Council EUR 4.10bn, An Garda Siochana, IDA, NAMA, RSA): can we fetch each body's own published EUR 20k file to re-anchor figures to a citable primary URL before any display?
5. **Document the 53-name rename/abbreviation crosswalk** (esp. the NTMA 6-fund split and the U+202F Section 38 prefix) so future joins never double-count BQ vs our publishers.
6. **Hold everything under `pipeline_sandbox/disclosed_po_spend/`.** Do not promote to `data/silver` or `data/gold`, do not wire into the live app, until section 5 is cleared.
