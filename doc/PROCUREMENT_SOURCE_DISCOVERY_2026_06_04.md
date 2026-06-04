# Procurement Source Discovery — 2026-06-04

Triggered by the NPHDB finding (a body absent from the 59-publisher seed entirely). NPHDB
exposed a **structural blind spot**: single-purpose capital boards, voluntary health bodies,
and several state agencies that publish their OWN PO/payment listings over €20k fall outside
the original department/agency/semi-state taxonomy. This is a discovery sweep of category 2
("whole categories absent from the seed"). All URLs were seen in live search/fetch — but the
403-blocked ones and PDF byte-contents still need a browser/real-UA confirmation before wiring.

**Result: 17 NEW confirmed supplier-level publishers** (5 universities, 2 hospitals, 10 state
bodies). On top of the existing 19 generic + HSE/Tusla + NPHDB, this ~doubles coverage.

All emit into the existing `public_payments_fact` gold schema. Grain matters — most are
`purchase_order` (po_committed), a few are `payment`/paid-invoices (payment_actual), Pobal is
MIXED. Never union po_committed with payment_actual totals.

---

## Universities (5 confirmed — all PO grain, privacy low)

| Body | status | format | landing_url | direct_file_url | latest |
|---|---|---|---|---|---|
| **TUS** | CONFIRMED | xlsx (single rolling file, all years — easiest) | https://tus.ie/privacy/freedom-of-information/publications/financial-reports/ | https://tus.ie/app/uploads/ProfessionalServices/FOI/TUS_POs_over_20k_2021QTR4_2022_2023_2024_2025_Q1.2026.xlsx | Q1 2026 |
| **MTU** | CONFIRMED | pdf quarterly + xlsx | https://www.mtu.ie/about-mtu/legal/freedom-of-information/ | https://www.mtu.ie/media/mtu-website/files/foi/financial-information/MTU-POs-over-20k-Q4-2025.pdf | Q4 2025 |
| **TU Dublin** | CONFIRMED | pdf quarterly | https://www.tudublin.ie/explore/governance-and-compliance/foi/foi-publication-scheme/ | https://www.tudublin.ie/media/website/explore/foi/documents/PO-Report-over-20K-Quarter-2-2026.pdf | Q2 2026 |
| **UCD** | CONFIRMED (verify) | pdf | https://www.ucd.ie/foi/freedomofinformation/publicationscheme/procurementinformation/ | https://www.ucd.ie/t4cms/MBRS%20Purchase%20Orders%20over%2020k.pdf | ? |
| **SETU** | CONFIRMED (verify) | pdf | https://www.setu.ie/procurement-information | (per-page) | ? |

**FOI-only / no €20k file:** TCD, UCC, UL, University of Galway, Maynooth, DCU — these stop at
the €25k eTenders contract-award register; supplier-level €20k data is FOI-request only.
**Pattern:** the *Technological Universities* (TUS/MTU/TUD/SETU) all comply with the model PO
publication scheme; the *traditional universities* mostly don't (only UCD does). UCD & SETU
domains 403-block automated fetchers → confirm file inventory in a real browser.

## Voluntary / Section 38 hospitals (2 confirmed)

| Body | status | grain | format | landing_url | direct_file_url | latest |
|---|---|---|---|---|---|---|
| **Beaumont** | CONFIRMED | purchase_order €20k | pdf/xlsx/**csv** | https://www.beaumont.ie/page/financial-statements | https://www.beaumont.ie/sites/default/files/2026-04/POs%20Greater%20than%20%E2%82%AC20k%20-%20Q1%202026.xlsx | Q1 2026 |
| **CHI** ⭐ | CONFIRMED | payment (paid invoices) €25k | xlsx | https://www.childrenshealthireland.ie/about-us/corporate-information/payments-to-suppliers-over-20000/ | https://www.childrenshealthireland.ie/documents/3541/CHI_Paid_Invoices_over_25K_incl_VAT_Qtr_1_2026updated.xlsx | Q1 2026 |

**FOI-only / not found:** Mater, St James's, Rotunda, Coombe, Tallaght (TUH — note
`payments.tuh.ie` is a PATIENT billing portal, false lead), St Vincent's, NMH (Holles St),
RVEEH, NRH. **CUH** only via the central HSE **€100k** PO-payments PDF (filter CUH rows out).
**Class read:** voluntary hospitals mostly DON'T proactively publish — 2 of 12 do. CHI satisfies
the high-priority children's-hospital-operator target (complements NPHDB's construction side).

## State bodies / agencies (10 confirmed)

| Body | status | grain | format | landing_url | direct_file_url | latest | priv |
|---|---|---|---|---|---|---|---|
| **Courts Service** ⭐ | CONFIRMED | PO | pdf | https://www.courts.ie/publications/purchase-orders-greater-than-20k | https://www.courts.ie/docs/default-source/publications-files/purchase-orders-greater-than-20k/purchase-orders-greater-than-20k---q1-2026-v2.pdf | Q1 2026 | low |
| **Coimisiún na Meán** | CONFIRMED | PO | pdf | https://www.cnam.ie/about/reports-finances/procurement/ | https://www.cnam.ie/app/uploads/2026/05/Q1-2026_PO-Report-1.pdf | Q1 2026 | low |
| **NTMA** | CONFIRMED | payment | pdf (6 per-unit files/qtr: ADM, Nat-Debt, ISIF, NDFA, FIF, ICNF) | https://www.ntma.ie/information-pages/freedom-of-information/freedom-of-information-publication-scheme/financial-information | https://www.ntma.ie/uploads/general/Q1-2026-ADM.pdf | Q1 2026 | low |
| **NDFA** | CONFIRMED (under NTMA) | payment | pdf | (NTMA scheme) | https://www.ntma.ie/uploads/general/Q1-2026-NDFA.pdf | Q1 2026 | low |
| **Sport Ireland** | CONFIRMED | PO | pdf (single rolling log) | https://www.sportireland.ie/about-us/freedom-of-information/financial-information | https://www.sportireland.ie/sites/default/files/media/document/2026-04/2023-25%20Sport%20Ireland%20Purchase%20Order%20Log.pdf | 2023–25 | low |
| **SEAI** | CONFIRMED | PO | pdf (in general /publications search) | https://www.seai.ie/publications | https://www.seai.ie/sites/default/files/2025-08/Q2-2025-PO-Report-over-20K.pdf | Q2 2025 | med |
| **EPA** | CONFIRMED | PO | pdf + **.php HTML** | https://www.epa.ie/who-we-are/corporate-compliance/procurement/purchase-orders/ | https://www.epa.ie/publications/corporate/governance/purchase-orders-quarter-3-2024-over-20k.php | Q3 2024 | low |
| **Pobal** | CONFIRMED | **MIXED PO+payment** | pdf | https://www.pobal.ie/financial-information/ | https://www.pobal.ie/wp-content/uploads/2026/04/Purchase-Order-or-Payments-over-E20k-Q1-2026.pdf | Q1 2026 | med |
| **An Garda Síochána** | CONFIRMED (verify) | PO | pdf/html | https://www.garda.ie/en/freedom-of-information/publication-scheme/budgets-and-spending/ | (nested) | ? | **high** |
| **Irish Prison Service** | CONFIRMED (verify) | PO | pdf (annual) | https://www.irishprisons.ie/information-centre/procurement/ | https://www.irishprisons.ie/wp-content/uploads/documents_pdf/POs-greater-than-E20k-2024.pdf | 2024 | **high** |

**Do NOT publish €20k listings:** LDA (commercial DAC — annual reports only), NAMA (wound-down,
admin spend rolls into NTMA), Met Éireann (a division of Dept of Housing — under parent gov.ie
collection). Sport Ireland Facilities/Nat. Sports Campus merged into Sport Ireland (no separate page).

---

## Ingestion notes / parser implications
- **Easiest first wins** (clean tabular, current): Courts Service, Coimisiún na Meán, TUS (one
  xlsx), Beaumont (csv available), CHI (xlsx). These likely parse with the GENERIC reader.
- **Need a new reader path:** EPA serves some quarters as `.php` HTML pages (not PDF) → needs an
  HTML-table reader; SEAI/Sport Ireland files are buried in general search / single rolling logs.
- **Grain care:** Pobal mixes PO + payment in one file — must split by `value_kind`. NTMA/NDFA/CHI
  are payment-grain (payment_actual); universities/Courts/CnaM/EPA/Sport/Garda/Prisons are PO.
- **Privacy:** Garda + Irish Prison Service = high (security redactions); SEAI + Pobal = medium
  (grant-adjacent, possible individual names).
- **403 to confirm in browser:** UCD, SETU, Garda direct-file paths.

## Suggested next steps
1. Add the 17 confirmed bodies to `procurement_publishers_seed.py` with correct grain/format/status.
2. Run `probe_procurement_publishers.py` against the new seed entries to byte-verify (resolves the
   403s with the real harvester + curl fallback, locks direct file URLs).
3. Wire the generic-reader-clean ones into `procurement_public_body_extract.py`; bespoke only where
   needed (EPA HTML, any rotated PDFs à la NPHDB).
