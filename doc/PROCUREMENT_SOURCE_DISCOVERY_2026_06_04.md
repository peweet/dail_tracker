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

---

## RESUME CHECKLIST — round 2 wiring (PAUSED 2026-06-04, blocked on parallel reorg)

Steps 1–2 DONE (seeded + probed). 5 publishers wired in round 1 (NTMA/Courts/SportIreland/
TU Dublin + MTU-stub). **Round 2 was paused mid-flight** because a parallel context session
executed the deferred reorg `pipeline_sandbox/*.py → extractors/*.py` (staged, uncommitted) and
is live-editing pipeline.py/CRO/SIPO. Resume once that reorg is committed.

### ⚠️ First, fix the reorg fallout
- `pipeline_sandbox/procurement_nphdb_parser.py` was left behind (untracked) but its `pbe` import
  points at the OLD `ROOT/"pipeline_sandbox/procurement_public_body_extract.py"` path — now moved to
  `extractors/`. Update that path (and decide whether the nphdb parser itself moves to `extractors/`).
- `extractors/procurement_public_body_extract.py` already holds my Tier-D edits + TITLE_ROW fix.
  Confirm `from cro_normalise import name_norm_expr` still resolves (cro_normalise.py is at repo root;
  the module does `sys.path.insert(0, str(ROOT))`, ROOT=parents[1]=repo root — OK).

### Direct URLs VERIFIED to fetch (2026-06-04, byte-checked headers)
- CHI  xlsx (PK/zip, 17.5KB): `https://www.childrenshealthireland.ie/documents/3541/CHI_Paid_Invoices_over_25K_incl_VAT_Qtr_1_2026updated.xlsx`
- MTU  pdf (132KB):          `https://www.mtu.ie/media/mtu-website/files/foi/financial-information/MTU-POs-over-20k-Q4-2025.pdf`
- SEAI pdf (313KB):          `https://www.seai.ie/sites/default/files/2025-08/Q2-2025-PO-Report-over-20K.pdf`

### ⚑ PARSE-VALIDATED 2026-06-04 (round 2) — only MTU is generic-reader-clean
Ran emit_rows on a real file for each. Verdict:
- **MTU ✅ WIRED** — 123 rows/high-conf (Q4-2025); 3 quarter URLs pinned via direct_files (Q2/Q3/Q4 2025).
- **CHI ❌ needs fix** — xlsx row 0 is a TITLE ("CHI Vendor payments >25K"); real header "Vendor Name/Amount"
  is row 1, but the title row wins header detection on a tie → amount col mis-picked (got €21). Fix = skip
  title rows / prefer later header row in read_xlsx (shared — test carefully) or a 1-line per-publisher offset.
- **SEAI ❌ needs bespoke** — generic reader returns supplier=None and €400m garbage amounts (header/column
  detect fails on this PO-report layout).
- **Pobal ❌ needs bespoke** — geometric find_header returns nothing (cols=[], 0 rows) despite digital text;
  header has TWO 'SUPPLIER' columns (code + name). NPHDB-class problem → reading-order or column-x parser.
- **TUS ❌ wrong files** — landing exposes prompt-PAYMENT summary PDFs (supplier=None, "Total invoices paid
  in Quarter"), not PO supplier tables. Combined-Files-202X.pdf don't parse. Need the rolling xlsx the
  discovery agent originally cited (different URL — re-locate).
- **Beaumont ❌ wrong/!malformed** — the CSV is ragged (expected 63 rows, got 16); guessed xlsx URL 404s.
  Re-harvest the correct file URL from the landing and decide grain (payment files vs the Q1-2026 PO file).

The cfg() block below was the PRE-validation guess; keep only MTU (done). The other five are NOT generic-
reader-clean — they join the bespoke tier (like HSE/Tusla/NPHDB). Original guess retained for reference:
```
cfg("ie_beaumont", "Beaumont Hospital", "hospital", "health",
    listing="https://www.beaumont.ie/page/financial-statements",
    semantics="payment_actual", grain="payment", privacy="low", tier="D",
    include=r"payments.*over.*\.csv",   # CSV-ONLY: xlsx/pdf/csv are 3 copies of same data → triple-count
    caveat="picks the 2-year 'Payments Over €20k' CSVs (payment grain); the 'POs Greater than' Q1-2026 file is a separate PO-grain file, excluded"),
cfg("ie_chi", "Children's Health Ireland (CHI)", "state_body", "health",
    listing="https://www.childrenshealthireland.ie/about-us/corporate-information/payments-to-suppliers-over-20000/",
    semantics="payment_actual", grain="payment", privacy="low", tier="D",
    direct=["https://www.childrenshealthireland.ie/documents/3541/CHI_Paid_Invoices_over_25K_incl_VAT_Qtr_1_2026updated.xlsx"],
    caveat="paid invoices at €25k incl VAT (not €20k); landing exposes no direct links → direct_files"),
cfg("ie_tus", "Technological University of the Shannon (TUS)", "education_body", "education",
    listing="https://tus.ie/privacy/freedom-of-information/publications/financial-reports/",
    semantics="po_committed", grain="purchase_order", tier="D",
    caveat="landing yields Combined-Files-2021/2022/2023 PDFs + Q1-2026.pdf; verify Q1-2026 isn't the prompt-payment appendix"),
cfg("ie_mtu", ...as already wired but ADD:
    direct=["https://www.mtu.ie/media/mtu-website/files/foi/financial-information/MTU-POs-over-20k-Q4-2025.pdf"],
    # landing only exposes the tender-register xlsx + FOI logs; PO PDFs live under /media/.../foi/financial-information/ — needs direct_files or that sub-page as listing_url),
cfg("ie_seai", "Sustainable Energy Authority of Ireland (SEAI)", "agency", "energy_utilities",
    listing="https://www.seai.ie/publications", semantics="po_committed", grain="purchase_order",
    privacy="medium", tier="D",
    direct=["https://www.seai.ie/sites/default/files/2025-08/Q2-2025-PO-Report-over-20K.pdf"],
    caveat="files buried in general /publications search → direct_files"),
cfg("ie_pobal", "Pobal", "agency", "social",
    listing="https://www.pobal.ie/financial-information/", semantics="po_committed",
    grain="purchase_order", privacy="medium", tier="D",
    caveat="files titled 'Purchase Order OR Payments over €20k' but rows carry PO/SUPPLIER/TOTAL/PAID columns = POs with a paid-flag (not truly mixed). Full 2020-2026 series, 25 PDFs"),
```

### Still BLOCKED / out of scope
- **Garda** — landing (`.../budgets-and-spending/`) harvests 69 *fleet-management* PDFs, NOT POs.
  The PO listing is on a different subpage ("purchases over €20,000") — URL not yet located. Find it.
- **EPA** — serves PO data as `.php` HTML pages; the extractor only reads pdf/xlsx/xls/csv → needs an
  HTML-table reader path (not OCR; a new reader). Defer.
- **UCD / SETU** — domains 403-block the fetcher; need a real browser/UA pass to lock file URLs.
- **OCR bodies (Coimisiún na Meán, Irish Prison Service)** — EXCLUDED by request (no OCR).

---

## DISCOVERY SWEEP 2 — commercial semi-states + non-commercial regulators (2026-06-04)

Triggered by "any other semi-states we missed — RTÉ? Electric Ireland?". Two parallel agents.
**Key principle: the €20k PO-publication circular binds NON-COMMERCIAL bodies; COMMERCIAL state
bodies are exempt and publish annual reports only.** So the commercial asks are the least likely.

### Commercial semi-states — mostly dead ends (as expected)
| Body | verdict |
|---|---|
| **RTÉ** | CATEGORY-ONLY — about.rte.ie/purchase-orders-over-e20000/ Q3.pdf is spend by category (Capital/Communication circuits + PO counts), NOT supplier-level. (agent's "supplier-level" was wrong for this file) |
| **ESB / Electric Ireland** | NOT_FOUND — commercial, FOI-exempt, eTenders/OJEU only. Annual report only. |
| **EirGrid / Gas Networks Ireland / Uisce Éireann** | ⚠️ CATEGORY-ONLY TRAP — pages titled "PO over €20k" but data is rolled up by expense category, NO supplier names (EirGrid frozen at 2016). Do NOT ingest as supplier facts. |
| daa, An Post, ports (Dublin/Cork/Shannon-Foynes), Shannon Group, IAA | annual report only (commercial) |
| Bord na Móna, Coillte | annual report only; explicitly outside FOI, fought disclosure at OCEI |
| Irish Rail, HRI, TG4, CIÉ buses | FOI-request-only, no proactive file |

### Non-commercial regulators / cultural bodies — the real haul (none were in the seed)
| Body | status | format | wired? |
|---|---|---|---|
| **HPRA** | CONFIRMED | pdf (assets.hpra.ie CDN, quarterly) | ✅ WIRED Tier E (Deloitte/Eircom/Kano clean) |
| **CCPC** | CONFIRMED | pdf (assets.ccpc.ie, quarterly Q1-2026) | ✅ WIRED Tier E (CPL recruitment clean, 28 rows) |
| **National Library** | CONFIRMED | pdf (Drupal) | ✅ WIRED Tier E (Synergy/Electric Ireland, 13 rows) |
| **An Bord Pleanála** | CONFIRMED | pdf, **Irish bilingual headers** | DEFERRED — multi-line Irish header bleeds date into supplier; added Irish keywords to ROLE_RE (Soláthraí/Glanmhéid/Méid Comhlán/Dáta) so amount now detects, but supplier still needs header-wrap handling |
| **EPA** | CONFIRMED | **.php HTML pages** (Q1-2026) | needs HTML-table reader |
| **RTB** | CONFIRMED | pdf **+ .docx drift by quarter** | needs Word reader |
| **National Gallery** | CONFIRMED | native **.docx** | needs Word reader |
| **WRC** | CONFIRMED | **HTML annual tables** (2023-25) + pdf (2018-22) | needs HTML reader |
| **National Museum / FSAI** | CONFIRMED (exists) | 403/404 bot-blocked | need real-browser/UA |
| **Legal Aid Board** | CONFIRMED (PO grain) | pdf; only a 2014 file confirmed, index 404'd | re-locate live index |
| **CRU** | stale (last file Q2-2019) | skip |
| ComReg, DPC, Pensions Authority, PSRA | no supplier-level €20k listing |
| **Central Bank** | publishes CONTRACTS-AWARDED only (award grain, not PO-over-20k) |

**Shared improvement banked:** ROLE_RE now includes Irish-as-Gaeilge header terms (regression-clean
on OPW/HPRA). Helps any bilingual publisher. **Run to bank HPRA/CCPC/NLI was DEFERRED** — parallel
context had 4 live python processes (re-clobber risk on public_payments_fact.parquet); run when quiet.
**Next reader investment with best ROI: an HTML-table reader** — unlocks EPA + WRC (and is reusable).
