"""Tier/publisher CONFIG table + shared constants for the public-body payments
extractor package (split out of extractors/procurement_public_body_extract.py,
doc/REFACTORING_CANDIDATES.md C7, pure move-function -- no logic changes).

Owns: PUBLISHERS (the cfg() list every publisher is declared in), the schema
column order (PAYMENTS_FACT_SCHEMA_COLS -- sibling parsers reference this by
name), and the file-path/parser-version constants main.py writes against. No
dependency on any sibling module in this package, so every other module can
import from here without a cycle.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

OUT_FACT = ROOT / "data/silver/parquet/public_payments_fact.parquet"
OUT_COV = ROOT / "data/_meta/public_payments_coverage.json"
PARSER_VERSION = "0.1.0"
# Row floor for the overwrite-each-run fact (85,602 rows 2026-06-20). A SAFE
# `--only X --merge` keeps total ~85k (kept + reparsed), so it passes; a plain
# `--only` without --merge wipes the fact to one publisher's slice -- the floor
# refuses that write rather than silently downgrading silver. ~30% headroom; a
# deliberate scoped rebuild uses DAIL_SKIP_ROW_FLOOR=1.
MIN_FACT_ROWS = 60_000

DATA_EXT = (".pdf", ".xlsx", ".xls", ".csv")

# The canonical column contract for the public-body payments fact (order-defining).
# Sibling publisher parsers (nphdb/nta/seai/hse_tusla) all load the flat shim module
# as `pbe` and reference `pbe.PAYMENTS_FACT_SCHEMA_COLS` so the schema lives in ONE
# place. The downstream consolidator validates by name/set and concats name-aligned,
# so the fact is order-independent -- but keep this list authoritative for new columns.
PAYMENTS_FACT_SCHEMA_COLS = [
    "publisher_id",
    "publisher_name",
    "publisher_type",
    "sector",
    "source_landing_url",
    "source_file_url",
    "source_file_hash",
    "period",
    "year",
    "quarter",
    "supplier_raw",
    "supplier_normalised",
    "amount_eur",
    "amount_semantics",
    "value_safe_to_sum",
    "description",
    "po_number",
    "paid_flag",
    "source_row_number",
    "source_page_number",
    "parser_name",
    "parser_version",
    "extraction_status",
    "extraction_confidence",
    "caveat_text_detected",
    "supplier_class",
    "privacy_status",
    "public_display",
    "source_caveat",
]

# ============================================================================ CONFIG
# amount_semantics controlled vocab (PROCUREMENT_INVESTIGATION.md value taxonomy):
#   po_committed     -> "ordered €X"  (PO-over-20k order lists)  summable
#   payment_actual   -> "paid €X"     (payments/paid lists)      summable (true spend)
#   contract_award_value -> "awarded €X" (Tailte contracts)      caution
# listing_url = page to harvest period files from; direct_files = known-good file URLs
# (used as a floor so a publisher still yields data if its listing is JS/awkward).
def cfg(
    pid,
    name,
    ptype,
    sector,
    *,
    listing,
    semantics,
    grain,
    privacy="low",
    tier="A",
    direct=None,
    include=None,
    exclude=None,
    caveat="",
    reader=None,
) -> dict:
    return {
        "id": pid,
        "name": name,
        "ptype": ptype,
        "sector": sector,
        "listing_url": listing,
        "amount_semantics": semantics,
        "grain": grain,
        "privacy_risk": privacy,
        "tier": tier,
        "direct_files": direct or [],
        "include": re.compile(include, re.I) if include else None,
        "exclude": re.compile(exclude, re.I) if exclude else None,
        "caveat": caveat,
        # optional bespoke reader override (e.g. "reading_order" for the line-pair DCEDIY layout
        # the generic word-geometry reader yields 0 rows for); None = generic header-anchored reader.
        "reader": reader,
    }


PUBLISHERS: list[dict] = [
    # ---- Tier A: clean tabular / high-confidence PDF -------------------------------
    cfg(
        "ie_opw",
        "Office of Public Works",
        "state_body",
        "property_land",
        listing="https://www.gov.ie/en/office-of-public-works/collections/payments-greater-than-20000/",
        semantics="payment_actual",
        grain="payment",
        direct=["https://assets.gov.ie/static/documents/b526ff76/OPW_Payments_of_20000_or_over_in_Q1_2026.xlsx"],
    ),
    cfg(
        "dept_climate",
        "Dept of Climate, Energy and the Environment",
        "department",
        "central_government",
        listing="https://www.gov.ie/en/department-of-climate-energy-and-the-environment/collections/payments-over-20000/",
        semantics="payment_actual",
        grain="payment",
        direct=["https://assets.gov.ie/static/documents/ae8b1a0a/DPER_Payments_over_20K_Q1_2026_Report.xlsx"],
    ),
    cfg(
        "dept_defence",
        "Department of Defence",
        "department",
        "central_government",
        listing="https://www.gov.ie/en/department-of-defence/collections/purchase-orders-over-20000/",
        semantics="po_committed",
        grain="purchase_order",
        # 5-column NUMBER/CATEGORY/SUPPLIER/CURRENCY/AMOUNT layout (two header orderings) that the
        # generic word-geometry reader scrambles — category→po_number, supplier suffix split — leaving
        # 1,191 empty-supplier rows whose real name sits in po_number (DQ audit 2026-06; validated in
        # pipeline_sandbox/courts_reader/). The bespoke reading-order reader recovers supplier+ref+amount.
        reader="reading_order_defence",
    ),
    cfg(
        "dept_culture",
        "Department of Culture, Communications and Sport",
        "department",
        "central_government",
        listing="https://www.gov.ie/en/department-of-culture-communications-and-sport/collections/purchase-orders/",
        semantics="po_committed",
        grain="purchase_order",
        caveat="contains very large NBI infrastructure POs; check outlier share before any total",
        # No PO column: supplier / 'amount description' reading-order; 3 sub-layouts. The generic
        # reader lost whole quarters (coverage_qa: ~86% with files at 0).
        reader="reading_order_culture",
    ),
    # ---- Cheap wins 2026-06-08: gov.ie / enterprise.gov.ie departments already published, files
    # cached in c:/tmp but never wired. Both parse clean with the generic reader (offline-validated).
    cfg(
        "dept_dper",
        "Dept of Public Expenditure, Infrastructure, PSR and Digitalisation",
        "department",
        "central_government",
        listing="https://www.gov.ie/en/department-of-public-expenditure-infrastructure-public-service-reform-and-digitalisation/collections/dpendr-ogcio-and-ogp-purchase-order-payments-2024/",
        semantics="po_committed",
        grain="purchase_order",
        caveat="DPENDR+OGCIO+OGP PO-over-20000 listing; note the SEPARATE pre-existing bug that the "
        "Dept Climate gov.ie collection mis-serves a 'DPER_Payments_over_20K' file (filename/dept "
        "mismatch) — that is payment-grain and ingested under dept_climate, distinct from these POs",
        # OGCIO (dated) + DPENDR (undated) reading-order layouts, 13-digit PO; generic reader got
        # OGCIO at 58.8% and ingested grand-total lines as phantom payments (coverage_qa).
        reader="reading_order_dper",
    ),
    cfg(
        "dept_enterprise",
        "Department of Enterprise, Tourism and Employment",
        "department",
        "central_government",
        listing="https://enterprise.gov.ie/en/publications/payments-over-20k.html",
        semantics="payment_actual",
        grain="payment",
        include=r"\.xlsx(\?|$)|\.csv(\?|$)",
        caveat="DETE 'Payments over €20,000'; XLSX only (2024-2026 + 2017). Amount col is 'Total' (the "
        "'Payment Number' ref matches the amount regex via 'payment' but is excluded by NON_AMOUNT_HDR). "
        "The older quarterly PDFs (2016-2025) are DEFERRED: merged 'Supplier Name Total (€)' header + "
        "inconsistent line-wrapping corrupt the supplier column under the generic word-geometry reader "
        "(amount parses fine, supplier becomes 'DELL (IRELAND) 84,255') — would need a bespoke reading-order parser",
    ),
    cfg(
        "ie_teagasc",
        "Teagasc",
        "semi_state",
        "agri_food_marine",
        listing="https://www.teagasc.ie/about/corporate-responsibility/information-for-suppliers/",
        semantics="po_committed",
        grain="purchase_order",
    ),
    cfg(
        "ie_bordbia",
        "Bord Bia",
        "semi_state",
        "agri_food_marine",
        listing="https://www.bordbia.ie/about/governance/corporate-governance/purchase-orders/",
        semantics="po_committed",
        grain="purchase_order",
    ),
    cfg(
        "ie_bim",
        "Bord Iascaigh Mhara (BIM)",
        "semi_state",
        "agri_food_marine",
        listing="https://bim.ie/about/corporate-governance/purchase-orders-over-20k/",
        semantics="po_committed",
        grain="purchase_order",
        caveat="amounts excluding VAT",
    ),
    cfg(
        "ie_cib",
        "Citizens Information Board",
        "agency",
        "social",
        listing="https://www.citizensinformationboard.ie/en/freedom_of_information/financial_information/payments_or_purchase_orders_for_goods_and_services.html",
        semantics="payment_actual",
        grain="payment",
    ),
    cfg(
        "ie_hea",
        "Higher Education Authority",
        "agency",
        "education",
        listing="https://hea.ie/about-us/public-sector-information/",
        semantics="payment_actual",
        grain="payment",
        privacy="low",
    ),
    # ---- Tier F: government departments (gov.ie collections) — discovery sweep 2026-06-13.
    # All seven publish quarterly PO/payment-over-€20k lists as digital PDFs linked DIRECTLY on
    # the collection page (WebFetch-confirmed) — the proven Defence/Culture pattern the generic
    # header-anchored PDF reader already handles. grain per the page title (Purchase Orders ->
    # po_committed, Payments -> payment_actual). --list-verify before a full --merge run.
    cfg(
        "dept_agriculture",
        "Department of Agriculture, Food and the Marine",
        "department",
        "central_government",
        listing="https://www.gov.ie/en/collection/903f95-purchase-orders/",
        semantics="po_committed",
        grain="purchase_order",
        tier="F",
    ),
    cfg(
        "dept_social_protection",
        "Department of Social Protection",
        "department",
        "central_government",
        listing="https://www.gov.ie/en/department-of-social-protection/collections/purchase-orders-for-20000-or-above/",
        semantics="po_committed",
        grain="purchase_order",
        tier="F",
        caveat="PO-over-20000 quarterly, 2012-present",
    ),
    # dept_foreign_affairs (DFAT) DE-SCOPED 2026-06-13 — every "Payments over €20,000" PDF is a
    # single-column READING-ORDER layout ("<GL category> <SUPPLIER> <amount>" on one line), not a
    # column-geometry table, so the generic word-row reader cannot split supplier from category
    # (supplier comes out as "POSTAGE & OTHER COURIER COSTS AN POST"). Amounts ARE recoverable.
    # This is the NTA/SEAI/NPHDB bespoke family — needs a reading-order parser anchored on the
    # trailing amount. Listing (files-directly-linked, 2012-present):
    # https://www.gov.ie/en/department-of-foreign-affairs/organisation-information/payments-over-20000/
    # dept_justice DE-SCOPED 2026-06-13 — the "Purchase Orders Issued over €20,000" PDF is also a
    # single-column reading-order layout ("<PO#> <SUPPLIER> €<amount> <desc> <Y/N>"); the generic
    # reader scores a NOTES paragraph as the header and reads the 6-digit PO number as the amount
    # (€30bn+ garbage). The companion xlsx buries its real header under a 7-row notes preamble that
    # _tabular_from_raw's 8-row window misses. Both need bespoke handling (line-regex on the pdf, or
    # a deeper xlsx header search). Listing (annual, may lag a year):
    # https://www.gov.ie/en/department-of-justice-home-affairs-and-migration/collections/department-of-justice-purchase-orders-issued-over-20000-in-value/
    cfg(
        "dept_health",
        "Department of Health",
        "department",
        "central_government",
        listing="https://www.gov.ie/en/department-of-health/collections/department-of-health-payments-over-20000/",
        semantics="payment_actual",
        grain="payment",
        tier="F",
    ),
    cfg(
        "dept_education",
        "Department of Education and Youth",
        "department",
        "central_government",
        listing="https://www.gov.ie/en/department-of-education/collections/department-of-education-payments-greater-than-20000/",
        semantics="payment_actual",
        grain="payment",
        tier="F",
        caveat="Payments-of-20000-or-over quarterly PDFs, 2013-present (one 2023 quarter also offers xlsx)",
    ),
    # dept_transport DE-SCOPED 2026-06-13 — its PO-over-20000 PDFs (esp. the 2025
    # "Q#_Purchase_Order_20k_or_over.pdf" series) are reading-order, not column-geometry: 73% of
    # rows came out with a null supplier and the total inflated to €15.7bn (PO/value-column bleed).
    # Same bespoke reading-order family as DFAT/Justice. Listing (files directly linked, 2018-):
    # https://www.gov.ie/en/department-of-transport/organisation-information/departmental-purchase-orders-greater-than-20000/
    # ---- Tier B: OWNED BY A SEPARATE CONTEXT (procurement_hse_tusla_parser.py) -----
    # HSE + Tusla need bespoke per-publisher column-x specs (the generic header-anchored
    # reader misparses them: HSE fuses amount+quarter+date, Tusla's vendor bleeds into the
    # amount column). NTPF + SVUH (health, privacy=high) de-scoped here too pending that
    # context's reconciliation. Their output merges into THIS schema later — do not re-add
    # HSE/Tusla here or the generic reader will produce duplicate low-quality rows.
    # ---- Tier C: needed a corrected listing URL or a parser fix --------------------
    cfg(
        "ie_tii",
        "Transport Infrastructure Ireland",
        "agency",
        "transport",
        listing="https://www.tii.ie/en/compliance/payments/",
        semantics="payment_actual",
        grain="payment",
        tier="C",
        direct=["https://websitecms.tii.ie/media/sw3dzt2l/tii-payments-q1-2025-over-20k.csv"],
        caveat="CSV carries a category-total row (~€1.2bn) that must be excluded from any sum",
    ),
    cfg(
        "ie_revenue",
        "Revenue Commissioners",
        "agency",
        "regulator",
        listing="https://www.revenue.ie/en/corporate/statutory-obligations/freedom-of-information/section8/procurement.aspx",
        semantics="payment_actual",
        grain="payment",
        tier="C",
        direct=["https://www.revenue.ie/en/corporate/documents/procurement/payments-over-20000-quarter4-2025.pdf"],
        # ref/supplier/description each on their own line then the amount line — the generic
        # word-geometry reader mis-columned these (quarter4-2025 at 0% yield, coverage_qa).
        reader="reading_order_revenue",
    ),
    cfg(
        "ie_atu",
        "Atlantic Technological University",
        "education_body",
        "education",
        listing="https://www.atu.ie/freedom-of-information/freedom-of-information-financial-information",
        semantics="payment_actual",
        grain="payment",
        privacy="medium",
        tier="C",
        direct=["https://www.atu.ie/app/uploads/2026/03/atu-payments-purchase-orders-q1-2025.pdf"],
        caveat="supplier published with a leading numeric supplier-ID; stripped on read",
    ),
    # ie_nta DE-SCOPED to extractors/procurement_nta_parser.py — every NTA PO PDF is
    # 90deg-rotated (and the layout/date format varies by year), so the generic word-geometry
    # reader clusters a whole €-column into one row and yields 0. The bespoke reading-order
    # parser owns it (9 quarters, ~2.3k rows) and emits THIS schema. Do not re-add here.
    cfg(
        "ie_marine",
        "Marine Institute",
        "agency",
        "agri_food_marine",
        listing="https://www.marine.ie/site-area/about-us/purchase-orders",
        semantics="po_committed",
        grain="purchase_order",
        tier="C",
        direct=["https://marine.ie/sites/default/files/MIFiles/Docs/CS/Purchase%20Orders%20Qtr%201%202026.pdf"],
    ),
    cfg(
        "ie_esbnetworks",
        "ESB Networks DAC",
        "semi_state",
        "energy_utilities",
        listing="https://www.esbnetworks.ie/about-us/company/publication-scheme/financial-information",
        semantics="payment_actual",
        grain="payment",
        tier="C",
        caveat="prior sample was a category-total page; harvesting supplier-level file",
    ),
    cfg(
        "ie_tailte",
        "Tailte Éireann",
        "state_body",
        "property_land",
        listing="https://tailte.ie/category/publications/",
        semantics="po_committed",
        grain="purchase_order",
        tier="C",
        include=r"purchase|payment|po[s]?[-_ ]?over|20[,]?000|over.?20k",
        caveat="Purchase-Orders quarterly files (PO grain); contracts-awarded list excluded",
        # 13-digit 'PO supplier' merged line + leading € amount — Courts-style but wider; the
        # generic reader recovered ~6 of 163 rows (coverage_qa).
        reader="reading_order_tailte",
    ),
    cfg(
        "dept_housing",
        "Department of Housing, Local Government and Heritage",
        "department",
        "central_government",
        listing="https://www.gov.ie/en/department-of-housing-local-government-and-heritage/collections/procurement-related-payments-over-20000-euro/",
        semantics="payment_actual",
        grain="payment",
        tier="C",
        caveat="prior sample was a privacy statement; gov.ie payments collection. OWNED BY THE "
        "disclosed_bq lane (disclosed_bq_po_newbodies_fact, full 2014-2025 clean) — NOT wired here.",
        # reader=None (NOT reading_order_housing): read_housing parses 2014-2022 cleanly but its
        # 2024 split-COLUMN layout (amounts on their own pages) MIS-ALIGNS the orphan-amount zip →
        # phantom rows (e.g. "MARK O'CONNOR €20,042,845" vs disclosed_bq max €703k). Row-yield was
        # 99.2% but the amount↔supplier PAIRING is wrong; leave unwired until the zip is fixed.
    ),
    cfg(
        "ie_cdetb",
        "City of Dublin ETB",
        "education_body",
        "education",
        listing="https://www.cityofdublinetb.ie/about-us/finance-and-procurement/procurement/",
        semantics="po_committed",
        grain="purchase_order",
        privacy="medium",
        tier="C",
        include=r"purchase|payment|po[s]?[-_ ]?over|20[,]?000|quarter|q[1-4]",
        caveat="prior sample was the procurement policy; excluding policy docs",
    ),
    # ie_lmetb (Louth & Meath ETB) NOT registered here: its payments are OWNED BY the disclosed_bq
    # lane (disclosed_bq_po_newbodies_fact: 2,251 rows / full 2016-2025 vs the gov.ie PDFs' 2024-2026
    # only). The reading-order fallback parses lmetb's 7 clean quarters fine (validated), but
    # registering it would DOUBLE-COUNT against disclosed_bq for less coverage — left to disclosed_bq.
    cfg(
        "ie_enterprise_ireland",
        "Enterprise Ireland",
        "semi_state",
        "enterprise_tourism",
        listing="https://www.enterprise-ireland.com/en/legal/policies-guidelines/procurement-policy",
        semantics="po_committed",
        grain="purchase_order",
        tier="C",
        include=r"purchase|payment|po[s]?[-_ ]?over|20[,]?000|over.?20k",
        caveat="agency (not DETE dept) procurement-policy page; quarterly XLSX 'Payments over €20,000' 2012-present",
    ),
    # ---- Tier D: discovery sweep 2026-06-04 (doc/PROCUREMENT_MASTER.md) --
    # Probe-confirmed, generic-reader-clean. Held back for bespoke/render passes (NOT here):
    #   Beaumont + Pobal (dual/MIXED PO+payment grain — need value_kind split),
    #   Coimisiún na Meán + Irish Prison Service (scanned PDFs — need OCR),
    #   Garda (sampler hit a fleet report — needs the right PO subpage),
    #   UCD / SETU / CHI / SEAI / EPA (no links via landing — JS/403, EPA serves .php HTML).
    cfg(
        "ie_ntma",
        "National Treasury Management Agency (NTMA)",
        "state_body",
        "finance",
        listing="https://www.ntma.ie/information-pages/freedom-of-information/freedom-of-information-publication-scheme/financial-information",
        semantics="payment_actual",
        grain="payment",
        tier="D",
        exclude=r"revised-foi-publication|[-_ ]publication\.pdf",
        caveat="one quarterly scheme covers 6 business units incl NDFA (ADM/Nat-Debt/ISIF/NDFA/FIF/ICNF); "
        "do NOT also wire ie_ndfa or its rows double-count. The 6-row 'Revised-FOI-Publication' / "
        "'*-Publication.pdf' files are per-unit SUMMARIES (different grain) that overlap the "
        "line-level Q*-Payments files in 2018-19 — excluded to avoid double-counting. "
        "NOTE: the per-unit Q1-2020..Q2-2024 PDFs currently parse to 0 rows (layout/scan break) — known gap.",
    ),
    cfg(
        "ie_courts",
        "Courts Service of Ireland",
        "agency",
        "justice",
        listing="https://www.courts.ie/publications/purchase-orders-greater-than-20k",
        semantics="po_committed",
        grain="purchase_order",
        tier="D",
        include=r"purchase-order|over-20|po[s]?[-_ ]?over",
        # PDF "PO analysis report" layout merges PO+supplier and defeats word-geometry bucketing;
        # use the bespoke reading-order reader (DQ audit P1). XLSX quarters keep the tabular reader.
        reader="reading_order_courts",
    ),
    cfg(
        "ie_sportireland",
        "Sport Ireland",
        "agency",
        "sport",
        listing="https://www.sportireland.ie/about-us/freedom-of-information/financial-information",
        semantics="po_committed",
        grain="purchase_order",
        tier="D",
        caveat="single rolling PO log (not per-quarter); period likely null",
    ),
    cfg(
        "ie_tudublin",
        "Technological University Dublin",
        "education_body",
        "education",
        listing="https://www.tudublin.ie/explore/governance-and-compliance/foi/foi-publication-scheme/",
        semantics="po_committed",
        grain="purchase_order",
        tier="D",
        include=r"po-report|purchase-order|over-?20k",
    ),
    cfg(
        "ie_tus",
        "Technological University of the Shannon (TUS)",
        "education_body",
        "education",
        listing="https://tus.ie/privacy/freedom-of-information/publications/financial-reports/",
        semantics="po_committed",
        grain="purchase_order",
        privacy="medium",
        tier="C",
        # One rolling xlsx holding all quarters 2021Q4→2026Q1 (pinned: the listing is a JS widget).
        direct=[
            "https://tus.ie/app/uploads/ProfessionalServices/FOI/TUS_POs_over_20k_2021QTR4_2022_2023_2024_2025_Q1.2026.xlsx"
        ],
    ),
    cfg(
        "ie_mtu",
        "Munster Technological University (MTU)",
        "education_body",
        "education",
        listing="https://www.mtu.ie/about-mtu/legal/freedom-of-information/",
        semantics="po_committed",
        grain="purchase_order",
        tier="D",
        include=r"pos?-over-?20k|purchase-order|po[s]?[-_ ]?over",
        # Landing only exposes the tender-register xlsx + FOI logs; the actual PO PDFs live under
        # /media/.../foi/financial-information/ and aren't reachable by the one-hop crawl, so the
        # quarterly files are pinned directly. All 3 byte-verified 2026-06-04 (%PDF, 88-132KB);
        # Q4-2025 parses to 123 rows high-conf. Add more quarters as their URLs are confirmed.
        direct=[
            "https://www.mtu.ie/media/mtu-website/files/foi/financial-information/MTU-POs-over-20k-Q4-2025.pdf",
            "https://www.mtu.ie/media/mtu-website/files/foi/financial-information/MTU-POs-over-20k-Q3-2025.pdf",
            "https://www.mtu.ie/media/mtu-website/files/foi/financial-information/MTU-POs-over-20k-Q2-2025.pdf",
        ],
        caveat="PO PDFs pinned via direct_files (landing exposes only tender-register xlsx + FOI logs)",
    ),
    cfg(
        "ie_chi",
        "Children's Health Ireland (CHI)",
        "state_body",
        "health",
        listing="https://www.childrenshealthireland.ie/about-us/corporate-information/payments-to-suppliers-over-20000/",
        semantics="payment_actual",
        grain="payment",
        privacy="low",
        tier="D",
        # Children's-hospital OPERATOR side (complements NPHDB construction). Landing exposes no
        # direct links → file pinned. xlsx row 0 is a TITLE ("CHI Vendor payments >25K") above the
        # real "Vendor Name/Amount" header; the length-filtered header scorer now skips it (297 rows).
        direct=[
            "https://www.childrenshealthireland.ie/documents/3541/CHI_Paid_Invoices_over_25K_incl_VAT_Qtr_1_2026updated.xlsx"
        ],
        caveat="paid invoices at €25k incl VAT (not €20k); single Q1-2026 file; payment grain",
    ),
    cfg(
        "ie_pobal",
        "Pobal",
        "agency",
        "social",
        listing="https://www.pobal.ie/financial-information/",
        semantics="po_committed",
        grain="purchase_order",
        privacy="medium",
        tier="D",
        # Files titled 'Purchase Order OR Payments over €20k' but rows carry PO/SUPPLIER/TOTAL/PAID
        # columns = POs with a paid-flag (Paid/Not Paid captured in paid_flag), not truly mixed.
        # Generic reader handles it (29 rows/high-conf on Q1-2026). Full 2020-2026 series (25 PDFs).
        caveat="grant-adjacent (privacy=medium); harvest returns oldest-first so a low --max-files "
        "biases to 2020 — raise --max-files for full series",
    ),
    cfg(
        "ie_beaumont",
        "Beaumont Hospital",
        "hospital",
        "health",
        listing="https://www.beaumont.ie/page/financial-statements",
        semantics="payment_actual",
        grain="payment",
        privacy="low",
        tier="D",
        # Landing exposes 3 xlsx: two 'Payments Over €20k' (annual, payment grain, 2024+2025) and
        # one 'POs Greater than €20k' (PO grain). include= grabs ONLY the payment files to keep one
        # grain. Header 'No. of Payments > €20,000' is a COUNT trap; NON_AMOUNT_HDR routes amount to 'Value'.
        include=r"payments.*over",
        caveat="annual supplier payment totals (Value col), €20k threshold; the separate Q1-2026 PO file is excluded",
    ),
    # ---- Tier E: regulators / cultural bodies (discovery sweep 2 — commercial-vs-noncommercial) --
    # Commercial semi-states (ESB/Electric Ireland, daa, An Post, ports, CIÉ group) are FOI-exempt
    # and publish annual reports only — NOT here. EirGrid/GNI/Uisce publish CATEGORY-only rollups
    # (no supplier names) despite "PO over €20k" page titles — NOT here. RTÉ's published file is a
    # category summary (Capital/Communication circuits + counts), NOT supplier-level — NOT here.
    # ABP is supplier-level but its multi-line bilingual (Irish) header bleeds date into supplier —
    # deferred (needs header-wrap handling). These three parse clean with the generic reader:
    cfg(
        "ie_hpra",
        "Health Products Regulatory Authority (HPRA)",
        "agency",
        "regulator",
        listing="https://www.hpra.ie/transparency/financial-information/purchase-orders",
        semantics="po_committed",
        grain="purchase_order",
        tier="E",
        direct=[
            "https://assets.hpra.ie/data/docs/default-source/corporate/purchase-orders/purchase-orders---q3-2025.pdf"
        ],
        caveat="clean assets.hpra.ie CDN, predictable quarterly PDF filenames",
    ),
    cfg(
        "ie_ccpc",
        "Competition and Consumer Protection Commission (CCPC)",
        "agency",
        "regulator",
        listing="https://www.ccpc.ie/about-us/corporate-information/governance/payment-reports",
        semantics="payment_actual",
        grain="payment",
        tier="E",
        direct=[
            "https://assets.ccpc.ie/data/docs/default-source/about-us/corporate-information/governance/payment-reports/payments-over-20k-in-q1-2026.pdf"
        ],
        caveat="quarterly payments >€20k; description column repeats the € amount as a prefix (cosmetic)",
    ),
    cfg(
        "ie_nli",
        "National Library of Ireland",
        "agency",
        "media_culture",
        listing="https://www.nli.ie/corporate-information",
        semantics="payment_actual",
        grain="payment",
        tier="E",
        direct=["https://www.nli.ie/sites/default/files/2025-05/payments-over-eu20000-q1-2025.pdf"],
        caveat="Drupal /sites/default/files PDFs; some files bundle Q1-Q4 annually",
    ),
    # ---- Batch A 2026-06-19: clean candidates from the seed (procurement_publishers_seed.py) ----
    # gov.ie department collections (these crawl reliably, like the depts already above).
    cfg(
        "dept_finance",
        "Department of Finance",
        "department",
        "central_government",
        listing="https://www.gov.ie/en/department-of-finance/collections/purchase-orders/",
        semantics="po_committed",
        grain="purchase_order",
        tier="A",
    ),
    cfg(
        "dept_children",
        "Department of Children, Disability and Equality",
        "department",
        "central_government",
        listing="https://www.gov.ie/en/department-of-children-disability-and-equality/collections/department-of-children-equality-disability-integration-and-youth-purchase-orders-for-20000-or-above/",
        # the PDFs report "Total Paid" + "Payment Date" per row → payment_actual, not po_committed.
        semantics="payment_actual",
        grain="purchase_order",
        privacy="medium",
        tier="A",
        # READING-ORDER layout (ref+supplier / payment-date / €amount+desc across 2-3 lines, in
        # EITHER column order): the generic word-geometry reader yields 0. Dominated by the asylum/
        # IP + Ukraine accommodation-provider spend (Cape Wrath/Mosney/Guestford, €m each).
        reader="reading_order",
    ),
    # agencies / regulators publishing a dedicated PO-over-20k page.
    cfg(
        "ie_hsa",
        "Health and Safety Authority",
        "agency",
        "regulator",
        listing="https://www.hsa.ie/eng/about_us/public_sector_information/purchase_orders_in_excess_of_-20_000/",
        semantics="po_committed",
        grain="purchase_order",
        tier="C",
        include=r"purchase|po[s]?[-_ ]?over|20[,]?000|quarter|q[1-4]",
    ),
    cfg(
        "ie_cnam",
        "Coimisiún na Meán",
        "agency",
        "media_culture",
        listing="https://www.cnam.ie/about/reports-finances/procurement/",
        semantics="po_committed",
        grain="purchase_order",
        tier="C",
        include=r"po[-_ ]?report|purchase|20[,]?000|quarter|q[1-4]",
    ),
    cfg(
        "ie_prisons",
        "Irish Prison Service",
        "agency",
        "justice",
        listing="https://www.irishprisons.ie/information-centre/procurement/",
        semantics="po_committed",
        grain="purchase_order",
        privacy="medium",
        tier="C",
        include=r"po[s]?[-_ ]?(greater|over)|purchase|20k|e20k|20[,]?000",
        # Listing harvested 0 (files not linked from the procurement page); pin the known annual PDFs.
        direct=[
            "https://www.irishprisons.ie/wp-content/uploads/documents_pdf/POs-greater-than-E20k-2024.pdf",
            "https://www.irishprisons.ie/wp-content/uploads/documents_pdf/POs-greater-than-E20k-2023.pdf",
            "https://www.irishprisons.ie/wp-content/uploads/documents_pdf/POs-greater-than-E20k-2022.pdf",
        ],
        caveat="Annual (not quarterly) PO list, incl-VAT; security redactions possible.",
    ),
    cfg(
        "ie_screen",
        "Screen Ireland",
        "agency",
        "media_culture",
        listing="https://www.screenireland.ie/about/policies/purchase-orders-for-20000-or-above/2025",
        semantics="po_committed",
        grain="purchase_order",
        privacy="medium",
        tier="D",
        include=r"purchase|po[s]?[-_ ]?over|20[,]?000|quarter|q[1-4]",
        caveat="Film-funding body; payees may be individuals/production cos — privacy gate applies.",
    ),
    # ---- Batch B 2026-06-20: PROBE-FIRST (test parse quality before promoting to gold) ----
    cfg(
        "ie_loetb",
        "Laois & Offaly ETB",
        "education_body",
        "education",
        listing="https://loetb.ie/organisation-support-development/finance/purchase-orders-over-20000/",
        semantics="po_committed",
        grain="purchase_order",
        privacy="medium",
        tier="D",
        include=r"purchase|po[s]?[-_ ]?over|20[,]?000|quarter|q[1-4]",
    ),
    # ie_lmetb: DEFERRED 2026-06-20 — parse-quality probe FAILED. The 9 "payments-over-E20k" PDFs
    # parse (63-90 rows each) but the generic reader captures NO supplier (all 662 rows supplier=null,
    # min amount €2 = column misalignment). Its PDF layout needs a bespoke column-x spec (HSE/Tusla
    # pattern) before it can be ingested — shipping it would inject 662 nameless rows. Listing:
    # https://www.lmetb.ie/category/finance/purchase-orders-over-e20000/ (payment_actual grain).
    # ie_rsa: list-probe harvested 0 files (the /about/reporting page links no PO/payment files) —
    # deferred to the Playwright/direct-URL tail rather than shipped empty.
]


