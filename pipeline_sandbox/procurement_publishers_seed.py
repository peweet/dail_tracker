"""PHASE-1 SEED REGISTRY (PRE-ETL): public-body procurement / payment publishers.

Companion to the local-authority registry (procurement_la_seed.py /
procurement_la_registry.py). Where that one covers the 31 councils, this one seeds the
*other* public-money publishers named in doc/PROCUREMENT_SEMISTATE_EXPANSION_PLAN.md:
semi-state bodies, OPW/property, central departments, agencies/regulators, health, and
education — plus FOI/AIE-only leads kept separate.

This is JUST the hand-curated input list (the bit that can't be auto-discovered). It is
deliberately conservative: every row starts as NEEDS_MANUAL_CHECK / FOI_CLUE_ONLY /
NOT_FOUND and UNKNOWN format+grain. probe_procurement_publishers.py fetches each
landing_url and REFINES these from real HTTP evidence. Do NOT hand-upgrade a row to
CONFIRMED_SUPPLIER_LEVEL here — that claim must come from inspecting the actual file.

Nothing here is wired into pipeline.py. Run prints a summary and writes the committed
source-of-truth registry to data/_meta/procurement_publishers/publishers_seed.csv
(tracked via a .gitignore negation rule — the blanket *.csv ignore would swallow it).

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/procurement_publishers_seed.py
"""

from __future__ import annotations

import contextlib
import csv
import sys
from collections import Counter
from pathlib import Path

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parents[1]
# Committed source-of-truth location (plan §6 Phase 1). Tracked via a .gitignore negation
# rule — the blanket *.csv ignore would otherwise swallow it.
OUT = ROOT / "data/_meta/procurement_publishers/publishers_seed.csv"

# --- allowed enums (plan §4) ----------------------------------------------------------
SOURCE_STATUS = {
    "CONFIRMED_SUPPLIER_LEVEL", "CONFIRMED_CATEGORY_LEVEL", "CONFIRMED_PUBLIC_CONTRACTS_ONLY",
    "ANNUAL_REPORT_ONLY", "FOI_AIE_CONFIRMED_EXISTS", "FOI_CLUE_ONLY",
    "NEEDS_MANUAL_CHECK", "NOT_FOUND", "NOT_APPLICABLE",
}
SOURCE_FORMAT = {
    "PDF_DIGITAL", "PDF_SCANNED", "XLSX", "CSV", "HTML_TABLE",
    "LOOKER_OR_DASHBOARD_ONLY", "ANNUAL_REPORT_PDF", "MIXED", "UNKNOWN",
}
GRAIN = {
    "payment", "purchase_order", "contract_award", "framework_ceiling",
    "category_total", "annual_summary", "mixed", "unknown",
}

FIELDS = [
    "publisher_id", "publisher_name", "publisher_type", "sector", "landing_url",
    "source_status", "source_format", "grain", "years_available", "latest_period",
    "supplier_level_available", "amount_available", "paid_flag_available",
    "source_caveat", "privacy_risk", "notes",
]


def _row(pid, name, ptype, sector, url, *, status="NEEDS_MANUAL_CHECK",
         privacy_risk="low", notes="", caveat="") -> dict:
    """One seed row. Unknown-until-probed fields default to UNKNOWN/unknown/blank so the
    probe is the only thing that can assert format/grain/supplier-level/amount/paid."""
    return {
        "publisher_id": pid, "publisher_name": name, "publisher_type": ptype,
        "sector": sector, "landing_url": url,
        "source_status": status, "source_format": "UNKNOWN", "grain": "unknown",
        "years_available": "", "latest_period": "",
        "supplier_level_available": "", "amount_available": "", "paid_flag_available": "",
        "source_caveat": caveat, "privacy_risk": privacy_risk, "notes": notes,
    }


# Bodies named in the plan with no located landing page yet -> NOT_FOUND (URL unknown,
# not "doesn't publish"). FOI/AIE-only leads -> FOI_CLUE_ONLY.
NF = "NOT_FOUND"
FOI = "FOI_CLUE_ONLY"

SEEDS: list[dict] = [
    # ============================ 5.1 SEMI-STATE / COMMERCIAL STATE BODIES =============
    # transport
    _row("ie_cie", "Córas Iompair Éireann (CIÉ)", "semi_state", "transport", "",
         status=NF, notes="parent group; landing page for PO/payments not yet located"),
    _row("ie_irishrail", "Iarnród Éireann", "semi_state", "transport",
         "https://www.irishrail.ie/_kontent/d2e85689-fbf9-010a-a02e-587ed94b5032/480d2a0d-cd41-430d-956f-a64b2f5d6e8a/025b45de-29cc-4f2c-b5b4-184ba73ef865.pdf",
         status="FOI_AIE_CONFIRMED_EXISTS", notes="FOI publication-scheme PDF clue; not a clean file list"),
    _row("ie_buseireann", "Bus Éireann", "semi_state", "transport", "", status=NF),
    _row("ie_dublinbus", "Dublin Bus", "semi_state", "transport", "", status=NF),
    _row("ie_daa", "daa (Dublin/Cork Airports)", "semi_state", "transport", "", status=NF),
    _row("ie_dublinport", "Dublin Port Company", "semi_state", "transport", "", status=NF),
    _row("ie_shannonfoynes", "Shannon Foynes Port Company", "semi_state", "transport", "", status=NF),
    _row("ie_portofcork", "Port of Cork Company", "semi_state", "transport", "", status=NF),
    # energy / utilities
    _row("ie_esb", "ESB (Electricity Supply Board)", "semi_state", "energy_utilities", "", status=NF,
         notes="commercial state body; PO disclosure obligation unclear vs regulated ESBN"),
    _row("ie_esbnetworks", "ESB Networks DAC", "semi_state", "energy_utilities",
         "https://www.esbnetworks.ie/about-us/company/publication-scheme/financial-information",
         notes="regulated; publication scheme financial-information page"),
    _row("ie_eirgrid", "EirGrid", "semi_state", "energy_utilities", "", status=NF),
    _row("ie_gni", "Gas Networks Ireland", "semi_state", "energy_utilities", "", status=NF),
    _row("ie_uisce", "Uisce Éireann (Irish Water)", "semi_state", "energy_utilities", "", status=NF),
    _row("ie_bnm", "Bord na Móna", "semi_state", "forestry_land", "https://ocei.ie/",
         status=FOI, notes="AIE/OCEI route only per plan; no clean PO file list located"),
    _row("ie_coillte", "Coillte", "semi_state", "forestry_land", "https://ocei.ie/",
         status=FOI, notes="AIE/OCEI route only per plan; no clean PO file list located"),
    # agri / food / marine
    _row("ie_teagasc", "Teagasc", "semi_state", "agri_food_marine",
         "https://teagasc.ie/about/corporate-responsibility/information-for-suppliers/",
         notes="information-for-suppliers landing; plan flags as easy first candidate"),
    _row("ie_bordbia", "Bord Bia", "semi_state", "agri_food_marine",
         "https://www.bordbia.ie/about/governance/corporate-governance/purchase-orders/",
         notes="dedicated purchase-orders page; plan flags as easy first candidate"),
    _row("ie_bim", "Bord Iascaigh Mhara (BIM)", "semi_state", "agri_food_marine",
         "https://bim.ie/about/corporate-governance/purchase-orders-over-20k/",
         notes="PO-over-20k page"),
    # media / culture
    _row("ie_rte", "RTÉ", "semi_state", "media_culture", "https://about.rte.ie/procurement-2/",
         notes="procurement landing"),
    _row("ie_tg4", "TG4", "semi_state", "media_culture", "", status=NF),
    # enterprise / tourism
    _row("ie_enterprise_ireland", "Enterprise Ireland", "semi_state", "enterprise_tourism",
         "https://www.enterprise-ireland.com/", status=FOI,
         notes="FOI disclosure logs per plan; no clean PO file list located yet"),
    _row("ie_ida", "IDA Ireland", "semi_state", "enterprise_tourism", "", status=NF,
         notes="plan flags as easy first candidate but landing URL not supplied"),
    _row("ie_failte", "Fáilte Ireland", "semi_state", "enterprise_tourism",
         "https://www.failteireland.ie/", status=FOI, notes="FOI disclosure logs per plan"),
    _row("ie_tourism_ireland", "Tourism Ireland", "semi_state", "enterprise_tourism", "", status=NF),

    # ============================ 5.2 OPW / PROPERTY / LAND ============================
    _row("ie_opw", "Office of Public Works (OPW)", "state_body", "property_land",
         "https://www.gov.ie/en/office-of-public-works/collections/payments-greater-than-20000/",
         notes="clean gov.ie collection, likely quarterly files; plan flags as best first candidate"),
    _row("ie_tailte", "Tailte Éireann", "state_body", "property_land",
         "https://tailte.ie/category/publications/",
         notes="publications archive: PO>20k / prompt-pay / contracts>25k / annual reports / FOI logs mixed"),

    # ============================ 5.3 CENTRAL GOVERNMENT DEPARTMENTS ===================
    _row("dept_finance", "Department of Finance", "department", "central_government",
         "https://www.gov.ie/en/department-of-finance/collections/purchase-orders/"),
    _row("dept_climate", "Department of Climate, Energy and the Environment", "department",
         "central_government",
         "https://www.gov.ie/en/department-of-climate-energy-and-the-environment/collections/payments-over-20000/"),
    _row("dept_housing", "Department of Housing, Local Government and Heritage", "department",
         "central_government",
         "https://www.gov.ie/en/department-of-housing-local-government-and-heritage/organisation-information/procurement/"),
    _row("dept_children", "Department of Children, Disability and Equality", "department",
         "central_government",
         "https://www.gov.ie/en/department-of-children-disability-and-equality/collections/department-of-children-equality-disability-integration-and-youth-purchase-orders-for-20000-or-above/"),
    _row("dept_enterprise", "Department of Enterprise, Tourism and Employment", "department",
         "central_government", "https://enterprise.gov.ie/en/publications/payments-over-20k.html"),
    _row("dept_defence", "Department of Defence", "department", "central_government",
         "https://www.gov.ie/en/department-of-defence/collections/purchase-orders-over-20000/"),
    _row("dept_justice", "Department of Justice, Home Affairs and Migration", "department",
         "central_government",
         "https://www.gov.ie/en/department-of-justice-home-affairs-and-migration/collections/department-of-justice-purchase-orders-issued-over-20000-in-value/"),
    _row("dept_dper", "Dept of Public Expenditure / OGCIO / OGP", "department",
         "central_government",
         "https://www.gov.ie/en/department-of-public-expenditure-infrastructure-public-service-reform-and-digitalisation/collections/dpendr-ogcio-and-ogp-purchase-order-payments-2024/"),
    _row("dept_culture", "Department of Culture, Communications and Sport", "department",
         "central_government",
         "https://www.gov.ie/en/department-of-culture-communications-and-sport/collections/purchase-orders/"),
    _row("ie_ipas", "International Protection Accommodation Services (IPAS)", "state_body",
         "central_government",
         "https://www.gov.ie/en/international-protection-accommodation-services-ipas/publications/facts-and-figures/",
         privacy_risk="medium",
         notes="facts-and-figures landing; may be aggregate/category, not supplier-level"),

    # ============================ 5.4 AGENCIES / REGULATORS / STATUTORY BODIES =========
    _row("ie_nta", "National Transport Authority", "agency", "transport",
         "https://www.nationaltransport.ie/publications/2026-purchase-orders-e20000-and-over/",
         notes="year-specific PO page; plan flags as easy first candidate"),
    _row("ie_tii", "Transport Infrastructure Ireland", "agency", "transport",
         "https://websitecms.tii.ie/en/compliance/payments/", notes="compliance/payments page"),
    _row("ie_cib", "Citizens Information Board", "agency", "social",
         "https://www.citizensinformationboard.ie/en/freedom_of_information/financial_information/payments_or_purchase_orders_for_goods_and_services.html",
         notes="plan flags as easy first candidate"),
    _row("ie_hsa", "Health and Safety Authority", "agency", "regulator",
         "https://www.hsa.ie/eng/about_us/public_sector_information/purchase_orders_in_excess_of_-20_000/"),
    _row("ie_marine", "Marine Institute", "agency", "agri_food_marine",
         "https://marine.ie/site-area/about-us/purchase-orders"),
    _row("ie_revenue", "Revenue Commissioners", "agency", "regulator",
         "https://www.revenue.ie/en/corporate/statutory-obligations/freedom-of-information/section8/procurement.aspx"),
    _row("ie_tusla", "Tusla — Child and Family Agency", "agency", "social",
         "https://www.tusla.ie/about/your-personal-information/new-freedom-of-information/financial-information/",
         privacy_risk="high",
         notes="child/family agency; suppliers may include individual carers/practitioners"),
    _row("ie_rsa", "Road Safety Authority", "agency", "transport",
         "https://www.rsa.ie/about/reporting"),
    _row("ie_screen", "Screen Ireland", "agency", "media_culture",
         "https://www.screenireland.ie/about/policies/purchase-orders-for-20000-or-above/2025",
         privacy_risk="medium", notes="film funding; payees may be individuals/production cos"),
    _row("ie_arts_council", "Arts Council", "agency", "media_culture",
         "https://artscouncil.ie/", status=NF, privacy_risk="medium",
         notes="only homepage supplied; PO/payments page not yet located; grantees often individuals"),

    # ============================ 5.5 HEALTH BODIES AND HOSPITALS ======================
    _row("ie_hse", "Health Service Executive (HSE)", "state_body", "health",
         "https://healthservice.hse.ie/staff/information-healthcare-workers/procurement/",
         privacy_risk="high", notes="procurement landing; high public-interest, supplier-rich, but formats vary"),
    _row("ie_ntpf", "National Treatment Purchase Fund", "state_body", "health",
         "https://www.ntpf.ie/", status=NF, privacy_risk="high",
         notes="homepage only; pays private hospitals/practitioners — handle with care"),
    _row("ie_hiqa", "Health Information and Quality Authority (HIQA)", "agency", "health",
         "https://www.hiqa.ie/", status=NF, privacy_risk="medium",
         notes="homepage only; PO/payments page not yet located"),
    _row("ie_tuh", "Tallaght University Hospital", "hospital", "health",
         "https://www.tuh.ie/", status=NF, privacy_risk="high",
         notes="homepage only; voluntary hospital; may include individual practitioners"),
    _row("ie_svuh", "St Vincent's University Hospital", "hospital", "health",
         "https://www.stvincents.ie/about-us/financial-statements/", privacy_risk="high",
         notes="financial-statements page (likely ANNUAL_REPORT_ONLY, not supplier-level)"),

    # ============================ 5.6 EDUCATION / HIGHER ED / ETBs =====================
    _row("ie_hea", "Higher Education Authority", "agency", "education",
         "https://hea.ie/about-us/public-sector-information/"),
    _row("ie_atu", "Atlantic Technological University", "education_body", "education",
         "https://www.atu.ie/freedom-of-information/freedom-of-information-financial-information",
         privacy_risk="medium"),
    _row("ie_loetb", "Laois & Offaly ETB", "education_body", "education",
         "https://loetb.ie/organisation-support-development/finance/purchase-orders-over-20000/",
         privacy_risk="medium"),
    _row("ie_lmetb", "Louth & Meath ETB", "education_body", "education",
         "https://www.lmetb.ie/category/finance/purchase-orders-over-e20000/", privacy_risk="medium"),
    _row("ie_cdetb", "City of Dublin ETB", "education_body", "education",
         "https://www.cityofdublinetb.ie/about-us/finance-and-procurement/procurement/", privacy_risk="medium"),
    _row("ie_ncse", "National Council for Special Education", "agency", "education",
         "https://ncse.ie/", status=NF, privacy_risk="medium", notes="homepage only"),

    # ============================ 5.8 FOI / AIE / OCEI LEADS ===========================
    # Kept separate: data likely exists but not via a clean, repeatable, lawful file route.
    _row("foi_ocei", "Commissioner for Environmental Information (OCEI)", "foi_lead", "foi_aie",
         "https://ocei.ie/", status=FOI,
         notes="AIE decisions — clue source for Bord na Móna / Coillte etc., not a payment file"),
    _row("foi_thestory", "TheStory.ie / Right To Know", "foi_lead", "foi_aie",
         "https://www.thestory.ie/", status=FOI,
         notes="public-spend investigations; secondary clue source, do not ETL as primary"),
]


def validate() -> list[str]:
    """Cheap structural checks (mirrors the Phase-1 test goals in the plan)."""
    errs: list[str] = []
    ids = [s["publisher_id"] for s in SEEDS]
    dupes = [i for i, c in Counter(ids).items() if c > 1]
    if dupes:
        errs.append(f"duplicate publisher_id: {dupes}")
    for s in SEEDS:
        if s["source_status"] not in SOURCE_STATUS:
            errs.append(f"{s['publisher_id']}: bad source_status {s['source_status']!r}")
        if s["source_format"] not in SOURCE_FORMAT:
            errs.append(f"{s['publisher_id']}: bad source_format {s['source_format']!r}")
        if s["grain"] not in GRAIN:
            errs.append(f"{s['publisher_id']}: bad grain {s['grain']!r}")
        if s["source_status"] != "NOT_FOUND" and not s["landing_url"]:
            errs.append(f"{s['publisher_id']}: empty landing_url but status != NOT_FOUND")
    return errs


def main() -> None:
    errs = validate()
    print(f"{'=' * 74}\nPUBLIC-BODY PROCUREMENT PUBLISHER SEED — {len(SEEDS)} publishers\n{'=' * 74}")
    if errs:
        print("VALIDATION ERRORS:")
        for e in errs:
            print(f"  ! {e}")
    else:
        print("validation: OK (unique ids, enums valid, urls present where required)")

    by_sector = Counter(s["sector"] for s in SEEDS)
    by_status = Counter(s["source_status"] for s in SEEDS)
    by_type = Counter(s["publisher_type"] for s in SEEDS)
    print(f"\nby publisher_type : {dict(by_type)}")
    print(f"by sector         : {dict(by_sector)}")
    print(f"by source_status  : {dict(by_status)}")
    with_url = [s for s in SEEDS if s["landing_url"]]
    print(f"\nhave a landing_url to probe: {len(with_url)}/{len(SEEDS)}")
    print(f"NOT_FOUND (need URL hunt)  : {by_status['NOT_FOUND']}")
    print(f"FOI_CLUE_ONLY (no clean route): {by_status['FOI_CLUE_ONLY']}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(SEEDS)
    print(f"\nwrote {OUT}  (committed source-of-truth)")
    print("Next: probe_procurement_publishers.py refines status/format/grain.")


if __name__ == "__main__":
    main()
