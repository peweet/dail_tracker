"""IPAS corpus: document registry + canonical fact schema.

The point of this file is RESUMABILITY. Every source document in the asylum /
international-protection corpus is registered here with an extraction STATUS, so a
future session can see at a glance what is fully extracted, what is partial, and what
is deliberately context-only — and pick up exactly where this one stopped.

PRIORITY (user's rule 2026-07-14): DIRECT PDF REPORTS get full extraction. References,
SIs and third-party studies are CONTEXT — extract only a salient point that improves
the data, not the whole document.

CANONICAL FACT SCHEMA — every per-document extractor emits rows in this shape so they
can be unioned into one `ipas_facts` table (see ipas_facts_consolidate.py):

    fact_id          str   <doc_key>-<nnn>
    doc_key          str   registry key below
    doc_title        str
    page             int   PDF page (1-based)
    printed_page     str   page number as printed (may differ)
    ref              str   paragraph / figure / table / standard ref
    section          str
    category         str   see CATEGORIES
    subject          str   WHO/WHAT the fact is about (centre, county, provider, national)
    metric           str
    value_numeric    f64   null when unknown
    value_text       str   for judgments/verdicts (e.g. 'Not compliant')
    unit             str   eur | eur_per_person_night | persons | beds | centres | percent | ...
    qualifier        str   exact | approx | almost | over | under | at_least | unknown
    period           str
    scope            str   denominator / sample the value refers to
    is_unknown       bool
    unknown_reason   str
    notes            str
    source_url       str
    source_document_hash str
    extraction_method str
    confidence       str   high | medium | low
    privacy_tier     str
    value_safe_to_sum bool  ALWAYS False for audit/report narrative figures
"""
from __future__ import annotations

import polars as pl
from _common import SILVER, now_iso

CATEGORIES = [
    "expenditure", "unit_cost", "overpayment", "vat", "contracts", "procurement_route",
    "residents_centres", "occupancy", "capacity", "applications", "due_diligence",
    "compliance", "inspections", "standards", "safeguarding", "vetting", "risk",
    "resident_experience", "complaints", "policy_target", "legal_obligation",
    "grant", "lobbying", "housing_impact", "sample_property", "unknown_at_source",
]

# doc_key: (title, kind, priority, status, output, url_or_path)
DOCS = {
    # ---------- TIER 1: direct reports — FULL extraction ----------
    "cag_roaps_2024_ch10": (
        "C&AG RoAPS 2024 Ch.10 — Management of international protection accommodation contracts",
        "audit_report", "P0", "EXTRACTED",
        "cag_ipas_chapter_figures.parquet (196) + cag_ipas_chart_recovery.parquet (193)",
        "https://www.audit.gov.ie/media/huahyz0u/10-management-of-international-protection-accommodation-contracts-copy.pdf"),
    "hiqa_ipas_overview_2024": (
        "HIQA — Monitoring of IPAS centres in 2024 (overview report)",
        "regulator_report", "P0", "EXTRACTED",
        "hiqa_ipas_figures.parquet (691)",
        "https://www.hiqa.ie/sites/default/files/2025-03/Monitoring-of-International-Protection-Accommodation-Service-centres-in-2024.pdf"),
    "hiqa_inspection_reports": (
        "HIQA — 101 individual IPAS centre inspection reports (2024-01 → 2026-03)",
        "regulator_inspection", "P0", "PENDING_FULL_EXTRACTION",
        "hiqa_ipas_inspections.parquet = METADATA ONLY (101 rows). PDFs cached (89 MB) but the "
        "per-centre, per-standard COMPLIANCE JUDGMENTS inside are NOT parsed. Biggest untapped asset: "
        "gives centre x standard x judgment x county — the real drill-down for the county map, and the "
        "only way to name providers HIQA inspected.",
        "bronze/hiqa_ipas/pdf/"),
    "cag_roaps_2015_ch06": (
        "C&AG 2015 Annual Report Ch.6 — Procurement and management of contracts for direct provision",
        "audit_report", "P1", "PENDING_FULL_EXTRACTION",
        "text cached (18pp) at bronze/cag_reports/text/ — the 2015 BASELINE for a then-vs-now comparison",
        "https://www.audit.gov.ie/media/n0tm40xg/2015-annual-report-chapter-6-procurement-and-management-of-contracts-for-direct-provision.pdf"),
    "igees_ipas_paper_2025": (
        "IGEES — Managing IPAS Expenditure Pressures: Demand-Side Drivers and Policy Responses (Jun 2025)",
        "analytical_paper", "P0", "PENDING_FULL_EXTRACTION",
        "THE source of the EUR 92/night private vs EUR 34/night State-owned figures (cited by C&AG 10.18) "
        "and of the 17-month median processing time. Almost certainly holds more cost/demand detail.",
        "https://assets.gov.ie/static/documents/IPAS_Analytical_Paper_03062025.pdf"),
    "accommodation_strategy": (
        "Comprehensive Accommodation Strategy for International Protection Applicants",
        "government_strategy", "P1", "PARTIAL",
        "read + key figures noted (35,000 beds by 2028 = 14,000 State + 21,000 commercial; cross-validates "
        "C&AG exactly; contains the State's OWN ADMISSION of breach). NOT yet in the canonical fact schema.",
        "https://assets.gov.ie/static/documents/comprehensive-accommodation-strategy-for-international-protection-applicants.pdf"),
    "project_initiation_document": (
        "Project Initiation Document — Implementation of the new Model of Accommodation and Supports (White Paper)",
        "governance_doc", "P2", "PENDING",
        "DCEDIY PID v1.4, 15 Oct 2021, owner Paula Quinn (IPSS Transition Team). Governance/milestones for "
        "the White Paper model that the Strategy later says was overtaken.",
        "repo root: project-initiation-document.pdf"),
    "ipas_weekly_stats": (
        "IPAS weekly accommodation & arrivals statistics (29 Dec 2024)",
        "official_statistics", "P0", "PARTIAL",
        "ipas_by_local_authority.parquet (31 LAs, sum validated to its own Grand Total 32,702). "
        "The other pages of the 10-page report are NOT extracted. WEEKLY cadence => the live feed for the map.",
        "https://assets.gov.ie/static/documents/29122024-ipas-stats-weekly-report.pdf"),
    "ip_integration_fund_2022": (
        "International Protection Integration Fund 2022 — successful applicants",
        "grant_register", "P1", "EXTRACTED",
        "ip_integration_fund_2022.parquet (65 grants, EUR 1,580,825)",
        "repo root: the-international-protection-integration-fund-2022.pdf"),
    "national_standards": (
        "National Standards for accommodation offered to people in the protection process (2021)",
        "standards", "P1", "PENDING",
        "The 10 themes / 33 standards HIQA judges against. Extracting the standard TEXT gives every "
        "compliance judgment a human-readable meaning in the UI.",
        "https://assets.gov.ie/static/documents/national-standards.pdf"),

    # ---------- TIER 2: context — salient points only, NOT full extraction ----------
    "si_230_2018": (
        "SI 230/2018 — European Communities (Reception Conditions) Regulations 2018",
        "legislation", "CONTEXT", "SALIENT_EXTRACTED",
        "ipas_legal_obligations.parquet — Reg 4 (material reception conditions), Reg 7 (designation), "
        "Reg 8 (MANDATORY vulnerability assessment), Reg 11 (LABOUR MARKET ACCESS — asylum seekers CAN work), "
        "Reg 6 (no capacity defence). Do NOT extract the whole SI.",
        "https://www.irishstatutebook.ie/eli/2018/si/230/made/en/print"),
    "si_649_2023": (
        "SI 649/2023 — Reception Conditions (Amendment) Regs 2023",
        "legislation", "CONTEXT", "SALIENT_EXTRACTED",
        "Reg 27A inserts HIQA monitoring (in force 9 Jan 2024 = Art.28 transposed ~5.5 yrs late). "
        "CRITICAL SALIENT POINT: ZERO enforcement powers (no sanction/penalty/deregister/cancel).",
        "https://www.irishstatutebook.ie/eli/2023/si/649/made/en/print"),
    "si_605_2022_and_376_2023": (
        "SI 605/2022 + SI 376/2023 — Planning & Development (Exempted Development) change-of-use",
        "legislation", "CONTEXT", "SALIENT_EXTRACTED",
        "Change-of-use planning exemption to Dec 2028 (Strategy seeks 2030); the LA-notification duty "
        "inside it is NOT monitored by IPAS.",
        "https://www.irishstatutebook.ie/eli/2023/si/376/made/en/"),
    "revenue_vat_guidance": (
        "Revenue — VAT on emergency accommodation and ancillary services",
        "tax_guidance", "CONTEXT", "SALIENT_EXTRACTED",
        "SALIENT: emergency accommodation is VAT-EXEMPT (catering separately liable at 13.5%) — unlike an "
        "ordinary hotel stay. This is the mechanism behind the EUR 7.4m VAT overcharge.",
        "https://www.revenue.ie/en/tax-professionals/tdm/value-added-tax/part03-taxable-transactions-goods-ica-services/Services/services-emergency-accommodation-and-ancillary-services.pdf"),
    "emn_reception_study": (
        "EMN — Organisation of Reception Facilities for Asylum Seekers (2014)",
        "research", "CONTEXT", "SALIENT_EXTRACTED",
        "SALIENT: the ~15% vacancy-buffer good practice the C&AG measures occupancy against. Nothing else needed.",
        "https://emn.ie/files/p_20140207073231EMN%20Organisation%20of%20Reception%20Facilities%20Synthesis%20Report.pdf"),
    "etenders_ipas_notice": (
        "eTenders — IPAS accommodation public RFT notice (205491)",
        "procurement_notice", "P2", "PENDING",
        "Links this corpus to the built procurement chain: the 2022 public RFT that produced 25 contracts "
        "/ 2,612 rooms. Check whether it is already in procurement_awards.",
        "https://irl.eu-supply.com/ctm/Supplier/PublicPurchase/205491/0/0?b=ETENDERS_SIMPLE"),
}


def main() -> None:
    rows = [{
        "doc_key": k, "doc_title": v[0], "kind": v[1], "priority": v[2],
        "status": v[3], "output_or_note": v[4], "source": v[5],
        "registered_at": now_iso(),
    } for k, v in DOCS.items()]
    df = pl.DataFrame(rows)
    out = SILVER / "ipas_doc_registry.parquet"
    df.write_parquet(out, compression="zstd", statistics=True)
    (SILVER / "_eyeball").mkdir(exist_ok=True)
    df.write_csv(SILVER / "_eyeball" / "ipas_doc_registry.csv")
    print(f"wrote {out} — {df.height} documents")
    with pl.Config(tbl_rows=30, fmt_str_lengths=52, tbl_width_chars=170):
        print(df.select("doc_key", "priority", "status", "kind").sort(["priority", "status"]))
    print("\nby status:")
    print(df.group_by("status").len().sort("len", descending=True))
    print(f"\ncanonical categories ({len(CATEGORIES)}): {', '.join(CATEGORIES)}")


if __name__ == "__main__":
    main()
