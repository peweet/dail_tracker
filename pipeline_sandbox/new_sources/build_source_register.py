"""Generate a HUMAN-REVIEWABLE register of every source used in the sandbox ingests.

Purpose: so the user can spot-check any extraction — open the ORIGINAL document and the
EXTRACTED rows side by side and see that they agree.

GENERATED FROM DISK, never hand-written: it walks the actual bronze cache (file, size,
pages, SHA-256) and the actual silver outputs (row counts, unknown counts), so the register
cannot drift away from what really exists. If a source is missing or an output is empty, the
register says so rather than quietly claiming success.

Outputs:
  pipeline_sandbox/new_sources/SOURCE_REGISTER.md   <- read this
  c:/tmp/dail_new_sources/silver/_eyeball/source_register.csv
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from _common import BRONZE, SILVER

REPO_DOC = Path(__file__).parent / "SOURCE_REGISTER.md"
EYE = SILVER / "_eyeball"

# source_key: (title, publisher, live_url, licence, bronze_glob, outputs[])
# bronze_glob is relative to BRONZE; outputs are silver parquet stems.
SOURCES = [
    # ---- IPAS / asylum corpus (the deep-dive) ----
    ("cag_roaps_2024_ch10", "C&AG RoAPS 2024 Ch.10 — Management of international protection accommodation contracts",
     "Comptroller & Auditor General", "https://www.audit.gov.ie/media/huahyz0u/10-management-of-international-protection-accommodation-contracts-copy.pdf",
     "CC-BY-4.0", "cag_reports/pdf/10-management-of-international-protection-accommodation-contracts-copy.pdf",
     ["cag_ipas_chapter_figures", "cag_ipas_chart_recovery"]),
    ("cag_roaps_2015_ch06", "C&AG 2015 Ch.6 — Procurement and management of contracts for direct provision",
     "Comptroller & Auditor General", "https://www.audit.gov.ie/media/n0tm40xg/2015-annual-report-chapter-6-procurement-and-management-of-contracts-for-direct-provision.pdf",
     "CC-BY-4.0", "cag_reports/pdf/2015-annual-report-chapter-6-procurement-and-management-of-contracts-for-direct-provision.pdf",
     ["cag_2015_direct_provision_facts"]),
    ("hiqa_ipas_overview_2024", "HIQA — Monitoring of IPAS centres in 2024 (overview report)",
     "Health Information and Quality Authority", "https://www.hiqa.ie/sites/default/files/2025-03/Monitoring-of-International-Protection-Accommodation-Service-centres-in-2024.pdf",
     "PSI re-use", "ipas_context/hiqa_ipas_monitoring_2024.pdf", ["hiqa_ipas_figures"]),
    ("hiqa_inspection_reports", "HIQA — 101 individual IPAS centre inspection reports (2024-01 → 2026-03)",
     "Health Information and Quality Authority", "https://www.hiqa.ie/areas-we-work/international-protection-accommodation-services",
     "PSI re-use", "hiqa_ipas/pdf/*.pdf", ["hiqa_centre_compliance", "hiqa_centre_facts", "hiqa_ipas_inspections"]),
    ("igees_ipas_paper_2025", "IGEES — Managing IPAS Expenditure Pressures (June 2025)",
     "Irish Government Economic & Evaluation Service", "https://assets.gov.ie/static/documents/IPAS_Analytical_Paper_03062025.pdf",
     "PSI re-use", "ipas_context/IPAS_Analytical_Paper_03062025.pdf", ["igees_ipas_facts"]),
    ("accommodation_strategy", "Comprehensive Accommodation Strategy for International Protection Applicants",
     "Dept of Children, Equality, Disability, Integration and Youth", "https://assets.gov.ie/static/documents/comprehensive-accommodation-strategy-for-international-protection-applicants.pdf",
     "PSI re-use", "ipas_context/comprehensive_accommodation_strategy.pdf", ["accommodation_strategy_facts"]),
    ("national_standards", "National Standards for accommodation offered to people in the protection process (2021)",
     "Dept of Justice / DCEDIY", "https://assets.gov.ie/static/documents/national-standards.pdf",
     "PSI re-use", "ipas_context/national_standards.pdf", ["national_standards_lookup", "national_standards_facts"]),
    ("ipas_weekly_stats", "IPAS weekly accommodation & arrivals statistics (29 Dec 2024)",
     "Dept of Justice (IPAS)", "https://assets.gov.ie/static/documents/29122024-ipas-stats-weekly-report.pdf",
     "PSI re-use", "ipas_weekly/29122024-ipas-stats-weekly-report.pdf",
     ["ipas_by_local_authority", "ipas_weekly_facts"]),
    ("project_initiation_document", "Project Initiation Document — new Model of Accommodation & Supports (White Paper)",
     "DCEDIY (IPSS Transition Team)", "(repo root: project-initiation-document.pdf)",
     "PSI re-use", None, ["pid_facts"]),
    ("ip_integration_fund_2022", "International Protection Integration Fund 2022 — successful applicants",
     "DCEDIY", "(repo root: the-international-protection-integration-fund-2022.pdf)",
     "PSI re-use", None, ["ip_integration_fund_2022"]),
    ("si_230_2018", "SI 230/2018 — European Communities (Reception Conditions) Regulations 2018",
     "Irish Statute Book", "https://www.irishstatutebook.ie/eli/2018/si/230/made/en/print",
     "PSI / Open Data Licence", None, ["ipas_legal_obligations", "ipas_entitlements", "ipas_si_amendment_chain"]),
    ("si_52_2021", "SI 52/2021 — Reception Conditions (Amendment) Regs 2021 (SHORTENED the right-to-work wait)",
     "Irish Statute Book", "https://www.irishstatutebook.ie/eli/2021/si/52/made/en/print",
     "PSI / Open Data Licence", None, ["ipas_si_amendment_chain"]),
    ("si_649_2023", "SI 649/2023 — Reception Conditions (Amendment) Regs 2023 (created the HIQA monitoring regime)",
     "Irish Statute Book", "https://www.irishstatutebook.ie/eli/2023/si/649/made/en/print",
     "PSI / Open Data Licence", None, ["ipas_legal_obligations", "ipas_si_amendment_chain"]),
    ("lobbying_register", "Lobbying returns (IP/asylum/refugee/Ukraine subset)",
     "lobbying.ie (via our silver lobbying chain)", "https://www.lobbying.ie/",
     "PSI re-use", None, ["ipas_lobbying", "ip_fund_lobbying_xref"]),
    ("payments_fact", "procurement_payments_fact (Dept of Justice — IPAS providers)",
     "Irish public bodies (our gold money fact)", "(internal: data/gold/parquet/procurement_payments_fact.parquet)",
     "per-publisher", None, ["cag_ipas_provider_candidates", "ipas_operator_money_compliance"]),

    # ---- wave-2 sources outside the IPAS corpus ----
    ("cag_reports_index", "C&AG reports index (267 reports: special / appropriation / RoAPS)",
     "Comptroller & Auditor General", "https://www.audit.gov.ie/en/find-report/publications/",
     "CC-BY-4.0", None, ["cag_reports", "cag_chapters"]),
    ("research_ireland", "Research Ireland / SFI grant commitments",
     "Research Ireland (via data.gov.ie)", "https://data.gov.ie/",
     "CC-BY-4.0", None, ["research_ireland_grants"]),
    ("irish_aid_iati", "Irish Aid ODA activities (IATI)",
     "Dept of Foreign Affairs (IATI Registry)", "https://iatiregistry.org/",
     "CC0", None, ["irish_aid_iati"]),
    ("ahbra", "AHBRA — register of Approved Housing Bodies + statutory notices",
     "Approved Housing Bodies Regulatory Authority", "https://www.ahbregulator.ie/",
     "PSI re-use", None, ["ahbra_register", "ahbra_notices"]),
    ("oic_foi", "OIC / FOI decisions",
     "Office of the Information Commissioner", "https://www.oic.ie/decisions/",
     "PSI re-use", None, ["oic_foi_decisions"]),
    ("dpc", "DPC decisions",
     "Data Protection Commission", "https://www.dataprotection.ie/en/dpc-guidance/decisions",
     "PSI re-use", None, ["dpc_decisions"]),
    ("datagov", "data.gov.ie CKAN catalogue monitor",
     "data.gov.ie", "https://data.gov.ie/", "CC-BY-4.0", None, ["datagov_catalogue"]),
]


def sha12(p: Path) -> str:
    try:
        return hashlib.sha256(p.read_bytes()).hexdigest()[:12]
    except Exception:
        return "—"


def pages(p: Path) -> str:
    try:
        import fitz
        with fitz.open(p) as d:
            return str(d.page_count)
    except Exception:
        return "—"


def main() -> None:
    rows = []
    for key, title, pub, url, lic, bglob, outs in SOURCES:
        # bronze
        cached, sha, npages, mb = "—", "—", "—", "—"
        if bglob:
            if "*" in bglob:
                fs = sorted(BRONZE.glob(bglob))
                if fs:
                    cached = f"{BRONZE / bglob.split('*')[0]} ({len(fs)} files)"
                    mb = f"{sum(f.stat().st_size for f in fs)/1e6:.1f}"
                    sha, npages = "(per file)", "(per file)"
            else:
                f = BRONZE / bglob
                if f.exists():
                    cached = str(f)
                    sha = sha12(f)
                    npages = pages(f)
                    mb = f"{f.stat().st_size/1e6:.1f}"
        # silver
        outbits, total, unk = [], 0, 0
        for o in outs:
            p = SILVER / f"{o}.parquet"
            if not p.exists():
                outbits.append(f"`{o}` **NOT BUILT**")
                continue
            d = pl.read_parquet(p)
            total += d.height
            if "is_unknown" in d.columns:
                unk += int(d["is_unknown"].sum())
            csv = EYE / f"{o}.csv"
            outbits.append(f"`{o}` ({d.height:,} rows{'' if not csv.exists() else ', CSV ✓'})")
        rows.append({
            "source_key": key, "title": title, "publisher": pub, "live_url": url,
            "licence": lic, "cached_file": cached, "sha256_12": sha,
            "pages": npages, "size_mb": mb,
            "outputs": " · ".join(outbits), "total_rows": total, "unknown_rows": unk,
        })

    df = pl.DataFrame(rows)
    EYE.mkdir(parents=True, exist_ok=True)
    df.write_csv(EYE / "source_register.csv")

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    L = [
        "# SOURCE REGISTER — sandbox ingests",
        "",
        f"_Generated from disk by `build_source_register.py` on {ts}. "
        "Do not hand-edit — re-run the script._",
        "",
        "**How to spot-check an extraction:** open the **live URL** (the original document), "
        "open the matching **CSV** in `c:/tmp/dail_new_sources/silver/_eyeball/`, and compare. "
        "The `cached file` is the exact bytes we parsed (SHA-256 given), so the original cannot "
        "have changed under us. Rendered page images for the chart/glyph recoveries are in the "
        "same `_eyeball/` folder.",
        "",
        f"**{df.height} sources · {df['total_rows'].sum():,} extracted rows · "
        f"{df['unknown_rows'].sum():,} explicitly marked unknown.**",
        "",
        "> Every figure we publish must trace to a row here. Unknowns are never guessed — "
        "they are carried as explicit rows with a reason.",
        "",
    ]
    for r in rows:
        L += [
            f"### {r['title']}",
            "",
            f"- **Publisher:** {r['publisher']}",
            f"- **Source:** {r['live_url']}",
            f"- **Licence:** {r['licence']}",
            f"- **Cached (what we actually parsed):** `{r['cached_file']}`"
            + (f" · {r['pages']} pp · {r['size_mb']} MB · sha256 `{r['sha256_12']}…`"
               if r["sha256_12"] != "—" else ""),
            f"- **Extracted →** {r['outputs']}",
            f"- **Rows:** {r['total_rows']:,}"
            + (f" (of which **{r['unknown_rows']:,} explicit unknowns**)" if r["unknown_rows"] else ""),
            "",
        ]
    REPO_DOC.write_text("\n".join(L), encoding="utf-8")

    print(f"wrote {REPO_DOC}")
    print(f"wrote {EYE / 'source_register.csv'}")
    print(f"\n{df.height} sources · {df['total_rows'].sum():,} rows · "
          f"{df['unknown_rows'].sum():,} unknowns")
    missing = df.filter(pl.col("outputs").str.contains("NOT BUILT"))
    if missing.height:
        print(f"\n⚠ outputs not yet built ({missing.height}):")
        for r in missing.iter_rows(named=True):
            print(f"   {r['source_key']}: {r['outputs']}")
    nocache = df.filter((pl.col("cached_file") == "—") & pl.col("live_url").str.starts_with("http"))
    print(f"\nsources with no local cache (fetched live / internal): {nocache.height}")


if __name__ == "__main__":
    main()
