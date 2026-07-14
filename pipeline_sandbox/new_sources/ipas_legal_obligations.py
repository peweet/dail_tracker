"""The IPAS legal-obligation chain: which law creates the duty, and where the
system fails it. SANDBOX ONLY — every row is anchored to a primary source
(the SI text, the C&AG chapter, the HIQA report, or the Government's own strategy).

NOTHING here is inference. `status` records what a NAMED SOURCE states. Where a
source stops short of asserting breach, status='gap_identified', not 'breach'.
The State's own admission of non-compliance is quoted verbatim from its strategy.
"""
from __future__ import annotations

import polars as pl
from _common import SILVER, now_iso

SRC = {
    "si230": "https://www.irishstatutebook.ie/eli/2018/si/230/made/en/print",
    "si649": "https://www.irishstatutebook.ie/eli/2023/si/649/made/en/print",
    "si605": "https://www.irishstatutebook.ie/eli/2022/si/605/made/en/print",
    "si376": "https://www.irishstatutebook.ie/eli/2023/si/376/made/en/",
    "cag": "https://www.audit.gov.ie/media/huahyz0u/10-management-of-international-protection-accommodation-contracts-copy.pdf",
    "hiqa": "https://www.hiqa.ie/sites/default/files/2025-03/Monitoring-of-International-Protection-Accommodation-Service-centres-in-2024.pdf",
    "strategy": "https://assets.gov.ie/static/documents/comprehensive-accommodation-strategy-for-international-protection-applicants.pdf",
    "directive": "https://eur-lex.europa.eu/eli/dir/2013/33/oj",
}

# (layer, instrument, provision, obligation, status, evidence, evidence_source)
R = [
    # ---- the source obligation ----
    ("eu_source", "Directive 2013/33/EU (recast Reception Conditions Directive)", "whole",
     "Lays down standards for the reception of applicants for international protection. Ireland opted in.",
     "in_force", "SI 230/2018 is made 'for the purpose of giving effect to Directive 2013/33/EU of 26 June 2013'.", "directive"),
    ("eu_source", "Directive 2013/33/EU", "Article 28",
     "Member States must put in place a system to ensure appropriate GUIDANCE, MONITORING and CONTROL of the level of reception conditions.",
     "transposed_late", "The monitoring system required by Art.28 was NOT in SI 230/2018 (2018) — it was inserted only by SI 649/2023, in operation 9 Jan 2024: a gap of ~5.5 years.", "si649"),

    # ---- the transposing instrument: THE instrument that lays out Ireland's duties ----
    ("ie_transposition", "SI 230/2018 — European Communities (Reception Conditions) Regulations 2018", "made 29 Jun 2018, in operation 30 Jun 2018",
     "THE instrument laying out Ireland's reception obligations. Made by the Minister for Justice under s.3 European Communities Act 1972.",
     "in_force", "Transposed in July 2018 — the Government's own strategy confirms: 'the Recast Reception Conditions Directive, which Ireland transposed into law in July 2018'.", "strategy"),
    ("ie_transposition", "SI 230/2018", "Reg 4 — Provision of material reception conditions",
     "The core DUTY: provide material reception conditions (housing, food, associated benefits in kind + daily expenses allowance) to every recipient.",
     "BREACH_ADMITTED_BY_STATE",
     "The State's own Comprehensive Accommodation Strategy: 'the Department is currently unable to offer accommodation to new single male applicants. There is a legal obligation on the state ... to provide accommodation to all who request it. FOR A SECOND TIME THIS YEAR THE STATE IS UNABLE TO FULFIL THESE OBLIGATIONS.' C&AG: 3,285 single male applicants unaccommodated at end-2024.", "strategy"),
    ("ie_transposition", "SI 230/2018", "Reg 8 — Vulnerable persons",
     "MANDATORY: 'The Minister SHALL, within 30 working days' of the protection indication, assess whether the recipient has special reception needs, and if so their nature.",
     "NOT_PERFORMED",
     "HIQA (2024 monitoring report, §7.7): 'At the time of writing, VULNERABILITY ASSESSMENTS ARE NOT BEING CARRIED OUT AT NATIONAL LEVEL.' Also: 43% of centres had no reception officer; 61% had no process to identify/assess residents' needs; 45% had no mechanism to monitor special reception needs.", "hiqa"),
    ("ie_transposition", "SI 230/2018", "Reg 7 — Designation of accommodation centre",
     "The Minister DESIGNATES accommodation centres. Designation is the hinge on which the whole regulatory regime turns.",
     "structural_gap",
     "HIQA's monitoring power (SI 649/2023 Reg 27A) bites on 'accommodation centres' + 'service providers' under an arrangement with the Minister. Per C&AG fn5, the centres subject to HIQA inspection are 'those operated from State-owned premises and those competitively procured' — i.e. EMERGENCY commercial accommodation, sourced by expression-of-interest/direct sourcing, largely sits OUTSIDE the inspected estate.", "cag"),
    ("ie_transposition", "SI 230/2018", "Reg 6 — Withdrawal or reduction of material reception conditions",
     "The Regs provide for the State to WITHDRAW or REDUCE conditions (e.g. for breach of house rules) — but contain NO provision excusing the State where its own capacity is exhausted.",
     "asymmetry", "The duty in Reg 4 is not conditioned on available capacity; there is no statutory 'capacity defence'. The obligation binds absolutely while supply does not.", "si230"),

    # ---- the monitoring regime, added 5.5 years late ----
    ("ie_monitoring", "SI 649/2023 — European Communities (Reception Conditions) (Amendment) Regulations 2023", "in operation 9 Jan 2024",
     "Inserts Regs 27A–27G into SI 230/2018: creates the inspection/monitoring regime required by Article 28 of the Directive and names HIQA as 'the Authority'.",
     "in_force_2024",
     "Reg 27A(1): 'The Authority shall, for the purposes of Article 28 of the Directive — (a) monitor compliance by service providers with the National Standards...'. This is why HIQA only began inspecting in January 2024 — 5.5 years after the duties took effect.", "si649"),
    ("ie_monitoring", "SI 649/2023", "Reg 27A — Monitoring of Accommodation Centres",
     "HIQA monitors compliance with the NATIONAL STANDARDS and reports each inspection to the Minister.",
     "coverage_gap",
     "HIQA inspected centres housing 6,544 residents; 32,702 were in State-provided accommodation at end-2024 => roughly 20% of residents are covered by the statutory monitoring regime. ~75% of residents (24,718) are in emergency accommodation (269 of 326 centres).", "hiqa"),
    ("ie_standards", "National Standards for accommodation offered to people in the protection process (2021)", "10 themes",
     "The benchmark HIQA monitors against (governance, workforce, contingency, accommodation, food, person-centred care, family/community life, safeguarding, health, special needs). Approved by the Minister — STANDARDS, not regulations.",
     "late_application",
     "C&AG 10.44: although the standards were set in 2019, they 'were only adopted for emergency IP accommodation centres in mid-2023', so properties contracted in 2022–23 were NOT subject to them. Bunk beds are prohibited for persons 15+ and 4.65 m2/person is required — yet bunk beds were in use for adults in 3 inspected centres.", "cag"),

    # ---- the planning carve-out ----
    ("ie_planning", "SI 605/2022 — Planning and Development (Exempted Development) (No. 4) Regulations 2022", "change of use",
     "EXEMPTS change of use of specified structures (hotels, offices, schools, barracks, etc.) to IP accommodation from planning permission — originally to 31 Dec 2024.",
     "in_force", "C&AG 10.27.", "si605"),
    ("ie_planning", "SI 376/2023 — Planning and Development (Exempted Development) (No. 4) Regulations 2023", "extension",
     "Extends the change-of-use exemption to 31 Dec 2028. The Government's strategy asks for a further extension to Dec 2030.",
     "in_force_extension_sought",
     "Strategy, Key enablers: 'Extension of SI 376 ... change of use of properties - from expiring in December 2028 to expiring in December 2030 to encourage the commercial sector to invest in office conversions.' (NB the strategy miscites it as 'SI 376 of 2022'.)", "strategy"),
    ("ie_planning", "SI 605/2022 + SI 376/2023", "notification duty inside the exemption",
     "The exemption is CONDITIONAL: the provider must notify the relevant local authority of the change of use before commencing development. This notification is the only local check that survives the exemption.",
     "NOT_MONITORED",
     "C&AG 10.28: 'The IPAS stated that it does not keep records of properties which have been notified to local authorities for change of use as IP accommodation.' Evidence of planning permission/exemption application was on file for only 4 of 20 sampled properties (20%).", "cag"),

    # ---- adjacent statutory duties ----
    ("ie_adjacent", "National Vetting Bureau (Children and Vulnerable Persons) Act 2012", "vetting",
     "Staff working with children/vulnerable persons must be Garda-vetted.",
     "widespread_non_compliance",
     "HIQA: 35% of staff across inspected centres were NOT appropriately Garda-vetted (front-line staff, security personnel and managers). Compounded because contracts do not specify a centre's population type, so an adults-only centre can receive families at any time.", "hiqa"),
    ("ie_adjacent", "International Protection Act 2015 (No. 66 of 2015)", "whole",
     "Governs the international protection process itself (s.13 indication of application triggers the reception-conditions clock).",
     "in_force", "C&AG 10.1 fn1; SI 230/2018 Reg 8 keys its 30-working-day deadline to s.13(1) of this Act.", "cag"),
    ("eu_adjacent", "Regulation (EU) 2022/2560 (Foreign Subsidies Regulation)", "declaration",
     "From April 2025 providers must declare foreign financial contributions that could distort competition.",
     "in_force", "C&AG 10.23.", "cag"),
]


def main() -> None:
    rows = [{
        "layer": layer, "instrument": inst, "provision": prov, "obligation": obl,
        "status": status, "evidence": ev, "evidence_source_url": SRC[src],
        "derived_at": now_iso(),
        "extraction_method": "primary_source_reading",
        "confidence": "high",
        "privacy_tier": "public_law",
        "caveat": ("status records what a NAMED PRIMARY SOURCE states, not our inference; "
                   "'BREACH_ADMITTED_BY_STATE' is the State's own words in its published strategy"),
    } for (layer, inst, prov, obl, status, ev, src) in R]

    df = pl.DataFrame(rows)
    out = SILVER / "ipas_legal_obligations.parquet"
    df.write_parquet(out, compression="zstd", statistics=True)
    (SILVER / "_eyeball").mkdir(exist_ok=True)
    df.write_csv(SILVER / "_eyeball" / "ipas_legal_obligations.csv")
    print(f"wrote {out} - {df.height} rows")
    with pl.Config(fmt_str_lengths=70, tbl_rows=20, tbl_width_chars=170):
        print(df.group_by("layer").len().sort("len", descending=True))
        print(df.select("instrument", "provision", "status"))


if __name__ == "__main__":
    main()
