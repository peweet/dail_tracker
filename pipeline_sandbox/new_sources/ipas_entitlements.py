"""ENTITLEMENT vs REALITY — what an international-protection applicant is legally
entitled to, and what the auditor and the inspector actually found.

This is the data layer for the "the person" tab on the accommodation-spend page.
It exists to hold the human end of a €1.07bn number: the page shows what the State
SPENDS; this shows what a person is OWED, and whether they got it.

EVERY entitlement is quoted from SI 230/2018 (the transposing instrument).
EVERY reality-check is quoted from a named source (C&AG RoAPS 2024 Ch.10, HIQA's 2024
monitoring report, or the Government's own strategy). NOTHING is inferred — where the
State's performance is not published, reality_status='not_published'.

PRIVACY / TONE (deliberate, and a hard rule for any UI built on this):
- This is about ENTITLEMENTS IN LAW, never about individuals. No resident is named,
  quoted, aged, or located. privacy_tier='public_law_and_aggregates'.
- The subject is the STATE'S obligation, not the applicant's character. Copy must not
  editorialise about migration; it states the law and the audited finding, and stops.
"""
from __future__ import annotations

import polars as pl
from _common import SILVER, now_iso

SI230 = "https://www.irishstatutebook.ie/eli/2018/si/230/made/en/print"
CAG = ("https://www.audit.gov.ie/media/huahyz0u/"
       "10-management-of-international-protection-accommodation-contracts-copy.pdf")
HIQA = ("https://www.hiqa.ie/sites/default/files/2025-03/"
        "Monitoring-of-International-Protection-Accommodation-Service-centres-in-2024.pdf")
STRAT = ("https://assets.gov.ie/static/documents/"
         "comprehensive-accommodation-strategy-for-international-protection-applicants.pdf")

# (order, entitlement, legal_basis, what_the_law_says, timeframe,
#  reality_status, reality_finding, reality_source_url, reality_ref)
E = [
    (1, "Accommodation, food and basic necessities",
     "SI 230/2018 Reg 4 + definition of 'material reception conditions'",
     "A recipient IS ENTITLED to material reception conditions where they lack sufficient means for an adequate standard of living: (a) housing, food and associated benefits in kind, (b) the daily expenses allowance, (c) clothing allowance (s.201 Social Welfare Consolidation Act 2005). Conditional only on being at a designated centre and complying with house rules.",
     "From the date the protection application is indicated.",
     "NOT_DELIVERED_TO_ALL",
     "The State's own Comprehensive Accommodation Strategy: 'the Department is currently unable to offer accommodation to new single male applicants … For a second time this year the state is unable to fulfil these obligations.' C&AG: 3,285 single male applicants were UNACCOMMODATED at end-2024 (down to ~720 by late Aug 2025).",
     STRAT, "Strategy p.2; C&AG 10.9"),

    (2, "A weekly cash allowance for personal expenses",
     "SI 230/2018 Reg 2 (definition of 'daily expenses allowance')",
     "A weekly payment, administered by the Minister for Social Protection, so the person can meet incidental personal expenses. It forms part of the material reception conditions.",
     "Weekly, while a recipient.",
     "AMOUNT_NOT_IN_THIS_SOURCE",
     "The SI creates the entitlement but does NOT set the rate — it is fixed administratively by the Department of Social Protection. The current weekly rate must be sourced from DSP before publication; do not state a figure without it.",
     SI230, "SI 230/2018 Reg 2"),

    (3, "An assessment of vulnerability / special reception needs",
     "SI 230/2018 Reg 8",
     "MANDATORY: 'The Minister SHALL, within 30 working days' of the protection indication, assess whether the person has special reception needs and, if so, their nature. The HSE and Minister for Health must assist.",
     "Within 30 WORKING DAYS.",
     "NOT_PERFORMED",
     "HIQA (2024): 'At the time of writing, vulnerability assessments are NOT BEING CARRIED OUT AT NATIONAL LEVEL.' 43% of centres had no reception officer; 61% had no process to identify or assess residents' needs; 45% had no mechanism to monitor anyone identified as having a special reception need.",
     HIQA, "HIQA 2024 §7.7"),

    (4, "The right to work (labour market access)",
     "SI 230/2018 Reg 11 AS AMENDED BY SI 52/2021 Reg 3 (+ Reg 12 withdrawal, Reg 14 employer duties)",
     "An applicant MAY WORK, but only under a labour market access permission. CURRENT LAW (as amended 2021): they may APPLY AFTER 5 MONTHS; the Minister may grant the permission once 6 MONTHS have passed with no first-instance decision, provided the delay is not the applicant's fault; the permission is valid for 12 MONTHS and is renewable. (Two common misconceptions: that applicants cannot work at all — true only before 2018; and that the wait is 9 months — that was the ORIGINAL 2018 rule, shortened in 2021.)",
     "APPLY AFTER 5 MONTHS; permission grantable at 6 MONTHS; valid 12 MONTHS, renewable. [ORIGINAL SI 230/2018: apply at 8 months, grant at 9, valid 6. SI 52/2021 Reg 3 substituted '5 months' for '8 months', '6 months' for '9 months', and '12 months' for '6 months'. Its explanatory note: 'to reduce the timeframe for labour market access from 9 to 6 months, and to extend the validity of a labour market permission from 6 to 12 months.']",
     "GATED_BY_DELAY",
     "Even at the shortened 6-month threshold, the permission is triggered by the State's OWN delay in deciding: an external estimate put the median end-to-end processing time at ~17 months (May 2024) — so the right to work arrives roughly 11 months before the decision does. Meanwhile 5,292 people who ALREADY HAVE status or permission to remain (about 16% of residents) were still living in accommodation centres at end-2024 because they could not find housing.",
     CAG, "C&AG 10.15, 10.18 fn; SI 52/2021"),

    (5, "Primary and post-primary education for children",
     "SI 230/2018 Reg 17",
     "A recipient who is a minor SHALL have access to primary and post-primary education in the like manner and to the like extent as an Irish citizen child. The Minister for Education SHALL ensure the necessary support and language services.",
     "Same as any child in the State.",
     "GAP_FOUND",
     "HIQA found 15% of school-age children in inspected centres were NOT ATTENDING SCHOOL. 38% of children had no desk or study area; 52% had been living in a centre for 2+ years.",
     HIQA, "HIQA 2024 §5.1"),

    (6, "Health care",
     "SI 230/2018 Reg 18",
     "The Minister for Health SHALL ensure access to: emergency health care; care necessary for serious illness and MENTAL DISORDERS; other care necessary to maintain health; and, where the person is vulnerable, appropriate mental health care having regard to their special reception needs.",
     "On becoming a recipient.",
     "DEPENDS_ON_UNPERFORMED_ASSESSMENT",
     "The vulnerable-person mental-health entitlement is expressly keyed to 'special reception needs' — which are established by the Reg 8 assessment that HIQA says is not being carried out at national level. The entitlement therefore cannot be reliably triggered. HIQA rated Standard 9.1 (health/wellbeing) 100% compliant at centre level, but that measures the CENTRE, not access to the health service.",
     HIQA, "HIQA 2024 §7.7, Std 9.1"),

    (7, "Accommodation that meets the national standards",
     "SI 649/2023 Reg 27A + the National Standards (2021)",
     "HIQA monitors service providers' compliance with the National Standards (10 themes) and reports each inspection to the Minister. Standards include a minimum 4.65 m2 per resident per bedroom and a prohibition on bunk beds for anyone aged 15+ unless requested.",
     "Continuous.",
     "UNENFORCEABLE_AND_MOSTLY_UNMONITORED",
     "HIQA has NO power to sanction, fine, deregister or close an IPAS centre — SI 649/2023 gives it only monitoring, information and 'advise the Minister' powers. And 86% of settings (278 settings / 31,563 beds) fall OUTSIDE its remit entirely. The C&AG found bunk beds in use for adults in 3 centres, and rooms appearing to breach the minimum space requirement.",
     HIQA, "SI 649/2023 Reg 27A; HIQA 2024; C&AG 10.43"),

    (8, "Privacy and dignity in one's living space",
     "National Standards, Theme 4 (Accommodation) — monitored under SI 649/2023",
     "Accommodation should uphold residents' privacy, dignity and safety.",
     "Continuous.",
     "WORST_PERFORMING_STANDARD",
     "Standard 4.3 (privacy, dignity and safety) has the HIGHEST not-compliant rate in HIQA's entire report: 38% NOT compliant, 51% not in full compliance. HIQA recorded up to SIX unrelated adults sharing a bedroom, 90 adults living in tents, mould in 27% of centres, and 20% of adult residents saying they had no privacy or dignity in their sleeping accommodation.",
     HIQA, "HIQA 2024 Std 4.3, Table 14"),

    (9, "Protection from harm and abuse (safeguarding)",
     "National Standards, Theme 8; National Vetting Bureau (Children and Vulnerable Persons) Act 2012",
     "Providers must safeguard residents, have policies to protect them from harm and abuse, and ensure staff are properly Garda-vetted.",
     "Continuous.",
     "SERIOUS_GAPS",
     "HIQA: 35% of STAFF across inspected centres were not appropriately Garda-vetted — including security personnel and managers. 37% of centres had no policies to protect residents from harm and abuse. Child-protection concerns exist in centres that are unknown to managers and therefore go unreported.",
     HIQA, "HIQA 2024 §7.5, §7.6"),

    (10, "Information about your rights, in a language you understand",
     "SI 230/2018 Reg 3 (+ Reg 4(4))",
     "The Minister must provide the recipient with information about their entitlements and obligations, in writing, in a language they may reasonably be supposed to understand.",
     "Without delay.",
     "NOT_PUBLISHED",
     "Neither the C&AG nor HIQA reports on whether this information duty is met. No published measure exists.",
     SI230, "SI 230/2018 Reg 3"),

    (11, "A complaint mechanism, and a right of appeal",
     "SI 230/2018 Reg 20-23 (review/appeal); appeal forms restated by SI 287/2026",
     "Decisions on reception conditions are reviewable and appealable to the International Protection Appeals Tribunal. The appeal form (Schedule 7, as substituted by SI 287/2026) enumerates what is appealable: refusal to grant or renew a labour market access permission (Reg 11); withdrawal of it (Reg 12); a decision that you are not entitled to reception conditions or to the daily expenses allowance (Reg 4(1)); reduction of the allowance (Reg 5(1)); a requirement to contribute to cost or refund it (Reg 5(2)/(3)/(6)); and reduction or withdrawal of reception conditions (Reg 6(1)). Residents may also complain to the Ombudsman.",
     "On decision.",
     "IN_USE",
     "In 2024 the IPAS received 581 complaints from residents (30% about the behaviour of centre management or staff, incl. security; 20% about other residents). 21 complaints went to the Ombudsman. 129 customer-service clinics were held.",
     CAG, "C&AG 10.98-10.99; SI 287/2026 Sch.1"),
]

# The amendment chain of the principal instrument — so the page never quotes a
# superseded figure. Confirmed against our gold `statutory_instruments` + the SI texts.
AMENDMENTS = [
    ("SI 230/2018", "2018-07-06", "PRINCIPAL",
     "European Communities (Reception Conditions) Regulations 2018 — transposes Directive 2013/33/EU. Original labour-market rule: apply at 8 months, grant at 9, permission valid 6 months.",
     "https://www.irishstatutebook.ie/eli/2018/si/230/made/en/print"),
    ("SI 52/2021", "2021-02-12", "AMENDS Reg 11 + Sch.3",
     "**SHORTENED THE WAIT.** Substitutes '5 months' for '8 months' (apply), '6 months' for '9 months' (grant), and '12 months' for '6 months' (validity). Explanatory note: 'to reduce the timeframe for labour market access from 9 to 6 months, and to extend the validity of a labour market permission from 6 to 12 months.' Signed by Helen McEntee.",
     "https://www.irishstatutebook.ie/eli/2021/si/52/made/en/print"),
    ("SI 178/2020 [sic]", "2021-04-20", "AMENDS (No. 2)",
     "European Communities (Reception Conditions) (Amendment) (No. 2) Regulations 2021. ⚠️ DQ: our gold statutory_instruments records si_year=2020 while the title and signing date are 2021 — an upstream/parse inconsistency, flagged not fixed.",
     "https://www.irishstatutebook.ie/eli/2020/si/178/made/en/html"),
    ("SI 649/2023", "2023-12-26", "AMENDS — inserts Reg 27A-27G",
     "Creates the HIQA monitoring regime required by Art.28 of the Directive; in operation 9 Jan 2024. Confers NO enforcement power (no sanction/penalty/deregistration).",
     "https://www.irishstatutebook.ie/eli/2023/si/649/made/en/print"),
    ("SI 118/2024", "2024-03-29", "AMENDS",
     "European Communities (Reception Conditions) (Amendment) Regulations 2024.",
     "https://www.irishstatutebook.ie/eli/2024/si/118/made/en/print"),
    ("SI 287/2026", "2026-06-26", "AMENDS Sch.7 + Sch.8",
     "⚠️ Titled 'Regulations 2026' (NOT '(Amendment)') but is in substance an AMENDING instrument: it substitutes Schedules 7 and 8 of SI 230/2018 — i.e. it replaces the IPAT NOTICE-OF-APPEAL FORMS. It does NOT replace the principal Regulations and does NOT change the substantive entitlements. SI 230/2018 remains THE instrument. Signed by Jim O'Callaghan. (Naming inconsistency flagged, not fixed.)",
     "https://www.irishstatutebook.ie/eli/2026/si/287/made/en/print"),
]


def main() -> None:
    rows = [{
        "display_order": o,
        "entitlement": ent,
        "legal_basis": basis,
        "what_the_law_says": law,
        "timeframe": tf,
        "reality_status": status,
        "reality_finding": finding,
        "legal_source_url": SI230,
        "reality_source_url": rsrc,
        "reality_ref": rref,
        "derived_at": now_iso(),
        "extraction_method": "primary_source_reading",
        "confidence": "high",
        "privacy_tier": "public_law_and_aggregates",
        "value_safe_to_sum": False,
        "tone_rule": ("state the law and the audited finding; never editorialise about "
                      "migration; never name, age, locate or quote an individual resident"),
    } for (o, ent, basis, law, tf, status, finding, rsrc, rref) in E]

    df = pl.DataFrame(rows).sort("display_order")
    out = SILVER / "ipas_entitlements.parquet"
    df.write_parquet(out, compression="zstd", statistics=True)
    (SILVER / "_eyeball").mkdir(exist_ok=True)
    df.write_csv(SILVER / "_eyeball" / "ipas_entitlements.csv")
    print(f"wrote {out} — {df.height} entitlements")
    with pl.Config(tbl_rows=15, fmt_str_lengths=50, tbl_width_chars=150):
        print(df.select("display_order", "entitlement", "reality_status"))

    amd = pl.DataFrame(
        [{"instrument": i, "signed_date": d, "effect": e, "note": n, "source_url": u,
          "derived_at": now_iso(), "confidence": "high",
          "extraction_method": "primary_source_reading"}
         for (i, d, e, n, u) in AMENDMENTS])
    amd.write_parquet(SILVER / "ipas_si_amendment_chain.parquet",
                      compression="zstd", statistics=True)
    amd.write_csv(SILVER / "_eyeball" / "ipas_si_amendment_chain.csv")
    print(f"\nwrote ipas_si_amendment_chain.parquet — {amd.height} instruments")
    with pl.Config(tbl_rows=10, fmt_str_lengths=44, tbl_width_chars=140):
        print(amd.select("instrument", "signed_date", "effect"))
    print("\n>>> CURRENT labour-market rule (SI 230/2018 Reg 11 as amended by SI 52/2021):")
    print("    apply after 5 MONTHS · grantable at 6 MONTHS · permission valid 12 MONTHS")
    print("    (was: 8 / 9 / 6 in the original 2018 text — do NOT publish those)")


if __name__ == "__main__":
    main()
