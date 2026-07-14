"""C&AG Report on the Accounts of the Public Services 2015, Chapter 6 —
"Procurement and management of contracts for direct provision".

Structured extraction into the canonical IPAS fact schema (see ipas_doc_registry.py).
SANDBOX ONLY. Nothing here writes to data/, edits pipeline.py, or promotes.

WHY THIS DOCUMENT: it is the 2015 BASELINE. Same auditor, same subject (State
accommodation contracts for protection applicants), ten years before RoAPS 2024
Chapter 10. The most valuable output is therefore not the 2015 figures on their own
but the RECURRENCE ANALYSIS at the bottom of this file: which 2015 findings — and
which ACCEPTED 2015 recommendations — are still live defects in 2024.

THREE EXTRACTION LANES
  1. manual_curation_from_fitz_text_full_read — the born-digital text layer, read in
     full, hand-curated with PDF page + printed page + paragraph/figure ref.
  2. vector_geometry_axis_calibrated — Figures 6.1, 6.2, 6.4, 6.5 and 6.7 carry series
     with NO data labels at all. They are pure vector charts, so bar rectangles and
     line vertices are read from the PDF drawing operators and calibrated against the
     real axis-label text. Every recovery is validated against a value the chapter
     states independently:
        Fig 6.1 expenditure   -> 2015 measures EUR 57m vs the stated EUR 57m (6.5)
        Fig 6.2 applicants    -> 2007 ~3,990 ('just under 4,000'), 2013 ~950 ('just
                                 under 1,000'), 2015 ~3,280 ('over 3,200') (6.10)
        Fig 6.4 length of stay-> the 7+ years bucket measures ~452 vs the stated 450 (6.15)
        Fig 6.7 supplier bars -> the two State-owned facilities managers sum to ~EUR 37m
                                 vs the stated EUR 36m total for State-owned (6.26)
     Fig 6.7 is the important one: it NAMES the nine companies paid over EUR 10m each
     and the values exist only as bar length.
  3. cross_document_recurrence_analysis — the 2015 -> 2024 defect comparison.

UNKNOWN DISCIPLINE: anything the chapter itself could not establish gets an EXPLICIT
row (value_numeric null, is_unknown=True, unknown_reason set). Nothing is guessed.

All money rows are value_safe_to_sum=False: audit-report NARRATIVE grain, never to be
unioned with the payments/awards/budget money facts.
"""
from __future__ import annotations

import hashlib

import fitz
import polars as pl

from _common import BRONZE, SILVER, now_iso

PDF = (BRONZE / "cag_reports" / "pdf" /
       "2015-annual-report-chapter-6-procurement-and-management-of-contracts-for-direct-provision.pdf")
SRC_URL = ("https://www.audit.gov.ie/media/n0tm40xg/2015-annual-report-chapter-6-"
           "procurement-and-management-of-contracts-for-direct-provision.pdf")
DOC_KEY = "cag_roaps_2015_ch06"
DOC_TITLE = ("C&AG Report on the Accounts of the Public Services 2015, Chapter 6 - "
             "Procurement and management of contracts for direct provision")

# PDF page N prints as page N+66 (PDF p2 = printed 68).
PRINTED = {1: "67", **{n: str(n + 66) for n in range(2, 19)}}

DEPT = "Department of Justice and Equality"
GRAIN = "audit-report narrative grain (2015 direct provision) - NEVER union with payments/awards facts"

# ===========================================================================
# LANE 1 — curated text-layer facts
# (page, ref, section, category, subject, metric, value, value_text, unit,
#  qualifier, period, scope, notes)
# ===========================================================================
R: list[tuple] = []

# ---------------- the 2015 baseline: money, centres, residents -------------
R += [
    (1, "6.5", "Introduction", "expenditure", DEPT,
     "Expenditure on direct provision", 57_000_000, None, "eur", "exact", "2015", GRAIN,
     "the 2015 BASELINE. Ten years later the C&AG's 2024 Ch.10 records EUR 1,066m on the successor "
     "programme (IPAS) - an ~18.7x increase on the same subject matter. Different scope (2015 = "
     "direct provision only; 2024 = all IP accommodation incl. emergency) - do NOT union."),
    (1, "6.5", "Introduction", "residents_centres", DEPT,
     "Direct provision centres", 35, None, "centres", "exact", "end Dec 2015",
     "all direct provision centres", "cf. 326 centres at end-2024 (C&AG Fig 10.1)"),
    (1, "6.5", "Introduction", "residents_centres", DEPT,
     "People accommodated in direct provision centres", 4_696, None, "persons", "exact",
     "end Dec 2015", "all direct provision centres",
     "cf. 32,702 IPAS residents at end-2024 (C&AG Fig 10.1) - a 7.0x increase"),
    (6, "6.23", "Contracts for direct provision centres", "capacity", DEPT,
     "Contracted capacity of the 35 direct provision centres", 5_400, None, "beds", "over",
     "Dec 2015", "35 centres", "'capacity to accommodate over 5,400 people'; the Annex A capacities "
     "sum to 5,449 - consistent"),
    (6, "6.24", "Contracts for direct provision centres", "residents_centres", DEPT,
     "State-owned centres", 7, None, "centres", "exact", "Dec 2015", "of 35 centres",
     "services provided by 2 contracted companies (Aramark and Onsite Facilities Management)"),
    (6, "6.24", "Contracts for direct provision centres", "residents_centres", DEPT,
     "Centres owned and operated by commercial suppliers", 28, None, "centres", "exact",
     "Dec 2015", "of 35 centres", "held by 22 commercial suppliers"),
    (6, "6.24", "Contracts for direct provision centres", "contracts", DEPT,
     "Commercial suppliers of accommodation", 22, None, "suppliers", "exact", "Dec 2015",
     "28 commercially owned centres", None),
    (6, "6.25", "Contracts for direct provision centres", "residents_centres", DEPT,
     "Purpose-built direct provision centres", 3, None, "centres", "exact", "2015", "of 35 centres",
     "the majority are premises originally designed for other purposes - former hotels, boarding "
     "schools and hostels. The same 'repurposed premises' model the 2024 chapter audits."),
    (6, "6.25", "Contracts for direct provision centres", "residents_centres", DEPT,
     "Self-catering centres", 2, None, "centres", "exact", "2015", "of 35 centres",
     "food provided on a full-board basis in almost all centres"),
    (1, "6.4", "Introduction", "residents_centres", DEPT,
     "Share of asylum seekers living in direct provision", 50, None, "percent", "over", "2015",
     "all asylum seekers", "'just over half'; availing of direct provision is not compulsory"),
    (3, "6.10", "Requirement for direct provision places", "applications", "the State",
     "Asylum applications", 4_000, None, "applications", "under", "2007", "the State",
     "'just under 4,000'"),
    (3, "6.10", "Requirement for direct provision places", "applications", "the State",
     "Asylum applications", 1_000, None, "applications", "under", "2013", "the State",
     "'just under 1,000' - the trough"),
    (3, "6.10", "Requirement for direct provision places", "applications", "the State",
     "Asylum applications", 3_200, None, "applications", "over", "2015", "the State",
     "'over 3,200'. cf. 18,561 IP applications in 2024 (IGEES Fig 3.3) - a 5.7x increase."),
]

# ---------------- Figure 6.6: payments to contractors, 2011-2015 -----------
_f66 = [("2011", 8.3, 59.5, 67.8), ("2012", 7.0, 53.0, 60.0), ("2013", 6.9, 46.4, 53.3),
        ("2014", 6.9, 44.2, 51.1), ("2015", 7.0, 47.9, 54.9)]
for yr, state, comm, tot in _f66:
    R += [
        (6, "Fig 6.6", "Contracts for direct provision centres", "expenditure",
         "State-owned centres (services contracts)", "Payments to direct provision contractors",
         round(state * 1_000_000), None, "eur", "exact", yr, GRAIN,
         "services only - excludes the cost of the accommodation itself (the State owns it)"),
        (6, "Fig 6.6", "Contracts for direct provision centres", "expenditure",
         "commercial centres", "Payments to direct provision contractors",
         round(comm * 1_000_000), None, "eur", "exact", yr, GRAIN,
         "accommodation and services in commercially owned and operated centres"),
        (6, "Fig 6.6", "Contracts for direct provision centres", "expenditure", DEPT,
         "Payments to direct provision contractors, total", round(tot * 1_000_000), None, "eur",
         "exact", yr, GRAIN, None),
    ]
R += [
    (6, "6.26", "Contracts for direct provision centres", "expenditure", "commercial centres",
     "Payments to commercial providers of centres", 251_000_000, None, "eur", "exact",
     "2011-2015 (5 years)", GRAIN,
     "the headline procurement exposure: EUR 251m awarded over five years with NO formal competitive "
     "process ever run (6.36: 'A request for tender has never been issued')"),
    (6, "6.26", "Contracts for direct provision centres", "expenditure",
     "State-owned centres (services contracts)", "Payments to providers of services in State-owned "
     "facilities", 36_000_000, None, "eur", "exact", "2011-2015 (5 years)", GRAIN,
     "these two contracts WERE competitively tendered on eTenders (6.29)"),
    (6, "6.27", "Contracts for direct provision centres", "expenditure", "commercial suppliers",
     "Companies each paid in excess of EUR 10 million", 9, None, "companies", "exact",
     "2011-2015 (5 years)", "of all direct provision contractors",
     "named in Fig 6.7; per-company values exist only as chart bars and are recovered separately"),
]

# ---------------- unit costs (the 2015 comparator for EUR 92 / EUR 34) -----
R += [
    (10, "6.45", "Contract payment terms", "unit_cost", "commercial centres",
     "Effective daily rate per contracted space - lower bound", 20.70, None,
     "eur_per_person_night", "exact", "2015", "commercial centres (full range incl. self-catering)",
     "THE 2015 UNIT-COST BASELINE. The IGEES 2025 paper puts privately provided (mostly emergency) "
     "accommodation at EUR 92/night in 2024 - a 2.6-4.4x increase on this 2015 range."),
    (10, "6.45", "Contract payment terms", "unit_cost", "commercial centres",
     "Effective daily rate per contracted space - upper bound", 35.50, None,
     "eur_per_person_night", "exact", "2015", "commercial centres", None),
    (10, "6.45", "Contract payment terms", "unit_cost", "commercial centres",
     "Effective daily rate per contracted space - lower bound EXCLUDING self-catering", 27.00, None,
     "eur_per_person_night", "exact", "2015", "commercial centres, full board only",
     "the lower EUR 20.70 relates to a self-catering facility where residents receive cash instead"),
    (10, "6.46", "Contract payment terms", "unit_cost", "State-owned centres (services contracts)",
     "Daily rate per person - lower bound", 11.27, None, "eur_per_person_night", "exact", "2015",
     "State-owned centres - SERVICES ONLY (excludes the cost of the accommodation itself)",
     "cf. IGEES 2024: EUR 34/night State-owned. NOTE the scopes differ - the 2015 State-owned rate is "
     "a services-only fee; the 2024 figure is a full cost per night. Do not treat as like-for-like."),
    (10, "6.46", "Contract payment terms", "unit_cost", "State-owned centres (services contracts)",
     "Daily rate per person - upper bound", 17.37, None, "eur_per_person_night", "exact", "2015",
     "State-owned centres - services only", None),
]

# ---------------- length of stay, status-holders, occupancy ---------------
R += [
    (4, "6.15", "Requirement for direct provision places", "residents_centres", DEPT,
     "Average length of stay in direct provision", 38, None, "months", "exact", "Jul 2016",
     "all direct provision residents",
     "the 2015-era length-of-stay baseline. NOT directly comparable with the IGEES 2025 median "
     "end-to-end PROCESSING time of 17 months (May 2024) - one is a mean stay, the other a median "
     "process duration. Recorded as printed; no comparison asserted."),
    (4, "6.15 / Fig 6.4", "Requirement for direct provision places", "residents_centres", DEPT,
     "Residents in direct provision for more than seven years", 450, None, "persons", "approx",
     "Jul 2016", "10% of all direct provision residents",
     "'Some 450 people - 10% of the total'. cf. IGEES: 1,614 IPAS residents over FIVE years at May 2024."),
    (3, "6.11 / Fig 6.3", "Requirement for direct provision places", "residents_centres", DEPT,
     "Residents whose application is still being processed", 3_241, None, "persons", "exact",
     "Aug 2016", "77% of direct provision residents", "excludes children who have not applied for protection"),
    (3, "6.11 / Fig 6.3", "Requirement for direct provision places", "residents_centres",
     "people with status", "Residents granted refugee status, subsidiary protection or leave to remain",
     667, None, "persons", "exact", "Aug 2016", "16% of direct provision residents",
     "THE RECURRING DEFECT IN ONE NUMBER: 16% of residents in 2016 held status and had not moved on. "
     "IGEES 2025 reports 5,292 people with status still in IPAS at end-2024 - ALSO 16%. Same share, "
     "7.9x the people, nine years and one accepted recommendation (6.1) later."),
    (3, "6.11 / Fig 6.3", "Requirement for direct provision places", "residents_centres", DEPT,
     "Residents subject to deportation orders", 283, None, "persons", "exact", "Aug 2016",
     "7% of direct provision residents", None),
    (3, "6.11", "Requirement for direct provision places", "residents_centres", DEPT,
     "Residents NOT awaiting a decision", 23, None, "percent", "exact", "Aug 2016",
     "direct provision residents", "status granted (16%) + deportation orders (7%)"),
    (4, "6.13", "Requirement for direct provision places", "residents_centres", "people with status",
     "Share of those granted leave to remain who left direct provision within six months", 87, None,
     "percent", "exact", "2015 (Department analysis)", "persons granted leave to remain",
     "the Department's own defence, quoted by the Accounting Officer at 6.71. IGEES 2025 finds the "
     "opposite a decade on: 45% of people with status are STILL in IPAS one year after receiving it."),
    (5, "6.21 / Fig 6.5", "Occupancy rates", "occupancy", DEPT,
     "Average occupancy rate of direct provision accommodation", 86, None, "percent", "exact",
     "2007-2015 average", "all direct provision accommodation",
     "cf. the 2010 VFM review's recommended minimum of >90%, and C&AG 2024 (10.76): 78.5% average "
     "across 19 sampled properties - occupancy has gone DOWN, not up, while payment stayed capacity-based"),
    (5, "6.21", "Occupancy rates", "policy_target", DEPT,
     "Recommended minimum occupancy rate (2010 Value for Money and Policy Review)", 90, None,
     "percent", "over", "recommended 2010", "all direct provision accommodation",
     "'would provide a reasonable level of spare capacity to cope with sudden increases in demand'. "
     "Never met on average across 2007-2015."),
]

# ---------------- procurement findings — the core of the chapter -----------
R += [
    (7, "6.29", "Procurement issues", "procurement_route",
     "State-owned centres (services contracts)", "Service contracts advertised on eTenders", 2, None,
     "contracts", "exact", "2015", "the 2 State-owned services contracts",
     "COMPETITIVE. The Department was, at the time, running an EU-wide competitive process for these."),
    (7, "6.30", "Procurement issues", "procurement_route", "commercial centres",
     "Formal competitive process used for suppliers of commercial centres", None, "No", "verdict",
     "exact", "2015", "28 commercially owned centres / EUR 251m over 5 years",
     "'The Department does not use formal competitive processes, as set out in public procurement "
     "rules, for suppliers of commercial centres.' It used a website call for 'expressions of "
     "interest' plus newspaper advertisements."),
    (8, "6.36", "Procurement issues", "procurement_route", "commercial centres",
     "Requests for tender ever issued for commercial direct provision accommodation", 0, None,
     "tenders", "exact", "to 2015 (a 15-year period)", "commercial direct provision accommodation",
     "'A request for tender has never been issued, there are many potential suppliers and the "
     "Department has been procuring these services continuously over a 15 year period.' The C&AG "
     "concludes the negotiated-procedure conditions do NOT apply."),
    (8, "6.34", "Procurement issues", "compliance", DEPT,
     "Compliance with the key requirements of the EU negotiated procedure the Department claims to "
     "have used", None, "Not compliant", "verdict", "exact", "2015",
     "publication of award criteria and award notices",
     "'it has not complied with key requirements of the negotiated procedure, including publication "
     "of award criteria and award notices'"),
    (8, "6.31", "Procurement issues", "due_diligence", DEPT,
     "Evidence provided of how responses to the expressions-of-interest notice were evaluated",
     None, "None provided", "verdict", "exact", "2015", "expressions of interest",
     "'The Department did not provide evidence to show how it evaluated those who responded to the "
     "notice.' The 2024 chapter finds the same evidential vacuum (Fig 10.6: proposal documents "
     "available for 7 of 20 sampled properties)."),
    (8, "6.33", "Procurement issues", "procurement_route", DEPT,
     "Responses to the most recent call for expressions of interest", 27, None, "responses", "exact",
     "2015", "call for expressions of interest", None),
    (8, "6.32", "Procurement issues", "contracts", DEPT,
     "Typical contract period agreed with commercial providers", 1, None, "years", "exact", "2015",
     "commercial centre contracts",
     "price negotiated bilaterally with selected providers; one-year terms"),
    (14, "6.73", "Views of the Accounting Officer", "procurement_route", DEPT,
     "Period during which no new expressions-of-interest advertisements were published", None,
     "2009 to July 2015", "date_range", "exact", "2009-2015", "call for expressions of interest",
     "the Accounting Officer's explanation: demand had fallen, then rose sharply again"),
    (14, "6.73", "Views of the Accounting Officer", "residents_centres", DEPT,
     "Increase in asylum seeker numbers, April to August 2015", 1_000, None, "persons", "approx",
     "Apr-Aug 2015", "asylum seekers", "plus a further 400 in September 2015; cited as the urgency "
     "that justified the negotiated route"),
    (9, "6.38", "Reporting of non competitive procurement", "compliance", DEPT,
     "Commercial accommodation contracts included in the annual return of contracts awarded without "
     "a competitive process", None, "Not included", "verdict", "exact", "2015",
     "annual statement to the C&AG and DPER of non-competitive contracts over EUR 25,000 ex-VAT",
     "The Department 'does not accept that these contracts should have been included' (6.39). This "
     "specific finding is NOT re-examined in the 2024 chapter - see the recurrence rows."),
    (9, "6.38", "Reporting of non competitive procurement", "legal_obligation", DEPT,
     "Threshold above which non-competitively awarded contracts must be reported annually", 25_000,
     None, "eur", "exact", "2015", "excluding VAT", None),
]

# ---------------- contract terms: capacity-based payment, penalties -------
R += [
    (9, "6.43", "Contract payment terms", "contracts", "commercial centres",
     "Payment basis under contracts for commercially owned centres", None,
     "Agreed centre CAPACITY, regardless of the level of occupancy", "payment_basis", "exact",
     "2015", "commercial centre contracts",
     "THE ORIGINAL CAPACITY-BASED PAYMENT FINDING. Payment made every four weeks IN ADVANCE. Ten "
     "years later the C&AG finds the identical term still in force (2024, 10.52: 8 of 10 available "
     "contracts based solely on capacity) and quantifies the consequence (10.76: 368 of 1,636 beds "
     "recorded vacant were actually unavailable - and are paid for regardless)."),
    (9, "6.44", "Contract payment terms", "contracts", "commercial centres",
     "Contractual penalty per bed space per day where contracted capacity is not available", 50,
     None, "eur_per_bed_day", "exact", "2015", "commercial centre contracts", None),
    (9, "6.44", "Contract payment terms", "compliance", DEPT,
     "Instances of the capacity penalty being applied", 0, None, "instances", "exact",
     "2011-2015 (5 years)", "commercial centre contracts",
     "'The Department has stated that there were no instances of such payments being required in the "
     "last five years.' The only financial sanction in the contract, never once used. In 2024 the "
     "C&AG again finds no financial penalties applied (10.94)."),
    (10, "6.47", "Contract payment terms", "compliance", "commercial centres",
     "Centres that accommodated more people than contracted for, with no additional payment", None,
     "Occurred - number not quantified", "verdict", "exact", "2015", "commercial centres",
     "the mirror image of the 2024 finding at 10.57 (invoiced capacity 97 vs contracted 92)"),
]

# ---------------- performance standards in contracts ----------------------
R += [
    (10, "6.49", "Setting performance standards", "standards", "commercial centres",
     "Contractual definition of the required standard of accommodation and services", None,
     "'to a standard which is reasonable having regard to the daily needs of asylum seekers' - "
     "'reasonable' is not defined in the contract", "contract_term", "exact", "2015",
     "direct provision contracts", None),
    (10, "6.50", "Setting performance standards", "standards", "commercial centres",
     "Share of contract deliverables stated in measurable terms", 50, None, "percent", "over",
     "2015", "direct provision contract deliverables",
     "'Just over half'. The remainder use 'appropriate', 'adequate', 'as required', or leave the "
     "matter to the contractor's discretion."),
    (11, "6.54", "Setting performance standards", "contracts", "commercial centres",
     "Performance measures set in the contracts", 0, None, "measures", "exact", "2015",
     "direct provision contracts",
     "'No performance measures are set in the contracts and there is no provision in the contract "
     "for penalties for under-performance, other than failure to provide the contracted capacity.'"),
    (11, "6.53", "Setting performance standards", "contracts", "commercial centres",
     "Performance/monitoring obligations present in the contracts", 5, None, "obligations", "exact",
     "2015", "direct provision contracts",
     "daily resident register submitted weekly; right to inspect at all times; 28-day menu cycle on "
     "request; staffing details; quarterly service delivery meetings. Note the weekly register duty "
     "recurs in 2024 (10.75 fn7) - where compliance with it was NOT TRACKED at all in 2024."),
]

# ---------------- inspections, clinics, complaints ------------------------
R += [
    (11, "6.56", "Inspections", "policy_target", DEPT,
     "Target inspections per centre per year", 3, None, "inspections", "exact", "2015",
     "each direct provision centre",
     "two by departmental staff, at least one by an independent fire/food-safety company"),
    (11, "6.57", "Inspections", "inspections", DEPT, "Total inspections of direct provision centres",
     100, None, "inspections", "exact", "2013", "34 centres", None),
    (11, "6.57", "Inspections", "inspections", DEPT, "Total inspections of direct provision centres",
     89, None, "inspections", "exact", "2015", "35 centres",
     "DOWN from 100 in 2013 - 'a decline in the inspection coverage achieved in 2013'"),
    (11, "6.57", "Inspections", "inspections", DEPT,
     "Centres inspected the target three times", 22, None, "centres", "exact", "2015",
     "of 35 centres", "63% - i.e. 13 of 35 centres missed the Department's own target"),
    (12, "6.59", "Inspections", "inspections", DEPT,
     "Inspection reports reviewed by the examination team", 5, None, "reports", "exact", "2015",
     "sample", "majority of findings related to maintenance; fire-safety issues dealt with on the day"),
    (12, "6.58", "Inspections", "contracts", DEPT,
     "Timescales set in the contracts for completion of actions required after an inspection", 0,
     None, "timescales", "exact", "2015", "direct provision contracts",
     "'no timescales are set in the contracts for the completion of required actions'. The 2024 "
     "chapter finds inspection findings are not even risk-rated (10.93) and 3 sampled centres had no "
     "evidence of active follow-up (10.94)."),
    (12, "6.63", "Information clinics", "inspections", DEPT,
     "Target information clinics per centre per year", 2, None, "clinics", "at_least", "2015",
     "each direct provision centre", "achieved in 2015; three clinics held for the majority of "
     "centres; monthly at Mosney, the largest centre"),
    (13, "6.64", "Information clinics", "compliance", DEPT,
     "Management information on issues raised at clinics is formally maintained and analysed", None,
     "No", "verdict", "exact", "2015", "information clinics",
     "'Management information on the issues raised by residents or identified by departmental staff "
     "is not formally maintained and analysed.'"),
    (13, "6.66", "Complaints", "complaints", DEPT,
     "Complaints made to the Department by direct provision residents", 38, None, "complaints",
     "exact", "2012-2015 (4 years)", "all direct provision centres",
     "'a very low level of complaints'. The C&AG reads this as a red flag rather than reassurance "
     "(6.78): a working-group public consultation found significant dissatisfaction. cf. 581 "
     "complaints in 2024 ALONE (C&AG 10.99), once the Ombudsman route existed."),
    (13, "6.68", "Complaints", "compliance", DEPT,
     "Independent appeals officer for the complaints procedure appointed", None,
     "No - not appointed", "verdict", "exact", "2015",
     "the complaints procedure revised in 2015",
     "the Ombudsman and Ombudsman for Children were later given remit over direct provision - one of "
     "the few 2015 gaps that WAS closed (C&AG 2024 10.99 records 21 Ombudsman complaints in 2024)"),
    (13, "6.69", "Complaints", "resident_experience", "direct provision residents",
     "Residents' confidence in the complaints procedure and the inspection process", None,
     "Lack of confidence identified", "verdict", "exact", "2015 working group consultation",
     "public consultation by the Working Group on the Protection Process", None),
]

# ---------------- due diligence / common directors ------------------------
R += [
    (9, "6.40", "Extension of contracts", "contracts", DEPT,
     "Commercial providers whose contracts were not renewed since 2010", 10, None, "providers",
     "exact", "2010-2015", "commercial providers",
     "key contract changes are negotiated ONE MONTH in advance of expiry; changes generally relate "
     "to bed capacity and payment rates"),
    (9, "6.41", "Extension of contracts", "due_diligence", "commercial suppliers",
     "New contracts agreed with a different company where at least one director was common to both",
     None, "Occurred - number not quantified in the text; flagged per centre in Annex A note b",
     "verdict", "exact", "2005-2015", "successive contractors for individual centres",
     "THE 2015 DUE-DILIGENCE FINDING. Annex A also flags companies providing DIFFERENT centres that "
     "are linked by common directors (note a). The C&AG identified these from CRO records - the "
     "Department had not. In 2024 the C&AG finds the Department still does not check provider CRO "
     "numbers against the CRO website (Fig 10.6)."),
    (9, "6.42", "Extension of contracts", "due_diligence", DEPT,
     "Department commitment on provider due diligence", None,
     "'will examine, and where possible, improve due diligence in this area'", "commitment",
     "exact", "2016", "provider/director due diligence",
     "the commitment given in 2016. See the 2024 due-diligence sample: proposal documents available "
     "for 7 of 20 properties, insurance certificates 8 of 20, fire certificates 9 of 20, evidence of "
     "ownership 1 of 20, planning evidence 4 of 20 (C&AG Fig 10.6)."),
]

# ---------------- recommendations (all four accepted) ---------------------
_recs = [
    ("6.1", 14, "The Department should engage with other stakeholders to assess the most appropriate "
     "way to accommodate those granted status permitting them to remain in Ireland.",
     "Agreed. 'While it is not the responsibility of the Department to provide housing for those "
     "legally entitled to remain in the State, the Department will continue to provide whatever "
     "assistance it can to help persons move to permanent accommodation.'"),
    ("6.2", 15, "The Department should review the standard contract and ensure that standards and "
     "timelines are set for all deliverables; that the performance information necessary to assess "
     "whether deliverables have been provided is clearly set out; and that the implications of "
     "failure to provide deliverables are specified.",
     "Agreed. 'All contracts are reviewed and updated as necessary in light of experiences and the "
     "provisions of this recommendation will feed into that process with immediate effect.'"),
    ("6.3", 16, "The Department should collate the results of inspections and information clinics, "
     "and details of complaints by residents, and use the information to assess the performance of "
     "individual centres and of suppliers delivering across a number of centres.",
     "Agreed. 'Formal procedures will be implemented in the future so that learning from inspections "
     "and clinics will be recorded against each centre.'"),
    ("6.4", 16, "The Department should review the complaints process and identify whether there are "
     "factors which may prevent residents from raising issues.",
     "Agreed. 'This recommendation has in fact already largely been implemented.'"),
]
for ref, pg, rec, resp in _recs:
    R.append((pg, f"Recommendation {ref}", "Conclusions and recommendations", "compliance", DEPT,
              f"C&AG Recommendation {ref}", None, rec, "recommendation", "exact", "2016",
              "Accounting Officer response: ACCEPTED",
              f"Accounting Officer's response: {resp}"))

# ---------------- what the 2015 chapter could NOT establish ---------------
R += [
    (4, "6.16", "Requirement for direct provision places", "unknown_at_source", DEPT,
     "Analysis of asylum processing times over the previous ten years to identify causes of delay in "
     "direct provision", None, None, "months", "unknown", "2006-2015", "asylum applications",
     "UNKNOWN AT SOURCE: the examination tried and could not. Before the 2012 AISIP system, the "
     "different systems used at each stage assigned DIFFERENT identification numbers to the same "
     "applicant, so records cannot be linked. 'An analysis of system data would not provide an "
     "accurate indication of processing times.' The State could not measure its own core cost driver."),
    (13, "6.70", "Conclusions and recommendations", "unknown_at_source", DEPT,
     "Causes of delay for residents in direct provision before 2012", None, None, "n/a", "unknown",
     "pre-2012", "direct provision residents",
     "UNKNOWN AT SOURCE: 'data prior to 2012 cannot be readily analysed to identify the causes of "
     "delay (they can be identified by manual review of individual files)'"),
    (3, "6.12", "Requirement for direct provision places", "unknown_at_source", "people with status",
     "Reason status-holders continue to reside in direct provision centres", None, None, "n/a",
     "unknown", "2016", "the 667 residents with status",
     "UNKNOWN AT SOURCE: 'While firm information is not available as to why they continue to reside "
     "in direct provision centres, difficulties in accessing accommodation outside of direct "
     "provision is likely to be a significant factor.' IGEES 2025 finally answers this: the lack of "
     "rental and affordable housing is the main barrier."),
    (10, "6.47", "Contract payment terms", "unknown_at_source", "commercial centres",
     "Number of centres that accommodated more people than contracted for", None, None, "centres",
     "unknown", "2015", "commercial centres",
     "UNKNOWN AT SOURCE: the chapter states the practice occurred ('Some centres have on occasion "
     "accommodated more people than was contracted for') but quantifies neither the centres nor the "
     "persons."),
    (9, "6.41", "Extension of contracts", "unknown_at_source", "commercial suppliers",
     "Number of centres where successive contractors shared common directors", None, None,
     "centres", "unknown", "2005-2015", "commercial centres",
     "UNKNOWN AT SOURCE in the text: the chapter says 'a number of those cases' and defers to the "
     "Annex A per-centre notes (a/b). Those notes are footnote markers attached to Annex rows and "
     "are not reliably recoverable from the text layer - so the COUNT is not asserted here."),
]


# ===========================================================================
# LANE 2 — vector chart recovery (no data labels exist for these series)
# ===========================================================================
def _spans(page):
    return [s for b in page.get_text("dict")["blocks"] if b["type"] == 0
            for l in b["lines"] for s in l["spans"]]


def _polyline(page, n_pts: int):
    """The data polyline on a chart page.

    The axis ticks and gridlines are also stroked paths, so filter on stroke WIDTH:
    every data series in this chapter is drawn at ~1.92pt while axes/ticks are <=0.96
    and gridlines 0.48. Without this the axis-tick path (22 vertices) wins on length.
    """
    cands = []
    for d in page.get_drawings():
        if d.get("color") is None or d.get("fill") is not None:
            continue
        if (d.get("width") or 0) < 1.5:
            continue
        pts = []
        for it in d["items"]:
            if it[0] == "l":
                for p in (it[1], it[2]):
                    q = (round(p.x, 1), round(p.y, 1))
                    if not pts or pts[-1] != q:
                        pts.append(q)
        if len(pts) == n_pts:
            cands.append(pts)
    assert len(cands) == 1, f"expected exactly 1 data polyline of {n_pts} vertices, got {len(cands)}"
    return cands[0]


def _near(colour, target, tol: float = 0.01) -> bool:
    return all(abs(a - b) < tol for a, b in zip(colour, target))


def _yaxis(page, wanted: set[int], xmax: float) -> dict[int, float]:
    out = {}
    for s in _spans(page):
        t = s["text"].strip().replace(",", "")
        if t.isdigit() and int(t) in wanted and s["bbox"][0] < xmax:
            out[int(t)] = (s["bbox"][1] + s["bbox"][3]) / 2
    return out


def recover_fig61(doc) -> list[dict]:
    """Figure 6.1 — expenditure on asylum seekers' accommodation 2007-2016(est), EUR m.
    Line chart, no data labels. Validation: 2015 must reproduce the stated EUR 57m (6.5).
    """
    page = doc[1]
    ax = _yaxis(page, {0, 20, 40, 60, 80, 100}, 215)
    per = (ax[0] - ax[100]) / 100.0
    pts = _polyline(page, 10)
    assert pts and len(pts) == 10, f"Fig 6.1: {pts and len(pts)} vertices, expected 10"
    years = ["2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016 (est)"]
    vals = [round((ax[0] - y) / per, 1) for _, y in pts]
    assert abs(vals[8] - 57) < 1.5, f"Fig 6.1 validation FAILED: 2015 measures {vals[8]} vs stated 57"
    print(f"  Fig 6.1 expenditure EURm: {dict(zip(years, vals))}  (2015 stated 57 -> validated)")
    rows = []
    for yr, v in zip(years, vals):
        rows.append(dict(
            page=2, ref="Fig 6.1", section="Introduction", category="expenditure", subject=DEPT,
            metric="Expenditure on asylum seekers' accommodation",
            value_numeric=round(v) * 1_000_000, value_text=None, unit="eur",
            qualifier="exact" if yr == "2015" else "approx", period=yr, scope=GRAIN,
            is_unknown=False, unknown_reason=None,
            notes=("RECOVERED from Fig 6.1 vector line geometry - the chart carries NO data labels. "
                   f"Calibration validated on 2015 (measured {vals[8]} vs the EUR 57m stated at 6.5). "
                   "2016 is the Department's own ESTIMATE, as printed. Wider scope than Fig 6.6 "
                   "(which is contractor payments only)."),
            extraction_method="vector_polyline_geometry_axis_calibrated",
            confidence="high" if yr == "2015" else "medium"))
    return rows


def recover_fig62(doc) -> list[dict]:
    """Figure 6.2 — number of asylum applicants 2007-2015. Line chart, no data labels.
    Validation against 6.10: 2007 'just under 4,000', 2013 'just under 1,000',
    2015 'over 3,200'.
    """
    page = doc[2]
    ax = _yaxis(page, {0, 1000, 2000, 3000, 4000}, 220)
    per = (ax[0] - ax[4000]) / 4000.0
    pts = _polyline(page, 9)
    assert pts and len(pts) == 9, f"Fig 6.2: {pts and len(pts)} vertices, expected 9"
    years = [str(y) for y in range(2007, 2016)]
    vals = [round((ax[0] - y) / per, -1) for _, y in pts]
    v = dict(zip(years, vals))
    assert 3850 < v["2007"] < 4000, f"Fig 6.2: 2007 = {v['2007']}, expected just under 4,000"
    assert 850 < v["2013"] < 1000, f"Fig 6.2: 2013 = {v['2013']}, expected just under 1,000"
    assert 3200 < v["2015"] < 3400, f"Fig 6.2: 2015 = {v['2015']}, expected just over 3,200"
    print(f"  Fig 6.2 asylum applicants: {v}  (6.10's three anchors -> validated)")
    rows = []
    for yr, val in v.items():
        rows.append(dict(
            page=3, ref="Fig 6.2", section="Requirement for direct provision places",
            category="applications", subject="the State", metric="Asylum applications",
            value_numeric=float(val), value_text=None, unit="applications", qualifier="approx",
            period=yr, scope="the State", is_unknown=False, unknown_reason=None,
            notes=("RECOVERED from Fig 6.2 vector line geometry - the chart carries NO data labels. "
                   "Calibration validated against the three values 6.10 states in words ('just under "
                   "4,000' in 2007, 'just under 1,000' in 2013, 'over 3,200' in 2015). Rounded to the "
                   "nearest 10; treat as approximate."),
            extraction_method="vector_polyline_geometry_axis_calibrated", confidence="medium"))
    return rows


def recover_fig64(doc) -> list[dict]:
    """Figure 6.4 — length of stay in direct provision at July 2016 (12 buckets).
    Column chart, no data labels. Validation: the 7+ years bucket must reproduce the
    ~450 residents stated at 6.15.
    """
    page = doc[3]
    ax = _yaxis(page, {0, 200, 400, 600, 800}, 215)
    per = (ax[0] - ax[800]) / 800.0
    bars = []
    for d in page.get_drawings():
        f = d.get("fill")
        if f and tuple(round(c, 2) for c in f) == (0.76, 0.84, 0.61):
            for it in d["items"]:
                r = it[1]
                bars.append(((r.x0 + r.x1) / 2, r.y0, r.y1))
    bars.sort()
    assert len(bars) == 12, f"Fig 6.4: {len(bars)} bars, expected 12"
    zero = max(b[2] for b in bars)  # shared baseline
    buckets = ["0-3 months", "3-6 months", "6-9 months", "9-12 months", "12-18 months",
               "18-24 months", "2-3 years", "3-4 years", "4-5 years", "5-6 years", "6-7 years",
               "7+ years"]
    vals = [round((zero - y0) / per) for _, y0, _ in bars]
    total = sum(vals)
    assert abs(vals[-1] - 450) < 15, \
        f"Fig 6.4 validation FAILED: 7+ years measures {vals[-1]} vs the stated 450"
    print(f"  Fig 6.4 length of stay: {dict(zip(buckets, vals))}  total ~{total} "
          f"(7+ yrs stated 450 -> validated)")
    rows = []
    for b, v in zip(buckets, vals):
        rows.append(dict(
            page=4, ref="Fig 6.4", section="Requirement for direct provision places",
            category="residents_centres", subject="direct provision residents",
            metric=f"Residents by length of stay - {b}", value_numeric=float(v), value_text=None,
            unit="persons", qualifier="approx", period="Jul 2016",
            scope=f"of ~{total} residents measured across all 12 buckets",
            is_unknown=False, unknown_reason=None,
            notes=("RECOVERED from Fig 6.4 vector bar geometry - the chart carries NO data labels. "
                   f"Calibration validated on the 7+ years bucket (measured {vals[-1]} vs the 'some "
                   "450 people - 10% of the total' stated at 6.15). Length of stay is calculated by "
                   "the Department from each resident's LATEST entry date into a centre (Fig 6.4 note "
                   "a) - so it UNDERSTATES stay for anyone who moved centre. Preserved, not adjusted."),
            extraction_method="vector_bar_geometry_axis_calibrated", confidence="medium"))
    return rows


def recover_fig65(doc) -> list[dict]:
    """Figure 6.5 — occupancy rate 2007-2015, reported every two months (note a).
    54 vertices = 9 years x 6 bi-monthly readings. No data labels.
    """
    page = doc[4]
    ax = _yaxis(page, {75, 80, 85, 90, 95}, 215)
    per = (ax[75] - ax[95]) / 20.0
    pts = _polyline(page, 54)  # 9 years x 6 bi-monthly readings (note a)
    vals = [round((ax[75] - y) / per + 75, 1) for _, y in pts]
    mean = round(sum(vals) / len(vals), 1)
    assert abs(mean - 86) < 1.5, \
        f"Fig 6.5 validation FAILED: mean occupancy measures {mean}% vs the stated 86% (6.21)"
    print(f"  Fig 6.5 occupancy: {len(vals)} bi-monthly points, mean {mean}% "
          f"(stated 86% -> validated), min {min(vals)}% max {max(vals)}%")
    rows = []
    for i, v in enumerate(vals):
        year, period = 2007 + i // 6, i % 6 + 1
        rows.append(dict(
            page=5, ref="Fig 6.5", section="Occupancy rates", category="occupancy", subject=DEPT,
            metric="Occupancy rate of direct provision accommodation", value_numeric=v,
            value_text=None, unit="percent", qualifier="approx",
            period=f"{year} reading {period} of 6", scope="all direct provision accommodation",
            is_unknown=False, unknown_reason=None,
            notes=("RECOVERED from Fig 6.5 vector line geometry - the chart carries NO data labels. "
                   "Fig 6.5 note a states the rate is reported every two months, and the line has "
                   "exactly 54 vertices = 9 years x 6 readings, which fixes the period of each point. "
                   f"Calibration validated on the series mean (measured {mean}% vs the 86% average "
                   "stated at 6.21). The 2010 VFM review's recommended minimum was >90%."),
            extraction_method="vector_polyline_geometry_axis_calibrated", confidence="medium"))
    return rows


def recover_fig67(doc) -> list[dict]:
    """Figure 6.7 — the nine companies paid over EUR 10m each, 2011-2015.

    THE HIGH-VALUE RECOVERY: the company NAMES are real text, but the amounts exist
    only as bar length. Bars are vector rects sharing a left edge; the x-axis '0'..'50'
    (EUR m) labels calibrate them, and each company's y-label centre picks its bar.

    Validation: the two State-owned facilities managers (Aramark, Onsite) are the ONLY
    providers of services in State-owned centres (6.24), so their two bars must sum to
    the EUR 36m stated at 6.26.
    """
    page = doc[6]
    sp = _spans(page)
    ax = {int(s["text"].strip()): (s["bbox"][0] + s["bbox"][2]) / 2 for s in sp
          if s["text"].strip() in ("0", "10", "20", "30", "40", "50") and s["bbox"][1] > 285}
    x0, per = ax[0], (ax[50] - ax[0]) / 50.0

    bars = []
    for d in page.get_drawings():
        f = d.get("fill")
        # the olive series fill; match on tolerance, not rounded equality (0.235 floats
        # to 0.2349999 and rounds DOWN, so an == test on round(c,2) silently misses)
        if f and _near(f, (0.467, 0.576, 0.235)):
            for it in d["items"]:
                r = it[1]
                bars.append(((r.y0 + r.y1) / 2, r.x1))
    assert len(bars) == 9, f"Fig 6.7: {len(bars)} bars, expected 9"

    # company labels: some wrap onto a second line ('(for State-owned)') - keep the
    # first line of each and take its vertical centre
    NAMES = ["Bridgestock Ltd", "East Coast Catering (Ireland) Ltd", "Mosney PLC",
             "Campbell Catering Ltd t/a Aramark", "Millstreet Equestrian Services",
             "Fazyard Ltd", "Onsite Facilities Management Ltd", "Maplestar Ltd",
             "Tattonward Ltd"]
    FULL = {"Campbell Catering Ltd t/a Aramark": "Campbell Catering Ltd t/a Aramark Ireland",
            "Onsite Facilities Management Ltd": "Onsite Facilities Management Ltd"}
    STATE_OWNED = {"Campbell Catering Ltd t/a Aramark", "Onsite Facilities Management Ltd"}
    lab = {}
    for s in sp:
        t = s["text"].strip()
        if t in NAMES:
            lab[t] = (s["bbox"][1] + s["bbox"][3]) / 2
    assert len(lab) == 9, f"Fig 6.7: {len(lab)} company labels matched, expected 9"

    out = {}
    used = set()
    for name, yc in sorted(lab.items(), key=lambda kv: kv[1]):
        # the wrapped labels sit one line HIGHER than their bar centre; pick the nearest
        # unused bar
        cands = [b for b in bars if b not in used]
        b = min(cands, key=lambda b: abs(b[0] - yc))
        used.add(b)
        out[name] = round((b[1] - x0) / per, 1)

    state_sum = round(sum(v for k, v in out.items() if k in STATE_OWNED), 1)
    assert abs(state_sum - 36) < 2.5, (
        f"Fig 6.7 validation FAILED: the two State-owned facilities managers measure "
        f"EUR {state_sum}m vs the EUR 36m stated at 6.26")
    comm_sum = round(sum(v for k, v in out.items() if k not in STATE_OWNED), 1)
    print(f"  Fig 6.7 supplier payments EURm: {out}")
    print(f"    validation: State-owned pair sums to {state_sum} vs the stated 36 -> OK; "
          f"the 7 commercial names sum to {comm_sum} of the EUR 251m commercial total "
          f"({comm_sum / 251 * 100:.0f}%)")

    rows = []
    for name, v in sorted(out.items(), key=lambda kv: -kv[1]):
        so = name in STATE_OWNED
        rows.append(dict(
            page=7, ref="Fig 6.7", section="Contracts for direct provision centres",
            category="expenditure", subject=FULL.get(name, name),
            metric="Payments to direct provision contractor (companies paid over EUR 10m each)",
            value_numeric=round(v * 1_000_000), value_text=None, unit="eur", qualifier="approx",
            period="2011-2015 (5 years)", scope=GRAIN, is_unknown=False, unknown_reason=None,
            notes=("RECOVERED from Fig 6.7 vector bar geometry - the company NAMES are text but the "
                   f"amounts exist only as bar length. Measured EUR {v}m; every bar lands within 0.05 "
                   "of a whole EUR m. Calibration validated against 6.26: the two State-owned "
                   f"facilities managers (the only two, per 6.24) sum to EUR {state_sum}m vs the "
                   "stated EUR 36m."
                   + (" THIS IS A STATE-OWNED-CENTRE FACILITIES-MANAGEMENT contract - competitively "
                      "tendered on eTenders (6.29), unlike the commercial accommodation contracts."
                      if so else " Commercial accommodation provider - awarded WITHOUT any formal "
                      "competitive process (6.30, 6.36).")),
            extraction_method="vector_bar_geometry_axis_calibrated", confidence="medium"))
    return rows


# ---------------- Annex A: all 35 centres, contractor + capacity -----------
# (contractor, centre, capacity, dp_centre_since, ownership)
ANNEX_A = [
    ("Mosney PLC", "Mosney", 600, "December 2000", "commercial"),
    ("Bridgestock Ltd", "Old Convent, Abbey Street, Ballyhaunis", 267, "August 2001", "commercial"),
    ("Bridgestock Ltd", "Globe House, Chapel Hill, Sligo", 226, "August 2004", "commercial"),
    ("Old George Ltd/Fazyard Ltd/Mint Horizon Ltd", "The Towers, Clondalkin", 225, "October 2006", "commercial"),
    ("Old George Ltd/Fazyard Ltd/Mint Horizon Ltd", "Georgian Court, Dublin 1", 110, "January 2005", "commercial"),
    ("Old George Ltd/Fazyard Ltd/Mint Horizon Ltd", "The Montague, Emo, Co Laois", 202, "October 2007", "commercial"),
    ("Old George Ltd/Fazyard Ltd/Mint Horizon Ltd", "The Richmond Court, Co Longford", 80, "July 2015", "commercial"),
    ("Barlow Properties/Bideau Ltd/Stompool Investments Ltd/Baycaster Ltd/D and A Ltd", "Ashbourne House, Glounthane", 95, "June 2000", "commercial"),
    ("Barlow Properties/Bideau Ltd/Stompool Investments Ltd/Baycaster Ltd/D and A Ltd", "Glenvera, Wellington Rd, Cork", 107, "December 2001", "commercial"),
    ("Barlow Properties/Bideau Ltd/Stompool Investments Ltd/Baycaster Ltd/D and A Ltd", "Birchwood, Ballytuckle Rd, Waterford", 125, "May 2001", "commercial"),
    ("Barlow Properties/Bideau Ltd/Stompool Investments Ltd/Baycaster Ltd/D and A Ltd", "Mount Trenchard, Foynes, Co Limerick", 85, "January 2007", "commercial"),
    ("Barlow Properties/Bideau Ltd/Stompool Investments Ltd/Baycaster Ltd/D and A Ltd", "Clonakilty Lodge, Clonakilty, Co Cork", 108, "November 2007", "commercial"),
    ("East Coast Catering (Ireland) Ltd", "Balseskin, St Margaret's, Co Dublin", 310, "December 2001", "commercial"),
    ("East Coast Catering (Ireland) Ltd", "Hatch Hall, 28A Lower Hatch St, Dublin 2", 175, "February 2005", "commercial"),
    ("East Coast Catering (Ireland) Ltd", "Carroll Village, Dundalk (self-catering)", 60, "April 2005", "commercial"),
    ("Millstreet Equestrian Services", "Millstreet", 237, "November 2000", "commercial"),
    ("Millstreet Equestrian Services", "Bridgewater House, Carrick-on-Suir", 95, "December 2001", "commercial"),
    ("Millstreet Equestrian Services", "Viking House, Waterford", 82, "May 2001", "commercial"),
    ("Tattonward Ltd/Mo Bhaile Ltd", "Staircase, 21 Aungier St, Dublin 2", 33, "May 2012", "commercial"),
    ("Tattonward Ltd/Mo Bhaile Ltd", "St. Patricks, Monaghan", 200, "December 2001", "commercial"),
    ("Maplestar Ltd", "Eglington, Salthill, Galway", 200, "January 2000", "commercial"),
    ("Shaun Hennelly", "Great Western House, Galway", 152, "September 2000", "commercial"),
    ("Birch Rentals Ltd", "Hanrattys, Glentworth St, Limerick", 112, "June 2009", "commercial"),
    ("Westbourne Holiday Hostel Ltd", "Westbourne, Dock Rd, Limerick", 90, "June 2001", "commercial"),
    ("Peachport Ltd", "Eyre Powell, Newbridge", 90, "April 2003", "commercial"),
    ("Maison Builders Ltd", "Watergate House, Dublin 8 (self-catering)", 68, "April 2003", "commercial"),
    ("Ocean View Accommodation Ltd", "Ocean View, Tramore", 65, "April 2007", "commercial"),
    ("Atlantic Blue Ltd", "Atlantic House, Tramore", 80, "May 2007", "commercial"),
    ("Campbell Catering Ltd t/a Aramark Ireland", "Kinsale Road", 275, "April 2000", "state_owned"),
    ("Campbell Catering Ltd t/a Aramark Ireland", "Knockalisheen", 250, "October 2001", "state_owned"),
    ("Campbell Catering Ltd t/a Aramark Ireland", "Athlone", 300, "May 2000", "state_owned"),
    ("Onsite Facilities Management (OFM)", "Johnson Marina", 90, "April 2001", "state_owned"),
    ("Onsite Facilities Management (OFM)", "Atlas Tralee", 110, "August 2001", "state_owned"),
    ("Onsite Facilities Management (OFM)", "Atlas Killarney", 90, "January 2002", "state_owned"),
    ("Onsite Facilities Management (OFM)", "Park Lodge", 55, "April 2001", "state_owned"),
]
for contractor, centre, cap, since, own in ANNEX_A:
    R.append((17, "Annex A", "Annex A - Direct provision centres", "capacity", centre,
              f"Contracted capacity - {centre} ({contractor})", cap, None, "beds", "exact",
              "Dec 2015",
              f"{'State-owned centre, facilities management contract' if own == 'state_owned' else 'commercial centre'}",
              f"contractor: {contractor}; a direct provision centre since {since}. Annex A note a: "
              "one or more directors COMMON to the listed companies per CRO records; note b: the "
              "company providing the centre CHANGED since 2010 but shares one or more directors. "
              "The 35 Annex A capacities sum to 5,449 - consistent with the 'over 5,400' at 6.23."))


# ===========================================================================
# LANE 3 — RECURRENCE: which 2015 findings are still live in 2024?
# (finding, verdict, ref_2015, ref_2024, evidence)
# verdict: recurs | recurs_worse | resolved | not_assessed_in_2024
# ===========================================================================
RECURRENCE = [
    ("Accommodation contracts awarded without a formal competitive process",
     "recurs",
     "6.30 / 6.36 (no RFT ever issued in 15 years; EUR 251m awarded 2011-2015 on an "
     "'expressions of interest' list)",
     "10.12 / Annex 10A",
     "PARTIALLY remedied then relapsed. A public RFT finally ran in 2022 - but it yielded only 25 "
     "contracts / 2,612 rooms, and 7 of those had already expired by publication. In the C&AG's 2024 "
     "sample of 20 properties, 15 were DIRECT AWARDS and only 2 came from a request for tender. The "
     "2015 finding - that the State buys this accommodation without competing it - stands."),
    ("Payment based on contracted CAPACITY regardless of actual occupancy",
     "recurs",
     "6.43 (payment based on agreed centre capacity, four weeks in advance)",
     "10.52 / 10.76",
     "IDENTICAL TERM, IDENTICAL DEFECT, TEN YEARS ON. 2024: 8 of the 10 available contracts are "
     "based solely on capacity. The consequence is now quantified: of 1,636 beds recorded vacant "
     "across 19 sampled properties, 368 (22%) were actually UNAVAILABLE - and were paid for anyway."),
    ("The only financial sanction in the contract is never applied",
     "recurs",
     "6.44 / 6.54 (EUR 50 per bed space per day for capacity shortfall; ZERO instances in five "
     "years; no other under-performance penalty exists)",
     "10.94 / 10.85",
     "2024: 'no financial penalties applied' to the 3 sampled centres with no evidence of follow-up. "
     "Contracts now allow a 10-20% withholding for dissatisfaction, but the pattern of a sanction "
     "that exists on paper and is not used persists."),
    ("Contract deliverables not specified in measurable terms; no performance measures",
     "recurs",
     "6.49 / 6.50 / 6.54 + Recommendation 6.2 (ACCEPTED 'with immediate effect')",
     "10.69 / 10.41",
     "Recommendation 6.2 was ACCEPTED in 2016. In 2024 contracts still do not split the "
     "accommodation and food elements of the rate (10.69), 5 of 10 quote VAT-inclusive rates and 5 "
     "VAT-exclusive, and standards enforcement has been displaced onto HIQA under a 2024 SI rather "
     "than the contract itself."),
    ("Weak provider due diligence; corporate structures not checked against the CRO",
     "recurs_worse",
     "6.41 / 6.42 (successive contractors linked by COMMON DIRECTORS, found by the C&AG from CRO "
     "records, not by the Department, which undertook to 'improve due diligence in this area')",
     "Fig 10.6 / 10.32",
     "WORSE. 2024 pre-contract due diligence across 20 sampled properties: completed proposal "
     "documents 7/20, insurance certificates 8/20, fire certificates 9/20 (NONE reflecting proposed "
     "occupancy), evidence of ownership or lease 1/20, planning evidence 4/20, pre-contract "
     "inspections 2/20. The examination team found CRO number mismatches and NO evidence that IPAS "
     "checks provider CRO numbers at all. The 2016 undertaking was not delivered."),
    ("Inspection coverage misses the Department's own target; follow-up not enforced",
     "recurs",
     "6.56 / 6.57 / 6.58 (target 3 inspections per centre; total inspections FELL 100 -> 89; no "
     "contractual timescales for remedial action)",
     "10.91 / 10.93 / 10.94",
     "2024: the target of 2 inspections per commercial property was MISSED; inspection findings are "
     "not risk-rated; 3 sampled centres show no evidence of active follow-up. The 2015 gap - no "
     "contractual deadline for fixing what an inspection finds - was never closed."),
    ("Inspection, clinic and complaint information not collated to assess supplier performance",
     "recurs",
     "6.64 / 6.78 + Recommendation 6.3 (ACCEPTED: 'formal procedures will be implemented in the "
     "future so that learning from inspections and clinics will be recorded against each centre')",
     "10.48",
     "THE STARKEST RECURRENCE. Recommendation 6.3 was accepted in 2016. A compliance tracker was "
     "introduced in APRIL 2025 - nine years later - and the C&AG records that BEFORE THAT, breaches "
     "were not formally logged at all. Within two months it held 118 issues across ~80 providers, "
     "over 80% of them rated 16+ on a 25-point risk scale."),
    ("People granted status remain in State accommodation with no route out",
     "recurs_worse",
     "6.11 / 6.12 / 6.71 + Recommendation 6.1 (ACCEPTED: engage with stakeholders on how best to "
     "accommodate those granted status). 667 residents = 16% of the total, Aug 2016",
     "10.15 / IGEES 2025 s.4",
     "SAME SHARE, 7.9x THE PEOPLE. End-2024: 5,292 people with status still in IPAS - again 16% of "
     "residents. IGEES 2025 prices the cohort at EUR 200m a year (up from EUR 29m in 2020) and finds "
     "45% are still there a year after getting status. Ireland is now 'one of the only EU Member "
     "States' with no time limit and no means-tested contribution. Nine years after the Department "
     "agreed to address it, the Accounting Officer's 2016 position - that housing them is 'not the "
     "responsibility of the Department' - is still the operative one."),
    ("Occupancy below the recommended level while payment stays capacity-based",
     "recurs_worse",
     "6.21 (average occupancy 86% across 2007-2015 vs the 2010 VFM review's recommended >90%)",
     "10.76 / 10.78 / Fig 10.7",
     "WORSE. 2024-25: average occupancy 78.5% across 19 sampled properties; 76% across the 7 "
     "site-visited properties; 61% at the two reception & dispersal centres. Against a good-practice "
     "vacancy buffer of ~15% (EMN), and with payment still based on capacity, the gap between what "
     "is paid for and what is used has WIDENED since 2015."),
    ("The State cannot measure the length of stay that drives its own costs",
     "recurs",
     "6.8 / 6.16 ('the length of stay in direct provision is the factor over which the State can "
     "exercise most control' - yet pre-2012 records could not be linked to measure it)",
     "10.16 fn4 / 10.17 / IGEES s.1",
     "2024-25: the C&AG cannot compute a cost per night from IPAS's own records (10.17) because "
     "State-owned facilities-management costs are booked as commercial; and it cannot compare "
     "monthly spend to residents accommodated (10.16 fn4). IGEES had to BUILD A PROXY out of DSP "
     "Daily Expense Allowance durations because the State does not publish end-to-end processing "
     "times, and it records that length of stay is not captured for all IPAS residents at all. Same "
     "diagnosis, same blind spot, a decade apart."),
    ("Provider overcapacity / invoiced capacity exceeding contracted capacity",
     "recurs",
     "6.47 (centres 'on occasion accommodated more people than was contracted for' - unquantified)",
     "10.57 / 10.60",
     "2024: invoiced capacity of 97 against a contracted 92 at one property (EUR 11,600 potential "
     "overcharge in a single month); at least EUR 15,000 a month overcharged for three contracted "
     "rooms not in use. In 2015 the mismatch cost the State nothing and was not counted; by 2024 the "
     "same loose control of capacity is generating quantified overpayments."),
    ("Non-competitive contracts omitted from the annual return to the C&AG and DPER",
     "not_assessed_in_2024",
     "6.38 / 6.39 (the Department did not include commercial accommodation contracts in the annual "
     "statement of contracts awarded without a competitive process over EUR 25,000, and did not "
     "accept that it should)",
     "not examined",
     "NOT ASSESSED. RoAPS 2024 Chapter 10 does not re-examine the annual non-competitive procurement "
     "return, so NO recurrence verdict can be drawn from the two documents. Recorded as an explicit "
     "unknown rather than assumed either way."),
    ("Residents have no independent complaints appeal and little confidence in the process",
     "resolved",
     "6.68 / 6.69 + Recommendation 6.4 (ACCEPTED). No independent appeals officer had been appointed",
     "10.99",
     "RESOLVED - the clearest thing that actually got fixed. The Ombudsman and Ombudsman for Children "
     "were given remit over direct provision. In 2024 IPAS received 581 resident complaints and the "
     "Ombudsman handled 21 - against just 38 complaints in the FOUR YEARS to 2015, which the C&AG had "
     "read as evidence that the channel was not trusted."),
]


# ===========================================================================
def build(sha: str) -> pl.DataFrame:
    doc = fitz.open(PDF)
    print("chart recovery:")
    recovered = (recover_fig61(doc) + recover_fig62(doc) + recover_fig64(doc)
                 + recover_fig65(doc) + recover_fig67(doc))
    assert sum(c for *_, c, _, _ in ANNEX_A) == 5_449, "Annex A capacities must sum to 5,449"

    rows = []
    for (page, ref, section, cat, subj, metric, v, vtext, unit, qual, period, scope, notes) in R:
        rows.append(dict(
            page=page, ref=ref, section=section, category=cat, subject=subj, metric=metric,
            value_numeric=float(v) if v is not None else None, value_text=vtext, unit=unit,
            qualifier=qual, period=period, scope=scope, is_unknown=(qual == "unknown"),
            unknown_reason=notes if qual == "unknown" else None, notes=notes,
            extraction_method="manual_curation_from_fitz_text_full_read", confidence="high"))
    rows += recovered

    for finding, verdict, r15, r24, evidence in RECURRENCE:
        unk = verdict == "not_assessed_in_2024"
        rows.append(dict(
            page=0, ref=f"2015: {r15}  ->  2024: {r24}",
            section="RECURRENCE ANALYSIS (2015 Ch.6 vs RoAPS 2024 Ch.10)",
            category="compliance", subject="Department / IPAS accommodation contracting",
            metric=f"2015 finding recurs in 2024: {finding}",
            value_numeric=None, value_text=verdict, unit="verdict",
            qualifier="unknown" if unk else "exact", period="2015 -> 2024",
            scope="C&AG RoAPS 2015 Ch.6 compared with C&AG RoAPS 2024 Ch.10 (+ IGEES Jun 2025)",
            is_unknown=unk,
            unknown_reason=("the 2024 chapter does not re-examine this finding, so no recurrence "
                            "verdict can be established from the two documents") if unk else None,
            notes=evidence, extraction_method="cross_document_recurrence_analysis",
            confidence="medium" if unk else "high"))

    out = []
    for i, r in enumerate(sorted(rows, key=lambda r: (r["page"], r["category"])), 1):
        out.append({
            "fact_id": f"{DOC_KEY}-{i:03d}",
            "doc_key": DOC_KEY,
            "doc_title": DOC_TITLE,
            "page": r["page"],
            "printed_page": PRINTED.get(r["page"], "n/a (cross-document analysis)"),
            "ref": r["ref"],
            "section": r["section"],
            "category": r["category"],
            "subject": r["subject"],
            "metric": r["metric"],
            "value_numeric": r["value_numeric"],
            "value_text": r["value_text"],
            "unit": r["unit"],
            "qualifier": r["qualifier"],
            "period": r["period"],
            "scope": r["scope"],
            "is_unknown": r["is_unknown"],
            "unknown_reason": r["unknown_reason"],
            "notes": r["notes"],
            "source_url": SRC_URL,
            "source_document_hash": sha,
            "extraction_method": r["extraction_method"],
            "confidence": r["confidence"],
            "privacy_tier": "public_aggregates_and_bodies",
            "value_safe_to_sum": False,  # audit-report narrative grain - NEVER union with money facts
            "derived_at": now_iso(),
        })
    return pl.DataFrame(out, schema_overrides={"value_numeric": pl.Float64},
                        infer_schema_length=None)


def main() -> None:
    sha = hashlib.sha256(PDF.read_bytes()).hexdigest()
    df = build(sha)
    out = SILVER / "cag_2015_direct_provision_facts.parquet"
    df.write_parquet(out, compression="zstd", statistics=True)
    eye = SILVER / "_eyeball"
    eye.mkdir(exist_ok=True)
    df.write_csv(eye / "cag_2015_direct_provision_facts.csv")
    print(f"\nwrote {out} - {df.height} rows  (sha256={sha[:16]}...)")
    with pl.Config(tbl_rows=30, fmt_str_lengths=60):
        print(df.group_by("category").agg(pl.len(), pl.col("is_unknown").sum().alias("unknown"))
              .sort("len", descending=True))
        print(df.group_by("extraction_method").len())
        print("\nRECURRENCE (2015 -> 2024):")
        print(df.filter(pl.col("extraction_method") == "cross_document_recurrence_analysis")
              .select("value_text", "metric").sort("value_text"))
    print(f"\nunknown rows: {df['is_unknown'].sum()} / {df.height}")
    assert not df["value_safe_to_sum"].any(), "money rows must never be summable"


if __name__ == "__main__":
    main()
