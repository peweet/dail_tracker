"""Comprehensive Accommodation Strategy for International Protection Applicants (DCEDIY, 2024).

Full extraction into the canonical IPAS fact schema (see ipas_doc_registry.py).

THREE THINGS THIS DOCUMENT GIVES US THAT NOTHING ELSE DOES:

1. The State's OWN ADMISSION OF LEGAL BREACH, in its own words:
   "There is a legal obligation on the state to meet the requirements under the EU Recast
    Reception Conditions Directive to provide accommodation to all who request it. For a
    second time this year the state is unable to fulfil these obligations."
   ... and, in the SAME document, it lists "protection from adverse legal decisions" as a
   BENEFIT of the strategy. Both captured verbatim as value_text, category='legal_obligation'.

2. The 2024 BASELINE bed mix — 28,181 beds, of which only 1,184 (4.2%) are State-owned.
   This exists ONLY as printed data labels inside a flat raster chart on p10; it is absent
   from the text layer entirely. Recovered and sum-validated (20,824+6,173+1,184=28,181).

3. The delivery Gantt (p17) — six strands x three phases, 2024-2028. Also raster-only, with
   NO printed dates: recovered by axis-calibrated bar measurement (gridlines 2024..2028 at a
   uniform 202.5 px/year), so dates carry confidence='medium' and are marked as MEASURED,
   never printed.

UPSTREAM ODDITIES ARE PRESERVED AS FLAG ROWS, NEVER SILENTLY FIXED:
  - the Strategy miscites the change-of-use SI as "SI 376 of 2022"; the real instrument is
    SI 376 of 2023 (SI 605 of 2022 is the other one in the chain).
  - it misnames two of the ten National Standards themes ("Heath, Wellbeing and Development";
    "Identification and Response to Special Needs" drops "Assessment").
  - the p17 chart legend misspells "Commerical Property Conversion".

SANDBOX ONLY. All rows value_safe_to_sum=False (policy-target / narrative grain).
"""
from __future__ import annotations

from pathlib import Path

import fitz
import polars as pl

from _common import BRONZE, SILVER, now_iso, sha256_bytes

DOC_KEY = "accommodation_strategy"
DOC_TITLE = "Comprehensive Accommodation Strategy for International Protection Applicants"
SRC_URL = ("https://assets.gov.ie/static/documents/"
           "comprehensive-accommodation-strategy-for-international-protection-applicants.pdf")
PDF = BRONZE / "ipas_context" / "comprehensive_accommodation_strategy.pdf"

# PDF page N prints as "Page | N-2" from p3 onward.
def printed(p: int) -> str | None:
    return str(p - 2) if p >= 3 else None


# (page, ref, section, category, subject, metric, value_numeric, value_text, unit,
#  qualifier, period, scope, notes)
T: list[tuple] = []

# ---------------- 1. Introduction: the scale shock ----------------
T += [
    (3, "Intro", "1. Introduction", "capacity", "State",
     "Additional people accommodated (IP + Ukraine)", 100_000, None, "persons", "over",
     "24 months to 2024", "State total (IP + BOTP)",
     "'the State has mobilised to provide accommodation to over 100,000 additional people in "
     "24 months, or 2% of the State's population'"),
    (3, "Intro", "1. Introduction", "capacity", "State",
     "Additional people accommodated as a share of the State population", 2, None, "percent",
     "exact", "24 months to 2024", "State population", "the Strategy's own framing"),
    (3, "Intro", "1. Introduction", "capacity", "IP reception system",
     "Annual arrivals the reception system was DESIGNED to cope with", 3_000, None,
     "persons_per_year", "at_least", "system design", "lower bound of the 3,000-4,000 range",
     "'The present International Protection reception and accommodation system was designed to "
     "cope with approximately 3,000 - 4,000 arrivals per year.' This is the denominator of the "
     "whole crisis: see the 13,000-16,000/yr projection below."),
    (3, "Intro", "1. Introduction", "capacity", "IP reception system",
     "Annual arrivals the reception system was DESIGNED to cope with", 4_000, None,
     "persons_per_year", "under", "system design", "upper bound of the 3,000-4,000 range",
     "upper bound of the design range"),
    (3, "Intro", "1. Introduction", "applications", "State",
     "People who sought protection (IP)", 25_000, None, "persons", "exact",
     "2022-2023 ('the last two years')", "IP applicants",
     "'an additional and unexpected 25,000 people have sought protection in the last two years'"),
    (3, "Intro", "1. Introduction", "capacity", "State",
     "People from Ukraine accommodated from the same commercial sources", 75_000, None,
     "persons", "exact", "2022-2023", "BOTP (Ukraine)",
     "the Strategy's own point: IP and Ukraine competed for the SAME commercial beds"),
    (4, "Intro", "1. Introduction", "policy_target", "White Paper to End Direct Provision",
     "Annual new arrivals the White Paper was based on", 3_500, None, "persons_per_year",
     "exact", "White Paper (2021)", "White Paper planning assumption",
     "'it was originally based on 3,500 new arrivals each year'. The PID (project_initiation_"
     "document) governs this model. The Strategy says the assumptions 'have shifted "
     "dramatically' - compare against the 13,000-16,000/yr projection."),
    (11, "Near Term Strategy", "2. Near Term Strategy", "capacity", "State",
     "People accommodated before the growth (baseline)", 7_000, None, "persons", "exact",
     "pre-2022", "people seeking refuge",
     "'the State moving from accommodating 7,000 people seeking refuge to accommodating over "
     "100,000'"),
]

# ---------------- 2. THE LEGAL BREACH ADMISSION (verbatim) ----------------
T += [
    (4, "Intro", "1. Introduction", "legal_obligation", "the State",
     "State's own admission that it is in breach of its accommodation obligation", None,
     "Due to the significant challenges outlined, the Department is currently unable to offer "
     "accommodation to new single male applicants. There is a legal obligation on the state to "
     "meet the requirements under the EU Recast Reception Conditions Directive to provide "
     "accommodation to all who request it. For a second time this year the state is unable to "
     "fulfil these obligations.",
     "text", "exact", "2024", "all who request accommodation",
     "VERBATIM. The State admitting, in its own published strategy, that it is failing a legal "
     "obligation - and for the SECOND time in one year. The obligation arises under the EU "
     "Recast Reception Conditions Directive (transposed by SI 230/2018; Reg 6 provides no "
     "capacity defence). Corroborated by C&AG 10.9: 3,285 single male IP applicants "
     "unaccommodated at end 2024."),
    (15, "3. Key Benefits", "3. Key Benefits", "legal_obligation", "the State",
     "'Protection from adverse legal decisions' listed as a BENEFIT of the strategy", None,
     "Ensure compliance with EU and International legal obligations, and protection from "
     "adverse legal decisions.",
     "text", "exact", "2024", "the State (not applicants)",
     "VERBATIM, from the Key Benefits list. Read against the breach admission on p4, the "
     "document frames legal compliance as insulation for the STATE against litigation, "
     "alongside - not subordinate to - the applicants' right to accommodation."),
    (15, "3. Key Benefits", "3. Key Benefits", "legal_obligation", "the State",
     "Date the Recast Reception Conditions Directive was transposed into Irish law", None,
     "July 2018", "date", "exact", "2018", "Ireland",
     "'the Recast Reception Conditions Directive, which Ireland transposed into law in July "
     "2018' (= SI 230/2018). The breach admitted on p4 is of an obligation the State had "
     "already carried for ~6 years."),
]

# ---------------- 3. The 2028 target: 35,000 beds ----------------
T += [
    (6, "Revised Larger Capacity Approach", "2. New Accommodation Strategy", "policy_target",
     "IP accommodation system", "Total bed capacity target", 35_000, None, "beds", "under",
     "by end 2028", "whole IP accommodation system (State + commercial)",
     "'a system with capacity for up to 35,000 by the end of 2028'. Sum-validated against the "
     "p10 chart: 13,000 + 1,000 + 11,000 + 10,000 = 35,000."),
    (6, "Revised Larger Capacity Approach", "2. New Accommodation Strategy", "policy_target",
     "IP accommodation system", "Projected annual arrivals underpinning the 35,000 target",
     13_000, None, "persons_per_year", "at_least", "2024-2028", "lower bound of 13,000-16,000",
     "'assuming that an average of 13,000-16,000 persons arrive between 2024 and 2028'. This is "
     "3.25-5.3x the 3,000-4,000/yr the system was designed for, and 3.7-4.6x the White Paper's "
     "3,500/yr."),
    (6, "Revised Larger Capacity Approach", "2. New Accommodation Strategy", "policy_target",
     "IP accommodation system", "Projected annual arrivals underpinning the 35,000 target",
     16_000, None, "persons_per_year", "under", "2024-2028", "upper bound of 13,000-16,000",
     "upper bound of the projection range"),
    (6, "Revised Larger Capacity Approach", "2. New Accommodation Strategy", "policy_target",
     "State-owned estate", "State-owned permanent beds to be delivered", 14_000, None, "beds",
     "exact", "by end 2028", "State-owned (RIC + AC + in-community)",
     "'will look to deliver 14,000 state owned permanent beds' = 13,000 RIC/AC + 1,000 "
     "in-community. Matches C&AG 10.5 exactly."),
    (6, "Revised Larger Capacity Approach", "2. New Accommodation Strategy", "policy_target",
     "status holders", "Time a person with status may remain in accommodation after grant",
     12, None, "months", "exact", "2024 policy", "persons granted protection status",
     "'persons with status move from their accommodation after a specified time (currently one "
     "year after grant of status)'. The 35,000 target DEPENDS on this exit actually happening - "
     "C&AG 10.15 found 5,292 status-holders still in IPAS accommodation at end 2024."),
]
# the 2028 breakdown table (p7)
_2028 = [
    ("Reception and Integration Centres and Accommodation Centres, at national standards",
     13_000, "State Owned", "the core of the new model; 6-month RIC stay then move to an AC"),
    ("In-Community Accommodation for vulnerable persons, at national standards",
     1_000, "State Owned, operated in partnership with NGOs",
     "currently 37 houses / up to 200 beds - a 5x scale-up required"),
    ("Contingency Accommodation, at national standards", 11_000, "Commercial Providers",
     "the office-conversion strand: leased to the State for 2-4 years"),
    ("Emergency Accommodation", 10_000, "Commercial Providers",
     "NOTE: this is the ONLY one of the four categories NOT qualified 'at national standards' "
     "in the source table - and it is the largest commercial block"),
]
for name, beds, owner, note in _2028:
    T.append((7, "p7 table", "2. New Accommodation Strategy - Revised Larger Capacity Approach",
              "policy_target", owner, f"2028 bed capacity - {name}", beds, None, "beds", "under",
              "by end 2028", f"{owner}; 'Up to {beds:,}'",
              f"Source table 'By the end of 2028 it is estimated that a breakdown of the "
              f"accommodation offering will be'. Every row is 'Up to' - these are CEILINGS, not "
              f"commitments. {note}"))

# ---------------- 4. Approach for delivery: the real current numbers ----------------
T += [
    (9, "Approach for Delivery", "2. Approach for Delivery", "policy_target",
     "IP applicants", "Minimum stay in a Reception and Integration Centre", 6, None, "months",
     "at_least", "2024 policy", "each IP applicant",
     "'Accommodation in a Reception and Integration Centre will be available to each IP "
     "applicant for at least 6 months', with an orientation/integration/supports programme; "
     "applicants then move to an Accommodation Centre until they exit the process."),
    (9, "Approach for Delivery", "2. Approach for Delivery", "capacity",
     "State-owned sites", "State-owned sites under consideration for Rapid Build", 3, None,
     "sites", "exact", "2024", "existing State-owned sites",
     "'Three sites are currently under consideration and Rapid Build will be initiated dependant "
     "on proposed technical considerations.' The sites are NOT named - see the UNKNOWN row."),
    (10, "Commercial and Private Providers", "2. Approach for Delivery", "policy_target",
     "commercial and private providers", "Persons to be accommodated under the commercial strand",
     9_000, None, "persons", "exact", "under the new model", "existing IPAS centres, upgraded",
     "'It is envisioned that 9,000 persons will be accommodated under this strand.' Note the "
     "unit: PERSONS, not beds - it is not stated whether these 9,000 sit inside the 11,000 "
     "contingency / 10,000 emergency ceilings or alongside them (see UNKNOWN row)."),
    (10, "Commercial and Private Providers", "2. Approach for Delivery", "capacity",
     "commercial providers", "Beds currently contracted in apartments", 2_844, None, "beds",
     "exact", "2024 (at publication)", "apartment stock within the commercial strand",
     "'(2844 beds are currently contracted in apartments)'. A rare hard current-state number in "
     "this document."),
    (10, "State owned Accommodation", "2. Approach for Delivery", "capacity",
     "State-owned in-community stock", "Houses purchased in the community for vulnerable cohorts",
     37, None, "houses", "exact", "2024 (at publication)", "in-community vulnerable strand",
     "'(37 houses currently with a capacity of up to 200 beds)'. Against a 2028 target of 1,000 "
     "in-community beds."),
    (10, "State owned Accommodation", "2. Approach for Delivery", "capacity",
     "State-owned in-community stock", "Bed capacity of the 37 community houses", 200, None,
     "beds", "under", "2024 (at publication)", "in-community vulnerable strand",
     "'up to a capacity of up to 200 beds' - i.e. 20% of the 1,000-bed 2028 target is in place."),
]

# ---------------- 5. Near-term strategy ----------------
T += [
    (11, "Near Term Strategy", "2. Near Term Strategy", "capacity", "adult males",
     "Adult males awaiting an offer of accommodation", 1_000, None, "persons", "over",
     "2024 (at publication)", "unaccommodated",
     "'over 1,000 adult males are awaiting offers of accommodation, with this number set to "
     "increase'. The Strategy also warns 'families, including women and children could find "
     "themselves without an offer of accommodation in the coming weeks or months'. C&AG 10.9 "
     "records 3,285 unaccommodated single males by end 2024."),
    (11, "Conversion of commercial properties", "2. Near Term Strategy", "housing_impact",
     "commercial real estate market", "Commercial vacancy rate in some counties", 15, None,
     "percent", "at_least", "2024", "'some Counties'",
     "'Research has indicated that some Counties have commercial vacancy rates of 15% or "
     "higher.' Attributed to the Central Bank 'Regulatory Supervisory outlook report 2024'. The "
     "counties are not named and the research is not cited - see UNKNOWN row."),
    (12, "Conversion of commercial properties", "2. Near Term Strategy", "policy_target",
     "office conversions", "Delivery time frame for a commercial-property conversion", 5, None,
     "months", "at_least", "2024-2025", "lower bound of the 5-9 month range",
     "'Time frame for delivery can be anything from 5 to 9 months'"),
    (12, "Conversion of commercial properties", "2. Near Term Strategy", "policy_target",
     "office conversions", "Delivery time frame for a commercial-property conversion", 9, None,
     "months", "under", "2024-2025", "upper bound of the 5-9 month range", "upper bound"),
    (12, "Conversion of commercial properties", "2. Near Term Strategy", "contracts",
     "commercial providers", "Lease term to the State for converted commercial properties", 2,
     None, "years", "at_least", "2024-2025", "lower bound of the 2-4 year range",
     "'Properties would be delivered by the commercial sector and leased to the State for 2 to "
     "4 years.' These become 'Contingency Accommodation' (the 11,000-bed category)."),
    (12, "Conversion of commercial properties", "2. Near Term Strategy", "contracts",
     "commercial providers", "Lease term to the State for converted commercial properties", 4,
     None, "years", "under", "2024-2025", "upper bound of the 2-4 year range", "upper bound"),
    (12, "HSE and State lands", "2. Near Term Strategy", "capacity", "HSE",
     "Locations agreed to be leased from the HSE", 2, None, "locations", "exact", "2024", "HSE",
     "'The Department has recently agreed to lease two locations from the HSE, along with a site "
     "from the Department of Justice.' Locations not named - see UNKNOWN row."),
    (12, "HSE and State lands", "2. Near Term Strategy", "capacity", "Department of Justice",
     "Sites agreed to be taken from the Department of Justice", 1, None, "sites", "exact",
     "2024", "Department of Justice", "as above"),
    (13, "Targeted purchases", "2. Near Term Strategy", "unit_cost", "the State",
     "Capital payback period on State-purchased properties vs the lease model", 4, None, "years",
     "approx", "2024", "purchased properties",
     "'State purchased solutions are far more cost effective than the current lease model, with "
     "capital pay back in the region of 4 years (sometimes less).' No underlying cost figures, "
     "no methodology and no total capital requirement are given anywhere in the document - see "
     "the UNKNOWN rows. Compare IGEES: EUR 92/night private vs EUR 34/night State-owned."),
    (13, "Selection criteria", "2. Selection criteria", "policy_target",
     "accommodation portfolio", "Spare-capacity threshold that triggers ending contracts", 10,
     None, "percent", "over", "future", "accommodation portfolio",
     "'As the State moves to a more stable footing and there is over 10% spare capacity in the "
     "accommodation portfolio ... the above criteria will be applied to those properties "
     "currently in the accommodation portfolio with a view to ending contracts.' Compare the "
     "EMN good-practice vacancy buffer of ~15% (C&AG 10.78) and the C&AG's measured 24% average "
     "vacancy across its 7 site visits (Fig 10.7)."),
]

# the 5 selection criteria + 3 key enablers, verbatim
_CRITERIA = [
    "Areas within a high density of IPs/BOTPs already will be deprioritised",
    "Target larger urban locations",
    "End reliance of hotels that are the 'last hotel' or last amenity in a community",
    "A priority on speed of delivery",
    "Ensure a cost benefit to the State, especially on purchased properties",
]
for i, c in enumerate(_CRITERIA, 1):
    T.append((13, f"Selection criteria {i}/5", "2. Selection criteria", "policy_target",
              "site selection", f"Selection criterion {i} of 5", None, c, "text", "exact",
              "2024", "purchase and lease options (NOT State-land builds)",
              "VERBATIM. Explicitly does NOT apply to building on State lands: 'Building on "
              "State lands is somewhat dependent on what lands/properties are offered by "
              "Departments/Agencies. As such, this Department will not have significant "
              "flexibility in terms of locations.' No weighting, threshold or scoring method is "
              "given for any criterion - they are not operationalised."))
_ENABLERS = [
    ("Capital funding - the State build and purchase options requires Capital funding to support "
     "both build and purchase options",
     "NO capital figure is stated anywhere in the document (see UNKNOWN row) - the entire "
     "strategy is conditional on an unquantified ask."),
    ("Extension of SI 376 of 2022 - change of use of properties - from expiring in December 2028 "
     "to expiring in December 2030 to encourage the commercial sector to invest in office "
     "conversions.",
     "MISCITATION - see the DQ flag row: the change-of-use instrument is SI 376 of 2023, not "
     "2022."),
    ("Support of Local Authorities and other organs of the State in delivering solutions on the "
     "ground.",
     "No mechanism, MoU or obligation is specified; C&AG 10.28 found IPAS keeps no record even "
     "of the change-of-use notifications it owes local authorities."),
]
for i, (e, note) in enumerate(_ENABLERS, 1):
    T.append((14, f"Key enabler {i}/3", "2. Specialist and Technical Expertise", "policy_target",
              "delivery of the strategy", f"Key enabler {i} of 3", None, e, "text", "exact",
              "2024", "whole strategy",
              "VERBATIM. The Strategy states its 'successful delivery will be hugely dependent "
              "on the following key enablers being in place'. " + note))

# ---------------- 6. Standards, governance, misc ----------------
T += [
    (8, "Standards of Accommodation", "2. Standards of Accommodation", "standards",
     "National Standards", "Themes in the National Standards framework", 10, None, "themes",
     "exact", "2024", "all IP accommodation",
     "The Strategy reproduces the ten themes - see national_standards_lookup.parquet for the "
     "40 standards and 306 indicators beneath them. Two theme names are reproduced INCORRECTLY "
     "- see the DQ flag rows."),
    (14, "Specialist and Technical Expertise", "2. Specialist and Technical Expertise",
     "policy_target", "Migration Agency", "A Migration Agency is contemplated", None,
     "The partnerships developed will form the basis for the structures that will be required "
     "if a Migration Agency is set-up.",
     "text", "exact", "2024", "future machinery of government",
     "VERBATIM. Conditional ('if'), undated, unfunded."),
    (4, "Intro", "1. Introduction", "policy_target", "IP applicant families",
     "A new International Protection Child Payment is to be provided", None,
     "Starting shortly, a new International Protection Child Payment will be provided to all "
     "applicant families in the system, which will provide an increase on the weekly allowance "
     "presently granted in respect of children.",
     "text", "exact", "2024", "all applicant families",
     "VERBATIM. No rate, no start date, no cost - 'starting shortly'."),
]

# ---------------- 7. DQ / upstream-oddity FLAG ROWS (never silently fixed) ----------------
T += [
    (14, "Key enabler 2/3", "2. Specialist and Technical Expertise", "unknown_at_source",
     "SI citation in the Strategy",
     "DQ FLAG: the Strategy miscites the change-of-use SI as 'SI 376 of 2022'", None,
     "Extension of SI 376 of 2022 - change of use of properties - from expiring in December 2028 "
     "to expiring in December 2030",
     "text", "exact", "2024", "one of the three key enablers",
     "PRESERVED, NOT FIXED. The planning change-of-use exemption relied on is SI 376 of 2023 "
     "(Planning and Development (Exempted Development) (No. 2) Regulations 2023), which runs to "
     "31 December 2028 - exactly the expiry the Strategy quotes. SI 376 of 2022 is a different "
     "instrument entirely. The related 2022 instrument in this chain is SI 605 of 2022. The "
     "Strategy has the YEAR wrong while quoting the 2023 instrument's expiry date correctly, so "
     "this is a citation error, not a different instrument. A key enabler of national policy is "
     "misidentified in the published strategy."),
    (8, "Standards of Accommodation", "2. Standards of Accommodation", "unknown_at_source",
     "National Standards theme names in the Strategy",
     "DQ FLAG: the Strategy misnames two of the ten National Standards themes", None,
     "Theme 9 is printed as 'Heath, Wellbeing and Development' (sic - 'Heath' for 'Health'); "
     "Theme 10 is printed as 'Identification and Response to Special Needs', dropping "
     "'Assessment' from the real title 'Identification, Assessment and Response to Special "
     "Needs'. Theme 5 also drops a comma ('Food Catering and Cooking Facilities').",
     "text", "exact", "2024", "the ten themes as reproduced on p8",
     "PRESERVED, NOT FIXED. Authoritative theme names are in national_standards_lookup.parquet "
     "(parsed from the National Standards themselves). Join on the standards doc, never on the "
     "Strategy's rendering of the names."),
]

# ---------------- 8. EXPLICIT UNKNOWNS ----------------
U: list[tuple] = [
    (None, "whole document", "expenditure", "the strategy",
     "Total capital funding required to deliver the 35,000-bed strategy",
     "UNKNOWN AT SOURCE: the Strategy names capital funding as key enabler #1 and repeatedly "
     "makes delivery 'dependent on ... the availability of required capital funding', but states "
     "NO figure - no total, no annual profile, no per-bed cost, anywhere in its 18 pages. There "
     "is no costing, no appendix and no reference to one."),
    (None, "whole document", "unit_cost", "the strategy",
     "The 'very significant savings' the strategy claims over commercial expenditure",
     "UNKNOWN AT SOURCE: the Strategy asserts it will 'incur very significant savings over "
     "current expenditure allocated to commercial providers' and 'Delivers significant cost "
     "savings for the taxpayer', but quantifies neither, and gives no methodology. (The only "
     "arithmetic offered anywhere is a ~4-year capital payback, itself unsupported.)"),
    (17, "Timelines chart", "policy_target", "the six delivery strands",
     "Beds each delivery strand will produce, and in what year",
     "UNKNOWN AT SOURCE: the p17 chart is a Gantt of PHASES ONLY (Planning / Build-Upgrade-"
     "Purchase / New Beds) - it carries no bed quantities on any bar and no y-axis of volume. "
     "The document therefore never says how the 35,000 beds decompose across its six delivery "
     "strands, nor how many beds land in each year. Bar START/END DATES were recovered by "
     "axis-calibrated measurement (see the chart rows); the QUANTITIES are simply not in the "
     "document."),
    (9, "Approach for Delivery", "capacity", "State-owned sites",
     "Identity/location of the 3 State-owned sites under consideration for Rapid Build",
     "UNKNOWN AT SOURCE: 'Three sites are currently under consideration' - not named, not "
     "located, not sized."),
    (12, "HSE and State lands", "capacity", "HSE / Department of Justice sites",
     "Identity/location of the 2 HSE locations and 1 Department of Justice site",
     "UNKNOWN AT SOURCE: agreed to be leased, but not named, located or sized. No bed count is "
     "attached to them."),
    (10, "Commercial and Private Providers", "policy_target", "commercial strand",
     "Whether the 9,000 persons under the commercial strand sit INSIDE the 11,000 contingency / "
     "10,000 emergency ceilings or in addition to them",
     "UNKNOWN AT SOURCE: the 9,000 is quoted in PERSONS while the 2028 table is in BEDS, and the "
     "document never reconciles the two. Do not add the 9,000 to the 35,000 - the grains differ "
     "and the overlap is unstated."),
    (11, "Conversion of commercial properties", "housing_impact", "commercial real estate",
     "Which counties have commercial vacancy rates of 15% or higher",
     "UNKNOWN AT SOURCE: 'Research has indicated that some Counties have commercial vacancy "
     "rates of 15% or higher' - the counties are not named and the 'research' is not cited. The "
     "Central Bank report named alongside it (Regulatory & Supervisory Outlook 2024) is cited "
     "for the market vulnerability, not for the county figure."),
    (6, "Distribution Model", "policy_target", "distribution criteria",
     "The equitable-geographic-distribution criteria themselves",
     "UNKNOWN AT SOURCE: the Strategy says 'A set of criteria WILL BE developed' taking account "
     "of 'current IP and BOTP numbers, population density and availability of public service' - "
     "the criteria do not exist in this document. The 5 'selection criteria' on p13 are a "
     "DIFFERENT list, governing which property offers to accept, not geographic equity."),
]


# ---------------- 9. RASTER CHART RECOVERY ----------------
# p10 "Current & Future International Protection Accommodation Portfolio Overview".
# A flat raster; its DATA LABELS ARE PRINTED ON THE CHART and read directly from the rendered
# image. Not measured, not inferred - and both stacks sum-validate exactly (asserted below).
CHART_P10_CURRENT = [  # (label, ownership, beds)
    ("Emergency accommodation", "Commercial", 20_824),
    ("Permanent IPAS Accommodation", "Commercial", 6_173),
    ("Permanent IPAS Accommodation", "State Owned", 1_184),
]
CHART_P10_FUTURE = [
    ("Emergency Accommodation", "Commercial", 10_000),
    ("Contingency Accommodation", "Commercial", 11_000),
    ("In community Accommodation", "State Owned", 1_000),
    ("Permanent IPAS Accommodation", "State Owned", 13_000),
]
CHART_P10_TOTAL_CURRENT, CHART_P10_TOTAL_FUTURE = 28_181, 35_000
CHART_P10_PRINCIPLES = [
    "Delivering a permanent, sustainable and agile system of both State owned and privately "
    "sourced accommodation",
    "An increased focus on addressing vulnerabilities, specific provisions for children and "
    "people with special needs",
    "Commercial entities will be required to provide a high standard of reception conditions, "
    "in line with the National Standards",
    "A strong focus on community engagement to ensure planning for the provision of essential "
    "public service and to assist with the welcome and integration of new arrivals",
]

# p17 Gantt: measured from the raster, calibrated on the year gridlines.
GANTT_PDF_PAGE, GANTT_XREF = 17, 1837
GANTT_STRANDS = ["Prefab/Modular State", "Commerical Property Conversion",
                 "Prefab/Modular Private Lands", "Purchase Turnkey Properties",
                 "Design & Build R&I Centres", "Upgrade IPAS Centres"]
GANTT_PHASES = {(166, 166, 166): "Planning", (26, 146, 72): "Build/Upgrade/Purchase",
                (179, 164, 124): "New Beds"}


def measure_gantt(doc) -> list[dict]:
    """Recover the p17 delivery Gantt: per strand x phase, the measured start/end date.

    The chart is a flat raster with NO text layer, so there is nothing to calibrate against in
    the PDF: the year gridlines are located IN THE IMAGE (uniform 202.5 px/year, verified) and
    the bars measured against them. Dates are therefore MEASURED, never printed -> medium
    confidence, +-1 month. Bed quantities do not exist on this chart (explicit UNKNOWN row).
    """
    img = doc.extract_image(GANTT_XREF)
    pix = fitz.Pixmap(img["image"])
    W, H, N = pix.width, pix.height, pix.n
    buf = bytes(pix.samples)  # cache once: per-pixel .samples access rebuilds the buffer

    def px(x, y):
        i = (y * W + x) * N
        return (buf[i], buf[i + 1], buf[i + 2])

    # 1. year gridlines: full-height greys inside the plot, not bar-coloured, not background
    grid = []
    for x in range(W):
        n = sum(1 for y in range(70, 560)
                if px(x, y) not in GANTT_PHASES and px(x, y) not in
                ((242, 242, 242), (255, 255, 255)) and abs(px(x, y)[0] - px(x, y)[2]) < 12
                and px(x, y)[0] < 250)
        # >200: the 2025-2027 gridlines are partly OVERPAINTED by bars, so their visible
        # grey run is shorter (250-350 px) than the unobstructed 2024/2028 lines (~450).
        if n > 200:
            grid.append(x)
    clusters: list[list[int]] = []
    for x in grid:
        if clusters and x - clusters[-1][-1] <= 3:
            clusters[-1].append(x)
        else:
            clusters.append([x])
    xs = [sum(c) / len(c) for c in clusters]
    xs = [x for x in xs if 250 < x < 1150]  # drop the outer image frame
    assert len(xs) == 5, f"expected 5 year gridlines (2024..2028), found {len(xs)}: {xs}"
    steps = [b - a for a, b in zip(xs, xs[1:])]
    assert max(steps) - min(steps) < 4, f"gridlines not uniform: {steps}"
    x0, per_year = xs[0], sum(steps) / len(steps)

    def to_date(x: float) -> tuple[float, str]:
        # clamped to the chart's own plotted domain (2024-01 .. 2028-12): a bar physically
        # cannot start before the axis origin, so the sub-pixel overshoot at a bar's rounded
        # left cap is antialiasing, not data. This is a bound, not a correction of a value.
        yr = min(2028.99, max(2024.0, 2024 + (x - x0) / per_year))
        y = int(yr)
        m = min(12, max(1, round((yr - y) * 12) + 1))
        return round(yr, 2), f"{y}-{m:02d}"

    # 2. bar pixels, indexed by (scanline, phase colour)
    seg: dict[tuple[int, tuple], list[int]] = {}
    for y in range(70, 560):
        for x in range(int(x0) - 5, 1160):
            c = px(x, y)
            if c in GANTT_PHASES:
                seg.setdefault((y, c), []).append(x)

    # 3. Cluster PER PHASE COLOUR, not per row. A Gantt row carries exactly one bar of each
    #    phase, and bars of the same colour are always separated vertically -- whereas the three
    #    phase bars WITHIN a row are sometimes contiguous (merging two strands into one band)
    #    and sometimes separated by a gap (splitting one strand into two). So the Nth run of a
    #    given colour, top to bottom, is strand N. Asserted: exactly 6 runs per colour.
    bars: dict[str, list[tuple[int, int]]] = {}
    for colour, phase in GANTT_PHASES.items():
        ys = sorted(y for (y, c) in seg if c == colour)
        runs: list[list[int]] = []
        for y in ys:
            if runs and y - runs[-1][-1] <= 4:
                runs[-1].append(y)
            else:
                runs.append([y])
        runs = [r for r in runs if len(r) >= 8]        # drop the legend swatches
        assert len(runs) == len(GANTT_STRANDS), \
            f"{phase}: expected 6 bars, found {len(runs)} ({[(r[0], r[-1]) for r in runs]})"
        ext = []
        for r in runs:
            xsx = [x for y in r for x in seg.get((y, colour), [])]
            ext.append((min(xsx), max(xsx)))
        bars[phase] = ext

    out = []
    for i, strand in enumerate(GANTT_STRANDS):
        for phase in ("Planning", "Build/Upgrade/Purchase", "New Beds"):
            xa, xb = bars[phase][i]
            s_yr, s_lab = to_date(xa)
            e_yr, e_lab = to_date(xb + 1)
            out.append({"strand": strand, "phase": phase, "start": s_lab, "end": e_lab,
                        "start_year_frac": s_yr, "end_year_frac": e_yr,
                        "months": round((e_yr - s_yr) * 12)})
    return out


def build() -> pl.DataFrame:
    sha = sha256_bytes(Path(PDF).read_bytes())
    doc = fitz.open(PDF)
    rows: list[dict] = []

    def add(page, ref, section, cat, subj, metric, vnum, vtext, unit, qual, per, scope, notes,
            *, unknown=False, ureason=None, method="manual_curation_from_fitz_text_full_read",
            conf="high"):
        rows.append({
            "page": page, "printed_page": printed(page) if page else None, "ref": ref,
            "section": section, "category": cat, "subject": subj, "metric": metric,
            "value_numeric": float(vnum) if vnum is not None else None, "value_text": vtext,
            "unit": unit, "qualifier": qual, "period": per, "scope": scope,
            "is_unknown": unknown, "unknown_reason": ureason, "notes": notes,
            "extraction_method": method, "confidence": conf,
        })

    for r in T:
        add(*r)
    for (pg, ref, cat, subj, metric, reason) in U:
        add(pg, ref, "whole document" if pg is None else ref, cat, subj, metric, None, None,
            None, "unknown", "2024", "not established by the document", None,
            unknown=True, ureason=reason)

    # --- p10 chart: printed data labels ---
    assert sum(b for _, _, b in CHART_P10_CURRENT) == CHART_P10_TOTAL_CURRENT
    assert sum(b for _, _, b in CHART_P10_FUTURE) == CHART_P10_TOTAL_FUTURE
    M = "raster_chart_printed_data_labels"
    NOTE10 = ("Chart 'Current & Future International Protection Accommodation Portfolio "
              "Overview' (p10) is a FLAT RASTER with no text layer - these values are absent "
              "from the PDF text entirely and were read from the labels PRINTED on the chart. "
              "Sum-validated against the chart's own printed totals (20,824+6,173+1,184=28,181; "
              "10,000+11,000+1,000+13,000=35,000).")
    add(10, "p10 chart", "2. Approach for Delivery", "capacity", "IP accommodation system",
        "Total bed capacity - CURRENT STATE", CHART_P10_TOTAL_CURRENT, None, "beds", "exact",
        "2024", "whole IP accommodation system",
        NOTE10 + " THE KEY BASELINE: 28,181 beds today vs 35,000 by 2028 = +6,819 net beds "
        "(+24%), but the MIX changes far more than the total.", method=M)
    add(10, "p10 chart", "2. Approach for Delivery", "policy_target", "IP accommodation system",
        "Total bed capacity - FUTURE STATE", CHART_P10_TOTAL_FUTURE, None, "beds", "exact",
        "2028", "whole IP accommodation system", NOTE10, method=M)
    for label, owner, beds in CHART_P10_CURRENT:
        add(10, "p10 chart - current state", "2. Approach for Delivery", "capacity", owner,
            f"2024 beds - {label} ({owner})", beds, None, "beds", "exact", "2024",
            f"{owner}; {beds / CHART_P10_TOTAL_CURRENT * 100:.1f}% of the 28,181 current beds",
            NOTE10, method=M)
    for label, owner, beds in CHART_P10_FUTURE:
        add(10, "p10 chart - future state", "2. Approach for Delivery", "policy_target", owner,
            f"2028 beds - {label} ({owner})", beds, None, "beds", "exact", "2028",
            f"{owner}; {beds / CHART_P10_TOTAL_FUTURE * 100:.1f}% of the 35,000 target",
            NOTE10, method=M)
    # the single most important derived comparison, stated by the chart itself
    add(10, "p10 chart", "2. Approach for Delivery", "capacity", "State-owned estate",
        "State-owned share of beds - CURRENT STATE", round(1_184 / 28_181 * 100, 1), None,
        "percent", "exact", "2024", "1,184 of 28,181 beds",
        NOTE10 + " Only 4.2% of today's estate is State-owned; the 2028 model requires 40.0% "
        "(14,000 of 35,000). That is the whole strategy in one number.", method=M)
    add(10, "p10 chart", "2. Approach for Delivery", "policy_target", "State-owned estate",
        "State-owned share of beds - FUTURE STATE", 40.0, None, "percent", "exact", "2028",
        "14,000 of 35,000 beds", NOTE10, method=M)
    for i, p in enumerate(CHART_P10_PRINCIPLES, 1):
        add(10, f"p10 chart - principle {i}/4", "2. Approach for Delivery", "policy_target",
            "the new model", f"Principle {i} of 4 of the revised model", None, p, "text",
            "exact", "2024", "whole strategy",
            "VERBATIM from the printed text inside the p10 raster chart (absent from the text "
            "layer).", method=M)

    # --- p17 Gantt: measured ---
    GNOTE = ("Measured from the p17 raster Gantt ('International Protection Accommodation "
             "Delivery Timelines'), calibrated on its 2024-2028 year gridlines (uniform "
             "202.5 px/year). Dates are MEASURED, NOT PRINTED: precision ~+-1 month, so "
             "confidence=medium. The chart carries NO bed quantities - see the UNKNOWN row.")
    gantt = measure_gantt(doc)
    assert len(gantt) == 18, f"expected 6 strands x 3 phases = 18 bars, measured {len(gantt)}"
    for g in gantt:
        typo = (" [sic: the chart legend misspells 'Commerical' - preserved verbatim]"
                if g["strand"].startswith("Commerical") else "")
        add(17, "p17 Gantt", "5. Timelines", "policy_target", g["strand"],
            f"Delivery timeline - {g['strand']}: {g['phase']} phase", g["months"],
            f"{g['start']} to {g['end']}", "months", "approx", f"{g['start']}/{g['end']}",
            f"one of 6 delivery strands; phase '{g['phase']}' of 3",
            GNOTE + typo, method="raster_bar_measurement_axis_calibrated", conf="medium")
    add(17, "5. Timelines", "5. Timelines", "policy_target", "the strategy",
        "Timelines are explicitly conditional", None,
        "These timelines however will be dependent on market appetite and capacity and, "
        "critically, the availability of required capital funding for the programme.",
        "text", "exact", "2024", "whole delivery programme",
        "VERBATIM. The delivery schedule is conditional on capital funding that the document "
        "never quantifies (see UNKNOWN row).")

    out = []
    for i, r in enumerate(sorted(rows, key=lambda r: (r["page"] or 999, r["category"])), 1):
        out.append({"fact_id": f"{DOC_KEY}-{i:03d}", "doc_key": DOC_KEY, "doc_title": DOC_TITLE,
                    **r, "source_url": SRC_URL, "source_document_hash": sha,
                    "privacy_tier": "public_document", "value_safe_to_sum": False,
                    "derived_at": now_iso()})
    cols = ["fact_id", "doc_key", "doc_title", "page", "printed_page", "ref", "section",
            "category", "subject", "metric", "value_numeric", "value_text", "unit", "qualifier",
            "period", "scope", "is_unknown", "unknown_reason", "notes", "source_url",
            "source_document_hash", "extraction_method", "confidence", "privacy_tier",
            "value_safe_to_sum", "derived_at"]
    return pl.DataFrame(out, schema_overrides={"value_numeric": pl.Float64, "page": pl.Int64},
                        infer_schema_length=None).select(cols)


def main() -> None:
    df = build()
    out = SILVER / "accommodation_strategy_facts.parquet"
    df.write_parquet(out, compression="zstd", statistics=True)
    eye = SILVER / "_eyeball"
    eye.mkdir(exist_ok=True)
    df.write_csv(eye / "accommodation_strategy_facts.csv")
    print(f"wrote {out} - {df.height} rows")
    with pl.Config(tbl_rows=30, fmt_str_lengths=60, tbl_width_chars=170):
        print(df.group_by("category").agg(pl.len(), pl.col("is_unknown").sum().alias("unknown"))
              .sort("len", descending=True))
        print(df.group_by("extraction_method").len())
        print("\nGantt (measured):")
        print(df.filter(pl.col("ref") == "p17 Gantt")
              .select("subject", "metric", "value_text", "value_numeric").head(20))
    print(f"\nunknown rows: {df['is_unknown'].sum()} / {df.height}")
    assert not df["value_safe_to_sum"].any()


if __name__ == "__main__":
    main()
