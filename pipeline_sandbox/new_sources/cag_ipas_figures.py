"""Structured figure extraction: C&AG RoAPS 2024 Chapter 10 —
"Management of international protection accommodation contracts".

SANDBOX ONLY. Source of truth = the born-digital PDF text (fitz), read in full;
every row below was hand-curated from that text with page + paragraph refs.
NOTHING is inferred: figures that exist only in chart/map/glyph form, or that
the C&AG itself could not establish, are recorded as EXPLICIT UNKNOWN rows
(value_numeric null, qualifier='unknown', unknown_reason set) rather than
guessed. All money rows are value_safe_to_sum=False — these are audited
NARRATIVE figures (audit-report grain), never to be unioned with the
payments/awards/budget money facts.

Row tuple: (page, ref, category, metric, value_numeric, unit, qualifier,
            period, scope, notes)
qualifier: exact | approx | almost | over | under | at_least | unknown
"""
from __future__ import annotations

import hashlib
from pathlib import Path

import polars as pl

from _common import BRONZE, SILVER, now_iso

PDF = BRONZE / "cag_reports" / "pdf" / \
    "10-management-of-international-protection-accommodation-contracts-copy.pdf"
SRC_URL = ("https://www.audit.gov.ie/media/kudhih1z/"
           "10-management-of-international-protection-accommodation-contracts-copy.pdf")

R = []  # (page, ref, category, metric, value, unit, qual, period, scope, notes)

# ---------------- expenditure (Figure 10.3, paras 10.14, 10.19) ----------------
R += [
    (5, "Fig 10.3", "expenditure", "Accommodation and related services for IP applicants, total", 1_066_000_000, "eur", "exact", "2024", "IPAS/Vote 40", "described as 'almost EUR 1.1 billion' in 10.14; structure: commercial 978.0m + other 88.2m"),
    (5, "Fig 10.3", "expenditure", "Accommodation and ancillary services - commercial", 978_000_000, "eur", "approx", "2024", "commercial providers", "note c: all contracted accommodation incl Citywest Transit Hub; some State-owned facilities-management costs commingled here (note f)"),
    (5, "Fig 10.3", "expenditure", "Other (non-commercial) expenditure bucket", 88_200_000, "eur", "exact", "2024", "IPAS/Vote 40", "= State-owned 12.5m + other fees 16.2m + grant funding/supports 59.5m; note d: includes EUR 41.7m to Tusla for separated children, LA integration teams, CYPSC projects, NGO/community supports"),
    (5, "Fig 10.3", "expenditure", "Accommodation - State-owned", 12_500_000, "eur", "exact", "2024", "State-owned properties", "note f: facilities mgmt, utilities, incidentals; where one operator serves both, costs may sit under commercial"),
    (5, "Fig 10.3", "expenditure", "Other fees", 16_200_000, "eur", "exact", "2024", "IPAS/Vote 40", "note e: consultancy, transport, inspections, licence fees, State Claims Agency, settlements"),
    (5, "Fig 10.3 note e", "expenditure", "Capital expenditure on purchase of modular homes (within other fees)", 3_700_000, "eur", "exact", "2024", "IPAS/Vote 40", None),
    (5, "Fig 10.3", "expenditure", "Grant funding and supports", 59_500_000, "eur", "exact", "2024", "IPAS/Vote 40", None),
    (5, "Fig 10.3 note d", "expenditure", "Payments to Tusla, primarily housing separated children seeking IP (within 'Other')", 41_700_000, "eur", "exact", "2024", "Tusla", None),
    (4, "10.14", "expenditure", "Increase in IP-applicant-related expenditure 2019 to 2024", 8, "ratio", "over", "2019-2024", "IPAS", "'more than eightfold'"),
    (6, "10.19", "expenditure", "Payments to top seven commercial providers (each > EUR 20m)", 230_000_000, "eur", "almost", "2024", "7 commercial providers", "24% of the EUR 978m commercial spend; 5 of 7 provide emergency accommodation, 1 long-term, 1 facilities mgmt/catering"),
]
# supplier-level values exist only as chart bars (Figure 10.4) -> UNKNOWN
for s, nprops in [("A", 8), ("B", 2), ("C", 3), ("D", 6), ("E", 1), ("F", 3), ("G", 1)]:
    R.append((6, "Fig 10.4", "expenditure", f"Aggregate 2024 payments to supplier {s} ({nprops} properties)", None, "eur", "unknown", "2024", f"supplier {s}", "UNKNOWN: value shown only as chart bar; known bound: exceeds EUR 20m (Fig 10.4 includes only >EUR 20m suppliers); supplier A note d includes set-up costs at centres never designated active"))
R += [
    (5, "Fig 10.3", "expenditure", "Annual expenditure series 2019-2023 (per-year values)", None, "eur", "unknown", "2019-2023", "IPAS", "UNKNOWN: chart-only; text layer carries only the 2024 callouts and the 'eightfold' ratio"),
]

# ---------------- overpayments / control findings ----------------
R += [
    (12, "10.49", "overpayment", "Overcharging identified by contracts team - 7 cases", 4_500_000, "eur", "almost", "as of late Aug 2025 (periods back to 2023)", "7 cases / contract renewals", "recoupment agreed for 5, 2 pending; 2 cases relate to one provider"),
    (12, "10.49", "overpayment", "Overcharges identified by bedspace audit (one provider)", 557_600, "eur", "exact", "Nov 2023-Nov 2024 and Jan-Jul 2025", "one provider", "recoupment plan agreed"),
    (12, "10.49", "overpayment", "Unsanctioned transition from full board to self-catering", 69_000, "eur", "approx", "Nov 2024-Mar 2025", "one provider", "recoupment agreed"),
    (13, "10.59", "overpayment", "VAT overcharged by one provider group (3 VAT registrations)", 7_400_000, "eur", "exact", "Mar 2022-Dec 2023", "one provider group", "includes the EUR 2.3m below; provider refunded EUR 1.5m to date; recovery of balance under review"),
    (13, "10.58", "overpayment", "VAT incorrectly charged on emergency accommodation, six properties", 2_300_000, "eur", "exact", "1 Mar 2022-30 Jun 2023", "one provider group", "emergency accommodation is VAT exempt"),
    (13, "10.58", "overpayment", "VAT refund netted against one invoice (Apr 2024)", 490_000, "eur", "under", "Apr 2024", "one provider group", "'just under EUR 490,000'; EUR 1.8m less than the six-property overcharge"),
    (13, "10.59", "overpayment", "VAT refunds received to date from the provider", 1_500_000, "eur", "exact", "to publication", "one provider group", None),
    (13, "10.57", "overpayment", "VAT charged on exempt accommodation element in four sampled payments", 884_000, "eur", "exact", "2024", "4 of 40 sampled payments", None),
    (13, "10.57", "overpayment", "Potential overcharge - invoiced capacity 97 vs contracted 92", 11_600, "eur", "approx", "Feb 2024", "one property", None),
    (14, "10.60", "overpayment", "Monthly overcharge for three contracted rooms not in use", 15_000, "eur_per_month", "at_least", "identified 2025", "one property", "single-occupancy daily rate EUR 169; forensic investigation of register returns planned"),
    (14, "10.66", "overpayment", "Overpayments at two sampled hybrid centres", 310_000, "eur", "exact", "2024", "2 hybrid centres", "recouped (via Vote 40)"),
    (15, "10.67", "overpayment", "Known potential overpayments under nine hybrid arrangements", 1_300_000, "eur", "approx", "as of Mar 2025", "9 hybrid arrangements", "includes the EUR 310,000 above; all recouped per IPAS"),
    (23, "10.117", "overpayment", "Further overpayments from contract non-compliances (~10 cases)", 5_100_000, "eur", "approx", "identified by late Aug 2025 (periods 2023-Jul 2025)", "~10 cases", "recoupment agreed in most; 2 pending"),
]

# ---------------- residents & centres (Figure 10.1 time series) ----------------
_f101 = {
    "Total IP applicants accommodated": ("persons", [7683, 6997, 7244, 19104, 26279, 32702, 32689]),
    "Total accommodation centres": ("centres", [78, 73, 70, 154, 258, 326, 324]),
    "IP applicants in emergency accommodation": ("persons", [1512, 1148, 1046, 10869, 17862, 24718, 25221]),
    "Emergency accommodation centres": ("centres", [37, 28, 24, 106, 204, 269, 269]),
    "IP applicants in IPAS long-term accommodation": ("persons", [5731, 5575, 5737, 6853, 7071, 6518, 6447]),
    "IPAS long-term accommodation centres": ("centres", [40, 44, 45, 46, 49, 49, 50]),
    "IP applicants in other State-owned accommodation": ("persons", [440, 274, 461, 1382, 1346, 1466, 1021]),
    "Other State-owned accommodation centres": ("centres", [1, 1, 1, 2, 5, 8, 5]),
}
_periods = ["end 2019", "end 2020", "end 2021", "end 2022", "end 2023", "end 2024", "end Jun 2025"]
for metric, (unit, vals) in _f101.items():
    for period, v in zip(_periods, vals):
        R.append((2, "Fig 10.1", "residents_centres", metric, v, unit, "exact", period, "State-provided IP accommodation", "State-owned incl National Reception Centre, Citywest Transit Hub (2022+), tented (2023+)"))

R += [
    (2, "10.8 fn1", "residents_centres", "Children among accommodated IP applicants", 9_015, "persons", "exact", "end 2024", "IPAS residents", "28% of 32,702"),
    (2, "10.8 fn2", "residents_centres", "State-owned centres among the 49 IPAS long-term centres", 7, "centres", "exact", "end 2024", "IPAS long-term", None),
    (2, "10.9", "residents_centres", "Single male IP applicants unaccommodated", 3_285, "persons", "exact", "end 2024", "unaccommodated", None),
    (2, "10.9", "residents_centres", "Single male IP applicants unaccommodated", 2_577, "persons", "exact", "end Jun 2025", "unaccommodated", None),
    (2, "10.9", "residents_centres", "Single male IP applicants unaccommodated", 720, "persons", "approx", "late Aug 2025", "unaccommodated", None),
    (4, "10.15/fn2", "residents_centres", "Persons with protection status still in State-provided accommodation", 5_292, "persons", "exact", "end 2024", "IPAS residents", "~16% of IPAS residents (IGEES); 'almost 5,300' in body text"),
    (4, "10.15", "residents_centres", "Persons with protection status remaining in IPAS accommodation", 5_000, "persons", "approx", "end Jul 2025", "IPAS residents", "with permission"),
    (2, "10.10", "residents_centres", "Change in emergency-centre residents, end 2024 to end Jun 2025", 2, "percent", "approx", "H1 2025", "emergency accommodation", "increase"),
    (2, "10.10", "residents_centres", "Change in long-term + State-owned residents, end 2024 to end Jun 2025", -6, "percent", "approx", "H1 2025", "IPAS long-term + State-owned", "decrease"),
    (14, "10.62", "residents_centres", "Contracted bed context cited by Department", 32_000, "beds", "over", "2025", "IPAS estate", None),
    (17, "10.80", "residents_centres", "IP applicant arrivals per month", 1_000, "persons_per_month", "over", "2025", "arrivals", None),
]

# ---------------- applications & processing ----------------
R += [
    (1, "10.3 fn3", "applications", "IP applications in the year", 13_600, "applications", "over", "2022", "State", "'just over 13,600'"),
    (1, "10.3 fn3", "applications", "IP applications in the year", 18_500, "applications", "over", "2024", "State", "'over 18,500'"),
    (4, "10.16", "applications", "Total IP applications", 9_000, "applications", "under", "2019-2021", "State", "'just under 9,000'"),
    (4, "10.16", "applications", "Total IP applications", 45_000, "applications", "almost", "2022-2024", "State", None),
    (4, "10.15", "applications", "Median end-to-end IP application processing time", 17, "months", "approx", "as of May 2024", "IGEES external estimate", "started decreasing late 2024"),
]

# ---------------- unit costs & standards ----------------
R += [
    (4, "10.18", "unit_cost", "Average cost per night, privately provided (mostly emergency) accommodation", 92, "eur_per_person_night", "exact", "2024", "IGEES analytical paper Jun 2025", "occupancy-weighted monthly average"),
    (4, "10.18", "unit_cost", "Average cost per night, State-owned accommodation", 34, "eur_per_person_night", "exact", "2024", "IGEES analytical paper Jun 2025", None),
    (4, "10.17", "unit_cost", "Average cost per night computed from IPAS's own records", None, "eur_per_person_night", "unknown", "2024", "IPAS records", "UNKNOWN: C&AG could not calculate - State-owned facilities-management costs recorded as commercial when one invoice covers both"),
    (4, "10.16 fn4", "unit_cost", "Monthly expenditure vs applicants-accommodated comparison", None, "n/a", "unknown", "2024", "IPAS records", "UNKNOWN: not possible per C&AG - payments in a month do not necessarily relate to that month's accommodation"),
    (10, "10.41", "standards", "Minimum space requirement per resident per bedroom", 4.65, "sq_m", "exact", "national standards 2019", "all IP accommodation", "bunk beds prohibited for persons 15+ unless requested"),
    (16, "10.78", "standards", "Good-practice vacancy buffer of available beds", 15, "percent", "approx", "EMN study 2014", "reception facilities", None),
]

# ---------------- contracts & procurement ----------------
R += [
    (3, "10.12", "contracts", "Contracts entered via the 2022 public RFT (re-advertised Nov 2022)", 25, "contracts", "exact", "2022 process", "IPAS long-term", None),
    (3, "10.12", "contracts", "Rooms provided under the 25 RFT contracts", 2_612, "rooms", "exact", "2022 process", "IPAS long-term", None),
    (3, "10.12", "contracts", "RFT contracts since expired", 7, "contracts", "exact", "at publication", "IPAS long-term", None),
    (9, "10.33", "contracts", "Centres with active ('in date') contracts", 164, "centres", "exact", "end 2024", "of 325 expected contracts (326 centres, 2 share one contract)", None),
    (9, "10.33", "contracts", "Centres operating without in-date contracts", 161, "centres", "exact", "end 2024", "housing 13,785 IP applicants", "60 recorded as in renewal process"),
    (9, "10.33", "contracts", "Centres with contract status NOT RECORDED by the IPAS", 101, "centres", "unknown", "end 2024", "of the 161 without in-date contracts", "UNKNOWN AT SOURCE: status unrecorded by IPAS"),
    (9, "10.34", "contracts", "Centres with active contracts", 168, "centres", "exact", "end Jul 2025", "marginal increase from 164", "expired contracts continue on original terms until renewal/termination"),
    (14, "10.63", "contracts", "Accommodation centres under hybrid IP/BOTP arrangements", 25, "centres", "exact", "during 2024", "hybrid arrangements", "separate invoices for IP and BOTP"),
    (10, "10.36", "contracts", "Sampled properties without signed contracts covering 2024 payments", 10, "properties", "exact", "2024", "sample of 20", "50%; payments made on non-contractual terms"),
    (12, "10.52", "contracts", "Available contracts based solely on capacity", 8, "contracts", "exact", "sample review", "of 10 available contracts", "2 based on capacity + occupancy supplement"),
    (15, "10.69", "contracts", "Available contracts quoting VAT-inclusive rates", 5, "contracts", "exact", "sample review", "of 10 available contracts", "other 5 VAT-exclusive; neither type split accommodation vs food"),
    (17, "10.85", "contracts", "Contractual withholding allowed when dissatisfied with service", 10, "percent", "exact", "current contracts", "lower bound of 10-20% range", "in practice 100% withheld on any compliance/invoicing issue; watchlist maintained"),
]

# ---------------- due diligence sample (20 properties; Fig 10.6, 10.26, 10.104) ----------------
R += [
    (8, "Fig 10.6", "due_diligence", "Completed proposal documents available", 7, "properties", "exact", "pre-contract", "sample of 20", "35%"),
    (8, "Fig 10.6", "due_diligence", "Insurance certificates available", 8, "properties", "exact", "pre-contract", "sample of 20", "40%"),
    (8, "Fig 10.6", "due_diligence", "Appropriate fire certificates available", 9, "properties", "exact", "pre-contract", "sample of 20", "45%; NONE reflected proposed occupancy; 5 further certs deficient (wrong year/building/use/2005-vintage/fewer rooms 22 vs 26)"),
    (8, "Fig 10.6", "due_diligence", "Evidence of ownership or lease provided", 1, "properties", "exact", "pre-contract", "sample of 20", "5% (one lease agreement)"),
    (8, "Fig 10.6", "due_diligence", "Evidence of planning permission / exemption application", 4, "properties", "exact", "pre-contract", "sample of 20", "20%"),
    (9, "10.32", "due_diligence", "Pre-contract inspections carried out", 2, "properties", "exact", "pre-contract", "sample of 20", "10% (10.104); of 9 contracts signed 2024, only 1 property inspected pre-contract (10.32)"),
    (8, "Fig 10.6", "due_diligence", "CRO number mismatches found by examination team", 1, "providers", "exact", "pre-contract", "sample of 20", "no evidence IPAS itself checked provider CRO numbers against the CRO website"),
    (7, "10.26", "due_diligence", "Insurance certificates on file after remediation", 13, "properties", "exact", "end Aug 2025", "sample of 20", "Department remediation after examination"),
    (7, "10.26", "due_diligence", "Fire certificates / fire-safety assurance on file after remediation", 15, "properties", "exact", "end Aug 2025", "sample of 20", None),
    (7, "10.26", "due_diligence", "Evidence of property ownership on file after remediation", 10, "properties", "exact", "end Aug 2025", "sample of 20", None),
    (7, "10.26", "due_diligence", "Planning documentation on file after remediation", 10, "properties", "exact", "end Aug 2025", "sample of 20", None),
    (7, "10.26 fn4", "due_diligence", "Sample-property contracts terminated by IPAS", 1, "contracts", "exact", "by Aug 2025", "sample of 20", None),
]

# ---------------- compliance tracker & payments review ----------------
R += [
    (11, "10.48", "compliance", "Issues recorded on the compliance tracker", 118, "issues", "exact", "mid-Jun 2025", "~80 providers", "tracker introduced Apr 2025; before that breaches not formally logged"),
    (11, "10.48", "compliance", "Tracker issues with risk rating 16 or higher (max 25)", 80, "percent", "over", "mid-Jun 2025", "of 118 issues", "~90% of high-rated relate to unavailable beds (10.110)"),
    (11, "10.48", "compliance", "Tracker issues relating to unavailable beds", 89, "percent", "exact", "mid-Jun 2025", "of high-rated issues", None),
    (11, "10.48", "compliance", "Tracker issues relating to health and safety", 9, "percent", "exact", "mid-Jun 2025", "of high-rated issues", None),
    (11, "10.48", "compliance", "Tracker issues relating to overcharging and overcapacity", 2, "percent", "exact", "mid-Jun 2025", "of high-rated issues", None),
    (13, "10.55/10.57", "compliance", "Payments reviewed by examination team", 40, "payments", "exact", "2024", "20 sampled properties", None),
    (13, "10.57", "compliance", "Reviewed payments where invoiced rate unclear/unverifiable", 14, "payments", "exact", "2024", "of 40 reviewed", "35% - signed contracts unavailable"),
    (12, "10.49", "compliance", "Sample properties inspected in both 2024 and 2025 showing improved standards", 14, "properties", "exact", "2024-2025", "of 20 inspected", "2 of the 20 closed due to inspection findings"),
]

# ---------------- occupancy ----------------
R += [
    (16, "10.76", "occupancy", "Average occupancy across 19 sampled properties (IPAS spreadsheet)", 78.5, "percent", "exact", "24 Jun 2025", "19 of 20 sample (1 contract terminated Jun 2025)", None),
    (16, "10.76", "occupancy", "Vacant beds per IPAS spreadsheet", 1_636, "beds", "exact", "24 Jun 2025", "19 sampled properties", None),
    (16, "10.76", "occupancy", "Vacant beds confirmed by provider follow-up calls", 1_268, "beds", "exact", "24 Jun 2025", "19 sampled properties", "368 beds (22%) recorded vacant were actually unavailable - payments are capacity-based, so overpayment risk"),
]
_f107 = [
    ("Transit hub", 650, 330, 320, 51, "22 May 2025"),
    ("Other", 149, 135, 14, 91, "27 May 2025"),
    ("Hotel", 500, 370, 130, 74, "29 May 2025"),
    ("Hotel", 842, 780, 62, 93, "30 May 2025"),
    ("Apartments", 98, 87, 11, 89, "3 Jun 2025"),
    ("Hotel", 80, 71, 9, 89, "4 Jun 2025"),
    ("Hotel", 144, 101, 43, 70, "5 Jun 2025"),
]
for i, (typ, cap, occ, unused, rate, date) in enumerate(_f107, 1):
    R.append((16, "Fig 10.7", "occupancy", f"Site-visit property {i} ({typ}): capacity", cap, "beds", "exact", date, f"site-visit sample property {i}", f"occupied {occ}, unused {unused}, occupancy {rate}%"))
R += [
    (16, "Fig 10.7", "occupancy", "Site-visit sample total capacity / occupied / unused", 2_463, "beds", "exact", "May-Jun 2025", "7 properties", "occupied 1,874; unused 589; occupancy 76%; avg vacancy 24%"),
    (16, "Fig 10.7", "occupancy", "Occupancy excluding the two reception & dispersal centres", 89, "percent", "exact", "May-Jun 2025", "5 of 7 properties (capacity 1,313)", "vacancy 11%"),
    (16, "Fig 10.7", "occupancy", "Occupancy of the two reception & dispersal centres", 61, "percent", "exact", "May-Jun 2025", "2 of 7 properties (capacity 1,150)", "vacancy 39%; Dept: high availability by design for surges; arrivals >1,000/month"),
    (16, "10.79", "occupancy", "Vacant beds not available for use at time of visits", 60, "beds", "exact", "May-Jun 2025", "of 589 vacant", "room configuration, maintenance, turnaround, held bookings"),
]

# ---------------- inspections (Figure 10.8) ----------------
_f108 = [
    ("2022", 2, 86, 88, 51, 154, 33, "37 centres inspected twice"),
    ("2023", 45, 3, 48, None, 258, 19, None),
    ("2024", 244, 56, 300, 267, 326, 82, "IPAS incl 30 pre-occupancy; external incl 4; 33 double-inspections"),
]
for yr, ipas, ext, tot, uniq, ncent, cov, note in _f108:
    R.append((18, "Fig 10.8", "inspections", "IPAS inspections of accommodation centres", ipas, "inspections", "exact", yr, "all centres", note))
    R.append((18, "Fig 10.8", "inspections", "External inspections of accommodation centres", ext, "inspections", "exact", yr, "all centres", None))
    R.append((18, "Fig 10.8", "inspections", "Inspection coverage of centres", cov, "percent", "exact", yr, f"{ncent} centres", f"total inspections {tot}" + (f"; unique centres {uniq}" if uniq else "")))
R += [
    (18, "10.91", "inspections", "Year-on-year increase in number of centres (2024 target of 2 inspections per commercial property missed)", 26, "percent", "exact", "2024", "all centres", None),
    (19, "10.93", "inspections", "Sampled properties with inspection reports reviewed", 18, "properties", "exact", "2024 (16) + 2025 (2)", "sample of 20", "findings not risk-rated; issues incl electrical equipment, mould/damp, overdue fire inspections, food storage, no defibrillators"),
    (19, "10.94", "inspections", "Sampled centres with no evidence of active follow-up of inspection issues", 3, "centres", "exact", "2024-2025", "sample review", "all re-inspected within 8 months; no financial penalties applied"),
]

# ---------------- complaints ----------------
R += [
    (20, "10.99", "complaints", "Complaints received by IPAS from IP residents", 581, "complaints", "exact", "2024", "all centres", "30% re centre management/staff behaviour; 20% re other residents"),
    (20, "10.99", "complaints", "Complaints to the Ombudsman re IPAS accommodation", 21, "complaints", "exact", "2024", "Ombudsman", None),
    (20, "10.98", "complaints", "Customer service clinics held", 129, "clinics", "exact", "2024", "IPAS", "just under 100 clinics Jan-Aug 2025"),
]

# ---------------- policy targets ----------------
R += [
    (1, "10.5", "policy_target", "Target State-owned bed capacity under Mar 2024 accommodation strategy", 14_000, "beds", "exact", "by 2028", "Comprehensive Accommodation Strategy", "aims to reduce reliance on commercial providers"),
    (1, "10.5", "policy_target", "Commercial contingency/emergency beds allowed under strategy", 21_000, "beds", "under", "by 2028", "Comprehensive Accommodation Strategy", "'up to a further 21,000'"),
]

# ---------------- Annex 10A: 20 sample properties ----------------
_annex = [
    ("Dormitory", "BOTP centre pivoted to IP centre", "North Dublin", None, "rate unclear per C&AG"),
    ("Apartment complex", "Request for tender", "Meath", 70.0, None),
    ("Hotel", "Direct award", "Limerick", 170.0, "EUR 170 single occupancy + EUR 40 per additional person"),
    ("Hotel", "Direct award", "South Dublin", 167.0, "EUR 167 single occupancy + EUR 20 per additional person"),
    ("Hotel", "Direct award", "Louth", 120.0, None),
    ("Dormitory", "Request for vacant buildings", "South Dublin", 40.0, "request by Minister to religious organisations and government departments"),
    ("Guesthouse", "Direct award", "Donegal", 70.0, None),
    ("Apartment complex", "Direct award", "South Dublin", 105.0, None),
    ("Apartment complex", "Direct award", "Donegal", 75.0, None),
    ("Hotel", "Direct award", "Tipperary", 65.0, None),
    ("Hotel", "Direct award", "Clare", 73.0, None),
    ("Hotel", "Direct award", "South Dublin", 155.0, "EUR 155 single + EUR 31 additional; dormitory style EUR 51 per person"),
    ("Hotel", "Direct award", "North Dublin", None, "rate unclear per C&AG"),
    ("Dormitory", "HSE Covid centre pivoted to IP centre", "South Dublin", None, "Department-run (no commercial rate)"),
    ("Apartment complex", "Request for tender", "Westmeath", 74.0, None),
    ("Dormitory", "Direct award", "Mayo", 55.0, None),
    ("Hotel", "BOTP centre pivoted to IP centre", "Mayo", 55.0, None),
    ("Dormitory", "Direct award", "Mayo", None, "rate unclear per C&AG"),
    ("Hotel", "Direct award", "Louth", 85.0, None),
    ("Guesthouse", "Direct award", "Kildare", 80.0, None),
]
for i, (typ, src, loc, rate, note) in enumerate(_annex, 1):
    qual = "exact" if rate is not None else "unknown"
    extra = "; where no contract was available the rate is based on payments made (Annex note a)"
    R.append((27 if i <= 11 else 28, "Annex 10A", "sample_property",
              f"Contracted daily rate - property {i}: {typ}, {loc} ({src})",
              rate, "eur_per_person_night", qual, "2024", f"sample property {i}",
              (note or "per-person rate") + extra))
R += [
    (27, "Annex 10A", "sample_property", "Per-property compliance grid (site visit/proposal/CRO/ownership/contract/fire/inspection/planning/insurance)", None, "n/a", "unknown", "2024", "all 20 sample properties", "UNKNOWN: complete/partial/not-complete tick marks are rendered as glyphs lost in text extraction - recoverable via PDF glyph/colour parsing; aggregates ARE captured under due_diligence"),
]

# ---------------- data unknown at source (explicit gaps the report states) ----------------
R += [
    (9, "10.28", "unknown_at_source", "Properties notified to local authorities for change of use as IP accommodation", None, "properties", "unknown", "to 2025", "IPAS records", "UNKNOWN AT SOURCE: IPAS keeps no records of change-of-use notifications"),
    (15, "10.75 fn7", "unknown_at_source", "Weekly occupancy-register submission compliance rate", None, "percent", "unknown", "2024", "providers", "UNKNOWN AT SOURCE: not tracked in 2024"),
    (3, "Fig 10.2", "unknown_at_source", "Per-county distribution of IP applicants per 1,000 population", None, "persons_per_1000", "unknown", "31 Dec 2024", "counties", "UNKNOWN: choropleth map only (bands 0-2 / 3-5 / 6-8 / 9-11 / 12); values not in text layer"),
]


def main() -> None:
    sha = hashlib.sha256(Path(PDF).read_bytes()).hexdigest()
    rows = [{
        "figure_id": f"IPAS24-{i:03d}",
        "page": p, "para_ref": ref, "category": cat, "metric": metric,
        "value_numeric": float(v) if v is not None else None,
        "unit": unit, "qualifier": qual, "period": period, "scope": scope,
        "notes": notes,
        "is_unknown": qual == "unknown",
        "report": "RoAPS 2024 Chapter 10 - Management of international protection accommodation contracts",
        "source_url": SRC_URL,
        "source_document_hash": sha,
        "derived_at": now_iso(),
        "extraction_method": "manual_curation_from_fitz_text_full_read",
        "confidence": "high",
        "privacy_tier": "public_aggregates_and_bodies",
        "value_safe_to_sum": False,
    } for i, (p, ref, cat, metric, v, unit, qual, period, scope, notes) in enumerate(R, 1)]

    df = pl.DataFrame(rows, schema_overrides={"value_numeric": pl.Float64},
                      infer_schema_length=None)
    out = SILVER / "cag_ipas_chapter_figures.parquet"
    df.write_parquet(out, compression="zstd", statistics=True)
    print(f"wrote {out} - {df.height} rows")
    print(df.group_by("category").agg(pl.len(), pl.col("is_unknown").sum().alias("unknown")).sort("len", descending=True))
    print(f"\nunknown rows: {df['is_unknown'].sum()} / {df.height}")


if __name__ == "__main__":
    main()
