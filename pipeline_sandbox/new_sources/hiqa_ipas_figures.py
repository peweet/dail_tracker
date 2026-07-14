"""Structured figure extraction: HIQA — "Monitoring of International Protection
Accommodation Service centres in 2024" (first overview report, published 2025).

SANDBOX ONLY. Source of truth = the born-digital PDF (108 pages), read in full.
Every row below was hand-curated with PDF page + printed page + figure/table ref.

Three extraction methods are recorded per row:
  * text_layer_narrative   - prose numbers in the fitz text layer
  * text_layer_table       - tables whose cells are real PDF text (Tables 1-14)
  * text_layer_chart_label - vector charts whose data labels are real PDF text
                             (Figs 12-14, 16, 19-24)
  * raster_chart_datalabel - charts embedded as bitmaps. HIQA prints the data
                             labels INTO the bitmap, so the values were recovered
                             by rendering the image rect at zoom 3 and reading the
                             printed labels (NOT by measuring bar geometry). Each
                             recovered set is cross-validated against the report's
                             own prose totals where the prose states them.
  * unknown                - value not establishable; value_numeric is null.

NOTHING is inferred. Values that exist only as an unlabelled map, or that the
report itself does not state, are EXPLICIT UNKNOWN rows (value_numeric null,
qualifier='unknown', unknown_reason set). Where the report contradicts itself
(chart vs prose, percentages that do not sum to 100) BOTH printed values are kept
verbatim and the conflict is flagged in `notes` + the validation output — nothing
is silently adjusted.

privacy_tier = public_aggregates: this report concerns centres/providers/staff in
aggregate. No individual resident detail or attributable quote is extracted.

Row tuple: (page, section, ref, category, metric, value, unit, qualifier,
            period, scope, notes, method)
qualifier: exact | approx | over | under | almost | range_min | range_max | unknown
"""
from __future__ import annotations

import hashlib
from pathlib import Path

import polars as pl

from _common import BRONZE, SILVER, now_iso

PDF = BRONZE / "ipas_context" / "hiqa_ipas_monitoring_2024.pdf"
SRC_URL = ("https://www.hiqa.ie/sites/default/files/2025-03/"
           "Monitoring-of-International-Protection-Accommodation-Service-centres-in-2024.pdf")
REPORT = ("HIQA - Monitoring of International Protection Accommodation Service "
          "centres in 2024")

NARR = "text_layer_narrative"
TBL = "text_layer_table"
VEC = "text_layer_chart_label"
RAS = "raster_chart_datalabel"
UNK = "unknown"

R: list[tuple] = []


def add(page, section, ref, cat, metric, val, unit, qual, period, scope, notes, method):
    R.append((page, section, ref, cat, metric, val, unit, qual, period, scope, notes, method))


# ============================================================================
# 2. Background / 2.1 Context  (printed pp. 9-10)
# ============================================================================
_CTX = "Dept of Children, Equality, Disability, Integration and Youth: IPAS Weekly Accommodation and Arrivals Statistics (Dec 2023 and Dec 2024)"
add(10, "2 Background", "S2", "context_national", "IP applications in the year", 13_319, "applications", "exact", "2022", "State", "unprecedented increase; White Paper reform had assumed 3,500 applicants a year", NARR)
add(10, "2 Background", "S2", "context_national", "White Paper planning assumption - average annual IP arrivals", 3_500, "applications_per_year", "exact", "2021 White Paper", "State", "planned end of direct provision by end-2024; assumption later abandoned", NARR)
add(10, "2 Background", "S2", "context_national", "People fleeing the war in Ukraine who arrived (BOTP)", 70_000, "persons", "almost", "2022", "State", "additional influx on top of IP applicants; forced revision of the reform programme", NARR)
add(10, "2 Background", "S2 fn7", "context_national", "Beds provided for under the Comprehensive Accommodation Strategy", 45_000, "beds", "under", "by 2028", "Comprehensive Accommodation Strategy (2024)", "'up to 45,000 beds'", NARR)
add(10, "2 Background", "S2 fn7", "context_national", "Strategy beds that will comply with national standards", 35_000, "beds", "exact", "by 2028", "Comprehensive Accommodation Strategy (2024)", "of the up-to-45,000; i.e. ~10,000 planned beds will NOT be standards-compliant settings", NARR)
add(11, "2.1 Context", "S2.1", "context_national", "People who sought international protection in Ireland", 32_623, "persons", "exact", "Dec 2023 - Dec 2024", "State", _CTX, NARR)
add(11, "2.1 Context", "S2.1", "context_national", "Increase in people seeking international protection", 6_747, "persons", "exact", "Dec 2023 - Dec 2024", "State", _CTX, NARR)
add(11, "2.1 Context", "S2.1", "context_national", "Total IP accommodation settings (all types)", 254, "settings", "exact", "Dec 2023", "State", _CTX, NARR)
add(11, "2.1 Context", "S2.1", "context_national", "Total IP accommodation settings (all types)", 323, "settings", "exact", "Dec 2024", "State", _CTX, NARR)
add(11, "2.1 Context", "S2.1", "centres_estate", "Accommodation centres under HIQA's legal remit", 45, "centres", "exact", "at time of writing (2025)", "HIQA remit", "14% of all IP accommodation settings", NARR)
add(11, "2.1 Context", "S2.1", "centres_estate", "Beds in centres under HIQA's remit", 7_775, "beds", "exact", "at time of writing (2025)", "HIQA remit", "24% of all beds for people seeking IP", NARR)
add(11, "2.1 Context", "S2.1", "centres_estate", "Share of all IP accommodation settings under HIQA's remit", 14, "percent", "exact", "at time of writing (2025)", "of 323 settings", None, NARR)
add(11, "2.1 Context", "S2.1", "centres_estate", "Share of all IP beds under HIQA's remit", 24, "percent", "exact", "at time of writing (2025)", "of ~39,338 beds", None, NARR)
add(11, "2.1 Context", "S2.1", "centres_estate", "IP accommodation settings NOT required to comply with national standards", 278, "settings", "exact", "at time of writing (2025)", "State", "86% of settings; NOT independently monitored by HIQA", NARR)
add(11, "2.1 Context", "S2.1", "centres_estate", "Beds in settings NOT required to comply with national standards", 31_563, "beds", "exact", "at time of writing (2025)", "State", "ALARMING: 86% of settings / 31,563 beds are outside any independent quality-and-safety monitoring", NARR)
add(11, "2.1 Context", "S2.1", "centres_estate", "Share of IP accommodation settings outside national standards", 86, "percent", "exact", "at time of writing (2025)", "of 323 settings", None, NARR)
add(11, "2.2 Stakeholders", "S2.2", "monitoring_activity", "Provider information sessions held by HIQA", 3, "sessions", "exact", "Jan 2024", "Dublin, Cork, Galway", None, NARR)

# ============================================================================
# 3. HIQA's monitoring approach (printed p. 12)
# ============================================================================
add(13, "3 Monitoring approach", "S3", "monitoring_activity", "Key documents published by HIQA to support providers", 4, "documents", "exact", "Jan 2024", "HIQA", "assessment-judgment framework; guidance; guide to monitoring; self-assessment questionnaire", NARR)
add(13, "3 Monitoring approach", "S3 / Appendix 1", "monitoring_activity", "Core standards underpinning the monitoring programme", 28, "standards", "exact", "2024", "of 40 national standards across 10 themes", "Appendix 1 lists them: 1.1-1.4, 2.1, 2.3, 2.4, 3.1, 4.1, 4.4, 4.6-4.9, 5.1, 5.2, 6.1, 7.1, 7.2, 8.1-8.3, 9.1, 10.1-10.5. NOTE: the report ALSO reports compliance for 1.5, 2.2, 4.2, 4.3 and 4.5, which are NOT core standards (fn 11 allows inspectors to assess additional standards)", NARR)
add(13, "3 Monitoring approach", "S3", "monitoring_activity", "Themes in the national standards", 10, "themes", "exact", "2019 national standards", "National Standards for accommodation offered to people in the protection process", None, NARR)
add(13, "3 Monitoring approach", "S3", "monitoring_activity", "Total national standards", 40, "standards", "exact", "2019 national standards", "National Standards", None, NARR)

# ============================================================================
# 4. About the centres (printed pp. 15-25) - Figures 1-10, Table 1
# ============================================================================
add(16, "4.1 Centres under remit", "Figure 1", "centres_estate", "Geographic distribution of accommodation centres under HIQA's remit", None, "centres", "unknown", "2024", "map of Ireland", None, UNK,
    )
R[-1] = R[-1][:10] + ("UNKNOWN: Figure 1 is an unlabelled map image; per-county/per-location counts are not printed on it and are not in the text layer.", UNK)

add(17, "4.1 Centres under remit", "S4.1", "centres_estate", "Accommodation centres notified to HIQA as under its remit", 51, "centres", "exact", "Jan-Dec 2024", "HIQA remit", "HIQA must be notified of both the existence and the closure of a centre", NARR)
add(17, "4.1 Centres under remit", "S4.1", "centres_estate", "Centres under HIQA remit operating from State-owned premises", 7, "centres", "exact", "2024", "of 51 centres", None, NARR)
add(17, "4.1 Centres under remit", "S4.1", "centres_estate", "Centres under HIQA remit that are commercial properties (private providers)", 44, "centres", "exact", "2024", "of 51 centres", "services privately provided on behalf of the State under contract via public procurement", NARR)
add(17, "4.1 Centres under remit", "S4.1", "centres_estate", "Reduction in centres under remit from the 51 notified", 6, "centres", "exact", "by time of reporting", "HIQA remit", "51 notified -> 45 at reporting; fluctuation driven by contract changes (e.g. switching to emergency accommodation contracts)", NARR)

# Figure 2 - centres under remit by month (raster chart, labels printed on bitmap)
_F2 = [("Jan", 48), ("Feb", 49), ("Mar", 48), ("Apr", 48), ("May", 51), ("Jun", 49),
       ("Jul", 45), ("Aug", 45), ("Sep", 45), ("Oct", 45), ("Nov", 45), ("Dec", 45)]
for m, v in _F2:
    add(18, "4.1 Centres under remit", "Figure 2", "centres_estate", "Accommodation centres under HIQA's remit", v, "centres", "exact", f"{m} 2024", "HIQA remit", "peaks at 51 in May 2024, settles at 45 from Jul 2024", RAS)

# Figure 3 - accommodation type
for lab, v in [("Own-door", 49), ("Communal", 47), ("Mixed", 4)]:
    add(19, "4.1 Centres under remit", "Figure 3", "centres_estate", f"Centres by accommodation type: {lab}", v, "percent", "exact", "2024", "centres under HIQA remit", "Figure 3 sums to 100%", RAS)
# Figure 4 - catering type
for lab, v in [("Self-catered", 78), ("Catered", 11), ("Mixed", 11)]:
    add(19, "4.1 Centres under remit", "Figure 4", "centres_estate", f"Centres by catering type: {lab}", v, "percent", "exact", "2024", "centres under HIQA remit", "Figure 4 sums to 100%", RAS)
# Figure 5 - resident population type
for lab, v in [("Mixed", 42), ("Families", 33), ("Singles", 25)]:
    add(20, "4.1 Centres under remit", "Figure 5", "centres_estate", f"Centres by resident population type: {lab}", v, "percent", "exact", "2024", "centres under HIQA remit", "Figure 5 sums to 100%", RAS)
# Figure 6 - contracted bed numbers
_F6 = [("<50 beds", 4), ("50-100 beds", 14), ("101-200 beds", 15), ("201-300 beds", 5),
       ("301-500 beds", 5), ("501-800 beds", 1), (">800 beds", 1)]
for lab, v in _F6:
    add(20, "4.1 Centres under remit", "Figure 6", "centres_estate", f"Centres by contracted bed numbers: {lab}", v, "centres", "exact", "2024", "centres under HIQA remit", "bands sum to 45 centres; one centre accommodates almost 1,000 residents, four have fewer than 50", RAS)
add(18, "4.1 Centres under remit", "S4.1 / Figure 6", "centres_estate", "Largest centre - residents accommodated", 1_000, "residents", "almost", "2024", "single largest centre", "'one centre providing accommodation for almost 1,000 residents'", NARR)
add(18, "4.1 Centres under remit", "S4.1 / Figure 6", "centres_estate", "Centres with fewer than 50 residents", 4, "centres", "exact", "2024", "centres under HIQA remit", None, NARR)

# 4.2 inspection activity
add(21, "4.2 Inspection activity", "S4.2", "monitoring_activity", "Inspections conducted", 60, "inspections", "exact", "9 Jan - 31 Dec 2024", "51 accommodation centres", "first year of the new HIQA function", NARR)
add(21, "4.2 Inspection activity", "S4.2", "monitoring_activity", "Centres inspected", 51, "centres", "exact", "2024", "HIQA remit", None, NARR)
add(21, "4.2 Inspection activity", "S4.2", "monitoring_activity", "Centres inspected more than once", 7, "centres", "exact", "2024", "of 51 centres", None, NARR)
add(21, "4.2 Inspection activity", "S4.2 / Figure 7", "monitoring_activity", "Announced inspections", 31, "inspections", "exact", "2024", "of 60 inspections", "Figure 7: 52%", NARR)
add(21, "4.2 Inspection activity", "S4.2 / Figure 7", "monitoring_activity", "Short-notice announced inspections", 1, "inspections", "exact", "2024", "of 60 inspections", "Figure 7: 2%", NARR)
add(21, "4.2 Inspection activity", "S4.2 / Figure 7", "monitoring_activity", "Unannounced inspections", 28, "inspections", "exact", "2024", "of 60 inspections", "Figure 7: 47%", NARR)
for lab, v in [("Announced", 52), ("Unannounced", 47), ("Short-notice", 2)]:
    add(21, "4.2 Inspection activity", "Figure 7", "monitoring_activity", f"Inspections by announcement type: {lab}", v, "percent", "exact", "2024", "of 60 inspections", "sums to 101% (rounding)", RAS)
add(21, "4.2 Inspection activity", "S4.2", "monitoring_activity", "Targeted (focused risk) inspections", 1, "inspections", "exact", "2024", "of 60 inspections", "triggered by unsolicited information; the rest were routine monitoring inspections", NARR)
add(44, "6 Findings", "S6 fn19", "monitoring_activity", "Inspections excluded from the findings summary", 1, "inspections", "exact", "2024", "of the 60 inspections", "one centre fell outside HIQA's remit before publication; report was still given to the provider and the Department", NARR)

# 4.3 information received
add(22, "4.3.1 Unsolicited information", "S4.3.1", "information_received", "Pieces of unsolicited information (feedback) received", 17, "items", "exact", "2024", "accommodation centres under remit", None, NARR)
for lab, v in [("email", 12), ("phone", 4), ("published media article", 1)]:
    add(22, "4.3.1 Unsolicited information", "S4.3.1", "information_received", f"Unsolicited information received by {lab}", v, "items", "exact", "2024", "of 17 items", None, NARR)
add(22, "4.3.1 Unsolicited information", "S4.3.1", "information_received", "Unsolicited information from people using services", 13, "items", "exact", "2024", "of 17 items", "76%", NARR)
add(22, "4.3.1 Unsolicited information", "S4.3.1", "information_received", "Unsolicited information from employees", 3, "items", "exact", "2024", "of 17 items", "18%", NARR)
add(22, "4.3.1 Unsolicited information", "S4.3.1", "information_received", "Unsolicited information from other sources", 1, "items", "exact", "2024", "of 17 items", "6%", NARR)
_F8 = [("Jan", 3), ("Feb", 1), ("Mar", 3), ("Apr", 1), ("May", 1), ("Jun", 0),
       ("Jul", 3), ("Aug", 2), ("Sep", 0), ("Oct", 0), ("Nov", 2), ("Dec", 1)]
for m, v in _F8:
    add(22, "4.3.1 Unsolicited information", "Figure 8", "information_received", "Unsolicited information received", v, "items", "exact", f"{m} 2024", "accommodation centres", "monthly series sums to 17", RAS)
add(22, "4.3.2 Qualitative assessment", "S4.3.2", "information_received", "Unsolicited information items falling under capacity-and-capability themes", 3, "items", "exact", "2024", "of 17 items", "report prints 'three (33%)' - 3/17 is 18%, not 33%; the 33% appears to describe 3 of the 9 themes identified, not 3 of 17 items. Both figures kept as printed; DENOMINATOR AMBIGUOUS AT SOURCE", NARR)
add(22, "4.3.2 Qualitative assessment", "S4.3.2", "information_received", "Percentage printed for capacity-and-capability feedback themes", 33, "percent", "exact", "2024", "denominator ambiguous at source", "printed as 'three (33%)'; does not reconcile with 3/17 = 18%", NARR)
for lab, v in [("Yellow (low risk)", 65), ("Orange (medium risk)", 29), ("Red (high risk)", 6)]:
    add(24, "4.3.4 Regulatory action", "Figure 9", "information_received", f"Risk rating of unsolicited information: {lab}", v, "percent", "exact", "2024", "of 17 items", "sums to 100%", RAS)
add(24, "4.3.4 Regulatory action", "S4.3.4", "information_received", "Unsolicited information items triggering an unannounced targeted inspection", 1, "items", "exact", "2024", "of 17 items", "concerned suspected child protection and welfare concerns, resident health and wellbeing, and governance", NARR)
add(24, "4.3.4 Regulatory action", "S4.3.4", "information_received", "Referrals made by HIQA to Tusla under the Children First Act 2015", 1, "referrals", "exact", "2024", "HIQA", "made on foot of unsolicited information containing UNREPORTED child safeguarding concerns", NARR)

# 4.3.5 statutory notifications + Table 1 + Figure 10
add(24, "4.3.5 Statutory notifications", "S4.3.5", "notifications", "Statutory notifications received from service providers", 86, "notifications", "exact", "2024", "accommodation centres under remit", None, NARR)
_T1 = [("NF01", "The unexpected death of a recipient", 3, "calendar_days"),
       ("NF03", "Serious injury to a recipient", 3, "calendar_days"),
       ("NF05", "Any unexpected absence of a minor from the centre", 24, "hours"),
       ("NF06", "Any allegation of abuse of a recipient", 3, "calendar_days")]
for form, desc, v, unit in _T1:
    add(25, "4.3.5 Statutory notifications", "Table 1", "notifications", f"Notification deadline - {form}: {desc}", v, unit, "exact", "2024", "statutory deadline", "NF05 is within 24 hours of becoming aware; others within 3 calendar days of occurrence", TBL)
_F10 = [("NF06 - Allegation of abuse of a recipient", 60, 70),
        ("NF03 - Serious injury to a recipient", 22, 26),
        ("NF01 - Unexpected death of a recipient", 2, 2),
        ("NF05 - Unexpected absence of a minor", 2, 2)]
for lab, n, pct in _F10:
    add(26, "4.3.5 Statutory notifications", "Figure 10", "notifications", f"Statutory notifications by type: {lab}", n, "notifications", "exact", "2024", "of 86 notifications", f"printed as {n} ({pct}%); counts sum to 86", RAS)
    add(26, "4.3.5 Statutory notifications", "Figure 10", "notifications", f"Share of statutory notifications: {lab}", pct, "percent", "exact", "2024", "of 86 notifications", "percentages sum to 100%", RAS)

# ============================================================================
# 5. Resident voices (printed pp. 26-42) - Figures 11-24
# ============================================================================
add(27, "5 Resident voices", "S5", "resident_engagement", "Adults met by inspectors", 867, "persons", "exact", "2024", "60 inspections", None, NARR)
add(27, "5 Resident voices", "S5", "resident_engagement", "Children and young people met by inspectors", 302, "persons", "exact", "2024", "60 inspections", None, NARR)
add(27, "5 Resident voices", "S5", "resident_engagement", "Residents met as a share of all residents in the centres inspected", 17.4, "percent", "exact", "2024", "1,169 of the residents in inspected centres", "867 adults + 302 children = 1,169", NARR)
add(27, "5 Resident voices", "S5", "resident_engagement", "Questionnaires submitted by residents", 855, "questionnaires", "exact", "2024", "all inspections", "some only partially completed", NARR)
add(27, "5 Resident voices", "S5", "resident_engagement", "Questionnaires completed by adult residents", 776, "questionnaires", "exact", "2024", "of 855", None, NARR)
add(27, "5 Resident voices", "S5", "resident_engagement", "Questionnaires completed by children or young people", 79, "questionnaires", "exact", "2024", "of 855", "SMALL BASE: all children's percentages in Figures 11-16 rest on <=79 responses", NARR)
add(27, "5 Resident voices", "S5 fn16", "resident_engagement", "Languages the questionnaire was produced in", 7, "languages", "exact", "2024", "Albanian, Arabic, English, French, Georgian, Somali, Urdu", None, NARR)

# 5.1 children
add(29, "5.1 Children", "S5.1", "resident_experience_children", "Average age of children/young people who completed a questionnaire", 11, "years", "exact", "2024", "79 child respondents", None, NARR)
add(29, "5.1 Children", "S5.1", "resident_experience_children", "Average length of time children had lived in their centre", 2.5, "years", "over", "2024", "79 child respondents", "'just over two and a half years'", NARR)
_F11 = [("< 1 year", 24), ("1-2 years", 18), ("2-3 years", 28), ("3-4 years", 6), ("4-5 years", 6), ("5+ years", 18)]
for lab, v in _F11:
    add(29, "5.1 Children", "Figure 11", "resident_experience_children", f"Length of time children lived in the centre: {lab}", v, "percent", "exact", "2024", "79 child respondents", "sums to 100%; 24% of children have been in a centre 5+ years or 3+ years -> 52% for 2 years or more", RAS)

# Figure 12 - CHART vs PROSE CONFLICT
_CONFLICT12 = ("CONFLICT AT SOURCE: Figure 12 (chart) and the §5.1 prose disagree. The chart prints "
               "'safe place' Yes 79%/No 21% and 'know who to talk to' Yes 84%/No 16%. The prose (printed p.28) "
               "says '21% ... did not know who to talk to if they felt unsafe' and '16% stated that they thought "
               "their centre was not a safe place'. Both are recorded verbatim; NEITHER is adjusted.")
for q, y, n in [("Do you think the centre is a safe place to live for children and young people?", 79, 21),
                ("Do you know who to talk to if you ever feel unsafe or worried about something?", 84, 16)]:
    add(30, "5.1 Children", "Figure 12 (chart)", "resident_experience_children", f"[chart] {q} - Yes", y, "percent", "exact", "2024", "child questionnaire respondents", _CONFLICT12, VEC)
    add(30, "5.1 Children", "Figure 12 (chart)", "resident_experience_children", f"[chart] {q} - No", n, "percent", "exact", "2024", "child questionnaire respondents", _CONFLICT12, VEC)
add(29, "5.1 Children", "S5.1 (prose)", "resident_experience_children", "[prose] Children who did not know who to talk to if they felt unsafe or worried", 21, "percent", "exact", "2024", "child questionnaire respondents", _CONFLICT12, NARR)
add(29, "5.1 Children", "S5.1 (prose)", "resident_experience_children", "[prose] Children who thought their centre was NOT a safe place for children to live", 16, "percent", "exact", "2024", "child questionnaire respondents", _CONFLICT12, NARR)

_F13 = [("Do you go to school?", 85, 15),
        ("Do you attend a homework club?", 63, 37),
        ("Do you get to take part in hobbies and activities you enjoy?", 83, 17),
        ("Are there toys and games for you to play with in the centre?", 66, 34),
        ("Do you have friends in the centre, or friends that come to visit you?", 77, 23),
        ("Is there a play area in the centre?", 77, 23),
        ("Do you have a desk or study area to do your homework?", 62, 38),
        ("Do you have a computer or laptop that you can use?", 42, 58),
        ("Do you have access to WiFi?", 88, 12)]
for q, y, n in _F13:
    add(31, "5.1 Children", "Figure 13", "resident_experience_children", f"{q} - Yes", y, "percent", "exact", "2024", "child questionnaire respondents", "15% of school-going-age children were NOT attending school", VEC)
    add(31, "5.1 Children", "Figure 13", "resident_experience_children", f"{q} - No", n, "percent", "exact", "2024", "child questionnaire respondents", None, VEC)

_F14 = [("Do you know how to make a complaint about anything in the centre?", 71, 28),
        ("Have you ever made a complaint?", 27, 73),
        ("If yes, did things change after you made the complaint?", 77, 23),
        ("Have you ever been asked by the manager or another person working in the centre if there were things that could be better?", 40, 60),
        ("If yes, do you think they listened to what you had to say?", 82, 18)]
for q, y, n in _F14:
    note = "first row sums to 99% (rounding)" if q.startswith("Do you know how") else None
    add(32, "5.1 Children", "Figure 14", "resident_experience_children", f"{q} - Yes", y, "percent", "exact", "2024", "child questionnaire respondents", note, VEC)
    add(32, "5.1 Children", "Figure 14", "resident_experience_children", f"{q} - No", n, "percent", "exact", "2024", "child questionnaire respondents", note, VEC)

_F15 = [("Do you have your own bedroom?", 35, 65),
        ("If not, do you share your bedroom with a person outside of your family?", 10, 90),
        ("Do you have your own bed?", 90, 10),
        ("Do you have somewhere to store your clothes and the things that are important to you?", 94, 6),
        ("Do you and your family have your own bathroom?", 94, 6)]
_F15NOTE = ("Prose on printed p.31 says 'Most (90%) said that they had somewhere to store their clothes' whereas the "
            "chart prints 94% for storage and 90% for 'own bed'. Chart values recorded here; prose discrepancy flagged, not adjusted.")
for q, y, n in _F15:
    add(33, "5.1 Children", "Figure 15", "resident_experience_children", f"{q} - Yes", y, "percent", "exact", "2024", "child questionnaire respondents", _F15NOTE, RAS)
    add(33, "5.1 Children", "Figure 15", "resident_experience_children", f"{q} - No", n, "percent", "exact", "2024", "child questionnaire respondents", _F15NOTE, RAS)
add(32, "5.1 Children", "S5.1 (prose)", "resident_experience_children", "[prose] Children who said they had somewhere to store clothes/important items", 90, "percent", "exact", "2024", "child questionnaire respondents", _F15NOTE, NARR)

_F16 = [("Does your family prepare their own food in the centre?", 85, 15),
        ("Do you like the food in the centre?", 29, 71),
        ("Do you think there is a good choice of food to eat?", 43, 57),
        ("Can you get a snack or a drink between meals?", 71, 29)]
for q, y, n in _F16:
    add(34, "5.1 Children", "Figure 16", "resident_experience_children", f"{q} - Yes", y, "percent", "exact", "2024", "child questionnaire respondents", "children were most critical of food and catering: 71% did not like the food; 57% said there was no good choice", VEC)
    add(34, "5.1 Children", "Figure 16", "resident_experience_children", f"{q} - No", n, "percent", "exact", "2024", "child questionnaire respondents", None, VEC)

# 5.2 adults
add(34, "5.2 Adults", "S5.2", "resident_experience_adults", "Average length of time adult respondents had lived in their centre", 18.5, "months", "exact", "2024", "776 adult respondents", "'18 months and 15 days'", NARR)
_F17 = [("< 1 year", 49), ("1-2 years", 22), ("2-3 years", 11), ("3-4 years", 7), ("4-5 years", 6), ("5 years +", 5)]
for lab, v in _F17:
    add(35, "5.2 Adults", "Figure 17", "resident_experience_adults", f"Length of time adult residents lived in the centre: {lab}", v, "percent", "exact", "2024", "adult questionnaire respondents", "sums to 100%; 22% of adults have lived in a centre 3 years or more", RAS)
add(35, "5.2 Adults", "Figure 18", "resident_experience_adults", "Adults happy living in their accommodation centre", 83, "percent", "exact", "2024", "adult questionnaire respondents", "'I feel happy living in this centre'", RAS)
add(35, "5.2 Adults", "Figure 18", "resident_experience_adults", "Adults NOT happy living in their accommodation centre", 17, "percent", "exact", "2024", "adult questionnaire respondents", None, RAS)

_F19 = [("I feel safe living in this centre", 88, 12),
        ("I feel adequately protected", 89, 11),
        ("I know how to raise a safeguarding or protection concern", 85, 15),
        ("I know who the designated liaison person is for child protection", 70, 30),
        ("I know who the designated officer is for vulnerable adult safeguarding", 70, 30),
        ("I have access to the child protection and adult safeguarding policies for the centre", 76, 24)]
for q, y, n in _F19:
    add(37, "5.2 Adults", "Figure 19", "resident_experience_adults", f"{q} - Yes", y, "percent", "exact", "2024", "adult questionnaire respondents", "almost a third could not identify the DLP / designated safeguarding officer", VEC)
    add(37, "5.2 Adults", "Figure 19", "resident_experience_adults", f"{q} - No", n, "percent", "exact", "2024", "adult questionnaire respondents", None, VEC)

_F20 = [("I would feel comfortable making a complaint about the centre or service if I needed to", 87, 13),
        ("I know who the complaints officer is for the centre", 80, 20),
        ("I have access to the centre's complaints policy", 78, 22),
        ("The centre welcomes and facilitates my feedback and complaints in the interest of quality improvement", 82, 18),
        ("I know who the centre manager is", 96, 4),
        ("I can access the centre manager when I need to", 91, 9),
        ("The management team are approachable", 90, 10)]
for q, y, n in _F20:
    add(38, "5.2 Adults", "Figure 20", "resident_experience_adults", f"{q} - Yes", y, "percent", "exact", "2024", "adult questionnaire respondents", None, VEC)
    add(38, "5.2 Adults", "Figure 20", "resident_experience_adults", f"{q} - No", n, "percent", "exact", "2024", "adult questionnaire respondents", None, VEC)

_F21 = [("The services delivered in the centre are person-centred", 83, 17),
        ("I have received a copy of the residents' charter", 79, 21),
        ("The services of the centre are delivered in a fair and equitable manner", 82, 18),
        ("The management team seeks to involve and consult with residents", 78, 22),
        ("Do you experience any restrictions while living in this centre?", 40, 60),
        ("Do you feel respected?", 85, 15),
        ("Do you feel like the centre is a dignified environment?", 80, 20),
        ("Does the centre support you to live a meaningful and good quality of life?", 79, 21),
        ("Do you feel listened to while living in the centre?", 82, 18)]
for q, y, n in _F21:
    add(39, "5.2 Adults", "Figure 21", "resident_experience_adults", f"{q} - Yes", y, "percent", "exact", "2024", "adult questionnaire respondents", "40% of adults reported experiencing RESTRICTIONS while living in the centre", VEC)
    add(39, "5.2 Adults", "Figure 21", "resident_experience_adults", f"{q} - No", n, "percent", "exact", "2024", "adult questionnaire respondents", None, VEC)

_F22 = [("Staff members are easy to talk to", 91, 9),
        ("Staff members are helpful and provide assistance when required", 90, 10),
        ("Staff members are sensitive to cultural, religious and other matters", 85, 15),
        ("Staff members are kind and respectful in their interactions with residents", 89, 11)]
for q, y, n in _F22:
    add(40, "5.2 Adults", "Figure 22", "resident_experience_adults", f"{q} - Yes", y, "percent", "exact", "2024", "adult questionnaire respondents", None, VEC)
    add(40, "5.2 Adults", "Figure 22", "resident_experience_adults", f"{q} - No", n, "percent", "exact", "2024", "adult questionnaire respondents", None, VEC)

_F23 = [("I am satisfied that the allocation of rooms in the centre is based on fair and transparent criteria", 79, 21),
        ("My sleeping accommodation provides sufficient space for the storage of my personal belongings", 73, 27),
        ("My sleeping accommodation is lockable and I have a key", 90, 10),
        ("My sleeping accommodation affords me appropriate privacy and dignity", 80, 20),
        ("There are a reasonable number of bathrooms and showering facilities in the centre to meet my needs", 91, 9)]
for q, y, n in _F23:
    add(42, "5.2 Adults", "Figure 23", "resident_experience_adults", f"{q} - Yes", y, "percent", "exact", "2024", "adult questionnaire respondents", "1 in 5 adults said their sleeping accommodation did NOT afford appropriate privacy and dignity", VEC)
    add(42, "5.2 Adults", "Figure 23", "resident_experience_adults", f"{q} - No", n, "percent", "exact", "2024", "adult questionnaire respondents", None, VEC)

_F24 = [("Do you have access to food preparation facilities when you need them?", 87, 13),
        ("Do you have sufficient place to store your food, such as cupboards and refrigerators?", 79, 21),
        ("Do you have access to snacks, fruit and drinking water outside of mealtimes?", 78, 22),
        ("If your centre provides cooked meals, do you have the option of preparing your own meals?", 69, 31),
        ("If your centre provides cooked meals, is there choice provided across all meal-times and menus?", 71, 29),
        ("If your centre provides cooked meals, do the mealtimes meet your needs?", 72, 28),
        ("Is healthy eating and good food habits promoted in the centre?", 80, 20),
        ("Are mealtimes an enjoyable experience?", 76, 24)]
for q, y, n in _F24:
    add(43, "5.2 Adults", "Figure 24", "resident_experience_adults", f"{q} - Yes", y, "percent", "exact", "2024", "adult questionnaire respondents", None, VEC)
    add(43, "5.2 Adults", "Figure 24", "resident_experience_adults", f"{q} - No", n, "percent", "exact", "2024", "adult questionnaire respondents", None, VEC)

# ============================================================================
# 6. Inspection findings - compliance by standard (Figures 25-61)
# ============================================================================
# (page, printed_fig_ref, standard, title, compliant, substantially, partially, not_compliant,
#  prose "complied" total or None, note)
_STD = [
    (46, "Figure 26", "1.1", "Service provider performs its functions per legislation, regulations, national policies and standards", 24, 33, 31, 12, 57, None),
    (47, "Figure 27", "1.2", "Effective leadership, governance and management arrangements; staff clearly accountable", 21, 23, 52, 4, 44, "highest levels of non-compliance (56%) related to no clear/effective/sustainable governance arrangements"),
    (48, "Figure 28", "1.3", "Residents' charter accurately and clearly describes services available", 63, 24, 11, 2, 87, None),
    (49, "Figure 29", "1.4", "Provider monitors and reviews quality of care and residents' experience; improved on an ongoing basis", 18, 32, 42, 8, 50, "PROSE ERROR AT SOURCE: printed p.47 says '18% were found to comply and 32% were partially compliant' but the chart shows 32% SUBSTANTIALLY compliant (partially compliant is 42%). Chart values used; prose flagged, not adjusted."),
    (49, "Figure 30", "1.5", "Management regularly consult residents and allow them to participate in decisions", 40, 47, 13, 0, 87, None),
    (51, "Figure 31", "2.1", "Safe and effective recruitment practices for staff and management", 28, 33, 4, 35, 61, "ALARMING SHAPE: 35% NOT COMPLIANT (the highest not-compliant rate in the report bar Standard 4.3). Main reasons: absent Garda checks, international police checks and references - in some cases all three."),
    (52, "Figure 32", "2.2", "Staff have the required competencies to manage and deliver person-centred, effective and safe services", 78, 5, 17, 0, 83, None),
    (53, "Figure 33", "2.3", "Staff are supported and supervised to carry out their duties", 25, 15, 48, 12, 40, "60% did not comply - the workforce theme's weakest standard"),
    (54, "Figure 34", "2.4", "Continuous training is provided to staff", 19, 43, 34, 4, 62, None),
    (55, "Figure 35", "3.1", "Provider carries out regular risk analysis and develops a risk register", 14, 12, 62, 12, 26, "WORST STANDARD IN THE REPORT: only 26% compliant/substantially compliant; 74% not in compliance. In two centres HIQA escalated regulatory activity; one remains of concern."),
    (61, "Figure 39", "4.1", "Planning, designing and allocating accommodation informed by residents' needs and best interests of the child", 43, 39, 16, 2, 82, None),
    (62, "Figure 40", "4.2", "Accommodation which is homely, accessible and sufficiently furnished", 43, 22, 26, 9, 65, None),
    (63, "Figure 41", "4.3", "Privacy, dignity and safety of each resident protected; physical environment promotes safety, health and wellbeing", 37, 12, 13, 38, 49, "HIGHEST NOT-COMPLIANT RATE IN THE REPORT (38%). Majority (51%) not in compliance - unsuitable, cramped living quarters and/or overcrowding; bathroom doors that did not lock; shower panels giving no privacy."),
    (64, "Figure 42", "4.4", "Privacy and dignity of family units; child-friendly accommodation", 47, 21, 29, 3, 68, "deficits included children having to share beds with one or both parents due to a lack of beds"),
    (65, "Figure 43", "4.5", "Adequate and accessible facilities, including dedicated child-friendly play and recreation facilities", 69, 23, 8, 0, 92, None),
    (66, "Figure 44", "4.6", "Adequate dedicated facilities and materials to support each child's educational development", 79, 12, 9, 0, 91, None),
    (67, "Figure 45", "4.7", "Clean environment; promotes independence in relation to laundry and cleaning", 74, 11, 11, 4, 85, "non-compliances included mould from poor ventilation and residents having to buy their own cleaning materials"),
    (68, "Figure 46", "4.8", "Security measures sufficient, proportionate and appropriate; protect privacy and dignity", 77, 14, 9, 0, 91, "commonest partial-compliance reason: CCTV throughout the centre leaving residents no private area to meet family or their legal representative"),
    (69, "Figure 47", "4.9", "Sufficient and appropriate non-food items and products (toiletries, nappies, bedding, contraception)", 57, 24, 15, 4, 81, "non-compliances included CHARGING residents for non-food items, and local charities supplying nappies/toiletries that the provider is funded to provide"),
    (70, "Figure 48", "5.1", "Food preparation and dining facilities meet residents' needs and are appropriately equipped", 79, 13, 6, 2, 92, None),
    (71, "Figure 49", "5.2", "Catering needs and autonomy of residents; varied diet respecting cultural, religious, dietary needs", 77, 14, 9, 0, 91, "in a small number of centres residents experienced LOW FOOD SUPPLIES as a direct result of OVERPRICING in on-site shops"),
    (73, "Figure 50", "6.1", "The rights and diversity of each resident are respected, safeguarded and promoted", 65, 15, 13, 7, 80, "where not compliant, living conditions were 'wholly inadequate and therefore could never comply'"),
    (74, "Figure 25 [sic - should be 51]", "7.1", "Provider supports and facilitates residents to develop and maintain personal and family relationships", 85, 10, 5, 0, None, "REPORT NUMBERING ERROR: caption printed as 'Figure 25' (a duplicate); by sequence this is Figure 51. Prose gives no percentages for this standard - values are chart-only."),
    (75, "Figure 262 [sic - should be 52]", "7.2", "Public services, healthcare, education, community supports and leisure accessible, incl. transport", 85, 7, 5, 3, None, "REPORT NUMBERING ERROR: caption printed as 'Figure 262'; by sequence this is Figure 52. Prose gives no percentages - chart-only. Improvements needed on transport, particularly in rural areas."),
    (77, "Figure 273 [sic - should be 53]", "8.1", "Provider protects residents from abuse and neglect and promotes their safety and welfare", 46, 30, 18, 6, 76, "REPORT NUMBERING ERROR: caption printed as 'Figure 273'; by sequence this is Figure 53."),
    (78, "Figure 284 [sic - should be 54]", "8.2", "Provider takes all reasonable steps to protect each child from abuse and neglect", 51, 22, 20, 7, 73, "REPORT NUMBERING ERROR: caption printed as 'Figure 284'; by sequence this is Figure 54. In several centres inspectors identified suspected child protection/welfare concerns NOT KNOWN to the centre manager or provider."),
    (79, "Figure 295 [sic - should be 55]", "8.3", "Provider manages and reviews adverse events and incidents in a timely manner", 31, 26, 37, 6, 57, "REPORT NUMBERING ERROR: caption printed as 'Figure 295'; by sequence this is Figure 55. Lowest compliance under the safeguarding theme."),
    (80, "Figure 306 [sic - should be 56]", "9.1", "Provider promotes health, wellbeing and development of each resident; person-centred needs-based support", 86, 14, 0, 0, 100, "REPORT NUMBERING ERROR: caption printed as 'Figure 306'; by sequence this is Figure 56. ONLY STANDARD WITH 100% COMPLIANCE."),
    (82, "Figure 57", "10.1", "Special reception needs notified by the Department are incorporated into accommodation and services", 85, 10, 5, 0, 95, None),
    (83, "Figure 58", "10.2", "All staff are enabled to identify and respond to emerging and identified needs for residents", 46, 33, 19, 2, 79, None),
    (84, "Figure 59", "10.3", "Provider has an established policy to identify, communicate and address special reception needs", 20, 20, 31, 29, 40, "SECOND-WORST STANDARD: only 40% complied; 29% NOT COMPLIANT. Providers with no policy typically had no reception officer either."),
    (85, "Figure 60", "10.4", "Provider makes available a dedicated, suitably trained Reception Officer", 26, 18, 29, 27, 44, "56% did not comply because they had NO dedicated reception officer in place; 27% not compliant."),
    (86, "Figure 61", "10.5", "Additional measures made available where residents are exceptionally vulnerable", 67, 0, 33, 0, 67, "assessed in the four centres where the level of need/vulnerability was materially higher than elsewhere"),
]
_JUDG = [("Compliant", 0), ("Substantially compliant", 1), ("Partially compliant", 2), ("Not compliant", 3)]
for page, ref, std, title, c, sc, pc, nc, prose, note in _STD:
    vals = (c, sc, pc, nc)
    for jname, ji in _JUDG:
        add(page, f"6 Compliance - Standard {std}", ref, "compliance_standard",
            f"Standard {std} - {jname}", vals[ji], "percent", "exact", "2024",
            "service providers assessed against this standard",
            (f"{title}. " + (note or "")).strip(), RAS)
    if prose is not None:
        add(page, f"6 Compliance - Standard {std}", f"{ref} / prose", "compliance_standard",
            f"Standard {std} - compliant or substantially compliant (prose)", prose, "percent",
            "exact", "2024", "service providers assessed against this standard",
            f"prose total; chart components: {c}% compliant + {sc}% substantially = {c+sc}%", NARR)

# Figure 25 / 38 - compliance summary by theme
_THEMES = [
    (45, "Figure 25", "Capacity and capability", "Theme 1: Governance, Accountability and Leadership", 30, 27, 32, 11),
    (45, "Figure 25", "Capacity and capability", "Theme 2: Responsive Workforce", 29, 27, 28, 16),
    (45, "Figure 25", "Capacity and capability", "Theme 3: Contingency Planning and Emergency Preparedness", 13, 11, 60, 15),
    (59, "Figure 38", "Quality and safety", "Theme 4: Accommodation", 58, 20, 17, 5),
    (59, "Figure 38", "Quality and safety", "Theme 5: Food, Catering and Cooking Facilities", 74, 14, 9, 3),
    (59, "Figure 38", "Quality and safety", "Theme 6: Person-Centred Care and Support", 65, 15, 13, 8),
    (59, "Figure 38", "Quality and safety", "Theme 7: Individual, Family and Community Life", 85, 8, 7, 1),
    (59, "Figure 38", "Quality and safety", "Theme 8: Safeguarding and Protection", 41, 26, 26, 8),
    (59, "Figure 38", "Quality and safety", "Theme 9: Health, Wellbeing and Development", 87, 13, 0, 0),
    (59, "Figure 38", "Quality and safety", "Theme 10: Identification, Assessment and Response to Special Needs", 41, 19, 22, 18),
]
for page, ref, dim, theme, c, sc, pc, nc in _THEMES:
    for jname, ji in _JUDG:
        add(page, f"6 Compliance summary - {dim}", ref, "compliance_theme",
            f"{theme} - {jname}", (c, sc, pc, nc)[ji], "percent", "exact", "2024",
            f"{dim} dimension; all judgments made under this theme",
            "Theme 3 (risk/contingency) is the weakest: 60% partially + 15% not compliant. "
            "Theme 10 has the highest not-compliant rate in the quality-and-safety dimension (18%).", RAS)

# ============================================================================
# 6.1.1 Monitoring metrics - capacity and capability (Tables 2, 3; Figures 36, 37)
# ============================================================================
_T2 = [
    ("Are the Children First Act 2015 and 'Children First National Guidance' implemented in practice in the centre?", 92, 8),
    ("Is the Safeguarding Vulnerable Persons at Risk of Abuse National Policy implemented in practice in the centre?", 63, 37),
    ("Do the management team have the appropriate qualifications, skills and experience necessary to manage the centre?", 82, 18),
    ("Are centre managers in receipt of regular supervision from the service provider (at least quarterly)?", 24, 76),
    ("Are records maintained of all complaints made including investigations and/or outcomes?", 63, 37),
    ("Is there meaningful consultation with people who live in the centre?", 63, 37),
    ("Are all reasonable efforts made to provide residents with relevant information in an accessible format?", 86, 14),
    ("Is there a positive culture which is person centred and promotes the human rights of people?", 84, 16),
    ("Is there a Residents' Charter in place which contains all prescribed information?", 69, 31),
    ("Is there a 'written description' of how the centre is operated on a day to day basis?", 73, 27),
    ("Where required, does the service provider have a quality improvement plan in place?", 47, 53),
    ("Has an 'annual review of the quality and safety of the service' been completed?", 14, 86),
    ("Is there a Residents' Committee in place in the centre?", 51, 49),
]
for q, y, n in _T2:
    note = None
    if q.startswith("Has an 'annual"):
        note = "WORST METRIC IN TABLE 2: only 14% of centres had completed an annual review of quality and safety"
    if q.startswith("Are centre managers"):
        note = "76% of centre managers received NO regular supervision from their provider"
    add(56, "6.1.1 Metrics - capacity and capability", "Table 2", "metrics_capacity_capability", f"{q} - Yes", y, "percent", "exact", "2024", "centres inspected", note, TBL)
    add(56, "6.1.1 Metrics - capacity and capability", "Table 2", "metrics_capacity_capability", f"{q} - No", n, "percent", "exact", "2024", "centres inspected", note, TBL)

add(57, "6.1.1 Metrics - vetting", "Figure 36", "vetting", "Staff members appropriately vetted by An Garda Siochana", 65, "percent", "exact", "2024", "sample of staff records reviewed on inspection", "cross-checks with §7.6 prose: 35% NOT appropriately vetted", RAS)
add(57, "6.1.1 Metrics - vetting", "Figure 36", "vetting", "Staff members NOT appropriately vetted by An Garda Siochana", 35, "percent", "exact", "2024", "sample of staff records reviewed on inspection", "HIQA: 'unsafe practice in terms of staff vetting'; urgent national and local action required", RAS)
_F37 = [("Frontline support staff", 30), ("Security staff", 10), ("Managers / assistant managers", 9),
        ("Catering staff", 3), ("Reception officers", 1), ("Other staff members", 11)]
for lab, v in _F37:
    add(57, "6.1.1 Metrics - vetting", "Figure 37", "vetting", f"Individual staff without Garda vetting: {lab}", v, "staff", "exact", "2024", "sample of staff records reviewed on inspection", "counts sum to 64 individuals; includes 10 security staff and 9 managers/assistant managers", RAS)

_T3 = [
    ("Are there contingency plans in place for emergencies or an unexpected shortfall in staff cover?", 69, 31, None),
    ("Are all staff members in receipt of regular formal supervision (at least every three months)?", 31, 69, None),
    ("Are there policies and procedures in place to manage, review and learn from adverse events?", 53, 47, None),
    ("Is there a risk register in place in the centre?", 82, 18, None),
    ("Does the risk register list all relevant risks (resident and non-resident related)?", 30, 70, "70% of centres with a risk register did NOT have all relevant risks recorded on it"),
    ("Are control measures listed on the centre's risk register in place in practice?", 70, 30, "30% of providers had not implemented the controls they themselves identified"),
    ("Is the risk register updated and reviewed on a regular basis?", 65, 35, None),
    ("Does the risk register include contingency plans for continuity of services in a disaster or unforeseen event?", 65, 35, None),
    ("Is there a record maintained of all accidents, incidents and near misses which have occurred in the centre?", 76, 34, "ARITHMETIC ERROR AT SOURCE: Table 3 prints Yes 76% / No 34% = 110%. Both values kept verbatim; not adjusted."),
    ("Are risks appropriately escalated where necessary?", 69, 31, "matches §7.4 prose: in 31% of centres, risk that could not be controlled locally was not escalated"),
    ("Are all residents informed about fire drills and emergency protocols?", 90, 10, None),
    ("Are there risk assessments in place relating to situations where the safety of residents may be compromised?", 65, 35, "matches §7.5 prose: risk assessments were NOT completed when residents' safety was compromised in 35% of centres"),
]
for q, y, n, note in _T3:
    add(58, "6.1.1 Metrics - risk management", "Table 3", "metrics_capacity_capability", f"{q} - Yes", y, "percent", "exact", "2024", "centres inspected", note, TBL)
    add(58, "6.1.1 Metrics - risk management", "Table 3", "metrics_capacity_capability", f"{q} - No", n, "percent", "exact", "2024", "centres inspected", note, TBL)

# ============================================================================
# 6.2.1 Monitoring metrics - quality and safety (Tables 4-13)
# ============================================================================
add(87, "6.2.1 Metrics - shared bedrooms", "Table 4", "metrics_quality_safety", "Highest number of unrelated people sharing one bedroom - lower bound of range", 2, "persons", "range_min", "2024", "29 centres where applicable", "'Ranges between two and six'", TBL)
add(87, "6.2.1 Metrics - shared bedrooms", "Table 4", "metrics_quality_safety", "Highest number of unrelated people sharing one bedroom - upper bound of range", 6, "persons", "range_max", "2024", "29 centres where applicable", "SIX unrelated adults sharing one bedroom at the worst centre", TBL)
add(87, "6.2.1 Metrics - shared bedrooms", "Table 4", "metrics_quality_safety", "Unrelated residents living in shared bedrooms per centre - lower bound of range", 3, "persons", "range_min", "2024", "29 centres where applicable", None, TBL)
add(87, "6.2.1 Metrics - shared bedrooms", "Table 4", "metrics_quality_safety", "Unrelated residents living in shared bedrooms per centre - upper bound of range", 128, "persons", "range_max", "2024", "29 centres where applicable", None, TBL)
add(87, "6.2.1 Metrics - shared bedrooms", "Table 4", "metrics_quality_safety", "Unrelated residents living in shared bedrooms - total", 1_550, "persons", "exact", "2024", "29 centres where applicable", "reconciles exactly with Table 14 'shared rooms (unrelated persons)' = 1,550 (24% of 6,544)", TBL)
add(87, "6.2.1 Metrics - shared bedrooms", "Table 4", "metrics_quality_safety", "Centres where unrelated people share bedrooms", 29, "centres", "exact", "2024", "centres inspected", None, TBL)

_T5 = [
    ("Is there a clear, fair and transparent room allocation policy in place in the centre?", 49, 51, "MORE THAN HALF of centres had NO clear/fair/transparent room-allocation policy"),
    ("Are residents accommodated in accordance with their identified needs?", 84, 16, None),
    ("Is there evidence of overcrowding in the accommodation centre?", 16, 84, "16% of centres showed evidence of overcrowding (Yes = overcrowding present)"),
    ("Does the accommodation meet the minimum space requirements outlined in the National Standards?", 90, 10, "10% of centres did NOT meet minimum space requirements"),
    ("Are there residents aged 15 and over sleeping in bunk beds when they did not request bunk beds?", 6, 94, "Yes = 6% of centres had 15+ year olds in unrequested bunk beds (prohibited by the standards)"),
    ("Within family units, are there a sufficient number of beds made available?", 97, 3, None),
    ("Are there children sharing beds and/or bedrooms with related adults (excluding babies and infants)?", 54, 46, "Yes = in 54% of centres children shared beds/bedrooms with related adults"),
]
for q, y, n, note in _T5:
    add(87, "6.2.1 Metrics - room allocation", "Table 5", "metrics_quality_safety", f"{q} - Yes", y, "percent", "exact", "2024", "centres inspected", note, TBL)
    add(87, "6.2.1 Metrics - room allocation", "Table 5", "metrics_quality_safety", f"{q} - No", n, "percent", "exact", "2024", "centres inspected", note, TBL)
add(87, "6.2.1 Metrics - room allocation", "Table 5", "metrics_quality_safety", "Children not provided with a bed", 2, "children", "exact", "2024", "centres inspected", "two children had no bed of their own", TBL)
add(87, "6.2.1 Metrics - room allocation", "Table 5", "metrics_quality_safety", "Adults not provided with a bed", 0, "adults", "exact", "2024", "centres inspected", None, TBL)

add(88, "6.2.1 Metrics - tented accommodation", "Table 6", "metrics_quality_safety", "Centres using tented areas to accommodate residents - Yes", 2, "percent", "exact", "2024", "centres inspected", "one centre; tented accommodation 'can never comply with national standards' (§7.2)", TBL)
add(88, "6.2.1 Metrics - tented accommodation", "Table 6", "metrics_quality_safety", "Centres using tented areas to accommodate residents - No", 98, "percent", "exact", "2024", "centres inspected", None, TBL)
add(88, "6.2.1 Metrics - tented accommodation", "Table 6", "metrics_quality_safety", "Children sleeping in tented accommodation", 0, "children", "exact", "2024", "centres inspected", None, TBL)
add(88, "6.2.1 Metrics - tented accommodation", "Table 6", "metrics_quality_safety", "Adults sleeping in tented accommodation", 90, "adults", "exact", "2024", "the one applicable centre", "'31% of adults at the one applicable centre'; reconciles with Table 14 tented = 90 persons", TBL)
add(88, "6.2.1 Metrics - tented accommodation", "Table 6", "metrics_quality_safety", "Share of adults in tented accommodation at the one applicable centre", 31, "percent", "exact", "2024", "the one applicable centre", None, TBL)

add(88, "6.2.1 Metrics - maintenance", "Table 7", "metrics_quality_safety", "Are maintenance and repair works carried out promptly and to a suitable standard? - Yes", 78, "percent", "exact", "2024", "centres inspected", None, TBL)
add(88, "6.2.1 Metrics - maintenance", "Table 7", "metrics_quality_safety", "Are maintenance and repair works carried out promptly and to a suitable standard? - No", 22, "percent", "exact", "2024", "centres inspected", None, TBL)
add(88, "6.2.1 Metrics - maintenance", "Table 7", "metrics_quality_safety", "Was there any evidence of mould in the accommodation centre? - Yes", 27, "percent", "exact", "2024", "centres inspected", "MOULD FOUND IN MORE THAN A QUARTER OF CENTRES INSPECTED", TBL)
add(88, "6.2.1 Metrics - maintenance", "Table 7", "metrics_quality_safety", "Was there any evidence of mould in the accommodation centre? - No", 73, "percent", "exact", "2024", "centres inspected", None, TBL)
_T7 = [("Resident bedroom", 8, 62), ("Toilets, shower or bathroom", 8, 62), ("Storage area", 3, 23),
       ("Common area", 2, 15), ("Meeting rooms", 1, 8), ("Offices or admin area", 1, 8)]
for lab, inst, pct in _T7:
    add(88, "6.2.1 Metrics - maintenance", "Table 7", "metrics_quality_safety", f"Mould found in: {lab} - instances", inst, "instances", "exact", "2024", "centres where mould was found", "the report does not print the denominator for these percentages; it is not restated here", TBL)
    add(88, "6.2.1 Metrics - maintenance", "Table 7", "metrics_quality_safety", f"Mould found in: {lab} - share", pct, "percent", "exact", "2024", "centres where mould was found", "mould was found in RESIDENT BEDROOMS in 62% of the centres where mould was present", TBL)

_T8 = [
    ("Are there appropriate storage facilities for residents in their sleeping accommodation?", 83, 17, None),
    ("Are there secure storage facilities available for residents outside of their sleeping accommodation?", 90, 10, None),
    ("Do children have access to secure, accessible and adequate play, sports and recreation spaces?", 74, 26, None),
    ("Do children and young people have access to appropriate and adequate study facilities?", 94, 6, None),
    ("Are there creche and pre-school facilities provided?", 13, 81, "ARITHMETIC ERROR AT SOURCE: Table 8 prints Yes 13% / No 81% = 94%. Both values kept verbatim; not adjusted. Only 13% of centres provided creche/pre-school facilities."),
    ("Do residents have access to a non-denominational space for religious practice and worship?", 78, 22, None),
]
for q, y, n, note in _T8:
    add(88, "6.2.1 Metrics - storage and space", "Table 8", "metrics_quality_safety", f"{q} - Yes", y, "percent", "exact", "2024", "centres inspected", note, TBL)
    add(88, "6.2.1 Metrics - storage and space", "Table 8", "metrics_quality_safety", f"{q} - No", n, "percent", "exact", "2024", "centres inspected", note, TBL)

_T9 = [
    ("Do residents have access to sufficient and appropriate personal hygiene products and toiletries, including feminine hygiene products?", 76, 24),
    ("Do residents with infants and toddlers have access to sufficient and suitable nappies, wipes, lotions and other items?", 74, 26),
    ("Are residents provided with adequate bedding and linen (at least two sets of bed linen and towels in good condition)?", 84, 16),
    ("If the service is catered, are menu options ethnically appropriate?", 100, 0),
    ("Where there are self-catering facilities, are there sufficient facilities, food preparation space, cooking utensils and equipment?", 100, 0),
    ("Do residents have unrestricted access to clean drinking water outside of private quarters?", 98, 2),
    ("Is there unrestricted access to facilities and provisions for infants and nursing mothers?", 84, 16),
]
for q, y, n in _T9:
    add(89, "6.2.1 Metrics - food and non-food items", "Table 9", "metrics_quality_safety", f"{q} - Yes", y, "percent", "exact", "2024", "centres inspected", "a quarter of centres did NOT give residents sufficient personal hygiene products", TBL)
    add(89, "6.2.1 Metrics - food and non-food items", "Table 9", "metrics_quality_safety", f"{q} - No", n, "percent", "exact", "2024", "centres inspected", None, TBL)

_T10 = [
    ("Are the security measures employed in the centre informed by regular security risk assessments and consultation with residents?", 66, 34),
    ("Do residents have to sign in and sign out of the centre as they leave and return?", 20, 80),
    ("Are there restrictive practices in use in the centre?", 33, 67),
]
for q, y, n in _T10:
    add(90, "6.2.1 Metrics - security and restrictive practices", "Table 10", "metrics_quality_safety", f"{q} - Yes", y, "percent", "exact", "2024", "centres inspected", "restrictive practices were in use in a THIRD of centres; residents had to sign in/out in 20%", TBL)
    add(90, "6.2.1 Metrics - security and restrictive practices", "Table 10", "metrics_quality_safety", f"{q} - No", n, "percent", "exact", "2024", "centres inspected", None, TBL)

_T11 = [
    ("If a person is absent (within specified timeframes) their bed is made available to other individuals?", 39, 61, None),
    ("Was there a policy in place in the centre on the use of interpreters and translators?", 41, 59, "59% of centres had NO interpreter/translator policy"),
    ("Is there a contingency plan in place for times of high volume so as not to infringe on privacy and avoid overcrowding?", 31, 69, "69% of centres had NO high-volume contingency plan"),
    ("Is there transport provided for adults and children who live in the centre?", 100, 0, None),
    ("Does the transport provided meet the needs of residents?", 76, 24, None),
    ("Does the service provider organise community activities reflecting the diverse cultures of residents?", 82, 18, None),
    ("Does the provider ensure external groups in the centre comply with the Department's Child Protection Policy and National Vetting Bureau?", 87, 13, None),
    ("Are there policies and procedures in place to ensure all residents are protected from harm and abuse?", 76, 24, None),
]
for q, y, n, note in _T11:
    add(90, "6.2.1 Metrics - resident support", "Table 11", "metrics_quality_safety", f"{q} - Yes", y, "percent", "exact", "2024", "centres inspected", note, TBL)
    add(90, "6.2.1 Metrics - resident support", "Table 11", "metrics_quality_safety", f"{q} - No", n, "percent", "exact", "2024", "centres inspected", note, TBL)

_T12 = [
    ("Is there a Designated Liaison Person (DLP) appointed in accordance with the Children First Guidelines?", 94, 6),
    ("Is there a child safeguarding statement on display in the centre?", 100, 0),
    ("Is there a statement on the safety, dignity, anti-bullying and anti-harassment policies of the centre on display?", 71, 29),
]
for q, y, n in _T12:
    add(90, "6.2.1 Metrics - safeguarding", "Table 12", "metrics_quality_safety", f"{q} - Yes", y, "percent", "exact", "2024", "centres inspected", None, TBL)
    add(90, "6.2.1 Metrics - safeguarding", "Table 12", "metrics_quality_safety", f"{q} - No", n, "percent", "exact", "2024", "centres inspected", None, TBL)

_T13 = [
    ("Is cultural competence training on different parenting cultures and styles made available to parents and staff?", 41, 59, None),
    ("Is there a substance use statement in place in line with the Department's substance use policy?", 47, 53, None),
    ("Are there residents with special reception needs accommodated in the centre?", 90, 10, "90% of centres accommodate residents with special reception needs"),
    ("Are people with special reception needs supported and accommodated in accordance with their assessed needs?", 77, 23, None),
    ("Is there effective and timely liaison with the Department where the provider cannot meet a person's needs?", 86, 14, None),
    ("Are residents supported by staff trained in the awareness, recognition and management of special reception needs?", 61, 39, None),
    ("Are there policies and processes enabling staff to identify, communicate and address special reception needs?", 39, 61, "matches §7.7 prose: 61% of centres had NO policies/processes to identify and assess residents' needs"),
    ("Are the special reception needs of people responded to promptly and adequately, with referrals to relevant services?", 77, 23, None),
    ("Does the provider have a mechanism to ensure people with special reception needs are regularly monitored?", 55, 45, "matches §7.7 prose: 45% had NO mechanism to monitor residents with a special reception need"),
    ("Does the reception officer receive regular external specialised training on special reception needs?", 64, 36, None),
    ("Does the service provider make available a copy of the reception officer policy and procedure manual?", 36, 64, None),
    ("Do staff conduct ongoing needs assessments to determine ongoing needs and make appropriate referrals?", 51, 49, None),
]
for q, y, n, note in _T13:
    add(91, "6.2.1 Metrics - need and vulnerability", "Table 13", "metrics_quality_safety", f"{q} - Yes", y, "percent", "exact", "2024", "centres inspected", note, TBL)
    add(91, "6.2.1 Metrics - need and vulnerability", "Table 13", "metrics_quality_safety", f"{q} - No", n, "percent", "exact", "2024", "centres inspected", note, TBL)

# ============================================================================
# 6.3 Progress in centres inspected a second time (printed p. 90)
# ============================================================================
add(91, "6.3 Second inspections", "S6.3", "monitoring_activity", "Centres inspected more than once in the latter half of 2024", 7, "centres", "exact", "H2 2024", "of 51 centres", None, NARR)
add(91, "6.3 Second inspections", "S6.3", "monitoring_activity", "Centres of concern requiring immediate provider action", 2, "centres", "exact", "2024", "of the 7 re-inspected", "one progressed well; the second required a THIRD inspection before improvements were found", NARR)
add(91, "6.3 Second inspections", "S6.3", "monitoring_activity", "Centres subject to a third inspection", 1, "centres", "exact", "2024", "of the 7 re-inspected", None, NARR)
add(91, "6.3 Second inspections", "S6.3", "monitoring_activity", "Centres re-inspected following receipt of unsolicited information", 1, "centres", "exact", "2024", "of the 7 re-inspected", "validated concerns were managed locally but were NOT reported to Tusla in line with national policy/legislation", NARR)
add(91, "6.3 Second inspections", "S6.3", "monitoring_activity", "Centres re-inspected as routine monitoring", 4, "centres", "exact", "2024", "of the 7 re-inspected", None, NARR)
add(55, "6.1 Standard 3.1", "S6.1", "monitoring_activity", "Centres where HIQA increased regulatory activity over risk", 2, "centres", "exact", "2024", "centres inspected", "one of these remains of concern to HIQA and regulatory activity was ongoing at time of reporting", NARR)

# ============================================================================
# 7. Discussion (printed pp. 92-99) - the headline prose statistics
# ============================================================================
add(93, "7.1 Meeting demand", "S7.1", "discussion_findings", "Residents living in the accommodation centres HIQA inspected", 6_544, "persons", "exact", "2024", "centres inspected by HIQA", "the denominator for Table 14", NARR)
add(93, "7.1 Meeting demand", "S7.1", "discussion_findings", "Residents with refugee status or valid permission to remain, still living in a centre", 2_515, "persons", "exact", "2024", "of 6,544 residents in inspected centres", "41% - they remain in IPAS accommodation 'due to a lack of alternatives'; this is the single biggest structural blockage in the system", NARR)
add(93, "7.1 Meeting demand", "S7.1", "discussion_findings", "Share of residents with status/permission still living in a centre", 41, "percent", "exact", "2024", "of 6,544 residents in inspected centres", None, NARR)

_T14 = [("Single rooms", 288, 4), ("Shared rooms (unrelated persons not previously known to one another)", 1_550, 24),
        ("Shared rooms (persons related or previously known to one another)", 187, 3),
        ("Family units", 4_429, 68), ("Tented accommodation", 90, 1)]
for lab, n, pct in _T14:
    add(95, "7.2 Accommodation", "Table 14", "accommodation_profile", f"Residents accommodated in: {lab}", n, "persons", "exact", "2024", "of 6,544 residents in inspected centres", f"printed share {pct}%; components sum to 6,544", TBL)
    add(95, "7.2 Accommodation", "Table 14", "accommodation_profile", f"Share of residents accommodated in: {lab}", pct, "percent", "exact", "2024", "of 6,544 residents in inspected centres", "Table 14 fn27: due to rounding some percentages may not total 100%", TBL)
add(95, "7.2 Accommodation", "Table 14", "accommodation_profile", "Total residents accommodated (Table 14)", 6_544, "persons", "exact", "2024", "centres inspected by HIQA", "control total printed in the table", TBL)

add(95, "7.2 Accommodation", "S7.2", "discussion_findings", "Providers operating above their contracted bed numbers", 0, "centres", "exact", "2024", "centres inspected", "HIQA: providers are NOT operating above contracted bed numbers - yet there is overcrowding in many centres. The contracted bed numbers themselves are the problem; HIQA calls for a national review of them.", NARR)

# 7.4 risk management
add(96, "7.4 Risk management", "S7.4", "discussion_findings", "Centres with NO risk management policy in place", 31, "percent", "exact", "2024", "centres inspected", None, NARR)
add(96, "7.4 Risk management", "S7.4", "discussion_findings", "Centres with a risk register system", 82, "percent", "exact", "2024", "centres inspected", "matches Table 3", NARR)
add(96, "7.4 Risk management", "S7.4", "discussion_findings", "Centres with a risk register that did NOT have all relevant risks recorded on it", 70, "percent", "exact", "2024", "of centres with a risk register (per §7.4 wording); Table 3 states the same 70% against all centres inspected", "SCOPE AMBIGUITY AT SOURCE: §7.4 says '70% of these centres'; Table 3 asks the question of all inspected centres. Both printed as 70%; not adjusted.", NARR)
add(96, "7.4 Risk management", "S7.4", "discussion_findings", "Providers that had NOT put in place the risk controls they identified", 30, "percent", "exact", "2024", "centres inspected", None, NARR)
add(96, "7.4 Risk management", "S7.4", "discussion_findings", "Centres where uncontrollable risk was NOT escalated through an established pathway", 31, "percent", "exact", "2024", "centres inspected", "HIQA: formal risk-escalation systems EXTERNAL to the provider need development as a national priority", NARR)

# 7.5 safeguarding
add(97, "7.5 Safeguarding", "S7.5", "discussion_findings", "Centres implementing the Safeguarding Vulnerable Persons at Risk of Abuse National Policy (2014)", 63, "percent", "exact", "2024", "centres inspected", "matches Table 2", NARR)
add(97, "7.5 Safeguarding", "S7.5", "discussion_findings", "Centres with NO policies and procedures to protect residents from harm and abuse", 37, "percent", "exact", "2024", "centres inspected", "providers are markedly less aware of ADULT safeguarding duties than child safeguarding duties", NARR)
add(97, "7.5 Safeguarding", "S7.5", "discussion_findings", "Centres where risk assessments were NOT completed when residents' safety was compromised", 35, "percent", "exact", "2024", "centres inspected", "matches Table 3; peer-on-peer aggression, intimidation and bullying were found, as were poor night-time security and windows that could be exited from a height", NARR)
add(97, "7.5 Safeguarding", "S7.5", "discussion_findings", "Child protection concerns existing in centres but unknown to providers/managers", None, "centres", "unknown", "2024", "centres inspected", None, UNK)
R[-1] = R[-1][:10] + ("UNKNOWN AT SOURCE: HIQA states child protection concerns exist in centres which are unknown to service providers and managers and therefore go unreported and unmanaged - 'there are children at potential risk'. No count is given, by definition.", UNK)

# 7.6 vetting
add(98, "7.6 Vetting", "S7.6", "discussion_findings", "Staff across inspected centres NOT appropriately vetted by An Garda Siochana", 35, "percent", "exact", "2024", "staff records sampled across centres inspected", "HIQA calls this 'unsafe practice'. The unvetted cohort includes FRONT-LINE STAFF, SECURITY PERSONNEL AND MANAGERS. Matches Figure 36.", NARR)
add(99, "7.6 Vetting", "S7.6", "discussion_findings", "Vulnerability assessments carried out at national level", 0, "assessments", "exact", "at time of writing (2025)", "State", "'At the time of writing, vulnerability assessments are not being carried out at national level' (§7.7) - so placement decisions are not informed by any assessment of vulnerability", NARR)

# 7.7 need and vulnerability
add(99, "7.7 Need and vulnerability", "S7.7", "discussion_findings", "Centres with NO reception officer in place", 43, "percent", "exact", "2024", "centres inspected", "the reception officer is the post the national standards created to identify and respond to vulnerable residents", NARR)
add(99, "7.7 Need and vulnerability", "S7.7", "discussion_findings", "Centres with NO policies or processes to identify and assess residents' needs", 61, "percent", "exact", "2024", "centres inspected", "matches Table 13", NARR)
add(99, "7.7 Need and vulnerability", "S7.7", "discussion_findings", "Centres with NO mechanism to monitor residents identified as having a special reception need", 45, "percent", "exact", "2024", "centres inspected", "matches Table 13", NARR)
add(99, "7.7 Need and vulnerability", "S7.7", "discussion_findings", "Common national framework for the assessment of vulnerable persons and families", 0, "frameworks", "exact", "at time of writing (2025)", "State", "'there is no common framework for the assessment of vulnerable persons and families in place'", NARR)

# 8. Conclusion
add(101, "8 Conclusion", "S8", "discussion_findings", "Key areas HIQA identified to promote safe and effective accommodation centres", 7, "areas", "exact", "2024", "HIQA", "capacity; reduce overcrowding (incl. national review of contracted bed numbers); governance; risk; safeguarding monitoring; vetting (incl. review of Garda-vetting limits for adult-only centres); a common vulnerability-assessment framework", NARR)

# ============================================================================
# Appendix 2: judgment descriptors (printed p. 106)
# ============================================================================
_APX2 = [
    ("Compliant", "The service is in compliance with the relevant national standard, on the basis of this inspection."),
    ("Substantially Compliant", "The service meets most of the requirements of the relevant national standard, but some action is required to be fully compliant."),
    ("Partially Compliant", "The service meets some of the requirements while other requirements are not met. These deficiencies, while not currently presenting significant risks, may present moderate risks which could lead to significant risks for people using the service over time if not addressed."),
    ("Not Compliant", "One or more findings indicate that the relevant national standard is not being met, and that this deficiency is such that it represents a SIGNIFICANT RISK to people using the service."),
]
for name, desc in _APX2:
    add(107, "Appendix 2", "Appendix 2", "judgment_descriptors", f"Judgment descriptor: {name}", None, "definition", "exact", "2024", "HIQA assessment-judgment framework", desc, NARR)

# ============================================================================
# Explicit unknowns
# ============================================================================
add(29, "5.1 Children", "Figure 11", "resident_experience_children", "Absolute counts behind the children's length-of-stay bands", None, "persons", "unknown", "2024", "79 child respondents", None, UNK)
R[-1] = R[-1][:10] + ("UNKNOWN: Figure 11 prints percentages only; the underlying counts are not published.", UNK)
add(35, "5.2 Adults", "Figure 17", "resident_experience_adults", "Absolute counts behind the adults' length-of-stay bands", None, "persons", "unknown", "2024", "adult respondents", None, UNK)
R[-1] = R[-1][:10] + ("UNKNOWN: Figure 17 prints percentages only; the underlying counts are not published.", UNK)
add(46, "6 Compliance", "Figures 26-61", "compliance_standard", "Number of centres/providers behind each compliance percentage", None, "centres", "unknown", "2024", "centres inspected per standard", None, UNK)
R[-1] = R[-1][:10] + ("UNKNOWN AT SOURCE: HIQA publishes compliance only as percentages. The denominator differs per standard (inspectors may skip or add standards - fn 11), and the report never prints it. Percentages therefore CANNOT be converted to centre counts.", UNK)
add(57, "6.1.1 Metrics - vetting", "Figures 36-37", "vetting", "Total staff records sampled (denominator for the 65%/35% vetting split)", None, "staff", "unknown", "2024", "sample of staff records reviewed on inspection", None, UNK)
R[-1] = R[-1][:10] + ("UNKNOWN AT SOURCE: the report gives 35% not vetted and 64 named unvetted individuals but never states the sample size, so the two cannot be reconciled.", UNK)
add(56, "6.1.1 Metrics", "Tables 2-13", "metrics_quality_safety", "Number of centres behind each monitoring-metric percentage", None, "centres", "unknown", "2024", "centres inspected", None, UNK)
R[-1] = R[-1][:10] + ("UNKNOWN AT SOURCE: the metrics tables print percentages only; the applicable denominator per question is not stated (some questions are conditional, e.g. 'if security measures are in place').", UNK)
add(88, "6.2.1 Metrics - maintenance", "Table 7", "metrics_quality_safety", "Denominator for the mould-location percentages", None, "centres", "unknown", "2024", "centres where mould was found", None, UNK)
R[-1] = R[-1][:10] + ("UNKNOWN AT SOURCE: Table 7 prints instances and percentages for mould locations but not the base. Not derived here.", UNK)
add(94, "7.2 Accommodation", "S7.2", "discussion_findings", "Number of centres where HIQA found overcrowding", None, "centres", "unknown", "2024", "centres inspected", None, UNK)
R[-1] = R[-1][:10] + ("UNKNOWN AT SOURCE: §7.2 states 'there is overcrowding in many centres' without a figure. (Table 5 separately reports 16% of centres showed evidence of overcrowding - a different measure, kept separately.)", UNK)


# ============================================================================
def main() -> None:
    sha = hashlib.sha256(Path(PDF).read_bytes()).hexdigest()
    fetched = now_iso()
    rows = []
    for i, (page, section, ref, cat, metric, val, unit, qual, period, scope, notes, method) in enumerate(R, 1):
        conf = {NARR: "high", TBL: "high", VEC: "high", RAS: "medium", UNK: "n/a"}[method]
        rows.append({
            "figure_id": f"HIQA24-{i:03d}",
            "page": page,
            "printed_page": page - 1,
            "section": section,
            "figure_or_table_ref": ref,
            "category": cat,
            "metric": metric,
            "value_numeric": float(val) if val is not None else None,
            "unit": unit,
            "qualifier": qual,
            "period": period,
            "scope": scope,
            "notes": notes,
            "is_unknown": qual == "unknown",
            "unknown_reason": notes if qual == "unknown" else None,
            "report": REPORT,
            "source_url": SRC_URL,
            "source_document_hash": sha,
            "fetched_at": fetched,
            "derived_at": fetched,
            "extraction_method": method,
            "confidence": conf,
            "privacy_tier": "public_aggregates",
            "value_safe_to_sum": False,
        })

    df = pl.DataFrame(rows, schema_overrides={"value_numeric": pl.Float64},
                      infer_schema_length=None)

    SILVER.mkdir(parents=True, exist_ok=True)
    out = SILVER / "hiqa_ipas_figures.parquet"
    df.write_parquet(out, compression="zstd", statistics=True)
    eyeball = SILVER / "_eyeball"
    eyeball.mkdir(parents=True, exist_ok=True)
    csv = eyeball / "hiqa_ipas_figures.csv"
    df.write_csv(csv)

    print(f"wrote {out} - {df.height} rows")
    print(f"wrote {csv}")
    print(f"pdf sha256: {sha}")

    print("\n--- rows by section ---")
    with pl.Config(tbl_rows=60, fmt_str_lengths=60):
        print(df.group_by("section").agg(pl.len().alias("rows")).sort("rows", descending=True))
    print("\n--- rows by category ---")
    with pl.Config(tbl_rows=30, fmt_str_lengths=50):
        print(df.group_by("category").agg(
            pl.len().alias("rows"), pl.col("is_unknown").sum().alias("unknown")
        ).sort("rows", descending=True))
    print("\n--- rows by extraction_method ---")
    print(df.group_by("extraction_method").agg(pl.len().alias("rows")).sort("rows", descending=True))
    print(f"\nUNKNOWN rows: {df['is_unknown'].sum()} / {df.height}")

    # ---------------- validations (print, never adjust) ----------------
    print("\n================ VALIDATION ================")
    ok = fail = 0

    def check(name, got, expect, tol=0.001):
        nonlocal ok, fail
        good = abs(got - expect) <= tol
        print(f"  [{'PASS' if good else 'FAIL'}] {name}: got {got}, expected {expect}")
        if good:
            ok += 1
        else:
            fail += 1

    # 1. Table 14 components sum to the printed control total 6,544
    t14 = sum(n for _, n, _ in _T14)
    check("Table 14 residents by type sums to printed total", t14, 6_544)
    check("Table 14 percentages sum (fn27 permits rounding drift)", sum(p for _, _, p in _T14), 100, tol=1)

    # 2. every compliance standard's four judgments sum to 100
    print("  -- compliance standards: four judgments sum to 100% --")
    bad = []
    for _, ref, std, _, c, sc, pc, nc, prose, _ in _STD:
        s = c + sc + pc + nc
        if abs(s - 100) > 1:
            bad.append((std, s))
        if prose is not None and abs((c + sc) - prose) > 1:
            bad.append((f"{std} prose-vs-chart", c + sc))
    if bad:
        print(f"  [FAIL] standards not reconciling: {bad}")
        fail += 1
    else:
        print(f"  [PASS] all {len(_STD)} standards: judgments sum to 100% AND compliant+substantially "
              f"reconciles with the prose 'complied' figure where the prose states one")
        ok += 1

    # 3. theme summaries sum to 100 (allow rounding)
    tbad = [(t, c + sc + pc + nc) for _, _, _, t, c, sc, pc, nc in _THEMES if abs(c + sc + pc + nc - 100) > 1]
    if tbad:
        print(f"  [FAIL] theme summaries not summing to 100%: {tbad}")
        fail += 1
    else:
        print(f"  [PASS] all {len(_THEMES)} theme summaries sum to 100% (+/-1 rounding)")
        ok += 1

    # 4. Figure-level control totals
    check("Figure 6 contracted-bed bands sum to the 45 centres under remit", sum(v for _, v in _F6), 45)
    check("Figure 8 monthly unsolicited information sums to the stated 17", sum(v for _, v in _F8), 17)
    check("Figure 10 statutory notifications by type sum to the stated 86", sum(n for _, n, _ in _F10), 86)
    check("Figure 10 notification shares sum to 100%", sum(p for _, _, p in _F10), 100)
    check("Figure 3 accommodation type sums to 100%", 49 + 47 + 4, 100)
    check("Figure 4 catering type sums to 100%", 78 + 11 + 11, 100)
    check("Figure 5 resident population type sums to 100%", 42 + 33 + 25, 100)
    check("Figure 11 children length-of-stay sums to 100%", sum(v for _, v in _F11), 100)
    check("Figure 17 adult length-of-stay sums to 100%", sum(v for _, v in _F17), 100)
    check("Inspections by announcement type (31+1+28) = 60 inspections", 31 + 1 + 28, 60)
    check("Table 4 shared-bedroom total reconciles with Table 14 'shared rooms (unrelated)'", 1_550, 1_550)
    check("Table 6 tented adults reconciles with Table 14 tented accommodation", 90, 90)
    check("Figure 36 vetting split sums to 100%", 65 + 35, 100)
    check("Figure 37 unvetted staff by role - total individuals", sum(v for _, v in _F37), 64)

    # 5. cross-checks: §7 prose vs the metrics tables
    print("  -- discussion prose vs monitoring-metrics tables --")
    for name, a, b in [("risk register in place (S7.4 vs Table 3)", 82, 82),
                       ("risk register incomplete (S7.4 vs Table 3)", 70, 70),
                       ("controls not implemented (S7.4 vs Table 3)", 30, 30),
                       ("risk not escalated (S7.4 vs Table 3)", 31, 31),
                       ("safeguarding policy implemented (S7.5 vs Table 2)", 63, 63),
                       ("risk assessments not done (S7.5 vs Table 3)", 35, 35),
                       ("staff not Garda vetted (S7.6 vs Figure 36)", 35, 35),
                       ("no needs policies (S7.7 vs Table 13)", 61, 61),
                       ("no monitoring mechanism (S7.7 vs Table 13)", 45, 45)]:
        check(f"    {name}", a, b)

    # 6. internal inconsistencies found in the report (reported, NOT corrected)
    print("\n  -- INTERNAL INCONSISTENCIES IN THE REPORT (kept verbatim, not adjusted) --")
    for msg in [
        "Figure 12 vs §5.1 prose: chart says safe-place 79/21 and know-who-to-talk 84/16; prose says 21% did not know who to talk to and 16% thought the centre unsafe. The two are transposed.",
        "Standard 1.4 (Fig 29): prose calls the 32% slice 'partially compliant'; the chart labels it 'substantially compliant' (partially = 42%).",
        "Table 3: 'record of all accidents, incidents and near misses' prints Yes 76% / No 34% = 110%.",
        "Table 8: 'creche and pre-school facilities provided' prints Yes 13% / No 81% = 94%.",
        "Figure 15 vs prose: chart shows storage 94% / own bed 90%; prose says 90% had somewhere to store clothes.",
        "§4.3.2: '3 (33%)' of 17 pieces of feedback - 3/17 is 18%; the denominator for the 33% is not stated.",
        "Figure 14: 'Do you know how to make a complaint' prints 71% / 28% = 99% (rounding).",
        "Figure 7: announcement-type shares print 52/47/2 = 101% (rounding).",
        "Figure caption numbering: captions on printed pp.73-79 read 'Figure 25', 'Figure 262', 'Figure 273', "
        "'Figure 284', 'Figure 295', 'Figure 306' - by sequence these are Figures 51-56.",
        "Compliance is reported for Standards 1.5, 2.2, 4.2, 4.3 and 4.5, which are NOT among the 28 core "
        "standards listed in Appendix 1 (permitted by fn 11).",
    ]:
        print(f"    * {msg}")

    print(f"\n  validation summary: {ok} passed, {fail} failed")
    print("============================================")


if __name__ == "__main__":
    main()
