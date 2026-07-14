"""Project Initiation Document — Implementation of the new Model of Accommodation and Supports
for International Protection applicants (White Paper to End Direct Provision).

DCEDIY, IPSS Transition Team, v1.4, 15 October 2021. Owner: Paula Quinn.
Ref: 211021(3)/03/Project Initiation Document/PBIPSS

WHY IT MATTERS: this is the governance document for the WHITE PAPER model — the model the
2024 Comprehensive Accommodation Strategy says was overtaken ("the assumptions underpinning
the White Paper have shifted dramatically"). Every PID target the Strategy later contradicts
is emitted as its own row, tagged in `scope` with SUPERSEDED and carrying the Strategy's
replacement value in `notes`, so the two documents can be compared directly:

    PID (Oct 2021)                                 -> Strategy (2024)
    3,500 applicants/yr flow-through               -> 13,000-16,000 arrivals/yr projected
    4 months in a Reception & Integration Centre   -> "at least 6 months"
    6 RICs, up to 2,000 residents                  -> 13,000 State-owned RIC/AC beds
    congregated settings phased out by end-2024    -> 10,000 emergency + 11,000 contingency
                                                      COMMERCIAL beds still planned for 2028
    not-for-profit model, away from private cos    -> 9,000 persons under the commercial strand
    complete + fully evaluated by 2024             -> horizon moved to end-2028
    first-instance decision within 6 months        -> ~17-month median (IGEES, via C&AG 10.15)

The PID states NO budget figure and includes NO appendix, though it refers to both — recorded
as explicit UNKNOWN rows, never guessed.

SANDBOX ONLY. All rows value_safe_to_sum=False (governance/narrative grain).
"""
from __future__ import annotations

from pathlib import Path

import polars as pl

from _common import BRONZE, SILVER, now_iso, sha256_bytes

DOC_KEY = "project_initiation_document"
DOC_TITLE = ("Project Initiation Document - Implementation of the new Model of Accommodation "
             "and Supports for International Protection applicants (White Paper)")
SRC_URL = "repo root: project-initiation-document.pdf (DCEDIY, PID v1.4, 15 Oct 2021)"
PDF = BRONZE / "ipas_context" / "project_initiation_document.pdf"

SUPERSEDED = "SUPERSEDED BY THE 2024 COMPREHENSIVE ACCOMMODATION STRATEGY"

# (page, ref, section, category, subject, metric, value_numeric, value_text, unit,
#  qualifier, period, scope, notes)
T: list[tuple] = []

# ---------------- document identity / governance ----------------
T += [
    (2, "Document Ownership and Approval", "Purpose", "policy_target", "DCEDIY",
     "PID version", 1.4, "V1.4", "version", "exact", "15 October 2021", "document control",
     "Prepared by the International Support Service (IPSS) Transition Team. Owner: Paula Quinn, "
     "Head of IPSS Transition Team. Ref: 211021(3)/03/Project Initiation Document/PBIPSS. The "
     "Approval Record table is PRINTED BLANK - no date, no name, no signature (see UNKNOWN row)."),
    (2, "Document Ownership and Approval", "Purpose", "policy_target", "DCEDIY",
     "Days between the PID date and its own next review date", 6, "15 Oct 2021 -> 21 Oct 2021",
     "days", "exact", "October 2021", "document control",
     "The PID sets its Next Review Date SIX DAYS after its own date. No later version is "
     "published alongside it; whether the 21 Oct 2021 review happened is unknown."),
    (3, "Definition", "Project Initiation Document Definition", "legal_obligation", "the State",
     "Statutory basis for the obligation to accommodate", None,
     "Under the recast Reception Conditions Directive, which Ireland transposed into law in July "
     "2018, there is a statutory obligation on the state to provide accommodation for any person "
     "who seeks International Protection.",
     "text", "exact", "2021", "any person who seeks International Protection",
     "VERBATIM, the PID's opening line. The SAME obligation the 2024 Strategy admits the State "
     "is failing ('For a second time this year the state is unable to fulfil these "
     "obligations') - see accommodation_strategy_facts."),
    (3, "Definition", "Project Initiation Document Definition", "policy_target", "White Paper",
     "Date the White Paper to End Direct Provision was published", None, "26 February 2021",
     "date", "exact", "2021", "State policy",
     "Published by Roderic O'Gorman T.D., Minister for Children, Equality, Disability, "
     "Integration and Youth, after consultation with civil society, residents of Direct "
     "Provision centres and other Departments."),
    (15, "Project Governance", "Project Governance", "policy_target", "DCEDIY",
     "Categories in the governance model", 3, None, "categories", "exact", "2021",
     "the project",
     "'The Department Project Structure', 'Monitoring Implementation of the Project', 'Key "
     "Implementation partners'. The PID says 'A full description of the governance model can be "
     "found at Appendix A' - THE PDF HAS NO APPENDIX A (see UNKNOWN row)."),
]

# ---------------- the targets the Strategy later contradicts ----------------
T += [
    (9, "Assumptions", "Scope & Project Delivery Details", "policy_target",
     "IP accommodation system", "Applicants the new model is designed to hold at any one time "
     "(Phase One)", 2_000, None, "persons", "exact", "2021-2024",
     f"Phase One (Reception & Integration Centres) - {SUPERSEDED}",
     "PID: 'The new model is being designed to accommodate 2,000 International Protection "
     "applicants at any one time in Phase One'. STRATEGY (2024): up to 13,000 State-owned "
     "RIC/AC beds by 2028 - a 6.5x increase on the PID's Phase One design."),
    (7, "Desired Outcomes/Deliverables", "Scope & Project Delivery Details", "policy_target",
     "State-owned estate", "New State-owned Reception and Integration Centres to be built", 6,
     None, "centres", "exact", "2021-2024", f"White Paper Phase One - {SUPERSEDED}",
     "PID: 'Build six new State-owned Reception and Integration Centres which can accommodate up "
     "to 2,000 residents, provide own-room or own-door accommodation'. One of the six was to be "
     "in Dublin (p9). STRATEGY (2024) replaces this with a 13,000-bed State-owned RIC/AC "
     "programme delivered via purchase, turnkey, rapid-build and design-and-build strands. "
     "C&AG (2024) records 7 State-owned centres among 49 IPAS long-term centres."),
    (9, "Assumptions", "Scope & Project Delivery Details", "policy_target",
     "IP accommodation system", "Annual applicant flow-through the model was sized for", 3_500,
     None, "persons_per_year", "exact", "2021-2024", f"Phase Two flow-through - {SUPERSEDED}",
     "PID: 'which will cater for the flow through of 3,500 every 12 months in Phase Two'. THE "
     "CENTRAL BROKEN ASSUMPTION. STRATEGY (2024): 'an average of 13,000-16,000 persons arrive "
     "between 2024 and 2028' - 3.7x to 4.6x the PID's number - and states the White Paper "
     "assumptions 'have shifted dramatically'."),
    (9, "Assumptions", "Scope & Project Delivery Details", "policy_target", "IP applicants",
     "Time an applicant remains in a Reception and Integration Centre", 4, None, "months",
     "exact", "2021-2024", f"Phase One stay - {SUPERSEDED}",
     "PID: 'Applicants will remain for four months in a Reception and Integration Centre and "
     "then move into Phase Two accommodation in the community'. STRATEGY (2024): 'available to "
     "each IP applicant for AT LEAST 6 months' - the Phase One stay lengthened by 50%+."),
    (9, "Assumptions", "Scope & Project Delivery Details", "policy_target", "IP applicants",
     "Average stay in Phase Two community accommodation", 14, None, "months", "approx",
     "2021-2024", f"Phase Two stay - {SUPERSEDED}",
     "PID: 'and then move into Phase Two accommodation in the community for an average of 14 "
     "months'. Implies a total ~18-month journey. STRATEGY (2024) has no Phase Two community "
     "stay of this kind; status-holders may remain 12 months after grant of status."),
    (4, "Definition", "Project Initiation Document Definition", "policy_target",
     "congregated accommodation", "Deadline to phase out congregated accommodation settings",
     None, "Congregated accommodation settings will be phased out by the end of 2024.", "date",
     "exact", "by end 2024", f"all IP accommodation - {SUPERSEDED}",
     "VERBATIM. FLATLY CONTRADICTED: the 2024 Strategy plans for up to 10,000 emergency + 11,000 "
     "contingency COMMERCIAL beds still in the system at end-2028, and the C&AG records 24,718 "
     "people in emergency accommodation at end 2024 - the year congregated settings were to have "
     "ended."),
    (4, "Definition", "Project Initiation Document Definition", "policy_target",
     "accommodation providers", "New accommodation to be managed by not-for-profit organisations",
     None,
     "New accommodation will be managed by independent not-for-profit organisations operating on "
     "behalf of the State.",
     "text", "exact", "2021-2024", f"the whole new model - {SUPERSEDED}",
     "VERBATIM. The PID's Constraints section adds: 'The new model is moving away from private "
     "companies and is focusing on a not-for-profit approach'. CONTRADICTED: the 2024 Strategy "
     "envisages 9,000 persons accommodated under the COMMERCIAL strand and 21,000 commercial "
     "beds by 2028; the C&AG reports EUR 978m of 2024 spend going to commercial providers."),
    (3, "Definition", "Project Initiation Document Definition", "policy_target", "IP applicants",
     "Accommodation type promised to every applicant", None,
     "All families will be offered own-door accommodation, while single people will be offered "
     "own-room accommodation.",
     "text", "exact", "2021-2024", f"all applicants - {SUPERSEDED}",
     "VERBATIM. Compare the National Standards, under which a single resident may only APPLY for "
     "a single bedroom after 9 months, and the C&AG's 2024 sample containing dormitory-style "
     "accommodation at EUR 51 per person per night."),
    (4, "Definition", "Project Initiation Document Definition", "policy_target", "DCEDIY",
     "Period over which the new model was to be implemented", None, "2021 to 2024", "date_range",
     "exact", "2021-2024", f"phased implementation - {SUPERSEDED}",
     "'A transition team has been established to implement the new accommodation model detailed "
     "in the White Paper on a phased basis between the years 2021 and 2024.' The 2024 Strategy "
     "moves the horizon to end-2028."),
    (9, "Assumptions", "Scope & Project Delivery Details", "applications", "IP applicants",
     "Target first-instance processing time", 6, None, "months", "under", "2021-2024",
     f"first-instance decisions - {SUPERSEDED}",
     "PID: 'The average processing times will be for first instance decisions to be made within "
     "6 months, and on appeal to International Protection Appeals Tribunal (IPAT), within a "
     "further 6 months, in line with the recommendation of the Advisory Group.' NOT ACHIEVED: "
     "the IGEES paper puts the MEDIAN END-TO-END time at ~17 months as of May 2024 (C&AG 10.15). "
     "The PID names processing time as a dependency on the Department of Justice and warns "
     "'Delays in processing times could have a significant impact on the efficacy of the model.'"),
    (9, "Assumptions", "Scope & Project Delivery Details", "applications", "IP applicants",
     "Target IPAT appeal processing time", 6, None, "months", "under", "2021-2024",
     f"appeal to IPAT - {SUPERSEDED}", "the second limb of the 6+6 month processing target."),
    (9, "Assumptions", "Scope & Project Delivery Details", "policy_target", "children",
     "Share of the 3,500 annual applicants assumed to be children", 33.3, "one third", "percent",
     "approx", "2021-2024", "planning assumption for service demand",
     "'One third of the 3,500 annual applicants will be children' (i.e. ~1,167/yr). Drives "
     "healthcare, education and Tusla service demand. For comparison the C&AG records 9,015 "
     "children among the 32,702 people accommodated at end 2024 (28%) - a share close to the "
     "PID's assumption, against a population ~9x larger than the model was sized for."),
    (7, "Desired Outcomes/Deliverables", "Scope & Project Delivery Details", "policy_target",
     "community housing", "Community housing to be secured for Phase Two flow-through", 3_500,
     None, "persons_per_year", "exact", "2021-2024", f"Phase Two - {SUPERSEDED}",
     "'Secure a supply of community housing, spread across the country, located in urban areas "
     "with access to public services that caters for the flow through of 3,500 International "
     "Protection applicants in a 12 month period.' The PID's own Constraints section flags this "
     "as doubtful: 'There are constraints on the housing supply nationally and it may be "
     "difficult to acquire the sufficient volume of accommodation units needed'."),
    (9, "Assumptions", "Scope & Project Delivery Details", "policy_target", "Dublin",
     "Reception and Integration Centres to be located in Dublin", 1, None, "centres", "exact",
     "2021-2024", f"1 of the 6 RICs - {SUPERSEDED}",
     "'One of the six Reception and Integration Centres will be located in Dublin.' "
     "Accommodation otherwise to be 'sourced across all parts of the country based [on] a Local "
     "Authority allocation key' - see the UNKNOWN row: the allocation key is never specified."),
]

# ---------------- deliverables (verbatim, not contradicted or not yet testable) ----------------
_DELIVERABLES = [
    ("Accommodation", "Commission ICT systems to support the operation of the new system of "
     "accommodation in Reception and Integration Centres and subsequently in community housing."),
    ("Accommodation", "Design and roll-out of Communication Strategies to support the "
     "implementation of the New Accommodation Model."),
    ("Accommodation", "Transition from the current IPAS system of accommodation to the new model "
     "of accommodation, moving all applicants in the current system to the new model as needed."),
    ("Integration", "Design and implement a comprehensive orientation programme, including an "
     "intensive English language course, which will be delivered to applicants in Phase One."),
    ("Integration", "Design and implement caseworker structures for Phase One and resettlement "
     "worker and intercultural worker structures for Phase Two to support the integration of "
     "applicants."),
    ("Integration", "Establish an Integration Support Fund Scheme to assist local communities in "
     "supporting the integration of applicants in Phase Two and beyond."),
    ("Support services", "Design and implement a holistic assessment process to evaluate "
     "applicants' language skills, work skills etc. on arrival in Reception and Integration "
     "Centres."),
    ("Support services", "Design and implement an inter-agency working group model located in "
     "each county, to coordinate the delivery of services to International Protection applicants "
     "at a local level."),
    ("Support services", "Design and implement a new International Protection support payment."),
    ("Support services", "Coordinate enhanced mainstream public services for International "
     "Protection applicants."),
    ("Support services", "Design and run a procurement programme for NGO support services."),
    ("Support services", "Establish an interpretation service."),
]
for i, (pillar, d) in enumerate(_DELIVERABLES, 1):
    T.append((7 if i <= 3 else (8 if i <= 6 else 9), f"Deliverable {i}", "Desired Outcomes/Deliverables",
              "policy_target", pillar, f"Deliverable {i} of {len(_DELIVERABLES)} ({pillar})", None,
              d, "text", "exact", "2021-2024", f"{pillar} pillar",
              "VERBATIM from the PID's Desired Outcomes/Deliverables. No target date, owner, cost "
              "or success measure is attached to any individual deliverable in this document."))

# ---------------- constraints & risks (verbatim) ----------------
_RISKS = [
    ("Capital delivery", "A major constraint will be delivering what is a large capital "
     "programme within the timeframe outlined in the White Paper. This will require active "
     "management throughout the implementation process as any delay could have a knock-on effect "
     "on a different area of the implementation process."),
    ("Housing supply", "Another considerable constraint is the housing capacity for Phase Two "
     "accommodation, especially in sourcing the accommodation that is needed for single people. "
     "There are constraints on the housing supply nationally and it may be difficult to acquire "
     "the sufficient volume of accommodation units needed in order for the new model to work "
     "efficiently."),
    ("Capital cost inflation", "In relation to financing this project, a potential constraint is "
     "that of higher capital costs that anticipated. Increased competition for construction "
     "resources may inflate the costs that will be incurred and it is difficult to estimate the "
     "potential extent of this."),
    ("AHB / NGO delivery capacity", "A potential constraint on the project is the reliance on the "
     "AHB sector to deliver community accommodation and support services for Phase Two of the "
     "new model. Delivery will depend on whether the AHBs have the capacity to provide these "
     "services. The new model is moving away from private companies and is focusing on a "
     "not-for-profit approach which is a new challenge in this area."),
    ("Community opposition", "There may also be an adverse reception from communities to the "
     "location of Reception and Integration centres. Locals may not want these centres being "
     "situated in their neighbourhoods but this constraint can be mitigated for by running "
     "public awareness campaigns and using the support of the inter-agency working groups in "
     "each local authority."),
    ("Public service capacity", "The use of mainstream services may also put excessive pressure "
     "on these services and may result in International Protection applicants not receiving the "
     "level of supports that they need."),
    ("Processing delays", "Delays in processing times could have a significant impact on the "
     "efficacy of the model."),
]
for i, (name, r) in enumerate(_RISKS, 1):
    T.append((9 if i <= 2 else (10 if i <= 6 else 11), f"Constraint {i}", "Constraints", "risk",
              name, f"Constraint/risk {i} of {len(_RISKS)}: {name}", None, r, "text", "exact",
              "2021", "the project",
              "VERBATIM from the PID's Constraints section. NOTE: the PID records constraints as "
              "NARRATIVE ONLY - there is no risk register, no likelihood/impact rating, no risk "
              "owner and no mitigation deadline anywhere in the document. Every one of these "
              "seven constraints materialised."))

# ---------------- dependencies ----------------
_DEPS = [
    ("Department of Justice", "responsible for achieving the shorter processing times that are "
     "key to the success of the new model"),
    ("Department of Education", "responsible for ensuring access to primary and post-primary "
     "education, including for children with special education needs"),
    ("Department of Further and Higher Education, Innovation and Science (with the ETBs)",
     "responsible for delivering more intensive English language training and for ensuring "
     "access to further and higher education"),
    ("Department of Enterprise and Employment", "responsible for delivery of commitments in "
     "relation to business and employment supports"),
    ("Department of Social Protection", "responsible for business/employment supports, Phase One "
     "income supports and the payments systems for Phase Two income supports"),
    ("Department of Rural and Community Development", "responsible for ensuring access to "
     "community integration funding"),
    ("Department of Health and the HSE", "responsible for ensuring access to healthcare, "
     "including for those with mental health needs"),
    ("Local authorities", "responsible for chairing integration supports at local level and for "
     "the allocation key"),
]
for i, (body, role) in enumerate(_DEPS, 1):
    T.append((11, f"Dependency {i}", "Dependencies", "policy_target", body,
              f"Cross-departmental dependency {i} of {len(_DEPS)}: {body}", None, role, "text",
              "exact", "2021-2024", "cross-departmental delivery",
              "'This is a cross-departmental project so the delivery of the new model requires "
              "the delivery by departments and agencies of the aspects of the model for which "
              "they are responsible.' No memorandum, SLA, deadline or escalation route is "
              "specified for any of these dependencies."))

# ---------------- milestones by year ----------------
_MILESTONES = {
    2021: [
        "Appointing a Transition Team who will oversee the implementation of the White Paper.",
        "Initiating the planning process for projects that will require capital programmes.",
        "Deciding on an accelerated programme to support people with International Protection "
        "status to move from IPAS accommodation to new homes in the community.",
        "Commissioning accommodation to enable single people to have single occupancy bedrooms.",
        "Establishing the inter-agency working group structures which will oversee community "
        "engagement.",
        "Commissioning a bed management IT system that can identify availability of "
        "accommodation with the system.",
        "Beginning a national information process to inform communities and consult with them "
        "about the new model.",
    ],
    2022: [
        "Beginning the process of moving single people into single occupancy accommodation.",
        "Beginning the decommissioning of permanent centres.",
        "Advancing the new build projects and the purchases of properties.",
        "Developing the IT systems required to manage the system.",
        "Establishing resettlement workers and intercultural workers for each county.",
        "Providing additional English language supports through ETBs.",
        "Developing the orientation programme and accompanying English language supports to be "
        "provided at the Reception and Integration Centres.",
    ],
    2023: [
        "Advancing the capital projects to build new Reception and Integration Centres that will "
        "accommodate applicants during Phase One.",
        "Supporting eligible families into private tenancies.",
        "Ensuring that relevant services are planned and ready to operate in a coordinated manner "
        "in these centres by year end.",
        "Advancing the new build projects and the purchasing of accommodation.",
        "Decommissioning further permanent centres operated by private providers.",
        "Moving applicants into community hosting arrangements.",
        "Reviewing accommodation strands to determine if they are delivering the necessary "
        "capacity and quality.",
        "Reviewing how services are being coordinated and delivered to see if changes are needed.",
    ],
    2024: [
        "Operating the new Reception and Integration Centres according to the coordinated model "
        "of service delivery.",
        "Completing the new build projects.",
        "Commissioning further not-for-profit services to supports families and single people, "
        "including vulnerable people.",
        "Moving applicants into these accommodation and service options and reviewing their "
        "effectiveness for applicants.",
        "Decommissioning remaining permanent centres operated by private providers.",
        "Carrying out a full review and evaluation of the new model.",
    ],
}
_MPAGE = {2021: 12, 2022: 13, 2023: 13, 2024: 14}
for yr, ms in _MILESTONES.items():
    for i, m in enumerate(ms, 1):
        extra = ""
        if yr == 2024:
            extra = (" 2024 IS THE COMPLETION YEAR: the PID has the new-build projects COMPLETE, "
                     "the remaining private permanent centres DECOMMISSIONED, and a full review "
                     "and evaluation DONE by end-2024. In the event, the 2024 Strategy was "
                     "published instead, replacing the model with a 2028 horizon that keeps "
                     "21,000 commercial beds.")
        T.append((_MPAGE[yr], f"Milestone {yr}.{i}", "Project Schedule & Key Milestones",
                  "policy_target", f"White Paper implementation {yr}",
                  f"Milestone {i} of {len(ms)} for {yr}", None, m, "text", "exact", str(yr),
                  f"key milestone - {SUPERSEDED}",
                  "VERBATIM. The PID states these are 'key high level milestones that will "
                  "indicate when the project is complete', set 'under an expected timeframe'. NO "
                  "milestone carries a specific date, owner, cost or completion criterion." + extra))

# ---------------- procurement / budget narrative ----------------
T += [
    (11, "Procurement Strategy", "Procurement Strategy", "procurement_route", "IPSS Transition Team",
     "Procurement route for the new model", None,
     "This procurement will be carried out by the IPSS Transition Team in conjunction with the "
     "International Protection Procurement Services team and the Office of Government "
     "Procurement. ... This is a complex project that is still in the planning stage. A more "
     "structured approach to procurement will be applied as the project progresses. ... OGP "
     "frameworks will be used as appropriate. If no framework is available, tendering will be "
     "carried out according to EU guidelines.",
     "text", "exact", "2021", "public relations, property valuations, property management etc.",
     "VERBATIM (joined). The PID admits the project 'is still in the planning stage' with no "
     "structured procurement approach yet, TWO AND A HALF YEARS into a 2021-2024 programme "
     "window. Read against C&AG 2024, which found 161 of 325 centres operating without in-date "
     "contracts and direct awards dominating the sample."),
    (12, "Budget", "Budget", "expenditure", "the project",
     "How the budget is to be set", None,
     "The White Paper provides preliminary capital and current expenditure costings for the "
     "delivery of the new model of accommodation. ... The budget allocation for each year will be "
     "determined by the exchequer budget. A process will be put in place to review project "
     "milestones once the budget allocation is known each year.",
     "text", "exact", "2021-2024", "the whole project",
     "VERBATIM (joined). The PID CONTAINS NO FIGURE - it defers entirely to the White Paper's "
     "'preliminary costings' and to the annual exchequer process. See the UNKNOWN row: a project "
     "initiation document for a multi-year capital programme with no cost in it."),
]

# ---------------- EXPLICIT UNKNOWNS ----------------
U: list[tuple] = [
    (12, "Budget", "expenditure", "the project",
     "Capital and current expenditure budget for the White Paper model",
     "UNKNOWN AT SOURCE: the PID states NO budget figure of any kind - no capital total, no "
     "current-expenditure total, no annual profile, no per-centre or per-bed cost. It defers to "
     "'preliminary capital and current expenditure costings' in the White Paper, which is a "
     "SEPARATE document not held in this corpus, and to the annual exchequer allocation. The "
     "project's cost cannot be established from this document."),
    (15, "Project Governance", "policy_target", "DCEDIY",
     "The governance model: roles, membership, meeting cadence, escalation",
     "UNKNOWN AT SOURCE: the PID names three governance categories and then says 'A full "
     "description of the governance model can be found at Appendix A'. THE 15-PAGE PDF CONTAINS "
     "NO APPENDIX A - the document ends at that sentence. Governance roles, membership, reporting "
     "lines, meeting cadence and escalation routes are therefore not established. (The document "
     "also refers to external stakeholders 'as outlined in the appendix' and to 'the project "
     "implementation plan' - neither is present.)"),
    (2, "Approval Record", "policy_target", "DCEDIY",
     "Who approved the PID, and when",
     "UNKNOWN AT SOURCE: the Approval Record table (Date / Name and Signature) is PRINTED BLANK "
     "in the published PDF. No approver, no approval date. The document is v1.4 and dated 15 Oct "
     "2021 with a next-review date of 21 Oct 2021, but carries no evidence of having been "
     "formally approved."),
    (9, "Assumptions", "policy_target", "local authorities",
     "The Local Authority allocation key",
     "UNKNOWN AT SOURCE: 'Accommodation will be sourced across all parts of the country based [on] "
     "a Local Authority allocation key' and local authorities are made 'responsible ... for the "
     "allocation key' - but the key itself, its formula, its inputs and its weightings appear "
     "NOWHERE in the PID. The 2024 Strategy likewise says distribution criteria 'will be "
     "developed'. Neither document contains one."),
    (7, "Desired Outcomes/Deliverables", "policy_target", "Reception and Integration Centres",
     "Locations of the six Reception and Integration Centres",
     "UNKNOWN AT SOURCE: only that 'One of the six Reception and Integration Centres will be "
     "located in Dublin'. The other five are unnamed and unlocated, and no site, size or "
     "delivery date is given for any of the six."),
    (12, "Project Schedule & Key Milestones", "policy_target", "the project",
     "Milestone dates, owners and success criteria",
     "UNKNOWN AT SOURCE: the milestones are grouped by YEAR ONLY (2021/2022/2023/2024). Not one "
     "carries a date, a named owner, a cost, a dependency link or a measurable completion "
     "criterion, and the PID explicitly says the plan 'must be flexible'. The project's success "
     "criteria are therefore not testable from this document - which is itself the finding: a "
     "PID for a multi-year national capital programme with no dated, owned, costed milestone."),
    (None, "whole document", "risk", "the project",
     "Risk register: likelihood, impact, owner, mitigation",
     "UNKNOWN AT SOURCE: the PID has a narrative 'Constraints' section (captured as 7 risk rows) "
     "but NO risk register, no likelihood or impact scoring, no risk owner and no mitigation "
     "deadline. Contrast National Standard 3.1, which REQUIRES every accommodation provider to "
     "'carry out a regular risk analysis of the service and develop a risk register'."),
]


def build() -> pl.DataFrame:
    sha = sha256_bytes(Path(PDF).read_bytes())
    rows: list[dict] = []
    for (pg, ref, sec, cat, subj, metric, vnum, vtext, unit, qual, per, scope, notes) in T:
        rows.append({
            "page": pg, "printed_page": str(pg), "ref": ref, "section": sec, "category": cat,
            "subject": subj, "metric": metric,
            "value_numeric": float(vnum) if vnum is not None else None, "value_text": vtext,
            "unit": unit, "qualifier": qual, "period": per, "scope": scope,
            "is_unknown": False, "unknown_reason": None, "notes": notes,
        })
    for (pg, ref, cat, subj, metric, reason) in U:
        rows.append({
            "page": pg, "printed_page": str(pg) if pg else None, "ref": ref,
            "section": "whole document" if pg is None else ref, "category": cat, "subject": subj,
            "metric": metric, "value_numeric": None, "value_text": None, "unit": None,
            "qualifier": "unknown", "period": "2021", "scope": "not established by the document",
            "is_unknown": True, "unknown_reason": reason, "notes": None,
        })

    out = []
    for i, r in enumerate(sorted(rows, key=lambda r: (r["page"] or 999, r["category"])), 1):
        out.append({
            "fact_id": f"pid-{i:03d}", "doc_key": DOC_KEY, "doc_title": DOC_TITLE, **r,
            "source_url": SRC_URL, "source_document_hash": sha,
            "extraction_method": "manual_curation_from_fitz_text_full_read",
            "confidence": "high", "privacy_tier": "public_document",
            "value_safe_to_sum": False, "derived_at": now_iso(),
        })
    cols = ["fact_id", "doc_key", "doc_title", "page", "printed_page", "ref", "section",
            "category", "subject", "metric", "value_numeric", "value_text", "unit", "qualifier",
            "period", "scope", "is_unknown", "unknown_reason", "notes", "source_url",
            "source_document_hash", "extraction_method", "confidence", "privacy_tier",
            "value_safe_to_sum", "derived_at"]
    return pl.DataFrame(out, schema_overrides={"value_numeric": pl.Float64, "page": pl.Int64},
                        infer_schema_length=None).select(cols)


def main() -> None:
    df = build()
    out = SILVER / "pid_facts.parquet"
    df.write_parquet(out, compression="zstd", statistics=True)
    eye = SILVER / "_eyeball"
    eye.mkdir(exist_ok=True)
    df.write_csv(eye / "pid_facts.csv")
    print(f"wrote {out} - {df.height} rows")
    with pl.Config(tbl_rows=20, fmt_str_lengths=54, tbl_width_chars=160):
        print(df.group_by("category").agg(pl.len(), pl.col("is_unknown").sum().alias("unknown"))
              .sort("len", descending=True))
        sup = df.filter(pl.col("scope").str.contains("SUPERSEDED"))
        print(f"\ntargets superseded/contradicted by the 2024 Strategy: {sup.height}")
        print(sup.filter(pl.col("value_numeric").is_not_null())
              .select("metric", "value_numeric", "unit", "period"))
    print(f"\nunknown rows: {df['is_unknown'].sum()} / {df.height}")
    assert not df["value_safe_to_sum"].any()


if __name__ == "__main__":
    main()
