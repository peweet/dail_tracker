"""/v1/catalog — a manifest of the published API resources.

Deliberately a curated list, not an auto-dump of every view/parquet: some
underlying tables carry PII (e.g. SIPO donor addresses, personal insolvency) and
must never be exposed. Only resources with a dedicated, reviewed endpoint appear
here. Live row counts are read from the registered views.
"""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, Request

from api.deps import get_cursor
from dail_tracker_core import dossiers

router = APIRouter(tags=["meta"])

_RESOURCES = [
    {
        "resource": "members",
        "list": "/v1/members",
        "item": "/v1/members/{code}/dossier",
        "description": "TDs and Senators; the dossier composes attendance, votes, payments, "
        "lobbying, questions, legislation and ministerial history into one record.",
        "filters": ["house", "party", "constituency", "fuzzy_name"],
        "count_view": "v_member_registry",
    },
    {
        "resource": "legislation",
        "list": "/v1/legislation",
        "item": "/v1/legislation/{bill_id}",
        "description": "Bills; the dossier adds lifecycle, amendment intensity, sources, "
        "PDFs, debates and the statutory instruments made under the bill.",
        "filters": ["status", "title_search", "start_date", "end_date"],
        "count_view": "v_legislation_index",
    },
    {
        "resource": "statutory-instruments",
        "list": "/v1/statutory-instruments",
        "item": None,
        "description": "Statutory instruments (secondary legislation), 2016 onwards.",
        "filters": ["year", "operation", "department", "eu_only"],
        "count_view": "v_statutory_instruments",
    },
    {
        "resource": "votes",
        "list": "/v1/votes",
        "item": "/v1/votes/{vote_id}",
        "description": "Dáil/Seanad divisions; the dossier adds party breakdown, every "
        "member's vote, and source links.",
        "filters": ["house", "date_from", "date_to", "outcome"],
        "count_view": "v_vote_index",
    },
    {
        "resource": "payments",
        "list": "/v1/payments",
        "item": None,
        "description": "All-time Travel & Accommodation Allowance ranking by member.",
        "filters": ["house"],
        "count_view": "v_payments_alltime_ranking",
    },
    {
        "resource": "lobbying",
        "list": "/v1/lobbying/organisations",
        "item": None,
        "description": "Registered lobbying organisations (CRO + charity-enriched), plus "
        "/v1/lobbying/revolving-door (former office-holders now lobbying).",
        "filters": ["name", "exclude_state_adjacent"],
        "count_view": "v_experimental_lobbying_org_index_enriched",
    },
    {
        "resource": "procurement",
        "list": "/v1/procurement/suppliers",
        "item": "/v1/procurement/suppliers/{supplier_norm}/dossier",
        "description": "eTenders/TED suppliers (CRO + lobbying-overlap enriched); the dossier "
        "composes the ranking summary with every award. Also /v1/procurement/competition "
        "(per-buyer single-bidder rate, a signal not a verdict) and "
        "/v1/procurement/lobbying-overlap (co-occurrence only, never causation).",
        "filters": ["year", "order_by"],
        "count_view": "v_procurement_supplier_summary",
    },
    {
        "resource": "committees",
        "list": "/v1/committees",
        "item": "/v1/committees/{committee}",
        "description": "Oireachtas committees per chamber; the item adds the long-format party-seat breakdown.",
        "filters": ["chamber"],
        "count_view": "v_committee_member_detail",
    },
    {
        "resource": "ministers",
        "list": "/v1/ministers",
        "item": None,
        "description": "Who held a department on a given date (fuzzy department + ISO date); "
        "/v1/cabinet returns the current line-up.",
        "filters": ["department", "on_date"],
        "count_view": "v_member_ministerial_tenure",
    },
    {
        "resource": "ministerial-diaries",
        "list": "/v1/ministerial/diary/organisations",
        "item": "/v1/ministerial/diary/organisations/{name}",
        "description": "Who ministers meet, from their OWN published diaries: organisations ranked by "
        "logged meetings (with lobbying-register corroboration), one organisation's full access record, "
        "and a meeting search (/v1/ministerial/diary/meetings). Access, never proof of influence — "
        "self-curated, non-exhaustive, quarterly-in-arrears.",
        "filters": ["outside_only", "minister", "topic"],
        "count_view": "v_ministerial_diary_meetings",
    },
    {
        "resource": "corporate",
        "list": "/v1/corporate/notices",
        "item": None,
        "description": "Corporate distress/register notices from Iris Oifigiúil (receiverships, wind-ups, "
        "examinerships, SCARP). CORPORATE ONLY — no individuals; a notice is a legal-status fact, not a "
        "verdict. Also /v1/corporate/repeat-distress (CBI firms, experimental) and /v1/corporate/receivers "
        "(appointer/operator-firm rankings).",
        "filters": ["query", "subtype", "year"],
        "count_view": "v_corporate_notices",
    },
    {
        "resource": "political-finance",
        "list": "/v1/political-finance/donations",
        "item": None,
        "description": "SIPO-disclosed party donations + GE2024 candidate election expenses "
        "(/v1/political-finance/election-spend). Two distinct money grains — never summed. No "
        "donor-address field is exposed.",
        "filters": ["party"],
        "count_view": "v_sipo_donations",
    },
    {
        "resource": "judiciary",
        "list": "/v1/judiciary/appointments",
        "item": None,
        "description": "Judicial appointments + elevation ladder + sitting-bench roster; "
        "/v1/judiciary/courts-health gives system clearance/waiting times (names no judge).",
        "filters": ["limit"],
        "count_view": "v_judiciary_appointments",
    },
    {
        "resource": "charities",
        "list": "/v1/charities",
        "item": None,
        "description": "Charity finances — register-wide sector totals per year, or one charity's "
        "filed series by RCN. Figures are AS FILED (some filer data-entry errors).",
        "filters": ["rcn"],
        "count_view": "v_charity_sector_totals_by_year",
    },
    {
        "resource": "public-body-payments",
        "list": "/v1/public-body-payments",
        "item": None,
        "description": "Public-body payments/POs over €20k — the realised-SPEND grain. NEVER add "
        "to procurement AWARD ceilings (different value_kind).",
        "filters": ["side", "order_by"],
        "count_view": "v_public_payments",
    },
    {
        "resource": "public-appointments",
        "list": "/v1/public-appointments",
        "item": None,
        "description": "State-board and similar public-appointment notices, one row per notice.",
        "filters": [],
        "count_view": "v_public_appointments",
    },
    {
        "resource": "attendance",
        "list": "/v1/attendance/turnout",
        "item": None,
        "description": "Participation model: division turnout, longest absence runs, and TAA "
        "(120-day allowance) compliance by year. Office-holders are flagged, not hidden — a low "
        "rate is context, not a verdict. Also /v1/attendance/absences, /taa-compliance, "
        "/missing-members and /years.",
        "filters": ["year", "house"],
        "count_view": "v_attendance_participation_turnout",
    },
    {
        "resource": "housing",
        "list": "/v1/housing/waiting-list",
        "item": None,
        "description": "National social-housing demand (waiting list by county/LA/national), supply "
        "& affordability (/v1/housing/supply), and state asylum/Ukraine accommodation spend from the "
        "over-€20k PO registers (/v1/housing/accommodation-spend).",
        "filters": ["grain"],
        "count_view": "v_ssha_waiting_list_totals",
    },
    {
        "resource": "public-finance",
        "list": "/v1/public-finance/government-finance",
        "item": None,
        "description": "CSO general-government revenue/expenditure/balance per year (GFA01) — the "
        "'share of total public spend' denominator. National-accounts aggregate; never summed with "
        "transaction-level registers.",
        "filters": [],
        "count_view": "v_gov_finance_annual",
    },
    {
        "resource": "local-government",
        "list": "/v1/local-government/councils",
        "item": "/v1/local-government/councils/{local_authority}",
        "description": "Council accountability: the 31-council index + per-council dossier (NOAC "
        "scorecard, cash signals, planning-overturn, over-€20k procurement scale). Each figure stands "
        "alone beside the national benchmark — never apportioned, never summed across measures.",
        "filters": [],
        "count_view": "v_la_chief_executives",
    },
    {
        "resource": "constituencies",
        "list": "/v1/constituencies",
        "item": "/v1/constituencies/{name}/dossier",
        "description": "Per-constituency dossier: demographics, current Dáil TDs, party breakdown, "
        "Dáil work since GE2024, housing context, and the serving councils' money (each council figure "
        "stands alone).",
        "filters": [],
        "count_view": "v_constituency_registry",
    },
    {
        "resource": "councillors",
        "list": "/v1/councillors",
        "item": None,
        "description": "Elected local-authority members by council/LEA + the council's meeting-coverage "
        "data-state and Chief Executive (/v1/councillors/councils lists councils). Roll-call vote "
        "coverage is sparse (Carlow only so far).",
        "filters": ["council", "lea"],
        "count_view": "v_la_councillors",
    },
]


def _count(conn, view: str) -> int | None:
    if conn is None:
        return None
    try:
        row = conn.execute(f"SELECT count(*) FROM {view}").fetchone()  # noqa: S608 — view is a constant
        return int(row[0]) if row else None
    except Exception:  # noqa: BLE001
        return None


@router.get("/catalog", summary="Manifest of published resources + live counts")
def catalog(request: Request) -> dict:
    conn = getattr(request.app.state, "conn", None)

    resources = []
    for r in _RESOURCES:
        resources.append({**{k: v for k, v in r.items() if k != "count_view"}, "count": _count(conn, r["count_view"])})

    return {
        "licence": "CC-BY-4.0",
        "attribution": "Data via Dáil Tracker",
        "source": "Built from api.oireachtas.ie + lobbying.ie + SIPO + Charities Regulator (see per-resource provenance).",
        "resources": resources,
    }


@router.get("/coverage", summary="Per-domain scope guard: year ranges, corpus sizes, money-grain rules")
def coverage(cur: duckdb.DuckDBPyConnection = Depends(get_cursor)) -> dict:
    """What the tracker covers and how far back — consult before answering a time- or
    completeness-sensitive question. States the hard money-grain rules so a client never
    sums across procurement awards / public-body payments / T&A allowances."""
    return dossiers.data_coverage(cur)
