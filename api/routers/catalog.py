"""/v1/catalog — a manifest of the published API resources.

Deliberately a curated list, not an auto-dump of every view/parquet: some
underlying tables carry PII (e.g. SIPO donor addresses, personal insolvency) and
must never be exposed. Only resources with a dedicated, reviewed endpoint appear
here. Live row counts are read from the registered views.
"""

from __future__ import annotations

from fastapi import APIRouter, Request

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
