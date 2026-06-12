"""Dáil Tracker — MCP server (stdio transport).

Exposes the read-only core (dail_tracker_core) to AI assistants — Claude Desktop,
Claude Code, and any MCP client — as callable tools, so Irish parliamentary
accountability questions asked in plain English get answers grounded in real data.

History: lived in C:\\tmp\\dail_mcp while experimental; moved in-repo 2026-06-11
(it is product surface now — the chatbot-to-chatbot channel). The app/Cloud
deploy never imports this package; the ``mcp`` extra gates its dependency.

Run (stdio, for Claude Desktop / local clients):

    ./.venv/Scripts/python.exe mcp_server/server.py

⚠️ Stdio-only today. Do NOT expose this beyond a local client until the remote
transport work lands (streamable HTTP + API keys + audit logging — see
doc/COMMERCIAL_UPLIFT_PLAN.md §5/§6).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Windows: force the stdio JSON-RPC stream to UTF-8 so Irish-language names
# (Ó, á, Féin, Gaeltachta…) can't crash the protocol on a cp1252 default.
try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    sys.stdin.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
except Exception:
    pass

# Repo root on sys.path so dail_tracker_core / config / the parquet data resolve
# when launched as a script (MCP clients exec the file, not the package).
REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from mcp.server.fastmcp import FastMCP  # noqa: E402
from mcp.types import ToolAnnotations  # noqa: E402

from dail_tracker_core import dossiers, serialize  # noqa: E402
from dail_tracker_core.connections import api_conn  # noqa: E402
from dail_tracker_core.db import register_views  # noqa: E402
from dail_tracker_core.queries import appointments as appt  # noqa: E402
from dail_tracker_core.queries import charities as char  # noqa: E402
from dail_tracker_core.queries import judiciary as jud  # noqa: E402
from dail_tracker_core.queries import lobbying as lb  # noqa: E402
from dail_tracker_core.queries import ministerial as min_  # noqa: E402
from dail_tracker_core.queries import procurement as proc  # noqa: E402
from dail_tracker_core.queries import public_payments as pubpay  # noqa: E402
from dail_tracker_core.queries import sipo  # noqa: E402
from dail_tracker_core.queries import votes as vot  # noqa: E402
from mcp_server import qs_valuation, ted_conduit  # noqa: E402

mcp = FastMCP("dail-tracker")

# Every tool here is a pure read over committed data — advertise that to clients so
# they can auto-approve without a destructive-action prompt.
_RO = ToolAnnotations(readOnlyHint=True)

# One read-only union connection, built LAZILY on first tool call (not at import)
# so the server starts instantly and the MCP handshake completes immediately —
# otherwise the client sits on "connecting…" through the ~77-view build. The
# first actual query pays the one-time build cost; every call after is fast.
_CONN = None

# View sets api_conn() does NOT register (they have no FastAPI page yet): SIPO
# political finance, the judiciary bench/courts, and public appointments. They
# need no path substitutions — their views absolutize parquet internally — so an
# additive glob registration is enough. swallow_errors so a missing optional
# parquet degrades that one domain to an "unavailable" tool result, not a dead
# server.
_EXTRA_VIEW_GLOBS = ["sipo_*.sql", "judiciary_*.sql", "appointments_*.sql"]


def _cur():
    global _CONN
    if _CONN is None:
        conn = api_conn()
        register_views(conn, _EXTRA_VIEW_GLOBS, swallow_errors=True)
        _CONN = conn
    return _CONN.cursor()


def _rows(qr) -> list[dict] | dict:
    """A QueryResult's rows as JSON, or an {error} dict when the source is unavailable
    (missing parquet / unregistered view) — so the client can tell 'source down' from
    'no rows' instead of seeing a silent empty list."""
    if not qr.ok:
        return {"error": qr.unavailable_reason}
    return serialize.to_records(qr.data)


def _one(qr) -> dict | None:
    """A QueryResult's first row as JSON, an {error} dict if unavailable, or None if empty."""
    if not qr.ok:
        return {"error": qr.unavailable_reason}
    return serialize.first_record(qr.data)


# ── Members ───────────────────────────────────────────────────────────────────


@mcp.tool()
def search_members(query: str) -> list[dict]:
    """Find TDs/Senators by name (case-insensitive substring). Returns up to 10
    candidates, each with unique_member_code, party, constituency and house. Pass
    the unique_member_code to get_member_record for the full dossier."""
    records, _total, _ = dossiers.list_members(_cur(), fuzzy_name=query, limit=10)
    return records


@mcp.tool()
def get_member_record(name_or_code: str) -> dict:
    """A member's full accountability dossier — identity, headline stats (days in
    chamber, votes cast, total payments), attendance by year, payments by year,
    legislation sponsored, ministerial roles, statutory instruments signed,
    revolving-door lobbying flags, parliamentary-questions profile, and external
    links. Accepts a unique_member_code, or a name (auto-resolved; returns the
    candidate list if the name is ambiguous)."""
    cur = _cur()
    d = dossiers.build_member_dossier(cur, name_or_code)
    if d is not None:
        return d
    records, total, _ = dossiers.list_members(cur, fuzzy_name=name_or_code, limit=10)
    if total == 1:
        return dossiers.build_member_dossier(_cur(), records[0]["unique_member_code"])
    if total == 0:
        return {"error": f"no member matches '{name_or_code}'"}
    return {
        "disambiguation": records,
        "note": "multiple matches — call get_member_record again with one unique_member_code",
    }


# ── Votes ─────────────────────────────────────────────────────────────────────


@mcp.tool()
def list_recent_votes(house: str = "Dáil", limit: int = 20) -> list[dict]:
    """Recent Dáil/Seanad divisions (votes), most recent first. Each row has a
    vote_id usable with get_division."""
    records, _total, _ = dossiers.list_votes(_cur(), house=house, limit=limit)
    return records


@mcp.tool()
def get_division(vote_id: str) -> dict:
    """One division's full record: the vote (date, title, outcome, tallies), the
    party breakdown, every member's individual vote, and source links."""
    d = dossiers.build_division_dossier(_cur(), vote_id)
    return d or {"error": f"no division '{vote_id}'"}


# ── Cross-reference: votes × Register of Members' Interests ─────────────────────


@mcp.tool(annotations=_RO)
def division_interest_breakdown(vote_id: str) -> dict:
    """For ONE division, break its Yes/Níl/Abstain tally down by the declared
    private interests of the voters: how many landlords, property-owners, company
    directors and shareholders (per the Register of Members' Interests) voted each
    way, and how many appear on the register at all. Use get_division for the named
    member list. The register covers 2020–2025, so older divisions match nothing."""
    d = dossiers.build_division_interest_breakdown(_cur(), vote_id)
    return d or {"error": f"no division '{vote_id}'"}


@mcp.tool(annotations=_RO)
def voting_vs_interests(
    vote_id: str = "",
    keyword: str = "",
    vote_type: str = "Voted No",
    interest: str = "landlord",
    house: str = "Dáil",
) -> dict:
    """Cross-reference HOW members voted against WHAT they declare on the Register
    of Members' Interests — e.g. "TDs who voted against a housing measure who are
    landlords".

    Identify the division(s) EITHER by an exact `vote_id` (from get_division /
    list_recent_votes) OR by a `keyword` matched against debate titles
    (e.g. 'housing', 'tenanc', 'rent', 'eviction'). `vote_type` is one of
    'Voted No', 'Voted Yes', 'Abstained'. `interest` is one of 'landlord',
    'property', 'director', 'shareholder'. Returns one match per (member, division)
    with `held_in_vote_year` flagging same-year declarations, plus a coverage
    caveat. The register covers 2020–2025 only — pre-2020 divisions match nothing."""
    if not vote_id and not keyword:
        return {"error": "pass a vote_id or a keyword to identify the division(s)"}
    return dossiers.cross_reference_votes_interests(
        _cur(),
        vote_id=vote_id or None,
        keyword=keyword or None,
        vote_type=vote_type,
        interest=interest,
        house=house,
    )


# ── Legislation ───────────────────────────────────────────────────────────────


@mcp.tool()
def search_legislation(query: str = "", status: str = "", limit: int = 20) -> list[dict]:
    """Find bills by title substring and/or status (e.g. 'Current'). Returns bill
    summaries; pass a bill_id to get_bill for the full record."""
    records, _total, _ = dossiers.list_bills(_cur(), title_search=query or None, status=status or None, limit=limit)
    return records


@mcp.tool()
def get_bill(bill_id: str) -> dict:
    """One bill's full record: detail, lifecycle timeline, amendment intensity,
    sources, PDFs, debates, and the statutory instruments made under it."""
    d = dossiers.build_bill_dossier(_cur(), bill_id)
    return d or {"error": f"no bill '{bill_id}'"}


@mcp.tool(annotations=_RO)
def search_statutory_instruments(
    year: int = 0, operation: str = "", department: str = "", eu_only: bool = False, limit: int = 20
) -> list[dict]:
    """Find statutory instruments directly (not via a parent bill), filterable by year,
    operation (e.g. 'Commencement', 'Amendment'), department label, and EU-derived only.
    Filters AND together; year=0 / blank means unfiltered. For SIs made under a specific
    bill, use get_bill instead."""
    records, _total, _ = dossiers.list_statutory_instruments(
        _cur(),
        year=year or None,
        operation=operation or None,
        department=department or None,
        eu_only=eu_only,
        limit=limit,
    )
    return records


# ── Payments / lobbying ───────────────────────────────────────────────────────


@mcp.tool()
def top_payments(house: str = "Dáil", limit: int = 20) -> list[dict]:
    """All-time Travel & Accommodation Allowance ranking by member."""
    records, _total, _ = dossiers.list_payments_ranking(_cur(), house=house, limit=limit)
    return records


@mcp.tool()
def lobbying_organisations(name: str = "", limit: int = 20) -> list[dict]:
    """Search registered lobbying organisations (CRO + charity-enriched index) by
    name substring."""
    records, _total, _ = dossiers.list_lobbying_orgs(_cur(), name=name or None, limit=limit)
    return records


@mcp.tool()
def revolving_door(limit: int = 20) -> list[dict]:
    """Former office-holders (designated public officials) now working as
    lobbyists — the revolving-door register."""
    records, _total, _ = dossiers.list_revolving_door(_cur(), limit=limit)
    return records


@mcp.tool(annotations=_RO)
def procurement_lobbying_overlap(limit: int = 50, order_by: str = "award_value", side: str = "") -> dict:
    """Companies that appear on BOTH the public-procurement award register (Office of
    Government Procurement / eTenders) AND the lobbying register (SIPO) — e.g. "which
    firms that won state contracts also lobbied government?".

    Returns ONE ROW PER SUPPLIER, each with its lobbying entities nested under
    `lobby_entities`, plus a `summary` (distinct supplier count + correct total) and a
    `caveat`. `order_by` is one of 'award_value', 'award_rows', 'lobby_returns',
    'authorities'. `side` filters the lobby role: '' (all), 'registrant' (the filer) or
    'client' (whom they lobbied for).

    IMPORTANT — this is ENTITY CO-OCCURRENCE ONLY: a company being on both registers is
    NOT evidence that lobbying influenced any contract (no key links a lobby to an award),
    and exact-name matching undercounts. Surface the `caveat` field; never imply causation.
    Use get... / search for the underlying supplier or lobbying detail."""
    return dossiers.list_procurement_lobbying_overlap(_cur(), limit=limit, order_by=order_by, side=side or None)


# ── Procurement ─────────────────────────────────────────────────────────────────


@mcp.tool(annotations=_RO)
def search_suppliers(year: int = 0, order_by: str = "awards", limit: int = 20) -> list[dict]:
    """Public-procurement (eTenders) supplier ranking — one row per supplier with
    award counts, sum-safe awarded value, CRO match, and a lobbying-register overlap
    flag. order_by is 'awards' (default) or 'value'. year=0 means all-time; pass a
    calendar year to scope it. Pass supplier_norm to get_supplier for the full record."""
    records, _total, _ = dossiers.list_suppliers(_cur(), year=year or None, order_by=order_by, limit=limit)
    return records


@mcp.tool(annotations=_RO)
def get_supplier(supplier_norm: str) -> dict:
    """One supplier's full procurement record: the ranking summary plus every award
    (authority, CPV, date, value), newest first. Use the supplier_norm from search_suppliers."""
    d = dossiers.build_supplier_dossier(_cur(), supplier_norm)
    return d or {"error": f"no supplier '{supplier_norm}'"}


@mcp.tool(annotations=_RO)
def procurement_competition(min_lots: int = 40, order_by: str = "single_bid", limit: int = 20) -> dict:
    """Procurement COMPETITION quality per contracting authority (buyer), from TED (EU
    Official Journal) Irish award notices, 2024+. Surfaces the single-bidder rate — the EU
    Single Market Scoreboard's flagship procurement-integrity indicator.

    `single_bid_lot_pct` = single-bid LOTS / lots-with-a-bid-count: each contract PART counted
    once (the honest lot-level rate; a notice-level reading over-states multi-lot buyers).
    `order_by` is 'single_bid' (rate, default) or 'lots' (volume). `min_lots` (default 40)
    drops noisy small samples — DON'T lower it for a ranking. Returns buyers + a `summary` +
    a `caveat`.

    ⚠️ FACTUAL SIGNAL, NEVER A VERDICT — a single bidder is often legitimate (niche/specialist
    supplier, bespoke research equipment, urgency; research universities single-source a lot).
    Surface the caveat; a high rate is a prompt to look, never proof of wrongdoing."""
    return dossiers.list_procurement_competition(_cur(), min_lots=min_lots, order_by=order_by, limit=limit)


# ── Committees ──────────────────────────────────────────────────────────────────


@mcp.tool(annotations=_RO)
def list_committees(chamber: str = "Dáil") -> list[dict]:
    """Every committee in a chamber (Dáil/Seanad) with its chair, member/party counts,
    and status. Pass a committee name to get_committee for its party-seat breakdown."""
    return dossiers.list_committees(_cur(), chamber=chamber)


@mcp.tool(annotations=_RO)
def get_committee(chamber: str, committee: str) -> dict:
    """One committee's rollup plus its long-format party-seat composition."""
    d = dossiers.get_committee(_cur(), chamber, committee)
    return d or {"error": f"no committee '{committee}' in {chamber}"}


# ── Interests (Register of Members' Interests) ──────────────────────────────────


@mcp.tool(annotations=_RO)
def get_member_interests(name_or_code: str) -> dict:
    """A member's declared interests from the Register of Members' Interests: a per-year
    summary (directorships, properties, shareholdings, landlord flag) and every individual
    declaration across years. Accepts a name or a unique_member_code."""
    d = dossiers.build_member_interests(_cur(), name_or_code)
    return d or {"error": f"no member matches '{name_or_code}'"}


# ── Ministerial accountability ──────────────────────────────────────────────────


@mcp.tool(annotations=_RO)
def who_was_minister(department: str, on_date: str) -> dict:
    """Who held a ministerial department on a specific date — the accountability primitive
    ('who was Justice Minister on 2020-06-01?'). department is a fuzzy label (e.g. 'justice',
    'health'); on_date is ISO 'YYYY-MM-DD'. Returns the holder, or a disambiguation list."""
    return dossiers.who_was_minister(_cur(), department, on_date)


# ── Parliamentary questions ─────────────────────────────────────────────────────


@mcp.tool(annotations=_RO)
def get_member_questions(
    name_or_code: str,
    year: int = 0,
    ministry: str = "",
    topic: str = "",
    text: str = "",
    limit: int = 200,
) -> dict:
    """A member's parliamentary-question feed, filterable by year, ministry, topic, and
    free-text (filters AND together). Answers 'what has this TD asked, and of whom?' —
    the detail behind the questions_profile summary in get_member_record. Accepts a name
    or unique_member_code."""
    d = dossiers.build_member_questions(
        _cur(),
        name_or_code,
        year=year or None,
        ministry=ministry or None,
        topic=topic or None,
        text=text or None,
        limit=limit,
    )
    return d or {"error": f"no member matches '{name_or_code}'"}


# ── Payments by year ────────────────────────────────────────────────────────────


@mcp.tool(annotations=_RO)
def payments_by_year(year: int, house: str = "Dáil", limit: int = 20) -> list[dict]:
    """Travel & Accommodation Allowance ranking for ONE calendar year (use this for
    'who claimed most in 2023?'; top_payments gives the all-time ranking instead)."""
    records, _total, _ = dossiers.list_payments_year_ranking(_cur(), year=year, house=house, limit=limit)
    return records


# ── Member floor speeches ───────────────────────────────────────────────────────


@mcp.tool(annotations=_RO)
def member_speeches(
    name_or_code: str,
    year: int = 0,
    contribution_type: str = "",
    business: str = "",
    irish_only: bool = False,
    text: str = "",
    limit: int = 100,
) -> dict:
    """A member's floor contributions (speeches + oral questions) from the Dáil/Seanad
    debate transcript, filterable by year, contribution_type ('speech'/'question'/'answer'),
    item of business, Irish-only, and free-text of the spoken words (filters AND together).
    Answers 'what did this TD/Senator actually SAY — and in Irish?' — the spoken-word
    counterpart to get_member_questions (tabled PQs). Accepts a name or unique_member_code."""
    d = dossiers.build_member_speeches(
        _cur(),
        name_or_code,
        year=year or None,
        contribution_type=contribution_type or None,
        business=business or None,
        irish_only=irish_only,
        text=text or None,
        limit=limit,
    )
    return d or {"error": f"no member matches '{name_or_code}'"}


# ── SIPO political finance (party donations + GE2024 election expenses) ──────────


@mcp.tool(annotations=_RO)
def party_donations(party: str = "") -> dict:
    """Political DONATIONS disclosed to SIPO. With no `party`, returns the per-party ranking
    plus an all-party `summary`. With a `party` (an exact label from that ranking, e.g.
    'Fine Gael'), returns that party's individual donor receipts (donor, amount, date, nature).
    Some rows are OCR-derived and carry a needs_verify flag; over-cap / under-threshold amounts
    are the real disclosed figures (state them, do not 'correct' them). Donor names + amounts
    are the public SIPO record; there is no donor-address field. Distinct money grain from
    party_election_spend — never add the two."""
    cur = _cur()
    if party:
        return {"party": party, "donations": _rows(sipo.party_donors(cur, party))}
    return {
        "summary": _one(sipo.donations_totals(cur)),
        "by_party": _rows(sipo.donations_by_party(cur)),
        "note": "call again with a party label for its individual donor receipts",
    }


@mcp.tool(annotations=_RO)
def party_election_spend(party: str = "") -> dict:
    """GE2024 candidate ELECTION EXPENSES disclosed to SIPO. With no `party`, returns the
    per-party ranking plus an all-party `summary`. With a `party`, returns that party's
    per-candidate expenditure (candidate, constituency, amount, flag). A flag of
    'over_limit_verify' marks an OCR figure above the statutory limit to RE-CHECK — not a
    confirmed breach. Distinct money grain from party_donations — never add the two together."""
    cur = _cur()
    if party:
        return {"party": party, "candidates": _rows(sipo.party_candidates(cur, party))}
    return {
        "summary": _one(sipo.expenses_totals(cur)),
        "by_party": _rows(sipo.expenses_by_party(cur)),
        "note": "call again with a party label for its per-candidate breakdown",
    }


# ── Judiciary (the bench + court-system health) ─────────────────────────────────


@mcp.tool(annotations=_RO)
def judicial_appointments(limit: int = 50) -> dict:
    """The judiciary as DATA: judicial appointment events (who was appointed to which court,
    with gov.ie nomination context), the elevation ladder (real promotions per court
    transition), and the current sitting-bench roster. Scope is appointment / office / rank /
    assignment only — NO performance, conduct, or ranking data exists here by design."""
    cur = _cur()
    appts = _rows(jud.appointments(cur))
    if isinstance(appts, list):
        appts = appts[:limit]
    return {
        "appointments": appts,
        "elevation_ladder": _rows(jud.elevation_ladder(cur)),
        "roster": _rows(jud.roster(cur)),
    }


@mcp.tool(annotations=_RO)
def courts_health() -> dict:
    """Court-SYSTEM health — NO judge is named here. Annual case clearance by court
    (2017–2024; incoming/resolved/clearance_pct), published waiting-time lists (latest two
    years, with parsed weeks for ranking), and the active geocoded courthouse list. A long
    waiting time is a system-capacity signal, never a judgment on any individual."""
    cur = _cur()
    return {
        "clearance": _rows(jud.courts_clearance(cur)),
        "waiting_times": _rows(jud.courts_waiting_times(cur)),
        "courthouses": _rows(jud.courthouses(cur)),
    }


# ── Public appointments (state boards) ──────────────────────────────────────────


@mcp.tool(annotations=_RO)
def public_appointments() -> list[dict] | dict:
    """Public-appointment notices (state-board and similar appointments) — one row per notice
    from the v_public_appointments surface."""
    return _rows(appt.public_appointments(_cur()))


# ── Charity finances ────────────────────────────────────────────────────────────


@mcp.tool(annotations=_RO)
def charity_financials(rcn: int = 0) -> dict:
    """Charity financial trajectory. With an `rcn` (Registered Charity Number), returns that
    charity's full multi-year income/expenditure/funding series. With rcn=0, returns the
    register-wide totals per year (money through the sector over the decade). Figures are AS
    FILED — some filers submit data-entry errors (implausible billions); treat single-charity
    outliers with care and never read one charity's row as a sector fact."""
    cur = _cur()
    if rcn:
        return {"rcn": rcn, "by_year": _rows(char.financials_by_year(cur, rcn))}
    return {
        "latest_year": _one(char.latest_year(cur)),
        "sector_totals_by_year": _rows(char.sector_totals_by_year(cur)),
        "note": "call again with an rcn for one charity's full filed series",
    }


# ── Public-body payments (the realised-SPEND grain) ─────────────────────────────


@mcp.tool(annotations=_RO)
def public_body_payments(side: str = "publisher", order_by: str = "value", limit: int = 25) -> dict:
    """Public-body payments / purchase-orders over €20k — the realised-SPEND grain (what bodies
    actually PAID), distinct from procurement AWARD ceilings. `side` is 'publisher' (the paying
    body) or 'supplier' (who was paid). `order_by` is 'value' (sum-safe €) or 'lines' (record
    count). Returns the ranking plus a corpus `coverage`. ⚠️ NEVER add this spend to
    eTenders/TED award values — different value_kind; only the sum-safe column is addable."""
    cur = _cur()
    if side == "supplier":
        ranking = _rows(pubpay.supplier_summary(cur, order_by=order_by, limit=limit))
    else:
        ranking = _rows(pubpay.publisher_summary(cur, order_by=order_by, limit=limit))
    return {
        "side": side,
        "coverage": _one(pubpay.coverage_stats(cur)),
        "ranking": ranking,
        "caveat": "sum-safe spend only; never add to procurement AWARD values (different grain)",
    }


# ── Procurement — deeper cuts (authority / CPV / live tenders) ───────────────────


@mcp.tool(annotations=_RO)
def procurement_by_authority(limit: int = 25) -> list[dict] | dict:
    """eTenders public-procurement AWARD activity grouped by contracting authority (buyer):
    award counts and sum-safe awarded value per body. These are award CEILINGS, not realised
    spend — use public_body_payments for what was actually paid."""
    return _rows(proc.authority_summary(_cur(), limit=limit))


@mcp.tool(annotations=_RO)
def procurement_by_cpv(limit: int = 25) -> list[dict] | dict:
    """eTenders procurement AWARD activity grouped by CPV code (WHAT was bought): award counts
    and sum-safe value per category. Award ceilings, not realised spend."""
    return _rows(proc.cpv_summary(_cur(), limit=limit))


@mcp.tool(annotations=_RO)
def open_tenders(only_open: bool = True, limit: int = 40) -> list[dict] | dict:
    """Current TED (EU Official Journal) Irish tender OPPORTUNITIES — live calls for tender.
    only_open=True keeps just those still open for bids. The forward-looking pipeline, distinct
    from already-awarded contracts."""
    return _rows(proc.ted_tenders(_cur(), limit=limit, only_open=only_open))


# ── Ministerial roll-up ──────────────────────────────────────────────────────────


@mcp.tool(annotations=_RO)
def current_cabinet() -> dict:
    """The current ministerial line-up (who holds which department now) plus the full department
    list. For who held a department on a PAST date, use who_was_minister instead."""
    cur = _cur()
    return {
        "current_ministers": _rows(min_.current_ministers(cur)),
        "departments": _rows(min_.departments(cur)),
    }


# ── Lobbying — revolving-door individual profile ────────────────────────────────


@mcp.tool(annotations=_RO)
def dpo_lobbying_profile(individual_name: str) -> dict:
    """One designated public official's revolving-door lobbying footprint: the firms they lobby
    for, their client breakdown, and which politicians / public bodies they targeted — the
    detail behind the revolving_door register. `individual_name` is a name from revolving_door.
    This is co-occurrence on the public lobbying register only; it is NOT evidence of improper
    influence — surface that caveat."""
    cur = _cur()
    return {
        "individual": individual_name,
        "summary": _one(lb.dpo_one(cur, individual_name)),
        "firms": _rows(lb.dpo_firms(cur, individual_name)),
        "client_breakdown": _rows(lb.dpo_client_breakdown(cur, individual_name)),
        "politicians_targeted": _rows(lb.dpo_politicians_targeted(cur, individual_name)),
    }


# ── Corpus search: divisions by topic ───────────────────────────────────────────


@mcp.tool(annotations=_RO)
def search_votes_by_topic(topics: str, house: str = "Dáil") -> dict:
    """How members voted on DEBATES matching given topic keywords — a corpus-wide search across
    ALL divisions (every other vote tool is per-division or per-member). `topics` is a
    comma-separated list (e.g. 'housing, rent, eviction, tenanc'); each is matched as a
    case-insensitive substring of the debate title, OR-combined. Returns a `debates` overview
    (one row per matched debate, newest first) plus the individual member Yes/No `votes` behind
    them (capped at 2000, most recent first). Use voting_vs_interests to cross these against the
    Register of Members' Interests."""
    kws = [t.strip() for t in topics.split(",") if t.strip()]
    if not kws:
        return {"error": "pass one or more comma-separated topic keywords"}
    # topical_votes takes ILIKE patterns — wrap each keyword so it matches as a substring.
    patterns = tuple(f"%{k}%" for k in kws)
    qr = vot.topical_votes(_cur(), patterns, house)
    rows = _rows(qr)
    if isinstance(rows, dict):  # unavailable
        return {"topics": kws, "house": house, **rows}
    # Compact distinct-debate overview (light dedup, not a metric) so the answer leads with the
    # debates, with per-member votes available underneath.
    debates: dict[tuple, dict] = {}
    for r in rows:
        key = (r.get("debate_title"), r.get("vote_date"))
        d = debates.setdefault(
            key, {"debate_title": r.get("debate_title"), "vote_date": r.get("vote_date"), "yes": 0, "no": 0}
        )
        if r.get("vote_type") == "Voted Yes":
            d["yes"] += 1
        elif r.get("vote_type") == "Voted No":
            d["no"] += 1
    return {"topics": kws, "house": house, "debates": list(debates.values()), "votes": rows}


# ── Data coverage (scope guard for honest answers) ──────────────────────────────


@mcp.tool(annotations=_RO)
def data_coverage() -> dict:
    """What this tracker covers and how far back — the scope guard to consult BEFORE answering a
    time- or completeness-sensitive question, so you never over-claim. Returns per-domain year
    ranges and corpus sizes (procurement awards, TED, public-body payments, SIPO finance,
    charities). Many sources start in a specific year and carry a hard money-grain rule — state
    the bound and never sum across grains."""
    cur = _cur()
    return {
        "procurement_awards": _one(proc.coverage_stats(cur)),
        "ted_awards": _one(proc.ted_corpus_stats(cur)),
        "public_body_payments": _one(pubpay.coverage_stats(cur)),
        "sipo_donations": _one(sipo.donations_totals(cur)),
        "sipo_election_expenses": _one(sipo.expenses_totals(cur)),
        "charities_latest_year": _one(char.latest_year(cur)),
        "caveats": {
            "register_of_interests": "Register of Members' Interests covers 2020–2025 only — older divisions match no interests",
            "ted_award_winners": "TED award WINNERS are 2024+ (pre-2024 notices carry buyer + CPV + total value but no winner)",
            "money_grains": "procurement AWARDS, public-body PAYMENTS, and T&A allowances are three different value grains — NEVER sum across them",
        },
    }


@mcp.tool(annotations=_RO)
def source_fetch_failures() -> dict:
    """Which procurement source sites failed to download on the last extractor runs, and
    what data is at stake — the research brief for finding ALTERNATIVE sources. Per failure:
    publisher, URL, error_class (`bot_challenge` = site added a JS anti-bot wall and needs a
    rendering fetch or a replacement source; `http_404` = file removed at the publisher;
    `timeout`/`connection_error` = likely transient), plus rows_in_gold and
    last_period_in_gold (what we stand to lose / how stale we go if the source stays broken).
    `zero_harvest` publishers are the urgent ones: their listing page yields no files at all
    (moved, emptied, or bot-walled). Good replacement hunting grounds: data.gov.ie, the
    body's FOI publication-scheme page, and web.archive.org for the old listing."""
    p = Path(__file__).resolve().parents[1] / "data/_meta/fetch_failures.json"
    if not p.exists():
        return {"error": "no fetch_failures.json yet — run the public_body_payments or la_payments chain first"}
    return json.loads(p.read_text(encoding="utf-8"))


# ── Procurement conduit (authoritative-source bridge + serve-vs-source reconcile) ─


@mcp.tool(annotations=_RO)
def procurement_notice(notice: str = "", supplier: str = "") -> dict:
    """Open the AUTHORITATIVE source behind a procurement award and read the meaning the
    gold layer drops. Pass a TED `notice` (publication-number like '291090-2024' or a full
    notice_url), OR a `supplier` name to find their notices. Returns, for one notice:

      • authoritative_source — fetched LIVE from TED: the real title, the deliverable being
        built, the true framework ceiling, the award-criteria weighting (e.g. quality/price),
        procedure, and the source xml/pdf links;
      • ingested_gold — the winner roster the tracker stored (names + CRO);
      • reconciliation — field-level DISCREPANCIES between source and gold (the feedback loop:
        each is a 'gold is thin/mis-parsed here' signal, also appended to a QA log);
      • value_chain — the four DISTINCT money grains (ceiling ≠ committed ≠ paid ≠ delivered)
        made explicit so a headline ceiling is never mistaken for spend.

    This is the app-as-conduit pattern: the tracker's value is routing you to the authority
    (TED) and reconciling against it, not just echoing a thin local row. Pure passthrough +
    equality checks — no inference. For an indicative build value use project_value_estimate."""
    cur = _cur()
    pn = ted_conduit.normalise_pn(notice) if notice else None
    if pn is None and supplier:
        df = cur.execute(
            "SELECT publication_number, count(*) AS winners, max(buyer_name) AS buyer,"
            " max(dispatch_date) AS dispatch_date"
            " FROM v_procurement_ted_winner_history WHERE winner_join_norm ILIKE ?"
            " GROUP BY 1 ORDER BY max(dispatch_date) DESC LIMIT 10",
            [f"%{supplier}%"],
        ).df()
        if df.empty:
            return {"error": f"no TED notices found for supplier '{supplier}'"}
        if len(df) > 1:
            return {
                "note": "multiple notices — call again with one publication-number",
                "notices": serialize.to_records(df),
            }
        pn = str(df.iloc[0]["publication_number"])
    if pn is None:
        return {"error": "pass a TED publication-number / notice_url, or a supplier name"}

    gold = serialize.to_records(
        cur.execute(
            "SELECT winner_name, cro_company_num, cro_company_status, value_kind, award_value_eur,"
            " n_winners, n_tenders_received, award_criteria_kind, buyer_name, notice_url, dispatch_date"
            " FROM v_procurement_ted_winner_history WHERE publication_number = ?",
            [pn],
        ).df()
    )
    try:
        source = ted_conduit.fetch_notice(pn)
    except Exception as exc:  # noqa: BLE001 — network/parse fault degrades to 'unreachable'
        source = None
        _ = exc
    discrepancies = ted_conduit.reconcile(pn, gold, source)

    fw_ceiling = source.get("framework_maximum_value_eur") if source else None
    return {
        "publication_number": pn,
        "authoritative_source": source or {"error": "TED notice unreachable — gold unverified"},
        "ingested_gold": {"n_winner_rows": len(gold), "winners": gold},
        "reconciliation": {
            "discrepancy_count": len(discrepancies),
            "discrepancies": discrepancies,
            "note": "each discrepancy is a feedback-loop signal (gold thin/stale/mis-parsed vs "
            "authoritative TED); appended to the reconciliation QA log for the pipeline",
        },
        "value_chain": {
            "framework_ceiling_eur": fw_ceiling,
            "per_project_band_eur": source.get("per_project_band_eur") if source else None,
            "called_off_committed_eur": None,
            "realised_paid_eur": None,
            "delivered_seed": source.get("deliverable_seed") if source else None,
            "note": "four DISTINCT grains — ceiling (legal headroom) ≠ committed ≠ paid ≠ "
            "delivered; NEVER sum across them. Realised spend (if any) is a different "
            "register — see public_body_payments / get_supplier.",
        },
    }


@mcp.tool(annotations=_RO)
def project_value_estimate(
    deliverable: str = "",
    units: int = 0,
    category: str = "",
    area_m2: float = 0.0,
    framework_ceiling_eur: float = 0.0,
) -> dict:
    """⚠️ ANALYST-ONLY quantity-surveyor BENCHMARK estimate — INFERENCE, never a disclosed
    figure and never to be shown as fact in the citizen-facing app. Sizes a construction
    deliverable the way a QS does before bills of quantities exist: units × floor-area m² ×
    Irish €/m² benchmark (SCSI 2025–26). Pass a free-text `deliverable` (e.g. '12 semi-detached
    dwellings') or explicit `units`/`category`/`area_m2`. Give `framework_ceiling_eur` to see
    how many times the headline ceiling exceeds a defensible build value. Returns a low/high
    RANGE with every assumption + source shown, so the estimate is auditable. Pair with
    procurement_notice (which yields the deliverable text and the real ceiling)."""
    return qs_valuation.estimate(
        deliverable,
        units=units,
        category=category or "",
        area_m2=area_m2,
        framework_ceiling_eur=framework_ceiling_eur or None,
    )


# ── Prompts (audit templates surfaced as client slash-commands) ─────────────────


@mcp.prompt()
def audit_member(name: str) -> str:
    """Build a full accountability picture of one TD/Senator from the tracker's data."""
    return (
        f"Build a complete accountability picture of {name} using the dail-tracker tools, "
        "in this order: get_member_record (identity, attendance, votes, payments, legislation), "
        "then get_member_questions (what they ask and of whom), get_member_interests (declared "
        "interests), and payments_by_year for any year that looks unusual. Present only what the "
        "data shows — no inference or conclusions — and include the source URLs the tools return."
    )


@mcp.prompt()
def trace_bill_sis(bill: str) -> str:
    """Trace a bill through to the statutory instruments made under it."""
    return (
        f"Trace the bill '{bill}': call search_legislation to find it, then get_bill to pull its "
        "lifecycle, amendments, and the statutory instruments made under it. Summarise the bill's "
        "status and list the SIs with their dates and signing minister. Cite the Oireachtas URLs."
    )


@mcp.prompt()
def procurement_lobbying_check(supplier: str) -> str:
    """Check a supplier's public contracts and any lobbying-register overlap."""
    return (
        f"Look up the supplier '{supplier}': call search_suppliers then get_supplier for its award "
        "history and the lobbying-register overlap flag. If the supplier appears on the lobbying "
        "register, report it strictly as co-occurrence on two public registers — NOT as evidence of "
        "influence or causation. State only what the data shows and cite source URLs."
    )


@mcp.prompt()
def audit_party_finance(party: str) -> str:
    """Summarise a party's disclosed political finance (donations + election spend)."""
    return (
        f"Summarise the disclosed political finance of {party}: call party_donations and "
        "party_election_spend (each with no argument first to find the exact party label, then "
        "again with that label for the breakdown). Report donations and GE2024 election expenses "
        "SEPARATELY — they are different money grains and must never be added together. Flag any "
        "needs_verify / over_limit_verify rows as OCR figures to re-check, not confirmed facts, "
        "and present only what SIPO disclosed (no inference). Cite the source pages the tools return."
    )


@mcp.prompt()
def judicial_appointment_trace(name: str) -> str:
    """Trace a judge's appointments and any TD/ministerial connection from the data."""
    return (
        f"Trace '{name}' through the judiciary data: call judicial_appointments to find their "
        "appointment event(s), court, appointing authority and any elevation. Report only "
        "appointment / office / rank facts — there is NO performance or conduct data. If a "
        "get_member_record lookup links a former TD to the bench, state it as a register match, "
        "not a judgment. Cite the gov.ie / source URLs the tools return."
    )


@mcp.prompt()
def assess_procurement_award(supplier: str) -> str:
    """Make a thin procurement award meaningful via the authoritative source + a QS estimate."""
    return (
        f"Assess the public-procurement position of '{supplier}'. Call procurement_notice with the "
        "supplier to open the AUTHORITATIVE TED notice behind their award(s); report what is actually "
        "being built, the real framework ceiling, the award-criteria weighting and the competing panel "
        "from the source — not the thin gold row. Surface the reconciliation discrepancies plainly as "
        "data-quality findings. Then call project_value_estimate on the deliverable to give an INDICATIVE "
        "build-value range, clearly labelled as a benchmark estimate (inference), and contrast it with the "
        "framework ceiling. Keep the four money grains separate (ceiling ≠ committed ≠ paid ≠ delivered) "
        "and link to the TED source notice."
    )


@mcp.resource("data://coverage")
def coverage_resource() -> dict:
    """Machine-readable scope manifest (same payload as the data_coverage tool) so a client can
    load it as ambient context and scope answers without spending a tool call."""
    return data_coverage()


if __name__ == "__main__":
    mcp.run()  # stdio transport — Claude Desktop launches this as a subprocess
