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
doc/archive/COMMERCIAL_UPLIFT_PLAN.md §5/§6).

# ── SECTION MAP ── ─────────────────────────────────────────
# ⚠️  DO NOT READ WHOLE — ~22,303 tokens (1,826 lines after this header).
#     Read this map, then jump:  Read(file, offset=<start>, limit=<n>)
#
#     197-205    _cur
#     206-214    _rows
#     215-221    _one
#     222-225    Members
#     226-234    search_members
#     235-256    get_member_record
#     257-260    Votes
#     261-268    list_recent_votes
#     269-275    get_division
#     276-279    Cross-reference: votes × Register of Members' Interests
#     280-290    division_interest_breakdown
#     291-354    voting_vs_interests
#     355-358    Legislation
#     359-366    search_legislation
#     367-374    get_bill
#     375-392    search_statutory_instruments
#     393-396    Payments / lobbying
#     397-403    top_payments
#     404-411    lobbying_organisations
#     412-418    revolving_door
#     419-422    Ministerial diaries — who ministers meet
#     423-435    ministerial_diary_top_organisations
#     436-448    ministerial_diary_organisation
#     449-458    who_ministers_meet
#     459-496    company_influence
#     497-515    _spine_lobbying_lookup
#     516-552    access_to_contracts
#     553-570    procurement_lobbying_overlap
#     571-574    Procurement
#     575-584    search_suppliers
#     585-592    get_supplier
#     593-609    procurement_competition
#     610-613    Committees
#     614-620    list_committees
#     621-626    get_committee
#     627-630    Interests (Register of Members' Interests)
#     631-638    get_member_interests
#     639-642    Ministerial accountability
#     643-649    who_was_minister
#     650-653    Parliamentary questions
#     654-677    get_member_questions
#     678-681    Payments by year
#     682-688    payments_by_year
#     689-692    Member floor speeches
#     693-719    member_speeches
#     720-723    SIPO political finance (party donations + GE2024 election ex
#     724-742    party_donations
#     743-758    party_election_spend
#     759-762    Judiciary (the bench + court-system health)
#     763-779    judicial_appointments
#     780-792    courts_health
#     793-796    Public appointments (state boards)
#     797-802    public_appointments
#     803-827    Charity finances
#     828-873    _charity_sector_dq_flags
#     874-903    charity_financials
#     904-906    Corporate distress notices (Iris Oifigiúil — companies only,
#     907-913    _trim_notice
#     914-950    corporate_distress_notices
#     951-976    corporate_repeat_distress
#     977-980    Public-body payments (the realised-SPEND grain)
#     981-999    public_body_payments
#    1000-1003   Procurement — deeper cuts (authority / CPV / live tenders)
#    1004-1011   procurement_by_authority
#    1012-1018   procurement_by_cpv
#    1019-1025   open_tenders
#    1026-1029   Ministerial roll-up
#    1030-1039   current_cabinet
#    1040-1043   Lobbying — revolving-door individual profile
#    1044-1059   dpo_lobbying_profile
#    1060-1063   Corpus search: divisions by topic
#    1064-1109   search_votes_by_topic
#    1110-1113   Data coverage (scope guard for honest answers)
#    1114-1136   data_coverage
#    1137-1152   source_fetch_failures
#    1153-1156   Procurement conduit (authoritative-source bridge + serve-vs-
#    1157-1234   procurement_notice
#    1235-1258   project_value_estimate
#    1259-1261   Siting check (planning-constraint triage for a point — the c
#    1262-1274   _brief_item
#    1275-1349   siting_check
#    1350-1353   Cross-register watchlist + organisation dossier (entity-cros
#    1354-1377   cross_register_watchlist
#    1378-1395   _org_name_key
#    1396-1407   _org_registers
#    1408-1456   _resolve_org_candidates
#    1457-1524   organisation_dossier
#    1525-1528   Local government (council accountability scorecard)
#    1529-1575   council_scorecard
#    1576-1579   AFS (local-authority audited accounts — the BUDGET grain)
#    1580-1625   afs_coverage
#    1626-1629   Housing money (national demand / supply / accommodation spen
#    1630-1659   housing_money
#    1660-1663   Attendance (division turnout + TAA compliance)
#    1664-1705   attendance_ranking
#    1706-1709   National public finance (CSO general-government)
#    1710-1725   gov_finance_annual
#    1726-1729   Prompts (audit templates surfaced as client slash-commands)
#    1730-1741   audit_member
#    1742-1751   trace_bill_sis
#    1752-1762   procurement_lobbying_check
#    1763-1775   audit_party_finance
#    1776-1787   judicial_appointment_trace
#    1788-1802   assess_procurement_award
#    1803-1817   siting_brief
#    1818-1826   coverage_resource
# ── END SECTION MAP ── ─────────────────────────────────
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

from dail_tracker_core import caveats, dossiers, serialize  # noqa: E402
from dail_tracker_core.connections import api_conn  # noqa: E402
from dail_tracker_core.db import register_views  # noqa: E402
from dail_tracker_core.queries import appointments as appt  # noqa: E402
from dail_tracker_core.queries import attendance as att  # noqa: E402
from dail_tracker_core.queries import charities as char  # noqa: E402
from dail_tracker_core.queries import corporate as corp  # noqa: E402
from dail_tracker_core.queries import entity as ent  # noqa: E402
from dail_tracker_core.queries import housing as hsg  # noqa: E402
from dail_tracker_core.queries import judiciary as jud  # noqa: E402
from dail_tracker_core.queries import lobbying as lb  # noqa: E402
from dail_tracker_core.queries import local_government as lg  # noqa: E402
from dail_tracker_core.queries import ministerial as min_  # noqa: E402
from dail_tracker_core.queries import ministerial_diary as mdiary  # noqa: E402
from dail_tracker_core.queries import procurement as proc  # noqa: E402
from dail_tracker_core.queries import public_payments as pubpay  # noqa: E402
from dail_tracker_core.queries import publicfinance as pf  # noqa: E402
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
_EXTRA_VIEW_GLOBS = ["sipo_*.sql", "judiciary_*.sql", "appointments_*.sql", "corporate_*.sql"]


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
    member list. The register covers 1995–2025, so only pre-1995 divisions match nothing."""
    d = dossiers.build_division_interest_breakdown(_cur(), vote_id)
    return d or {"error": f"no division '{vote_id}'"}


@mcp.tool(annotations=_RO)
def voting_vs_interests(
    vote_id: str = "",
    keyword: str = "",
    vote_type: str = "Voted No",
    interest: str = "landlord",
    house: str = "Dáil",
    summary_only: bool = False,
    limit: int = 50,
) -> dict:
    """Cross-reference HOW members voted against WHAT they declare on the Register
    of Members' Interests — e.g. "TDs who voted against a housing measure who are
    landlords".

    Identify the division(s) EITHER by an exact `vote_id` (from get_division /
    list_recent_votes) OR by a `keyword` matched against debate titles
    (e.g. 'housing', 'tenanc', 'rent', 'eviction'). `vote_type` is one of
    'Voted No', 'Voted Yes', 'Abstained'. `interest` is one of 'landlord',
    'property', 'director', 'shareholder'.

    A broad keyword can match hundreds of (member, division) pairs, so the detail is
    bounded: `summary_only=True` returns ONE row per division (vote_id, title, date,
    count of matching members + their names) — best for "how many / which divisions".
    Otherwise the full `matches` are returned but capped at `limit` (default 50), with
    `returned`/`truncated` flags; raise `limit` or pass a specific `vote_id` to widen.
    `match_count`/`distinct_members` always reflect the FULL result, not the page.
    The register covers 1995–2025 — only pre-1995 divisions match nothing."""
    if not vote_id and not keyword:
        return {"error": "pass a vote_id or a keyword to identify the division(s)"}
    res = dossiers.cross_reference_votes_interests(
        _cur(),
        vote_id=vote_id or None,
        keyword=keyword or None,
        vote_type=vote_type,
        interest=interest,
        house=house,
    )
    if "error" in res:
        return res
    matches = res.get("matches", [])
    if summary_only:
        by_div: dict[str, dict] = {}
        for m in matches:
            vid = m.get("vote_id")
            d = by_div.setdefault(
                vid,
                {
                    "vote_id": vid,
                    "vote_date": m.get("vote_date"),
                    "debate_title": m.get("debate_title"),
                    "matching_members": 0,
                    "members": [],
                },
            )
            d["matching_members"] += 1
            d["members"].append(m.get("member_name"))
        res.pop("matches", None)
        res["divisions"] = list(by_div.values())
        return res
    res["matches"] = matches[:limit]
    res["returned"] = len(res["matches"])
    res["truncated"] = len(matches) > limit
    return res


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


# ── Ministerial diaries — who ministers meet ────────────────────────────────────


@mcp.tool(annotations=_RO)
def ministerial_diary_top_organisations(limit: int = 25, outside_only: bool = True) -> list[dict]:
    """Organisations ranked by how many meetings ministers logged with them in their OWN
    published diaries — the access record. Each row has meetings, ministers_met,
    ministers_lobbied_and_met, total_lobbying_returns and `corroborated` (the org both MET a
    minister AND appears on a lobbying return — as the registrant or as a named client — naming
    that same minister). Lobbying counts are PER-ORGANISATION associations: never sum them across
    organisations, since one return attaches to its registrant and to each of its clients.
    `outside_only` drops state/semi-state bodies. Co-occurrence only — access, NOT proof of
    influence; diaries are self-curated, non-exhaustive and quarterly-in-arrears."""
    return _rows(mdiary.org_overlap_ranked(_cur(), limit=limit, outside_only=outside_only))


@mcp.tool(annotations=_RO)
def ministerial_diary_organisation(name: str) -> dict:
    """For ONE organisation (fuzzy name), the ministerial-access record from ministers' published
    diaries: a summary (meetings, distinct ministers met, corroboration vs the lobbying register)
    plus the individual logged meetings (which minister, date, subject, source link). Use for
    'who did <company> meet, when, about what'. Access, not influence — no causation implied."""
    cur = _cur()
    return {
        "summary": _rows(mdiary.organisation_summary(cur, name)),
        "meetings": _rows(mdiary.organisation_meetings(cur, name)),
    }


@mcp.tool(annotations=_RO)
def who_ministers_meet(minister: str = "", topic: str = "", limit: int = 30) -> list[dict]:
    """Search every external meeting ministers logged in their published diaries, by minister
    surname and/or a subject keyword (e.g. minister='Donohoe', topic='data centre'). Returns
    minister, department, date, the as-published subject and the source link. Diaries are
    self-curated, non-exhaustive and quarterly-in-arrears; a diary meeting is not a lobbying
    return."""
    return _rows(mdiary.meeting_search(_cur(), minister=minister, topic=topic, limit=limit))


@mcp.tool(annotations=_RO)
def company_influence(name: str) -> dict:
    """The ACCESS × MONEY profile for ONE company (fuzzy name): how many meetings it logged with
    ministers and how many distinct ministers, its lobbying-register returns, public contracts won
    (€) and public payments received (€) — the 'follow the access to the money' view, plus the
    matched supplier name for verification. CO-OCCURRENCE, NOT causation: this maps access and
    money, it does NOT imply a meeting caused a contract. Empty = the company isn't named in the
    published ministerial diaries (the procurement_* tools cover suppliers that never met a
    minister)."""
    # _rows(): serialise the QueryResult (and turn a dead source into {"error": ...}) exactly like
    # every other diary tool. Without it this returned a raw QueryResult nested in the dict, which
    # is not JSON-serialisable — the tool could never have answered a call.
    rows = _rows(mdiary.company_influence(_cur(), name))
    return rows if isinstance(rows, dict) else {"company_matches": rows}


# Two lobbying measures ride on each access_to_contracts row and they are NOT the same thing.
# `total_lobbying_returns` is the DIARY chain's grain: distinct returns that NAMED A POLITICIAN and
# on which the org is the REGISTRANT **or** a NAMED CLIENT. That client side was the MCP sweep's
# DQ #2 (the ROADSTONE contradiction: this tool said 0 returns while cross_register_watchlist said
# 2) — the diary joined the register on the registrant name ALONE, so every org that lobbies via a
# PR firm read 0. Fixed at the root 2026-07-14 in extractors/diary_lobbying_overlap.py; the join is
# still an EXACT match on the diary org_key, so 0 means "not matched", never "did not lobby".
# The spine fields below are the register-WIDE footprint (every return, not just politician-naming
# ones) on the canonical shared/name_norm key — the SAME spine cross_register_watchlist reads, so
# the two tools can no longer contradict each other.
_ACCESS_TO_CONTRACTS_CAVEAT = (
    "total_lobbying_returns counts distinct lobbying returns that named a politician and on which "
    "the organisation is the registrant OR a named client (so lobbying done for it by a hired "
    "PR/consultancy counts) — an exact-name join, so 0 can still mean 'not matched', not 'never "
    "lobbied'. on_lobbying_register / register_lobby_returns are the wider register footprint "
    "(all returns, registrant OR client, exact canonical-name match) from the same "
    "entity-crosswalk spine as cross_register_watchlist. Both are PER-ORGANISATION association "
    "counts: never sum them across organisations, because one return attaches to its registrant "
    "and to each of its clients. Access + money map, never causation; € figures carry the "
    "procurement caveats and are never summed across grains."
)


def _spine_lobbying_lookup(cur, keys: set[str]) -> dict[str, tuple[bool, int]]:
    """supplier_norm → (on_lobbying_register, lobby_returns) from the entity-crosswalk spine
    for a set of canonical keys. Empty on any failure (the spine view may be unregistered on a
    machine without that gold) — enrichment then degrades to absent flags, never a wrong zero."""
    if not keys:
        return {}
    try:
        placeholders = ",".join("?" * len(keys))
        rows = cur.execute(
            "SELECT supplier_norm, on_lobbying_register, lobby_returns FROM v_supplier_entity_xref"
            f" WHERE supplier_norm IN ({placeholders})",
            list(keys),
        ).fetchall()
    except Exception:
        return {}
    return {str(sn): (bool(onreg), int(lr or 0)) for sn, onreg, lr in rows}


@mcp.tool(annotations=_RO)
def access_to_contracts(limit: int = 25, order_by: str = "awards_eur") -> dict:
    """Companies that BOTH met ministers (in their published diaries) AND won/were paid public
    money, ranked. order_by ∈ {awards_eur, paid_eur, meetings, total_lobbying_returns}. Each row
    has meetings, ministers_met, awards_eur, paid_eur, the matched supplier, and TWO distinct
    lobbying measures: `total_lobbying_returns` (returns naming a politician on which the org is
    the registrant OR a named client — 0 can mean 'not matched', not 'never lobbied') and the
    wider `on_lobbying_register` / `register_lobby_returns` (ALL returns, registrant OR client,
    from the same entity-crosswalk spine as cross_register_watchlist). Never sum either across
    organisations — one return attaches to its registrant and each client. Access + money map,
    never causation; diaries are self-curated/quarterly-in-arrears and the € carry the procurement
    caveats. Surface the `caveat`."""
    rows = _rows(mdiary.access_to_contracts(_cur(), limit=limit, order_by=order_by))
    if isinstance(rows, dict):  # {error}: source unavailable
        return rows
    cur = _cur()
    # Canonical keys per row: the organisation name + every supplier spelling the diary chain
    # matched (pipe-joined) — all folded through the ONE canonical normaliser the spine was
    # built with, so this join can never drift from cross_register_watchlist's.
    row_keys: list[set[str]] = []
    all_keys: set[str] = set()
    for r in rows:
        names = [str(r.get("organisation") or "")]
        names += [s.strip() for s in str(r.get("matched_supplier") or "").split("|") if s.strip()]
        keys = {k for k in (_org_name_key(n) for n in names) if k}
        row_keys.append(keys)
        all_keys |= keys
    spine = _spine_lobbying_lookup(cur, all_keys)
    for r, keys in zip(rows, row_keys, strict=True):
        hits = [spine[k] for k in keys if k in spine]
        r["on_lobbying_register"] = any(h[0] for h in hits)
        # max, not sum: distinct canonical keys are distinct spine entities and the same
        # return could sit behind more than one matched spelling — report a floor, never inflate
        r["register_lobby_returns"] = max((h[1] for h in hits), default=0)
    return {"companies": rows, "caveat": _ACCESS_TO_CONTRACTS_CAVEAT}


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

# Hard caveat on EVERY charity_financials response (MCP sweep 2026-07-11 DQ #8: the 2023
# sector gross income serves €302.9bn — 2.4× total general-government revenue — and the
# govt/LA income line jumps €4.8m (2014) → €26.6bn (2019), previously uncaveated).
# We caveat and flag; we never alter or drop the filed numbers (provenance is the user's
# domain — filings are corrected by the regulator, not by this tool).
_CHARITY_FINANCIALS_CAVEAT = (
    "Figures are AS FILED and UNVALIDATED — charity regulatory filings are known to contain "
    "data-entry/unit errors (implausible billions), so magnitudes can be wrong by orders of "
    "magnitude. Register coverage GREW over the decade, so year-over-year sector totals are "
    "coverage artifacts: NEVER compare sector totals across years, and NEVER compare them to "
    "national accounts (a filed sector total can exceed total general-government revenue). "
    "A single charity's row is never a sector fact."
)

# Sanity rails for the SECTOR aggregates (flag-only — the numbers pass through untouched):
# any yearly total above €100bn is beyond the plausible scale of the whole Irish charity
# sector (total general-government REVENUE is ~€120-130bn), and a >10× year-over-year swing
# in a register-wide total is a coverage artifact and/or filing unit error, not sector growth.
_CHARITY_TOTAL_CEILING_EUR = 100e9
_CHARITY_YOY_RATIO_CEILING = 10.0
_CHARITY_MONEY_COLS = ("total_gross_income", "total_gross_expenditure", "total_income_govt_or_la")


def _charity_sector_dq_flags(rows: list[dict]) -> list[dict]:
    """Implausibility flags for the register-wide yearly aggregates. Flags only — no row is
    altered or dropped. One flag per (year, measure) breach."""
    flags: list[dict] = []
    ordered = sorted(rows, key=lambda r: r.get("period_year") or 0)
    prev: dict[str, float] = {}
    for r in ordered:
        year = r.get("period_year")
        for col in _CHARITY_MONEY_COLS:
            raw = r.get(col)
            if raw is None:
                continue
            v = float(raw)
            if v > _CHARITY_TOTAL_CEILING_EUR:
                flags.append(
                    {
                        "period_year": year,
                        "measure": col,
                        "value": v,
                        "implausible": True,
                        "reason": "yearly sector total exceeds the €100bn sanity ceiling — beyond the "
                        "plausible scale of the whole sector; filing unit errors inflate the aggregate",
                    }
                )
            p = prev.get(col)
            if (
                p is not None
                and p > 0
                and v > 0
                and (v / p > _CHARITY_YOY_RATIO_CEILING or p / v > _CHARITY_YOY_RATIO_CEILING)
            ):
                flags.append(
                    {
                        "period_year": year,
                        "measure": col,
                        "value": v,
                        "implausible": True,
                        "reason": f"swung more than 10× vs prior year ({p:,.0f} → {v:,.0f}) — a register-"
                        "coverage artifact and/or filing unit error, not sector change",
                    }
                )
            prev[col] = v
    return flags


@mcp.tool(annotations=_RO)
def charity_financials(rcn: int = 0) -> dict:
    """Charity financial trajectory. With an `rcn` (Registered Charity Number), returns that
    charity's full multi-year income/expenditure/funding series. With rcn=0, returns the
    register-wide totals per year plus a `data_quality` block flagging implausible aggregates.
    ⚠️ Figures are AS FILED and UNVALIDATED — filings contain unit errors (implausible
    billions), and register coverage grew year over year, so sector totals must NEVER be
    compared across years or to national accounts. Surface the `caveat` with any figure."""
    cur = _cur()
    if rcn:
        return {
            "rcn": rcn,
            "by_year": _rows(char.financials_by_year(cur, rcn)),
            "caveat": _CHARITY_FINANCIALS_CAVEAT,
        }
    totals = _rows(char.sector_totals_by_year(cur))
    out: dict = {
        "latest_year": _one(char.latest_year(cur)),
        "sector_totals_by_year": totals,
        "caveat": _CHARITY_FINANCIALS_CAVEAT,
        "note": "call again with an rcn for one charity's full filed series",
    }
    if isinstance(totals, list):
        out["data_quality"] = {
            "checks": "per-year sector totals: > €100bn ceiling, or >10× year-over-year swing "
            "(income / expenditure / govt-or-LA income)",
            "flags": _charity_sector_dq_flags(totals),
        }
    return out


# ── Corporate distress notices (Iris Oifigiúil — companies only, NO individuals) ─


def _trim_notice(rec: dict) -> dict:
    """Drop the bulky raw_text / title scratch columns from a corporate-notice row —
    display_title is the clean human label; raw_text is the whole OCR'd paragraph."""
    return {k: v for k, v in rec.items() if k not in ("raw_text", "title")}


@mcp.tool(annotations=_RO)
def corporate_distress_notices(query: str = "", subtype: str = "", year: int = 0, limit: int = 50) -> dict:
    """Corporate distress / register notices from Iris Oifigiúil (the State gazette) — receiverships,
    court & voluntary wind-ups, examinerships, SCARP rescues, and investment-vehicle register notices.
    CORPORATE ONLY: personal/individual insolvency is excluded by policy upstream, so no person is named
    here. Filters AND together — `query` (entity-name substring), `subtype` (e.g. 'receivership',
    'court_winding_up', 'examinership', 'creditors_voluntary_liquidation', 'members_voluntary_liquidation'),
    `year` (issue year; 0 = all). Returns matched notices (newest first) + a `caveat`.

    ⚠️ A wind-up/receivership notice is a FACT about a company's legal status on a date — not a judgment on
    any director or a verdict of wrongdoing. Members' Voluntary Liquidation is a SOLVENT wind-up (routine
    fund/company lifecycle), NOT distress — don't read it as failure."""
    qr = corp.corporate_notices(_cur())
    if not qr.ok:
        return {"error": qr.unavailable_reason}
    rows = serialize.to_records(qr.data)
    q = query.strip().lower()
    st = subtype.strip().lower()
    out = []
    for r in rows:
        if q and q not in str(r.get("entity_name", "")).lower():
            continue
        if st and st != str(r.get("notice_subtype", "")).lower():
            continue
        if year and not str(r.get("issue_date", "")).startswith(str(year)):
            continue
        out.append(_trim_notice(r))
        if len(out) >= limit:
            break
    return {
        "count": len(out),
        "notices": out,
        "caveat": "corporate notices only (no individuals); a wind-up/receivership is a legal-status fact, "
        "not a verdict — and Members' Voluntary Liquidation is a SOLVENT wind-up, not distress",
    }


@mcp.tool(annotations=_RO)
def corporate_repeat_distress(limit: int = 50) -> dict:
    """CBI-AUTHORISED firms that appear in REPEAT corporate-distress notices — regulated entities
    (per the Central Bank registers) with ≥2 genuine-distress events, or 3+ notices including at least one
    distress event. Each row carries per-subtype counts (n_receivership, n_court_winding_up, n_examinership,
    n_scarp, n_creditors_vl), the distress vs routine split (n_distress / n_routine), the date span, and the
    primary CBI register + reference. The watchlist cut: regulated firms in recurring distress.

    ⚠️ This surfaces the REGULATORY PROVENANCE of the entity on a notice (it is/was CBI-authorised under
    register X) — it does NOT claim the receiver/liquidator action is itself a regulatory matter, nor imply
    wrongdoing. Solvent Members' Voluntary Liquidations are suppressed from the distress count by design.
    Fragment entity names (mortgagee "made between X and [bank]" clauses) and misfiled non-distress notices
    (e.g. foreshore licences) are excluded at view level. Receiverships where the named firm acts in a
    TRUSTEE capacity (receiver over trust assets, per the notice's own wording) are counted separately in
    `n_trustee_capacity` and excluded from `n_distress` — a trustee-named notice is not the firm's own
    distress."""
    firms = _rows(corp.cbi_repeat_distress(_cur()))
    if isinstance(firms, list):
        firms = firms[:limit]
    return {
        "firms": firms,
        "caveat": "regulatory provenance only — not a verdict; "
        "exact normalised name match (may miss aliases); solvent MVLs excluded from distress count; "
        "trustee-capacity receiverships flagged in n_trustee_capacity, not counted as the firm's own distress",
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
def search_votes_by_topic(topics: str, house: str = "Dáil", include_member_votes: bool = False) -> dict:
    """How members voted on DIVISIONS matching given topic keywords — a corpus-wide search across
    ALL divisions (every other vote tool is per-division or per-member). `topics` is a
    comma-separated list (e.g. 'housing, rent, eviction, tenanc'); each is matched as a
    case-insensitive substring of the debate title, OR-combined.

    By default returns ONLY the `divisions` overview — one row per division (newest first), each
    carrying its `vote_id`, debate title, date and Yes/No tally. Feed a `vote_id` straight into
    `division_interest_breakdown` or `get_division`. Set `include_member_votes=True` to also get the
    individual member Yes/No `votes` behind them (capped at 2000) — large, so opt in only when you
    need the named breakdown. Use voting_vs_interests to cross divisions against the Register of
    Members' Interests."""
    kws = [t.strip() for t in topics.split(",") if t.strip()]
    if not kws:
        return {"error": "pass one or more comma-separated topic keywords"}
    # topical_votes takes ILIKE patterns — wrap each keyword so it matches as a substring.
    patterns = tuple(f"%{k}%" for k in kws)
    qr = vot.topical_votes(_cur(), patterns, house)
    rows = _rows(qr)
    if isinstance(rows, dict):  # unavailable
        return {"topics": kws, "house": house, **rows}
    # One row per DIVISION (keyed on vote_id, not title+date — distinct divisions can share a debate
    # title), so each overview row chains into division_interest_breakdown / get_division.
    divisions: dict[str, dict] = {}
    for r in rows:
        vid = r.get("vote_id")
        d = divisions.setdefault(
            vid,
            {
                "vote_id": vid,
                "debate_title": r.get("debate_title"),
                "vote_date": r.get("vote_date"),
                "yes": 0,
                "no": 0,
            },
        )
        if r.get("vote_type") == "Voted Yes":
            d["yes"] += 1
        elif r.get("vote_type") == "Voted No":
            d["no"] += 1
    out = {"topics": kws, "house": house, "divisions": list(divisions.values())}
    if include_member_votes:
        out["votes"] = rows
    return out


# ── Join map (the association guard — read BEFORE cross-referencing) ────────────


@mcp.tool(annotations=_RO)
def join_map() -> dict:
    """HOW TO CROSS-REFERENCE this data — consult BEFORE attempting ANY association between
    registers (procurement x lobbying, company x charity, member x interests, ...). Returns the
    two canonical join keys, the entity spine and its MEASURED yields, the structural blind spot,
    and the hard NEVER-JOIN / NEVER-SUM rules.

    Exists because the guardrails were previously scattered across prose and easy to miss. The
    single most important fact it carries: the entity spine is ANCHORED ON PROCUREMENT SUPPLIERS,
    so an organisation that never won a public contract is ABSENT ENTIRELY — a 0 or a low
    cross-register count is a FLOOR, never proof of absence. Full detail: doc/JOIN_MAP.md."""
    return {
        "canonical_keys": {
            "organisation": {
                "impl": "shared/name_norm.py :: name_norm_expr (Polars) / name_norm_str (Python)",
                "rule": "NFD accent-fold -> UPPER -> strip .,&'\" -> drop legal suffixes "
                "(THE|AND|LIMITED|LTD|DAC|PLC|CLG|UC|COMPANY|GROUP|HOLDINGS|IRELAND|IRL|OF) "
                "-> drop non-alnum -> collapse whitespace",
                "used_by": "CRO, procurement suppliers, lobbying registrants+clients, TED winners, "
                "ministerial-diary orgs, CBI registers (~18 call sites)",
                "note": "the two impls are pinned byte-identical by test/shared/test_name_norm.py — "
                "adding a third normaliser reintroduces the 'distress join = 0' bug class",
            },
            "person_member": {
                "impl": "shared/normalise_join_key.py :: normalise_df_td_name -> join_key",
                "rule": "lowercase -> NFD accent-fold -> strip apostrophes/non-alpha -> strip honorifics "
                "-> remove ALL whitespace -> SORT CHARACTERS ALPHABETICALLY",
                "warning": "this is an ANAGRAM key: order-insensitive by design (Richard Boyd Barrett == "
                "Barrett Richard Boyd), but genuine character-anagram COLLISIONS are possible. Treat a "
                "member match as a strong lead, not proof; verify against the member register.",
            },
            "rule": "NEVER use the ORG key on people or the PERSON key on organisations — they strip "
            "different things and only one is an anagram key.",
        },
        "entity_spine": {
            "view": "v_supplier_entity_xref (data/gold/parquet/supplier_entity_xref.parquet)",
            "key": "supplier_norm (the ORG key)",
            "orgs": 10017,
            "BLIND_SPOT": "ANCHORED ON PROCUREMENT SUPPLIERS — every org in the spine won public "
            "procurement. An org that never won a contract (lobbying-only body, charity with no "
            "contracts, company appearing only in Iris notices) is ABSENT ENTIRELY and cannot be "
            "associated via the spine at all.",
            "measured_yields_2026_07_14": {
                "has_cro": "6,469 / 10,017 (64.6%) — of a 819,429-company CRO universe",
                "on_lobbying_register": "239 (2.4%) — but the register holds ~2,557 lobbyist orgs "
                "+ 1,992 clients, so ~91% of lobbying orgs are NOT in the spine",
                "has_corporate_notice": "246 (2.5%)",
                "is_charity": "72 (0.7%) — of 14,448 registered charities",
                "has_epa_licence": "38 (0.4%)",
            },
            "how_to_read_a_zero": "Low/zero counts mix TWO effects the data cannot separate: a genuinely "
            "small overlap AND exact-name match failure (subsidiaries, trading names, spelling variants). "
            "So on_lobbying_register=False means 'NOT MATCHED', never 'did not lobby'.",
        },
        "never_join_never_sum": [
            "3 money grains NEVER sum/union across each other: BUDGET (LA adopted budgets, AFS) vs "
            "AWARD/ceiling (eTenders, TED) vs PAYMENT/supplier (LA payments, public_payments).",
            "NEVER sum TED with national awards — TED overlaps eTenders; summing double-counts.",
            "procurement x lobbying: NEVER sum awarded_value across the overlap — one return attaches to "
            "its registrant AND to each client, so any org-level sum double-counts.",
            "Lobbying return COUNTS are not additive across organisations (same reason).",
            "votes x member interests: only the LANDLORD/PROPERTY cross-reference is substantively real; "
            "do not build a general 'voted on their own interest' claim.",
            "CO-OCCURRENCE IS NOT CAUSATION — no key links a lobbying return to a contract award. Two "
            "registers sharing an entity is a RESEARCH LEAD, never evidence one explains the other.",
        ],
        "how_to_run_an_association": [
            "Resolve with organisation_dossier (it returns a `disambiguation` list rather than guessing).",
            "Read the returned `caveat` field — 13 tools return one in-band; it is not decoration.",
            "Treat every cross-register hit as a LEAD requiring verification; state the match tier.",
            "Beyond exact-match is a TIERED-MATCHING problem: keep EXACT as the only assertable tier and "
            "label fuzzy/embedding candidates as unverified leads. NEVER silently fuse a fuzzy match.",
        ],
        "doc": "doc/JOIN_MAP.md",
        "caveat": "Yields measured 2026-07-14 and are a FLOOR, not a ceiling. Absence of a cross-register "
        "match is never evidence of absence of the underlying activity.",
    }


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
            "register_of_interests": "Register of Members' Interests covers 1995–2025 (Dáil every year; Seanad missing 1996/1999/2004) — only pre-1995 divisions match no interests",
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


# ── Siting check (planning-constraint triage for a point — the citizen engine) ───


def _brief_item(it) -> dict:
    """One BriefItem (site-specific / standard / check) as JSON — drops empty fields."""
    out: dict = {"title": it.title, "why": it.why, "action": it.action}
    if getattr(it, "passfail", False):
        out["pass_fail"] = True  # e.g. the rural-housing-need gate inside the standard tier
    if it.reports:
        out["reports"] = list(it.reports)
    if it.path:
        out["mitigation_path"] = list(it.path)
    return out


@mcp.tool(annotations=_RO)
def siting_check(
    lat: float,
    lon: float,
    dev_type: str = "one_off_house",
    num_units: int = 0,
    floor_area_m2: float = 0.0,
) -> dict:
    """Planning-constraint TRIAGE for a single point in Ireland: which planning ISSUES a proposed
    development triggers at (lat, lon), each with the governing rule quoted verbatim from the
    per-council rulebook and the likely required reports. `dev_type` is 'one_off_house' (default),
    'multi_unit', or 'commercial'; `num_units` / `floor_area_m2` drive the scale-gated requirements
    (climate statement, EIA, etc.). Returns the council in force, a headline, statutory EXCLUSIONS
    (designations whose polygon covers the point — each with the narrow real route that could still
    permit development), and the fired issues TIERED for signal: `site_specific_hard` /
    `site_specific_shaping` (notable at THIS location), an access/entrance section,
    `standard_requirements` (apply to essentially every rural one-off here — the rural-housing-need
    gate is in here, marked `pass_fail`), `checks_to_confirm` (depend on a layer we can't read or
    site features we can't see, e.g. flood + bats), the likely-RFI report list, and `not_assessed`
    (layers not yet ingested at this point). The tiering groups truthfully — it suppresses nothing.

    ⚠️ TRIAGE, NOT A VERDICT — it NEVER outputs grant/refuse or a design prescription; the decision
    stays the council's/ABP's. An empty constraint list is NOT proof the land is developable, and a
    point outside ingested layer coverage (see `not_assessed` + `available_layers`) means we have NO
    data there — never read that as 'no issue'. Layer coverage is currently strongest in
    Galway/Cork/Dublin; elsewhere expect `not_assessed` entries. Risk language is 'likely', never
    'will'. Surface the `disclaimer`."""
    try:
        from dail_tracker_core.siting import brief as _brief
        from dail_tracker_core.siting import engine as _engine
        from dail_tracker_core.siting.layers import LayerStore
    except Exception as exc:  # noqa: BLE001 — optional 'siting' extra not installed
        return {"error": f"siting engine unavailable (optional 'siting' extra not installed): {exc}"}

    store = LayerStore()
    available = sorted(store.available())
    if not available:
        return {"error": "no planning-designation layers are ingested — siting check cannot run here"}

    dt = (dev_type or "one_off_house").strip()
    result = _engine.evaluate(
        lon,
        lat,
        dt,
        num_units=num_units or None,
        floor_area_m2=floor_area_m2 or None,
        store=store,
    )
    b = _brief.build_brief(result)
    return {
        "site": b.site,
        "headline": b.headline,
        "excluded": b.excluded,
        "exclusions": [
            {"designation": e.designation, "site_name": e.site_name, "layer": e.layer, "mitigation": e.mitigation}
            for e in b.exclusions
        ],
        # TIERED so the site-specific signal isn't drowned by boilerplate (see catalogue.Node):
        # site_specific_* = notable at THIS location; standard_requirements = apply to ~every rural
        # one-off here (the rural-housing-need gate is in here, marked pass_fail); checks_to_confirm =
        # depend on a layer we can't read / site features we can't see (flood, bats).
        "site_specific_hard": [_brief_item(i) for i in b.hard_constraints],
        "site_specific_shaping": [_brief_item(i) for i in b.shaping_constraints],
        "access": b.access,
        "standard_requirements": [_brief_item(i) for i in b.obligations],
        "checks_to_confirm": [_brief_item(i) for i in b.to_verify],
        "required_reports": list(b.required_reports),
        "rfi_note": b.rfi_note,
        "not_assessed": list(b.not_assessed),
        "available_layers": available,
        "disclaimer": b.disclaimer,
        "caveat": "planning-risk TRIAGE, never a grant/refuse verdict; an empty list is not proof the "
        "site is developable, and a point outside ingested layer coverage has NO data (not 'no issue')",
    }


# ── Cross-register watchlist + organisation dossier (entity-crosswalk spine) ─────


@mcp.tool(annotations=_RO)
def cross_register_watchlist(min_registers: int = 2, limit: int = 25) -> dict:
    """Organisations that appear on the MOST public registers at once — procurement
    suppliers that are ALSO on the lobbying register, in corporate (Iris Oifigiúil)
    notices, on the charity register and/or holding an EPA licence. `min_registers`
    counts registers BEYOND procurement (default 2 = a supplier on at least two other
    registers). Each row carries the per-register flags and counts, the CRO company
    match, and the supplier's sum-safe awarded €. Use for "which companies show up
    everywhere?" — then get_supplier / company_influence for the detail behind one row.

    ⚠️ CO-OCCURRENCE ONLY — the same organisation on several registers is a research
    lead, NEVER evidence that one register explains another (no key links a lobby or
    meeting to a contract). Counts are FLOORS: exact normalised-name / CRO matching
    misses subsidiary and trading-name variants, and fusion below the exact match tier
    is suppressed, not guessed — so a low count can be an undercount, and absence is
    never proof of absence. awarded_value_safe_eur is the AWARD-ceiling grain: never
    sum it with payments or any other money grain. No individuals — sole traders /
    natural persons are excluded upstream. Surface the `caveat` with any finding."""
    qr = ent.top_cross_register(_cur(), min_registers=min_registers, limit=limit)
    if not qr.ok:
        return {"error": qr.unavailable_reason}
    rows = serialize.to_records(qr.data)
    return {"count": len(rows), "entities": rows, "caveat": caveats.ENTITY_COOCCURRENCE}


def _org_name_key(name: str) -> str:
    """The CANONICAL company-name key for one string — shared/name_norm.name_norm_expr
    applied through a one-row frame, so this tool's key can never drift from the rule
    the spine was built with (the divergent-normaliser bug the crosswalk exists to fix)."""
    import polars as pl  # lazy — keeps server startup instant; only paid on first dossier call

    from shared.name_norm import name_norm_expr

    return str(pl.DataFrame({"n": [name]}).select(name_norm_expr("n")).item() or "")


_XREF_CANDIDATE_SQL = (
    "SELECT supplier_norm, display_name, company_num, has_cro, on_lobbying_register,"
    " has_corporate_notice, is_charity, has_epa_licence, cross_register_count"
    " FROM v_supplier_entity_xref"
)


def _org_registers(row: dict) -> list[str]:
    """The registers one spine row appears on (every row is procurement-anchored)."""
    flags = [
        ("has_cro", "cro"),
        ("on_lobbying_register", "lobbying"),
        ("has_corporate_notice", "corporate-notices"),
        ("is_charity", "charity"),
        ("has_epa_licence", "epa"),
    ]
    return ["procurement"] + [label for col, label in flags if row.get(col)]


def _resolve_org_candidates(cur, name: str, company_num: str) -> tuple[list[dict], str]:
    """name / CRO number → spine candidates, strongest signal first: CRO company_num,
    then the exact canonical name key, then the company_influence-style fuzzy substring
    fallback. Returns (candidates, via); >1 candidate disambiguates at the call site —
    never a guess (the council_scorecard pattern)."""
    if company_num.strip():
        num = company_num.strip()
        rows = serialize.to_records(
            cur.execute(
                _XREF_CANDIDATE_SQL
                + " WHERE CAST(company_num AS VARCHAR) = ? OR TRY_CAST(company_num AS BIGINT) = TRY_CAST(? AS BIGINT)",
                [num, num],
            ).df()
        )
        if rows:
            return rows, "company_num"
    key = _org_name_key(name)
    if key:
        rows = serialize.to_records(cur.execute(_XREF_CANDIDATE_SQL + " WHERE supplier_norm = ?", [key]).df())
        if rows:
            return rows, "exact_name"
    clauses, params = [], []
    if key:
        clauses.append("supplier_norm LIKE ?")
        params.append(f"%{key}%")
    if name.strip():
        clauses.append("lower(display_name) LIKE ?")
        params.append(f"%{name.strip().lower()}%")
    if not clauses:
        return [], "none"
    rows = serialize.to_records(
        cur.execute(
            _XREF_CANDIDATE_SQL
            + " WHERE "
            + " OR ".join(clauses)
            + " ORDER BY cross_register_count DESC, awarded_value_safe_eur DESC NULLS LAST LIMIT 8",
            params,
        ).df()
    )
    return rows, "fuzzy_name"


# PR3 SHIPPED EARLY on the INTERIM spine — owner sign-off 2026-07-10. The name /
# company_num interface below is the STABLE PR3 contract from
# doc/ENTITY_CROSSWALK_ORG_DOSSIER_DESIGN.md; resolution currently runs against the
# supplier-anchored v_supplier_entity_xref. When the unified v_entity_xref spine lands
# (PR0 normaliser unification + PR1), only the internals here and
# dossiers.build_organisation_dossier swap — callers never pass supplier_norm.
@mcp.tool(annotations=_RO)
def organisation_dossier(name: str, company_num: str = "") -> dict:
    """ONE-CALL organisation-360: resolve a company by plain-English `name` (optionally
    pinned by its CRO `company_num`) and return its cross-register dossier — CRO identity,
    procurement footprint (award rows + sum-safe awarded €) and its co-occurrence across
    the lobbying register, corporate (Iris Oifigiúil) notices, the charity register and
    EPA licensing. Use it when a question spans registers ("who is X, what state contracts
    did they win, do they also lobby / run a charity?") instead of stitching
    search_suppliers + get_supplier + lobbying tools yourself; for meeting-level ministerial
    access use company_influence.

    Resolution folds `name` to the same canonical key the spine is built on (NFKD
    accent-fold + legal-suffix strip — 'Tirlán' / 'TIRLAN LIMITED' land on one key). An
    ambiguous name returns a `disambiguation` candidate list (each with the registers it
    appears on), never a guess — call again with one exact name or its company_num. No
    match returns a structured hint (try search_suppliers).

    ⚠️ Matching is CONSERVATIVE: the spine is anchored on public-procurement suppliers and
    fused at the EXACT match tier only, so yields undercount (match_tier floor) —
    subsidiary/trading-name variants are missed, an organisation that never won public
    procurement is absent, and a low register count is a floor, never proof of absence.
    The `caveat` means: co-occurrence by ENTITY only — the same organisation on several
    registers is a research lead, NEVER evidence one register explains another (no key
    links a lobby or meeting to a contract). awarded_value_safe_eur is the AWARD-ceiling
    money grain — never sum it with payments or any other grain. No individuals — sole
    traders / natural persons are excluded upstream. Surface the `caveat` with findings."""
    if not name.strip() and not company_num.strip():
        return {"error": "pass an organisation name (and optionally a CRO company_num)"}
    cur = _cur()
    try:
        candidates, via = _resolve_org_candidates(cur, name, company_num)
    except Exception as exc:  # noqa: BLE001 — missing spine parquet/view degrades to 'unavailable'
        return {"error": f"organisation spine unavailable: {exc}"}
    if not candidates:
        return {
            "match": "none",
            "query": {"name": name, "company_num": company_num or None},
            "hint": "no organisation resolved on the cross-register spine (procurement-supplier "
            "anchored — an org that never appears as a public-procurement supplier is not on it). "
            "Try search_suppliers to browse supplier names, or company_influence for diary access.",
        }
    if len(candidates) > 1:
        return {
            "match": "ambiguous",
            "disambiguation": [
                {
                    "name": c.get("display_name"),
                    "company_num": c.get("company_num"),
                    "supplier_norm": c.get("supplier_norm"),
                    "registers": _org_registers(c),
                }
                for c in candidates
            ],
            "note": "multiple organisations match — call again with one exact name or its CRO company_num",
        }
    norm = str(candidates[0]["supplier_norm"])
    d = dossiers.build_organisation_dossier(cur, norm)
    if d is None:  # candidate came from the spine, so this is a source fault, not a miss
        return {"error": f"spine row for '{norm}' vanished mid-query — source unavailable"}
    d["matched"] = {"via": via, "supplier_norm": norm}
    xr = d.get("cross_register")
    if isinstance(xr, dict) and "register_count" in xr:
        # Presentation rename for LLM callers: the composer's count EXCLUDES procurement
        # and CRO identity (a 0 here means procurement-only, not off-register) — the bare
        # name `register_count` reads as the inclusive count the disambiguation list shows.
        xr["registers_beyond_procurement"] = xr.pop("register_count")
    return d


# ── Local government (council accountability scorecard) ─────────────────────────


@mcp.tool(annotations=_RO)
def council_scorecard(local_authority: str = "") -> dict:
    """A council's accountability scorecard. With no argument: the national headline plus
    the 31-council Chief Executive index (use it to find the exact council label). With a
    `local_authority` (fuzzy — 'Mayo' resolves to 'Mayo County Council'): that council's
    Chief Executive record, its NOAC 2024 performance indicators (finance / workforce /
    roads / fire / litter, each beside the national median), its rates-collection
    performance, and the three co-located cash signals (revenue balance, rates collection,
    derelict-sites levy).

    ⚠️ Each indicator is the council's OWN reported figure shown beside the national
    median benchmark — indicators are never apportioned or summed across measures, and a
    weak metric is context for scrutiny, not a verdict on the council or its executive.
    No relationship between the cash signals is asserted. Surface the `caveat`."""
    cur = _cur()
    ce_qr = lg.chief_executives(cur)
    if not ce_qr.ok:
        return {"error": ce_qr.unavailable_reason}
    ces = serialize.to_records(ce_qr.data)
    if not local_authority:
        return {
            "national": _one(lg.national_summary(cur)),
            "councils": ces,
            "note": "call again with one local_authority label for its full scorecard",
        }
    q = local_authority.strip().lower()
    hits = [r for r in ces if q in str(r.get("local_authority", "")).lower()]
    if not hits:
        return {
            "error": f"no council matches '{local_authority}'",
            "councils": [r.get("local_authority") for r in ces],
        }
    if len(hits) > 1:
        return {
            "disambiguation": [r.get("local_authority") for r in hits],
            "note": "multiple councils match — call again with one exact label",
        }
    la = str(hits[0]["local_authority"])
    return {
        "local_authority": la,
        "chief_executive": hits[0],
        "noac_scorecard": _rows(lg.noac_scorecard(cur, la)),
        "collection_rates": _rows(lg.collection_rates(cur, la)),
        "cash_signals": _rows(lg.cash_signals(cur, la)),
        "caveat": caveats.NOAC_SCORECARD,
    }


# ── AFS (local-authority audited accounts — the BUDGET grain) ────────────────────


@mcp.tool(annotations=_RO)
def afs_coverage(local_authority: str = "") -> dict:
    """Local-authority Annual Financial Statement (AFS) coverage — the audited BUDGET grain
    (councils' own Income & Expenditure account), a SIBLING fact to procurement awards/payments,
    never summed with them. With no argument: which of the 31 councils have audited accounts
    loaded and for which years (`coverage_by_council`), plus the national amalgamated AFS total
    per year (`national_by_year`). With a `local_authority` (fuzzy — 'Mayo' resolves to 'Mayo
    County Council'): that council's revenue-account spend per year (`by_year`) and the
    AFS-vs-PO traceability bridge (`afs_vs_po_bridge` — how much of its audited spend is
    traceable to a named >€20k supplier line; INDICATIVE only, not a reconciliation).

    ⚠️ gross/net_expenditure is Σ OPERATING expenditure BY SERVICE DIVISION — it EXCLUDES
    inter-account/reserve transfers and is NOT the council's headline printed total. NEVER sum
    AFS euros with procurement AWARD ceilings, public-body PAYMENTS, or T&A allowances — three
    different money grains. Surface the `caveat`."""
    cur = _cur()
    coverage = _rows(proc.afs_coverage_by_council(cur))
    if isinstance(coverage, dict):  # unavailable (missing source parquet)
        return {"local_authority": local_authority, **coverage}
    if not local_authority:
        return {
            "coverage_by_council": coverage,
            "national_by_year": _rows(proc.afs_national_by_year(cur)),
            "note": "call again with one local_authority label for its per-year AFS + AFS-vs-PO bridge",
            "caveat": caveats.AFS,
        }
    q = local_authority.strip().lower()
    hits = [r for r in coverage if q in str(r.get("council", "")).lower()]
    if not hits:
        return {
            "error": f"no council matches '{local_authority}'",
            "councils": [r.get("council") for r in coverage],
        }
    if len(hits) > 1:
        return {
            "disambiguation": [r.get("council") for r in hits],
            "note": "multiple councils match — call again with one exact label",
        }
    la = str(hits[0]["council"])
    return {
        "local_authority": la,
        "by_year": _rows(proc.afs_total_by_year(cur, la)),
        "afs_vs_po_bridge": _rows(proc.afs_vs_po_coverage(cur, la)),
        "caveat": caveats.AFS,
    }


# ── Housing money (national demand / supply / accommodation spend) ───────────────


@mcp.tool(annotations=_RO)
def housing_money(grain: str = "national") -> dict:
    """The national housing-money picture in one call: social-housing waiting-list totals
    (`grain` = 'national' for the one-row headline, or 'county' / 'la' for the league
    table), the supply & affordability headline (vacancy, average private rent, HAP), the
    HAP household profile, new-dwelling completions per year (CSO), and State asylum
    (international-protection) + Ukraine accommodation spend per year from the published
    over-€20,000 purchase-order registers.

    ⚠️ MONEY GRAIN — the accommodation figures are COMMITTED SPEND (purchase orders, by
    year and stream); never add them to procurement AWARD ceilings or any other money
    grain. Named accommodation PROVIDERS are deliberately not served by this tool;
    use public_body_payments for the privacy-gated supplier ranking. Supply/rent/HAP
    fields carry their source period plus `*_period_age_years` / `*_stale` flags — some
    CSO/RTB series lag by years (rent and HAP are 2022-period), so ALWAYS report the
    period alongside the figure and never present a stale field as current. Surface the
    `caveats` with any € figure."""
    cur = _cur()
    return {
        "waiting_list": _rows(hsg.waiting_list_totals(cur, grain)),
        "supply": _one(hsg.supply_national(cur)),
        "hap": _one(hsg.hap_national(cur)),
        "completions_by_year": _rows(hsg.completions_trend(cur)),
        "accommodation_spend_by_year": _rows(hsg.accommodation_spend_by_year(cur)),
        "caveats": {
            "accommodation_spend": caveats.ACCOMMODATION_SPEND,
            "money_grains": caveats.MONEY_GRAINS,
        },
    }


# ── Attendance (division turnout + TAA compliance) ───────────────────────────────


@mcp.tool(annotations=_RO)
def attendance_ranking(year: int = 0, house: str = "Dáil", limit: int = 25) -> dict:
    """Attendance PARTICIPATION for one (year, house): per-member division turnout
    (voted_in / missed / total_divisions / turnout_pct, WORST-first) plus the statutory
    Travel & Accommodation Allowance compliance cut — the members below the 120-day
    threshold and the cleared/below summary. `year`=0 resolves to the latest reporting
    year; `house` is 'Dáil' or 'Seanad'. For one member's own record use
    get_member_record instead.

    ⚠️ turnout_pct is computed IN the registered view (divisions voted in ÷ divisions
    held DURING THE MEMBER'S SERVICE WINDOW in that house — mid-term arrivals/departures
    are not penalised for divisions held while they were not a member) — report it as
    returned and NEVER recompute it against sitting days or any other denominator
    (different bases). is_minister/is_chair are date-bounded to the ranking year, not
    today's officeholders. Office-holders (ministers / chairs / leaders) are FLAGGED,
    not hidden: not voting can be their role, so a low rate is context, never a verdict —
    carry the role flags with any ranking you present. TAA rows exclude office-holders
    (not paid TAA on the attendance basis), and TAA € are a distinct allowance grain —
    never summed with any other money."""
    cur = _cur()
    if not year:
        yqr = att.participation_years(cur, house)
        if not yqr.ok:
            return {"error": yqr.unavailable_reason}
        if yqr.data.empty:
            return {"error": f"no attendance reporting years on file for {house}"}
        year = int(yqr.data.iloc[0]["year"])
    turnout = _rows(att.participation_turnout(cur, year, house))
    if isinstance(turnout, list):
        turnout = turnout[:limit]
    below = _rows(att.taa_compliance(cur, year, house))
    if isinstance(below, list):
        below = below[:limit]
    return {
        "year": year,
        "house": house,
        "turnout_worst_first": turnout,
        "taa_summary": _one(att.taa_compliance_summary(cur, year, house)),
        "taa_below_threshold": below,
        "caveat": caveats.ATTENDANCE,
    }


# ── National public finance (CSO general-government) ─────────────────────────────


@mcp.tool(annotations=_RO)
def gov_finance_annual() -> dict:
    """National general-government REVENUE / EXPENDITURE / BALANCE per year (CSO GFA01,
    national-accounts basis), newest first — the authoritative denominator for 'share of
    total public spend' context (e.g. 'that programme = 0.4% of general-government
    expenditure').

    ⚠️ NATIONAL-ACCOUNTS GRAIN — a macro aggregate, NEVER mixed or summed with the
    transaction-level registers (procurement awards, public-body payments, TD allowances).
    Use it to contextualise a figure against the national total, never to reconcile a
    register's sum against it. Surface the `caveat`."""
    rows = _rows(pf.gov_finance_annual(_cur()))
    if isinstance(rows, dict):
        return rows
    return {"by_year": rows, "caveat": caveats.GOV_FINANCE}


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


@mcp.prompt()
def siting_brief(location: str) -> str:
    """Triage the planning constraints at a location, honestly bounded by data coverage."""
    return (
        f"Triage the planning constraints for a proposed one-off house at '{location}'. First resolve "
        "the location to lat/lon, then call siting_check with them. Report the council in force, any "
        "statutory EXCLUSIONS (with the narrow route that could still permit development), the hard "
        "(pass/fail) and shaping constraints, the access/entrance findings, and the likely required "
        "reports — each grounded in the rule text the tool returns. Present the `check_yourself` items "
        "as user-verifiable checks, not findings. Be explicit about `not_assessed` layers and the "
        "coverage caveat: this is planning-risk TRIAGE, never a grant/refuse verdict, and an empty list "
        "is not proof the site is developable. Always surface the disclaimer."
    )


@mcp.resource("data://coverage")
def coverage_resource() -> dict:
    """Machine-readable scope manifest (same payload as the data_coverage tool) so a client can
    load it as ambient context and scope answers without spending a tool call."""
    return data_coverage()


if __name__ == "__main__":
    mcp.run()  # stdio transport — Claude Desktop launches this as a subprocess
