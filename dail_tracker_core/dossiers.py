"""Composed, Streamlit-free "dossier" builders.

The Streamlit page composes a member's cross-dataset record inside its render
functions; this module composes the SAME record as plain data (no rendering) so
the API — or a file-based pack product — can serve it. It reuses the
``queries.member_overview`` (``moq``) retrieval fns and applies the same shaping
the page wrappers apply (identity attendance→registry fallback, the SUM NaN
guard, Dáil-only constituency context).

This is the differentiator: no comparable parliamentary API serves a pre-composed
dossier — they all make the client fan a person id across resources.
"""

from __future__ import annotations

from typing import Any, cast

import duckdb
import pandas as pd

from dail_tracker_core import serialize
from dail_tracker_core.queries import appointments as appt
from dail_tracker_core.queries import charities as char
from dail_tracker_core.queries import committees as cmte
from dail_tracker_core.queries import cross_ref as xref
from dail_tracker_core.queries import interests as intr
from dail_tracker_core.queries import judiciary as jud
from dail_tracker_core.queries import legislation as leg
from dail_tracker_core.queries import lobbying as lb
from dail_tracker_core.queries import member_overview as moq
from dail_tracker_core.queries import ministerial as min_
from dail_tracker_core.queries import payments as pay
from dail_tracker_core.queries import procurement as proc
from dail_tracker_core.queries import public_payments as pubpay
from dail_tracker_core.queries import sipo
from dail_tracker_core.queries import votes as vot


def _identity(conn: duckdb.DuckDBPyConnection, code: str) -> dict[str, Any] | None:
    df = moq.identity_attendance(conn, code).data
    if not df.empty:
        return df.iloc[0].to_dict()
    df = moq.identity_registry(conn, code).data
    return df.iloc[0].to_dict() if not df.empty else None


def list_members(
    conn: duckdb.DuckDBPyConnection,
    *,
    house: str | None = None,
    party: str | None = None,
    constituency: str | None = None,
    fuzzy_name: str | None = None,
    skip: int = 0,
    limit: int = 50,
) -> tuple[list[dict[str, Any]], int, bool]:
    """(page_records, total, truncated) over the member registry.

    Roster selection (exact house/party/constituency, substring name) on a ~176-row
    frame — selection, not a metric, so it stays here rather than spawning a view.
    """
    df = moq.member_list(conn).data
    if df.empty:
        return [], 0, False
    # .loc[mask] (not df[mask]) keeps the static type a DataFrame for the pandas
    # stubs, so .iloc / .str below type-check without casts.
    if house:
        df = df.loc[df["house"] == house]
    if party:
        df = df.loc[df["party_name"] == party]
    if constituency:
        df = df.loc[df["constituency"] == constituency]
    if fuzzy_name:
        df = df.loc[df["member_name"].astype(str).str.contains(fuzzy_name, case=False, na=False)]
    total = int(len(df))
    page = df.iloc[skip : skip + limit]
    truncated = total > skip + len(page)
    return serialize.to_records(page), total, truncated


def build_member_dossier(conn: duckdb.DuckDBPyConnection, code: str) -> dict[str, Any] | None:
    """Full cross-dataset record for one member, or None if the code is unknown."""
    ident = _identity(conn, code)
    if ident is None:
        return None

    house_df = moq.member_house(conn, code).data
    house = str(house_df.iloc[0]["house"]) if not house_df.empty else "Dáil"
    is_minister = str(ident.get("is_minister", "")).lower() == "true"
    constituency = serialize.value(ident.get("constituency"))

    att = moq.att_all_years(conn, code).data
    latest_year = int(att.iloc[0]["year"]) if not att.empty else None
    days_latest = int(att.iloc[0]["attended_count"]) if not att.empty else None

    vs = moq.votes_summary(conn, code).data
    if not vs.empty:
        r = vs.iloc[0]
        votes_cast = (
            int(r.get("yes_count", 0) or 0) + int(r.get("no_count", 0) or 0) + int(r.get("abstained_count", 0) or 0)
        )
        divisions = int(r.get("division_count", 0) or 0)
    else:
        votes_cast = divisions = 0

    pg = moq.pay_grand_total(conn, code).data
    pay_total = float(pg.iloc[0]["total"]) if (not pg.empty and pd.notna(pg.iloc[0]["total"])) else 0.0

    constituency_context = None
    if house != "Seanad" and constituency:
        constituency_context = serialize.first_record(moq.constituency_context(conn, str(constituency)).data)

    return {
        "member": {
            "unique_member_code": code,
            "member_name": serialize.value(ident.get("member_name")),
            "party_name": serialize.value(ident.get("party_name")),
            "constituency": constituency,
            "house": house,
        },
        "is_minister": is_minister,
        "headline": {
            "latest_year": latest_year,
            "days_in_chamber_latest": days_latest,
            "votes_cast": votes_cast,
            "divisions": divisions,
            "payments_total_eur": pay_total,
        },
        "attendance_by_year": serialize.to_records(att),
        "payments_by_year": serialize.to_records(moq.pay_overview(conn, code).data),
        "legislation_sponsored": serialize.to_records(moq.legislation(conn, code).data),
        "ministerial_roles": serialize.to_records(moq.ministerial_roles(conn, code).data),
        "statutory_instruments_signed": serialize.to_records(moq.si_signed(conn, code).data),
        "revolving_door": serialize.to_records(moq.lobbying_rd(conn, code).data),
        "questions_profile": serialize.first_record(moq.question_profile(conn, code).data),
        "speeches_profile": serialize.first_record(moq.speech_summary(conn, code).data),
        "external_links": serialize.first_record(moq.external_links(conn, code).data) or {},
        "constituency_context": constituency_context,
    }


# ── Legislation + statutory instruments (legislation_conn) ────────────────────


def _page(df: pd.DataFrame, skip: int, limit: int) -> tuple[list[dict[str, Any]], int, bool]:
    total = int(len(df))
    page = df.iloc[skip : skip + limit]
    return serialize.to_records(page), total, total > skip + len(page)


def list_bills(
    conn: duckdb.DuckDBPyConnection,
    *,
    status: str | None = None,
    title_search: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    skip: int = 0,
    limit: int = 50,
) -> tuple[list[dict[str, Any]], int, bool]:
    df = leg.index_filtered(conn, start_date, end_date, status, title_search).data
    if df.empty:
        return [], 0, False
    return _page(df, skip, limit)


def build_bill_dossier(conn: duckdb.DuckDBPyConnection, bill_id: str) -> dict[str, Any] | None:
    """Composed bill record: detail + timeline + amendments + sources + PDFs +
    debates + the statutory instruments made under it. None if the id is unknown."""
    detail = leg.bill_detail(conn, bill_id).data
    if detail.empty:
        return None
    return {
        "bill": serialize.first_record(detail),
        "timeline": serialize.to_records(leg.bill_timeline(conn, bill_id).data),
        "amendment_intensity": serialize.first_record(leg.amendment_intensity_for_bill(conn, bill_id).data),
        "sources": serialize.first_record(leg.bill_sources(conn, bill_id).data),
        "pdfs": serialize.to_records(leg.bill_pdfs(conn, bill_id).data),
        "debates": serialize.to_records(leg.bill_debates(conn, bill_id).data),
        "si_composition": serialize.to_records(leg.si_composition(conn, bill_id).data),
        "statutory_instruments": serialize.to_records(leg.si_by_bill(conn, bill_id).data),
    }


def list_statutory_instruments(
    conn: duckdb.DuckDBPyConnection,
    *,
    year: int | None = None,
    operation: str | None = None,
    department: str | None = None,
    eu_only: bool = False,
    skip: int = 0,
    limit: int = 50,
) -> tuple[list[dict[str, Any]], int, bool]:
    df = leg.si_entity_index(conn).data
    if df.empty:
        return [], 0, False
    if year is not None:
        df = df.loc[df["si_year"] == year]
    if operation:
        df = df.loc[df["si_operation"] == operation]
    if department:
        df = df.loc[df["si_department_label"] == department]
    if eu_only:
        df = df.loc[df["si_is_eu"].fillna(False).astype(bool)]
    return _page(df, skip, limit)


# ── Votes ─────────────────────────────────────────────────────────────────────


def list_votes(
    conn: duckdb.DuckDBPyConnection,
    *,
    date_from: str | None = None,
    date_to: str | None = None,
    outcome: str | None = None,
    house: str = "Dáil",
    skip: int = 0,
    limit: int = 50,
) -> tuple[list[dict[str, Any]], int, bool]:
    df = vot.vote_index(conn, date_from, date_to, outcome, house).data
    if df.empty:
        return [], 0, False
    return _page(df, skip, limit)


def build_division_dossier(conn: duckdb.DuckDBPyConnection, vote_id: str) -> dict[str, Any] | None:
    """Composed division record: the vote + party breakdown + every member's vote + sources."""
    one = vot.vote_by_id(conn, vote_id).data
    if one.empty:
        return None
    return {
        "division": serialize.first_record(one),
        "party_breakdown": serialize.to_records(vot.party_breakdown(conn, vote_id).data),
        "members": serialize.to_records(vot.division_members(conn, vote_id).data),
        "sources": serialize.first_record(vot.sources(conn, vote_id).data),
    }


# ── Cross-reference: votes × Register of Members' Interests ────────────────────

# Coverage caveat every cross-reference response carries, so an AI consumer states
# it rather than implying full historical coverage.
_INTERESTS_CAVEAT = (
    "Register of Members' Interests covers 2020–2025 only; divisions before 2020 "
    "have no interests counterpart and match nothing. 'landlord'/'property' use the "
    "derived flags; 'director'/'shareholder' use the declared interest_category. "
    "held_in_vote_year=true means the interest was declared in the vote's own year."
)


def build_division_interest_breakdown(conn: duckdb.DuckDBPyConnection, vote_id: str) -> dict[str, Any] | None:
    """One division's Yes/Níl/Abstain tally split by the declared interests of its
    voters (landlords / property-owners / directors / shareholders)."""
    one = vot.vote_by_id(conn, vote_id).data
    if one.empty:
        return None
    res = xref.division_interest_breakdown(conn, vote_id)
    return {
        "division": serialize.first_record(one),
        "interest_breakdown": serialize.to_records(res.data),
        "caveat": _INTERESTS_CAVEAT,
    }


def cross_reference_votes_interests(
    conn: duckdb.DuckDBPyConnection,
    *,
    vote_id: str | None = None,
    keyword: str | None = None,
    vote_type: str = "Voted No",
    interest: str = "landlord",
    house: str = "Dáil",
) -> dict[str, Any]:
    """Members who voted ``vote_type`` on a division (by ``vote_id`` or debate-title
    ``keyword``) AND declare ``interest`` on the register."""
    res = xref.voting_vs_interests(
        conn, vote_id=vote_id, keyword=keyword, vote_type=vote_type, interest=interest, house=house
    )
    if not res.ok:
        return {"error": res.unavailable_reason}
    matches = serialize.to_records(res.data)
    return {
        "query": {
            "vote_id": vote_id,
            "keyword": keyword,
            "vote_type": vote_type,
            "interest": interest,
            "house": house,
        },
        "match_count": len(matches),
        "distinct_members": len({m["member_id"] for m in matches}),
        "matches": matches,
        "caveat": _INTERESTS_CAVEAT,
    }


# ── Payments ──────────────────────────────────────────────────────────────────


def list_payments_ranking(
    conn: duckdb.DuckDBPyConnection, *, house: str = "Dáil", skip: int = 0, limit: int = 50
) -> tuple[list[dict[str, Any]], int, bool]:
    df = pay.alltime_ranking(conn, house).data
    if df.empty:
        return [], 0, False
    return _page(df, skip, limit)


# ── Lobbying ──────────────────────────────────────────────────────────────────


def list_lobbying_orgs(
    conn: duckdb.DuckDBPyConnection,
    *,
    name: str | None = None,
    exclude_state_adjacent: bool = False,
    skip: int = 0,
    limit: int = 50,
) -> tuple[list[dict[str, Any]], int, bool]:
    df = lb.org_index(conn, exclude_state_adjacent, name_q=name).data
    if df.empty:
        return [], 0, False
    return _page(df, skip, limit)


def list_revolving_door(
    conn: duckdb.DuckDBPyConnection, *, skip: int = 0, limit: int = 50
) -> tuple[list[dict[str, Any]], int, bool]:
    """Former office-holders (DPOs) now lobbying — the revolving-door register."""
    df = lb.revolving_door(conn, None).data  # full list; paginate here
    if df.empty:
        return [], 0, False
    return _page(df, skip, limit)


# ── Member resolution (shared by member-scoped builders below) ─────────────────


def _resolve_member(conn: duckdb.DuckDBPyConnection, name_or_code: str) -> dict[str, Any] | None:
    """Resolve a code OR a name to ``{code, member_name, house}``.

    Tries exact code, then exact (case-insensitive) name, then substring; takes the
    first hit. Member-scoped views key differently — questions on
    ``unique_member_code``, interests on ``member_name`` + ``house`` — so callers
    need both surfaced from one lookup.
    """
    df = moq.member_list(conn).data
    if df.empty:
        return None
    hit = df.loc[df["unique_member_code"] == name_or_code]
    if hit.empty:
        hit = df.loc[df["member_name"].astype(str).str.lower() == name_or_code.lower()]
    if hit.empty:
        hit = df.loc[df["member_name"].astype(str).str.contains(name_or_code, case=False, na=False)]
    if hit.empty:
        return None
    row = hit.iloc[0]
    return {
        "code": str(row["unique_member_code"]),
        "member_name": str(row["member_name"]),
        "house": str(row["house"]),
    }


# ── Procurement ───────────────────────────────────────────────────────────────


def list_suppliers(
    conn: duckdb.DuckDBPyConnection,
    *,
    year: int | None = None,
    order_by: str = "awards",
    skip: int = 0,
    limit: int = 20,
) -> tuple[list[dict[str, Any]], int, bool]:
    """Supplier ranking (one row per distinct supplier), carrying the view's CRO match
    + lobbying-overlap flags. ``year`` scopes to one calendar year; ``None`` is all-time."""
    df = proc.supplier_summary(conn, order_by=order_by, year=year).data
    if df.empty:
        return [], 0, False
    return _page(df, skip, limit)


def build_supplier_dossier(conn: duckdb.DuckDBPyConnection, supplier_norm: str) -> dict[str, Any] | None:
    """Composed supplier record: the ranking-row summary + every award, newest first.
    None if the supplier_norm matches neither the summary nor any award."""
    summary_df = proc.supplier_summary(conn).data
    match = summary_df.loc[summary_df["supplier_norm"] == supplier_norm] if not summary_df.empty else summary_df
    summary = serialize.first_record(match) if not match.empty else None
    awards = serialize.to_records(proc.awards_for_supplier(conn, supplier_norm).data)
    if summary is None and not awards:
        return None
    return {"summary": summary, "awards": awards}


# Co-occurrence caveat — mirrors data/_meta/procurement_lobbying_overlap_coverage.json.
# Rides on every overlap response so an AI consumer can't present it as causation.
_PROC_LOBBY_CAVEAT = (
    "Co-occurrence by ENTITY only: each company appears on BOTH the public-procurement "
    "award register and the lobbying register. NOT evidence that lobbying influenced any "
    "contract — there is no shared key linking a specific lobby to a specific award. "
    "Exact normalised-name matching undercounts (subsidiary / trading-name variants are "
    "missed). awarded_value_safe_eur is a per-supplier total carried on each of that "
    "supplier's lobby entities — never sum it across the nested lobby_entities."
)

_PROC_LOBBY_ORDER = {
    "award_value": "awarded_value_safe_eur",
    "award_rows": "award_rows",
    "lobby_returns": "lobby_returns_total",
    "authorities": "authorities",
}


def list_procurement_lobbying_overlap(
    conn: duckdb.DuckDBPyConnection,
    *,
    limit: int = 50,
    order_by: str = "award_value",
    side: str | None = None,
) -> dict[str, Any]:
    """Companies on BOTH the procurement and lobbying registers, ONE ROW PER SUPPLIER
    with their lobby entities nested — so the per-supplier award value can never be
    double-counted across multiple lobby-name matches. Co-occurrence disclosure only
    (see caveat); never causation."""
    res = proc.lobbying_overlap(conn)
    if not res.ok:
        return {"error": res.unavailable_reason}
    df = res.data
    if side in ("registrant", "client"):
        df = df.loc[df["lobby_side"] == side]

    suppliers: list[dict[str, Any]] = []
    for norm, raw_grp in df.groupby("supplier_norm"):
        grp = cast("pd.DataFrame", raw_grp)  # groupby yields DataFrame | Series to the stubs
        first = grp.iloc[0]  # award_rows / authorities / value are per-supplier constants
        # Serialize then sort the records (most lobby returns first) in plain Python —
        # avoids a pandas-stubs sort_values overload false-positive on the groupby group.
        entities = serialize.to_records(grp.loc[:, ["lobby_name", "lobby_side", "n_lobby_returns"]])
        entities.sort(key=lambda r: r.get("n_lobby_returns") or 0, reverse=True)
        suppliers.append(
            {
                "supplier": serialize.value(first["supplier"]),
                "supplier_norm": serialize.value(norm),
                "award_rows": int(first["n_award_rows"]),
                "authorities": int(first["n_authorities"]),
                "awarded_value_safe_eur": float(first["awarded_value_safe_eur"]),
                "lobby_returns_total": int(grp["n_lobby_returns"].sum()),
                "lobby_entities": entities,
            }
        )

    sort_col = _PROC_LOBBY_ORDER.get(order_by, "awarded_value_safe_eur")
    suppliers.sort(key=lambda r: r[sort_col], reverse=True)
    # Summing per distinct supplier is the ONLY correct total (rows duplicate per match).
    total = sum(s["awarded_value_safe_eur"] for s in suppliers)
    return {
        "summary": {
            "distinct_suppliers": len(suppliers),
            "total_awarded_value_safe_eur": total,
            "side_filter": side,
            "order_by": order_by if order_by in _PROC_LOBBY_ORDER else "award_value",
        },
        "suppliers": suppliers[:limit] if limit else suppliers,
        "caveat": _PROC_LOBBY_CAVEAT,
    }


_COMPETITION_CAVEAT = (
    "single_bid_lot_pct = single-bid LOTS / lots-with-a-bid-count, from TED 2024+ award "
    "notices — each contract PART counted once (the honest lot-level rate; an earlier "
    "notice-level reading over-stated multi-lot buyers). A FACTUAL competition signal, NEVER "
    "a verdict: a single bidder is often legitimate — a niche/specialist supplier, bespoke "
    "research equipment, genuine urgency (research universities legitimately single-source a "
    "lot). It is the EU Single Market Scoreboard's procurement-integrity indicator: a prompt "
    "to look, not evidence of wrongdoing. Rank only buyers with a healthy n_lots_with_bidcount "
    "(min_lots default 40); small samples are noisy. Coverage is 2024+ only (the eForms era "
    "carries bid counts)."
)


def list_procurement_competition(
    conn: duckdb.DuckDBPyConnection,
    *,
    min_lots: int = 40,
    order_by: str = "single_bid",
    limit: int = 20,
) -> dict[str, Any]:
    """Per-buyer procurement competition quality (single-bidder rate at lot level) from
    ``v_procurement_competition``. ``order_by`` is 'single_bid' (rate) or 'lots' (volume);
    ``min_lots`` filters out noisy small samples. Carries the no-inference caveat."""
    res = proc.competition(conn, min_lots=min_lots, order_by=order_by, limit=limit)
    if not res.ok:
        return {"error": res.unavailable_reason}
    buyers = serialize.to_records(res.data)
    return {
        "summary": {
            "n_buyers": len(buyers),
            "min_lots": min_lots,
            "order_by": order_by if order_by in ("single_bid", "lots") else "single_bid",
        },
        "buyers": buyers,
        "caveat": _COMPETITION_CAVEAT,
    }


# ── Committees ────────────────────────────────────────────────────────────────


def list_committees(conn: duckdb.DuckDBPyConnection, *, chamber: str = "Dáil") -> list[dict[str, Any]]:
    """Per-committee rollup for a chamber (chair, member/party counts, party_seats_json)."""
    return serialize.to_records(cmte.member_detail(conn, chamber).data)


def get_committee(conn: duckdb.DuckDBPyConnection, chamber: str, committee: str) -> dict[str, Any] | None:
    """One committee's rollup + its long-format party-seat breakdown. None if unknown."""
    df = cmte.member_detail(conn, chamber).data
    if df.empty:
        return None
    match = df.loc[df["committee"] == committee]
    if match.empty:
        return None
    return {
        "detail": serialize.first_record(match),
        "party_seats": serialize.to_records(cmte.party_seats(conn, chamber, committee).data),
    }


# ── Interests (Register of Members' Interests) ────────────────────────────────


def build_member_interests(conn: duckdb.DuckDBPyConnection, name_or_code: str) -> dict[str, Any] | None:
    """A member's declared interests: per-year summary + every declaration across years.
    None if the name/code resolves to no member."""
    m = _resolve_member(conn, name_or_code)
    if m is None:
        return None
    return {
        "member": {"member_name": m["member_name"], "house": m["house"]},
        "by_year": serialize.to_records(intr.member_year_summary(conn, m["house"], m["member_name"]).data),
        "declarations": serialize.to_records(intr.td_interests(conn, m["house"], m["member_name"]).data),
    }


# ── Ministerial accountability ────────────────────────────────────────────────


def who_was_minister(conn: duckdb.DuckDBPyConnection, department_query: str, on_date: str) -> dict[str, Any]:
    """Who held a department on a given date (ISO 'YYYY-MM-DD'). ``department_query`` is a
    fuzzy label resolved against the department picker; returns the holder, a
    disambiguation list if several departments match, or the picker if none do."""
    depts = min_.departments(conn).data
    if depts.empty:
        return {"error": "ministerial data unavailable"}
    match = depts.loc[depts["department_label"].astype(str).str.contains(department_query, case=False, na=False)]
    if match.empty:
        match = depts.loc[depts["department_key"].astype(str).str.lower() == department_query.lower()]
    if match.empty:
        return {"error": f"no department matches '{department_query}'", "departments": serialize.to_records(depts)}
    if len(match) > 1:
        return {
            "disambiguation": serialize.to_records(match),
            "note": "multiple departments match — call again with a more specific name",
        }
    dept = match.iloc[0]
    minister = serialize.first_record(min_.minister_on_date(conn, str(dept["department_key"]), on_date).data)
    return {"department": str(dept["department_label"]), "on_date": on_date, "minister": minister}


# ── Member questions feed ─────────────────────────────────────────────────────


def build_member_questions(
    conn: duckdb.DuckDBPyConnection,
    name_or_code: str,
    *,
    year: int | None = None,
    qtype: str | None = None,
    ministry: str | None = None,
    topic: str | None = None,
    text: str | None = None,
    limit: int = 200,
) -> dict[str, Any] | None:
    """A member's parliamentary-question feed with optional filters (year, type, ministry,
    topic, free-text). Filters AND together. None if the name/code resolves to no member."""
    m = _resolve_member(conn, name_or_code)
    if m is None:
        return None
    df = moq.question_feed(
        conn, m["code"], year=year, qtype=qtype, ministry=ministry, topic=topic, search_text=text
    ).data
    rows = serialize.to_records(df.iloc[:limit]) if not df.empty else []
    return {
        "member": {"unique_member_code": m["code"], "member_name": m["member_name"], "house": m["house"]},
        "total_matched": int(len(df)),
        "returned": len(rows),
        "questions": rows,
    }


# ── Member speeches feed (floor contributions) ────────────────────────────────


def build_member_speeches(
    conn: duckdb.DuckDBPyConnection,
    name_or_code: str,
    *,
    year: int | None = None,
    contribution_type: str | None = None,
    business: str | None = None,
    irish_only: bool = False,
    text: str | None = None,
    limit: int = 200,
) -> dict[str, Any] | None:
    """A member's floor-contribution feed (speeches + oral questions) from the
    debate transcript record, with optional filters: year, contribution_type
    (speech/question/answer), item of business, Irish-only, and free-text search
    of the spoken words. Filters AND together. None if the name/code resolves to
    no member. Answers 'what did this TD/Senator say — and in Irish?'."""
    m = _resolve_member(conn, name_or_code)
    if m is None:
        return None
    df = moq.member_speeches(
        conn,
        m["code"],
        year=year,
        contribution_type=contribution_type,
        business=business,
        irish_only=irish_only,
        search=text,
    ).data
    rows = serialize.to_records(df.iloc[:limit]) if not df.empty else []
    return {
        "member": {"unique_member_code": m["code"], "member_name": m["member_name"], "house": m["house"]},
        "summary": serialize.first_record(moq.speech_summary(conn, m["code"]).data),
        "total_matched": int(len(df)),
        "returned": len(rows),
        "speeches": rows,
    }


# ── Payments by year ──────────────────────────────────────────────────────────


def list_payments_year_ranking(
    conn: duckdb.DuckDBPyConnection, *, year: int, house: str = "Dáil", skip: int = 0, limit: int = 20
) -> tuple[list[dict[str, Any]], int, bool]:
    """Travel & Accommodation Allowance ranking for ONE calendar year (the all-time
    ``list_payments_ranking`` can't answer 'who claimed most in <year>?')."""
    df = pay.year_ranking(conn, year, house).data
    if df.empty:
        return [], 0, False
    return _page(df, skip, limit)


# ── SIPO political finance (donations + GE2024 election expenses) ──────────────
# Two DISTINCT money grains — donations and election expenses are NEVER added
# together. Over-cap / under-threshold rows are the REAL disclosed figures (state
# them, never "correct" them). Some figures are OCR-derived (carry a verify flag).


def party_donations(conn: duckdb.DuckDBPyConnection, *, party: str | None = None) -> dict[str, Any]:
    """Political donations disclosed to SIPO. With ``party`` (an exact label from the
    ranking), that party's individual donor receipts; otherwise the per-party ranking +
    all-party summary. Donor names+amounts are the public SIPO record; no donor-address field."""
    if party:
        res = sipo.party_donors(conn, party)
        if not res.ok:
            return {"error": res.unavailable_reason}
        return {"party": party, "donations": serialize.to_records(res.data)}
    by_party = sipo.donations_by_party(conn)
    if not by_party.ok:
        return {"error": by_party.unavailable_reason}
    return {
        "summary": serialize.first_record(sipo.donations_totals(conn).data),
        "by_party": serialize.to_records(by_party.data),
        "note": "call again with a party label for its individual donor receipts",
    }


def party_election_spend(conn: duckdb.DuckDBPyConnection, *, party: str | None = None) -> dict[str, Any]:
    """GE2024 candidate election expenses disclosed to SIPO. With ``party``, that party's
    per-candidate expenditure; otherwise the per-party ranking + summary. A flag of
    'over_limit_verify' marks an OCR figure above the statutory limit to RE-CHECK — not a
    confirmed breach. Distinct money grain from donations — never sum the two."""
    if party:
        res = sipo.party_candidates(conn, party)
        if not res.ok:
            return {"error": res.unavailable_reason}
        return {"party": party, "candidates": serialize.to_records(res.data)}
    by_party = sipo.expenses_by_party(conn)
    if not by_party.ok:
        return {"error": by_party.unavailable_reason}
    return {
        "summary": serialize.first_record(sipo.expenses_totals(conn).data),
        "by_party": serialize.to_records(by_party.data),
        "note": "call again with a party label for its per-candidate breakdown",
    }


# ── Judiciary (the bench + court-system health) ────────────────────────────────
# Appointment / office / rank / assignment only — NO performance, conduct or ranking
# data exists by design. courts_health names NO judge (system-capacity signals only).


def judicial_appointments(conn: duckdb.DuckDBPyConnection, *, limit: int = 50) -> dict[str, Any]:
    """Judicial appointment events + the elevation ladder + the current sitting-bench roster."""
    res = jud.appointments(conn)
    if not res.ok:
        return {"error": res.unavailable_reason}
    appts = serialize.to_records(res.data)[:limit]
    return {
        "appointments": appts,
        "elevation_ladder": serialize.to_records(jud.elevation_ladder(conn).data),
        "roster": serialize.to_records(jud.roster(conn).data),
    }


def courts_health(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Court-SYSTEM health: annual case clearance, published waiting times, and the
    geocoded courthouse list. A long waiting time is a capacity signal, never a verdict."""
    res = jud.courts_clearance(conn)
    if not res.ok:
        return {"error": res.unavailable_reason}
    return {
        "clearance": serialize.to_records(res.data),
        "waiting_times": serialize.to_records(jud.courts_waiting_times(conn).data),
        "courthouses": serialize.to_records(jud.courthouses(conn).data),
    }


# ── Public appointments (state boards) ─────────────────────────────────────────


def list_public_appointments(
    conn: duckdb.DuckDBPyConnection, *, skip: int = 0, limit: int = 50
) -> tuple[list[dict[str, Any]], int, bool]:
    """Public-appointment notices (state-board and similar), one row per notice."""
    df = appt.public_appointments(conn).data
    if df.empty:
        return [], 0, False
    return _page(df, skip, limit)


# ── Charity finances ───────────────────────────────────────────────────────────
# Figures are AS FILED — some filers submit data-entry errors (implausible billions);
# never read one charity's row as a sector fact.


def charity_financials(conn: duckdb.DuckDBPyConnection, *, rcn: int | None = None) -> dict[str, Any]:
    """With ``rcn`` (Registered Charity Number), one charity's full multi-year income/
    expenditure/funding series; otherwise the register-wide totals per year."""
    if rcn:
        res = char.financials_by_year(conn, rcn)
        if not res.ok:
            return {"error": res.unavailable_reason}
        return {"rcn": rcn, "by_year": serialize.to_records(res.data)}
    totals = char.sector_totals_by_year(conn)
    if not totals.ok:
        return {"error": totals.unavailable_reason}
    return {
        "latest_year": serialize.first_record(char.latest_year(conn).data),
        "sector_totals_by_year": serialize.to_records(totals.data),
        "note": "call again with an rcn for one charity's full filed series",
    }


# ── Public-body payments (the realised-SPEND grain) ────────────────────────────
# ⚠️ NEVER add this spend to eTenders/TED AWARD ceilings — different value_kind.

_PUBPAY_CAVEAT = "sum-safe spend only; never add to procurement AWARD values (different grain)"


def public_body_payments(
    conn: duckdb.DuckDBPyConnection, *, side: str = "publisher", order_by: str = "value", limit: int = 25
) -> dict[str, Any]:
    """Public-body payments / POs over €20k (realised SPEND). ``side`` is 'publisher' (paying
    body) or 'supplier' (who was paid); ``order_by`` is 'value' (sum-safe €) or 'lines'."""
    res = (
        pubpay.supplier_summary(conn, order_by=order_by, limit=limit)
        if side == "supplier"
        else (pubpay.publisher_summary(conn, order_by=order_by, limit=limit))
    )
    if not res.ok:
        return {"error": res.unavailable_reason}
    return {
        "side": "supplier" if side == "supplier" else "publisher",
        "coverage": serialize.first_record(pubpay.coverage_stats(conn).data),
        "ranking": serialize.to_records(res.data),
        "caveat": _PUBPAY_CAVEAT,
    }


# ── Procurement — deeper cuts (authority / CPV / live tenders) ──────────────────
# Award CEILINGS, not realised spend — use public_body_payments for what was paid.


def list_procurement_authorities(
    conn: duckdb.DuckDBPyConnection, *, skip: int = 0, limit: int = 25
) -> tuple[list[dict[str, Any]], int, bool]:
    """eTenders AWARD activity by contracting authority (buyer): counts + sum-safe value."""
    df = proc.authority_summary(conn, limit=None).data
    if df.empty:
        return [], 0, False
    return _page(df, skip, limit)


def list_procurement_cpv(
    conn: duckdb.DuckDBPyConnection, *, skip: int = 0, limit: int = 25
) -> tuple[list[dict[str, Any]], int, bool]:
    """eTenders AWARD activity by CPV code (WHAT was bought): counts + sum-safe value."""
    df = proc.cpv_summary(conn, limit=None).data
    if df.empty:
        return [], 0, False
    return _page(df, skip, limit)


def list_open_tenders(
    conn: duckdb.DuckDBPyConnection, *, only_open: bool = True, skip: int = 0, limit: int = 40
) -> tuple[list[dict[str, Any]], int, bool]:
    """Current TED (EU Official Journal) Irish tender opportunities — the forward-looking
    pipeline. ``only_open`` keeps notices whose submission deadline has not passed.
    estimated_value is a buyer estimate, never summed with award/payment figures."""
    df = proc.ted_tenders(conn, limit=None, only_open=only_open).data
    if df.empty:
        return [], 0, False
    return _page(df, skip, limit)


# ── Ministerial roll-up (current cabinet) ──────────────────────────────────────


def current_cabinet(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """The current ministerial line-up (who holds which department now) + the department list.
    For a PAST date use who_was_minister."""
    res = min_.current_ministers(conn)
    if not res.ok:
        return {"error": res.unavailable_reason}
    return {
        "current_ministers": serialize.to_records(res.data),
        "departments": serialize.to_records(min_.departments(conn).data),
    }


# ── Lobbying — revolving-door individual (DPO) profile ─────────────────────────

_DPO_CAVEAT = "Co-occurrence on the public lobbying register only — NOT evidence of improper influence."


def dpo_lobbying_profile(conn: duckdb.DuckDBPyConnection, individual_name: str) -> dict[str, Any] | None:
    """One designated public official's revolving-door footprint: the firms they lobby for,
    their client breakdown, and which politicians/bodies they targeted. None if the name
    matches no DPO on the register."""
    summary_res = lb.dpo_one(conn, individual_name)
    if not summary_res.ok:
        return {"error": summary_res.unavailable_reason}
    summary = serialize.first_record(summary_res.data)
    if summary is None:
        return None
    return {
        "individual": individual_name,
        "summary": summary,
        "firms": serialize.to_records(lb.dpo_firms(conn, individual_name).data),
        "client_breakdown": serialize.to_records(lb.dpo_client_breakdown(conn, individual_name).data),
        "politicians_targeted": serialize.to_records(lb.dpo_politicians_targeted(conn, individual_name).data),
        "caveat": _DPO_CAVEAT,
    }


# ── Corpus search: divisions by topic ──────────────────────────────────────────


def search_votes_by_topic(conn: duckdb.DuckDBPyConnection, topics: str, *, house: str = "Dáil") -> dict[str, Any]:
    """How members voted on DEBATES matching topic keywords (comma-separated, OR-combined
    substring match on the debate title). Returns a distinct-debate overview + the per-member
    votes behind them (capped at 2000, newest first)."""
    kws = [t.strip() for t in topics.split(",") if t.strip()]
    if not kws:
        return {"error": "pass one or more comma-separated topic keywords"}
    patterns = tuple(f"%{k}%" for k in kws)
    res = vot.topical_votes(conn, patterns, house)
    if not res.ok:
        return {"error": res.unavailable_reason}
    rows = serialize.to_records(res.data)
    debates: dict[tuple, dict[str, Any]] = {}
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


# ── Data coverage (scope guard) ────────────────────────────────────────────────


def data_coverage(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """What the tracker covers and how far back — the scope guard to consult before answering
    a time- or completeness-sensitive question. Per-domain year ranges + corpus sizes + the
    hard money-grain rules."""
    return {
        "procurement_awards": serialize.first_record(proc.coverage_stats(conn).data),
        "ted_awards": serialize.first_record(proc.ted_corpus_stats(conn).data),
        "public_body_payments": serialize.first_record(pubpay.coverage_stats(conn).data),
        "sipo_donations": serialize.first_record(sipo.donations_totals(conn).data),
        "sipo_election_expenses": serialize.first_record(sipo.expenses_totals(conn).data),
        "charities_latest_year": serialize.first_record(char.latest_year(conn).data),
        "caveats": {
            "register_of_interests": "Register of Members' Interests covers 2020–2025 only — older divisions match no interests",
            "ted_award_winners": "TED award WINNERS are 2024+ (pre-2024 notices carry buyer + CPV + total value but no winner)",
            "money_grains": "procurement AWARDS, public-body PAYMENTS, and T&A allowances are three different value grains — NEVER sum across them",
        },
    }
