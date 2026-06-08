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

from typing import Any

import duckdb
import pandas as pd

from dail_tracker_core import serialize
from dail_tracker_core.queries import committees as cmte
from dail_tracker_core.queries import cross_ref as xref
from dail_tracker_core.queries import interests as intr
from dail_tracker_core.queries import legislation as leg
from dail_tracker_core.queries import lobbying as lb
from dail_tracker_core.queries import member_overview as moq
from dail_tracker_core.queries import ministerial as min_
from dail_tracker_core.queries import payments as pay
from dail_tracker_core.queries import procurement as proc
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
    for norm, grp in df.groupby("supplier_norm"):
        first = grp.iloc[0]  # award_rows / authorities / value are per-supplier constants
        entities = grp[["lobby_name", "lobby_side", "n_lobby_returns"]].sort_values("n_lobby_returns", ascending=False)
        suppliers.append(
            {
                "supplier": serialize.value(first["supplier"]),
                "supplier_norm": serialize.value(norm),
                "award_rows": int(first["n_award_rows"]),
                "authorities": int(first["n_authorities"]),
                "awarded_value_safe_eur": float(first["awarded_value_safe_eur"]),
                "lobby_returns_total": int(grp["n_lobby_returns"].sum()),
                "lobby_entities": serialize.to_records(entities),
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
