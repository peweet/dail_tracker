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

from collections.abc import Callable
from typing import Any, cast

import duckdb
import pandas as pd

from dail_tracker_core import caveats, serialize
from dail_tracker_core.queries import appointments as appt
from dail_tracker_core.queries import attendance as att
from dail_tracker_core.queries import charities as char
from dail_tracker_core.queries import committees as cmte
from dail_tracker_core.queries import constituency as cons
from dail_tracker_core.queries import corporate as corp
from dail_tracker_core.queries import cross_ref as xref
from dail_tracker_core.queries import entity as ent
from dail_tracker_core.queries import housing as hou
from dail_tracker_core.queries import interests as intr
from dail_tracker_core.queries import judiciary as jud
from dail_tracker_core.queries import legislation as leg
from dail_tracker_core.queries import lobbying as lb
from dail_tracker_core.queries import local_government as lg
from dail_tracker_core.queries import member_overview as moq
from dail_tracker_core.queries import ministerial as min_
from dail_tracker_core.queries import ministerial_diary as mdiary
from dail_tracker_core.queries import payments as pay
from dail_tracker_core.queries import procurement as proc
from dail_tracker_core.queries import public_payments as pubpay
from dail_tracker_core.queries import publicfinance as pubfin
from dail_tracker_core.queries import sipo
from dail_tracker_core.queries import votes as vot
from dail_tracker_core.queries import your_councillors as yc
from dail_tracker_core.results import QueryResult


def _section(res: QueryResult, name: str, sink: list[dict[str, str]]) -> pd.DataFrame:
    """A dossier SECTION read: degrade softly on outage, but record it.

    Returns the DataFrame (empty when unavailable) and, when the source was down,
    appends ``{"section": name, "reason": …}`` to ``sink`` so the response can
    carry an explicit ``unavailable_sections`` marker instead of silently
    rendering the section as empty. Gates use ``QueryResult.require()`` instead.
    """
    if not res.ok:
        sink.append({"section": name, "reason": res.unavailable_reason or "source unavailable"})
    return res.data


def _identity(conn: duckdb.DuckDBPyConnection, code: str) -> dict[str, Any] | None:
    # Gate reads: an outage here must raise, not render as "member not found".
    df = moq.identity_attendance(conn, code).require()
    if not df.empty:
        return df.iloc[0].to_dict()
    df = moq.identity_registry(conn, code).require()
    return df.iloc[0].to_dict() if not df.empty else None


def list_members(
    conn: duckdb.DuckDBPyConnection,
    *,
    house: str | None = None,
    party: str | None = None,
    constituency: str | None = None,
    fuzzy_name: str | None = None,
    dail: str | None = None,
    skip: int = 0,
    limit: int = 50,
) -> tuple[list[dict[str, Any]], int, bool]:
    """(page_records, total, truncated) over the member registry.

    Roster selection (exact house/party/constituency, substring name) on a ~176-row
    frame — selection, not a metric, so it stays here rather than spawning a view.
    ``dail`` keeps only members who served in that Dáil/Seanad term (e.g. '33'): the
    term split happens in SQL (moq.member_codes_for_dail) and the filter here is a
    plain isin() on the returned codes — the browse page's exact pattern.
    """
    df = moq.member_list(conn).require()
    if df.empty:
        return [], 0, False
    # .loc[mask] (not df[mask]) keeps the static type a DataFrame for the pandas
    # stubs, so .iloc / .str below type-check without casts.
    if dail:
        codes_df = moq.member_codes_for_dail(conn, dail).require()
        codes = set(codes_df["unique_member_code"].tolist()) if not codes_df.empty else set()
        df = df.loc[df["unique_member_code"].isin(codes)]
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

    unavail: list[dict[str, str]] = []
    house_df = _section(moq.member_house(conn, code), "house", unavail)
    house = str(house_df.iloc[0]["house"]) if not house_df.empty else "Dáil"
    is_minister = str(ident.get("is_minister", "")).lower() == "true"
    constituency = serialize.value(ident.get("constituency"))

    att = _section(moq.att_all_years(conn, code), "attendance_by_year", unavail)
    latest_year = int(att.iloc[0]["year"]) if not att.empty else None
    days_latest = int(att.iloc[0]["attended_count"]) if not att.empty else None

    vs = _section(moq.votes_summary(conn, code), "votes", unavail)
    if not vs.empty:
        r = vs.iloc[0]
        votes_cast = (
            int(r.get("yes_count", 0) or 0) + int(r.get("no_count", 0) or 0) + int(r.get("abstained_count", 0) or 0)
        )
        divisions = int(r.get("division_count", 0) or 0)
    else:
        votes_cast = divisions = 0

    pg = _section(moq.pay_grand_total(conn, code), "payments_total", unavail)
    pay_total = float(pg.iloc[0]["total"]) if (not pg.empty and pd.notna(pg.iloc[0]["total"])) else 0.0

    constituency_context = None
    if house != "Seanad" and constituency:
        constituency_context = serialize.first_record(
            _section(moq.constituency_context(conn, str(constituency)), "constituency_context", unavail)
        )

    dossier = {
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
            # v_td_vote_summary only has rows for votes cast, so this is the count of
            # divisions the member PARTICIPATED in — a career divisions-held
            # denominator is not derivable (named-division gold starts 2025).
            "divisions_participated": divisions,
            "payments_total_eur": pay_total,
        },
        "attendance_by_year": serialize.to_records(att),
        "payments_by_year": serialize.to_records(_section(moq.pay_overview(conn, code), "payments_by_year", unavail)),
        "legislation_sponsored": serialize.to_records(
            _section(moq.legislation(conn, code), "legislation_sponsored", unavail)
        ),
        "ministerial_roles": serialize.to_records(_section(moq.ministerial_roles(conn, code), "ministerial_roles", unavail)),
        "statutory_instruments_signed": serialize.to_records(
            _section(moq.si_signed(conn, code), "statutory_instruments_signed", unavail)
        ),
        "revolving_door": serialize.to_records(_section(moq.lobbying_rd(conn, code), "revolving_door", unavail)),
        "questions_profile": serialize.first_record(_section(moq.question_profile(conn, code), "questions_profile", unavail)),
        "speeches_profile": serialize.first_record(_section(moq.speech_summary(conn, code), "speeches_profile", unavail)),
        "external_links": serialize.first_record(_section(moq.external_links(conn, code), "external_links", unavail)) or {},
        "constituency_context": constituency_context,
    }
    if unavail:
        dossier["unavailable_sections"] = unavail
    return dossier


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
    df = leg.index_filtered(conn, start_date, end_date, status, title_search).require()
    if df.empty:
        return [], 0, False
    return _page(df, skip, limit)


def build_bill_dossier(conn: duckdb.DuckDBPyConnection, bill_id: str) -> dict[str, Any] | None:
    """Composed bill record: detail + timeline + amendments + sources + PDFs +
    debates + the statutory instruments made under it. None if the id is unknown."""
    detail = leg.bill_detail(conn, bill_id).require()
    if detail.empty:
        return None
    unavail: list[dict[str, str]] = []
    dossier = {
        "bill": serialize.first_record(detail),
        "timeline": serialize.to_records(_section(leg.bill_timeline(conn, bill_id), "timeline", unavail)),
        "amendment_intensity": serialize.first_record(
            _section(leg.amendment_intensity_for_bill(conn, bill_id), "amendment_intensity", unavail)
        ),
        "sources": serialize.first_record(_section(leg.bill_sources(conn, bill_id), "sources", unavail)),
        "pdfs": serialize.to_records(_section(leg.bill_pdfs(conn, bill_id), "pdfs", unavail)),
        "debates": serialize.to_records(_section(leg.bill_debates(conn, bill_id), "debates", unavail)),
        "si_composition": serialize.to_records(_section(leg.si_composition(conn, bill_id), "si_composition", unavail)),
        "statutory_instruments": serialize.to_records(_section(leg.si_by_bill(conn, bill_id), "statutory_instruments", unavail)),
    }
    if unavail:
        dossier["unavailable_sections"] = unavail
    return dossier


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
    df = leg.si_entity_index(conn).require()
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
    df = vot.vote_index(conn, date_from, date_to, outcome, house).require()
    if df.empty:
        return [], 0, False
    return _page(df, skip, limit)


def build_division_dossier(conn: duckdb.DuckDBPyConnection, vote_id: str) -> dict[str, Any] | None:
    """Composed division record: the vote + party breakdown + every member's vote + sources."""
    one = vot.vote_by_id(conn, vote_id).require()
    if one.empty:
        return None
    unavail: list[dict[str, str]] = []
    dossier = {
        "division": serialize.first_record(one),
        "party_breakdown": serialize.to_records(_section(vot.party_breakdown(conn, vote_id), "party_breakdown", unavail)),
        "members": serialize.to_records(_section(vot.division_members(conn, vote_id), "members", unavail)),
        "sources": serialize.first_record(_section(vot.sources(conn, vote_id), "sources", unavail)),
    }
    if unavail:
        dossier["unavailable_sections"] = unavail
    return dossier


# ── Cross-reference: votes × Register of Members' Interests ────────────────────

# Coverage caveat every cross-reference response carries, so an AI consumer states
# it rather than implying full historical coverage. (Canonical text in core caveats.)
_INTERESTS_CAVEAT = caveats.INTERESTS


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
    return {"summary": summary, "awards": awards, "caveat": caveats.PROCUREMENT_AWARDS}


def build_organisation_dossier(conn: duckdb.DuckDBPyConnection, supplier_norm: str) -> dict[str, Any] | None:
    """Organisation-360 cross-register summary for one company (by canonical ``supplier_norm``):
    CRO identity + its presence and counts across procurement, lobbying, corporate notices,
    charity and EPA — fused on the canonical name key (v_supplier_entity_xref). ``None`` if the
    supplier_norm is unknown to the spine. Co-occurrence by ENTITY only, never causation; counts
    are floors (exact-name / CRO matching undercounts); individuals are excluded upstream."""
    df = ent.xref_summary(conn, supplier_norm).data
    if df.empty:
        return None
    r = df.iloc[0]
    awarded = r.get("awarded_value_safe_eur")
    return {
        "identity": {
            "supplier_norm": serialize.value(r.get("supplier_norm")),
            "display_name": serialize.value(r.get("display_name")),
            "cro_company_num": serialize.value(r.get("company_num")),
            "has_cro": bool(r.get("has_cro")),
        },
        "procurement": {
            "award_rows": int(r.get("procurement_award_rows") or 0),
            "awarded_value_safe_eur": float(awarded) if pd.notna(awarded) else 0.0,
        },
        "cross_register": {
            "on_lobbying_register": bool(r.get("on_lobbying_register")),
            "lobby_returns": int(r.get("lobby_returns") or 0),
            "has_corporate_notice": bool(r.get("has_corporate_notice")),
            "corporate_notices": int(r.get("corporate_notices") or 0),
            "is_charity": bool(r.get("is_charity")),
            "has_epa_licence": bool(r.get("has_epa_licence")),
            "register_count": int(r.get("cross_register_count") or 0),
        },
        "caveat": caveats.ENTITY_COOCCURRENCE,
    }


# Co-occurrence caveat — mirrors data/_meta/procurement_lobbying_overlap_coverage.json.
# Rides on every overlap response so an AI consumer can't present it as causation.
_PROC_LOBBY_CAVEAT = caveats.PROC_LOBBY

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


_COMPETITION_CAVEAT = caveats.COMPETITION


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


# ── SIPO per-candidate GE2024 election expenses (the granular OCR tier) ────────
# Each individual candidate's Election Expenses Statement down to the Part-5 line
# items. Same money grain as party_election_spend's candidate tier — still NEVER
# summed with donations. No donor data exists on these views (they are EXPENSES);
# every response carries caveats.SIPO_CANDIDATE (OCR verify / incremental / detail
# is not a vendor list).

_SIPO_CANDIDATE_CAVEAT = caveats.SIPO_CANDIDATE


def candidate_election_spend(conn: duckdb.DuckDBPyConnection, *, limit: int = 100) -> dict[str, Any]:
    """GE2024 per-candidate election-expenses league: the headline totals across all
    candidates loaded so far, candidates ranked by total spend, and the candidates who
    filed a statement with no trustworthy total (searchable, NO amount by design —
    showing a corrupt OCR magnitude would be a fabricated number)."""
    ranked = sipo.candidate_ranked(conn, limit)
    if not ranked.ok:
        return {"error": ranked.unavailable_reason}
    return {
        "summary": serialize.first_record(sipo.candidate_totals(conn).data),
        "candidates": serialize.to_records(ranked.data),
        "filed_unquantified": serialize.to_records(sipo.candidate_filed_unquantified(conn).data),
        "caveat": _SIPO_CANDIDATE_CAVEAT,
    }


def candidate_election_detail(conn: duckdb.DuckDBPyConnection, candidate: str) -> dict[str, Any] | None:
    """One candidate's GE2024 expenses statement (headline + per-category grid + verify
    flag + the official SIPO PDF link) with the Part-5 line items behind it. None when no
    loaded statement matches the exact candidate name (OCR is incremental — absence means
    not-yet-extracted, never a nil return)."""
    one = sipo.candidate_one(conn, candidate)
    if not one.ok:
        return {"error": one.unavailable_reason}
    statement = serialize.first_record(one.data)
    if statement is None:
        return None
    return {
        "candidate": candidate,
        "statement": statement,
        "line_items": serialize.to_records(sipo.candidate_line_items(conn, candidate).data),
        "caveat": _SIPO_CANDIDATE_CAVEAT,
    }


def candidate_election_breakdown(conn: duckdb.DuckDBPyConnection, *, top: int = 25) -> dict[str, Any]:
    """Where GE2024 candidate money went, across all loaded candidates: the 8 statutory
    categories (5A–5H), the per-party rollup, and the top spend-detail lines (a MIX of
    suppliers and item descriptions — never a clean vendor list)."""
    cats = sipo.candidate_by_category(conn)
    if not cats.ok:
        return {"error": cats.unavailable_reason}
    return {
        "by_category": serialize.to_records(cats.data),
        "by_party": serialize.to_records(sipo.candidate_by_party(conn).data),
        "top_details": serialize.to_records(sipo.candidate_top_details(conn, top).data),
        "caveat": _SIPO_CANDIDATE_CAVEAT,
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

_PUBPAY_CAVEAT = caveats.PUBPAY


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

_DPO_CAVEAT = caveats.DPO


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
    # Unavailable raises (→ 503 via the interface's handler); any {"error"} this
    # function RETURNS is therefore always a client error (→ 400).
    rows = serialize.to_records(vot.topical_votes(conn, patterns, house).require())
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


# ── Corporate notices (Iris Oifigiúil — the State gazette) ─────────────────────
# CORPORATE ONLY: personal/individual insolvency is excluded upstream by policy, so
# no person is named. A wind-up / receivership notice is a FACT about a company's
# legal status on a date — never a verdict on a director or a finding of wrongdoing.

_CORP_NOTICE_CAVEAT = caveats.CORP_NOTICE
_CORP_REPEAT_CAVEAT = caveats.CORP_REPEAT
_CORP_RECEIVER_CAVEAT = caveats.CORP_RECEIVER


def corporate_distress_notices(
    conn: duckdb.DuckDBPyConnection, *, query: str = "", subtype: str = "", year: int = 0, limit: int = 50
) -> dict[str, Any]:
    """Corporate distress / register notices from Iris Oifigiúil — receiverships, court & voluntary
    wind-ups, examinerships, SCARP rescues, investment-vehicle register notices. Filters AND together:
    ``query`` (entity-name substring), ``subtype`` (e.g. 'receivership', 'examinership'), ``year``
    (issue year; 0 = all). Newest first; the bulky raw_text/title scratch columns are dropped."""
    res = corp.corporate_notices(conn)
    if not res.ok:
        return {"error": res.unavailable_reason}
    rows = serialize.to_records(res.data, drop_cols=["raw_text", "title"])
    q = query.strip().lower()
    st = subtype.strip().lower()
    out: list[dict[str, Any]] = []
    for r in rows:
        if q and q not in str(r.get("entity_name", "")).lower():
            continue
        if st and st != str(r.get("notice_subtype", "")).lower():
            continue
        if year and not str(r.get("issue_date", "")).startswith(str(year)):
            continue
        out.append(r)
        if len(out) >= limit:
            break
    return {"count": len(out), "notices": out, "caveat": _CORP_NOTICE_CAVEAT}


def corporate_repeat_distress(conn: duckdb.DuckDBPyConnection, *, limit: int = 50) -> dict[str, Any]:
    """CBI-authorised firms appearing in REPEAT corporate-distress notices — regulated entities with
    recurring receivership/wind-up/examinership events. Each row carries the per-subtype counts and the
    distress-vs-routine split. EXPERIMENTAL (sandbox CBI source)."""
    res = corp.cbi_repeat_distress(conn)
    if not res.ok:
        return {"error": res.unavailable_reason}
    return {"firms": serialize.to_records(res.data)[:limit], "caveat": _CORP_REPEAT_CAVEAT}


def corporate_receivers(conn: duckdb.DuckDBPyConnection, *, limit: int = 25) -> dict[str, Any]:
    """The receivership lens over the corporate-notices corpus (precomputed gold): the funds/banks that
    appoint receivers most, the professional firms named AS receiver, the appointer type-mix, the
    headline scalar counts and the notices-by-year sparkline series. ``limit`` caps each ranking."""
    summary = corp.receiver_summary(conn)
    if not summary.ok:
        return {"error": summary.unavailable_reason}
    return {
        "summary": serialize.first_record(summary.data),
        "appointers": serialize.to_records(corp.receiver_appointers(conn).data)[:limit],
        "firms": serialize.to_records(corp.receiver_firms(conn).data)[:limit],
        "appointer_type_mix": serialize.to_records(corp.receiver_bucket_mix(conn).data),
        "notices_by_year": serialize.to_records(corp.receiver_year_counts(conn).data),
        "caveat": _CORP_RECEIVER_CAVEAT,
    }


def corporate_firm_notices(
    conn: duckdb.DuckDBPyConnection, firm: str, *, limit: int = 50
) -> dict[str, Any] | None:
    """Every notice naming ONE receiver / insolvency firm. Curated firms match on the
    precomputed receiver_firms tag column; a free-text firm falls back to a word-bounded
    regexp over the notice text (both run in DuckDB — see corp.firm_notices). Matching is
    notice PRESENCE, not a confirmed appointment. None when no notice names the firm.
    The bulky raw_text/title scratch columns are dropped, as on /corporate/notices."""
    res = corp.firm_notices(conn, firm)
    if not res.ok:
        return {"error": res.unavailable_reason}
    rows = serialize.to_records(res.data, drop_cols=["raw_text", "title"])[:limit]
    if not rows:
        return None
    return {
        "firm": firm,
        "count": len(rows),
        "notices": rows,
        "match_note": caveats.CORP_FIRM_MATCH,
        "caveat": _CORP_NOTICE_CAVEAT,
    }


def corporate_firm_fund_counts(conn: duckdb.DuckDBPyConnection, firm: str) -> dict[str, Any]:
    """The fund↔firm connection for one curated firm: appointing parent funds/banks co-named
    on the firm's notices — n_recv (receivership-shaped notices only) and n_all (every
    notice). Precomputed in v_corporate_firm_fund_counts; free-text firms are absent by
    construction, so an empty list means the firm is not in the curated tag set."""
    res = corp.firm_fund_counts(conn, firm)
    if not res.ok:
        return {"error": res.unavailable_reason}
    return {
        "firm": firm,
        "funds": serialize.to_records(res.data),
        "note": "curated firms only — free-text firms are absent by construction",
        "match_note": caveats.CORP_FIRM_MATCH,
        "caveat": _CORP_RECEIVER_CAVEAT,
    }


def corporate_brand_aliases(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """The curated brand → parent-fund → fund-type alias map behind the receiver-appointer
    tags, rolled up to one row per (parent_fund, fund_type) with the brands joined — the
    methodology-expander table, precomputed in v_corporate_brand_alias_groups."""
    res = corp.brand_alias_groups(conn)
    if not res.ok:
        return {"error": res.unavailable_reason}
    return {"groups": serialize.to_records(res.data), "caveat": _CORP_RECEIVER_CAVEAT}


def corporate_isif_portfolio(conn: duckdb.DuckDBPyConnection, *, limit: int | None = None) -> dict[str, Any]:
    """ISIF (Ireland Strategic Investment Fund) sovereign-fund investment commitments — the
    State putting money INTO companies, one row per investee, newest first. Carries the
    not-summable caveat (mixed currencies, 'up to' ceilings)."""
    res = corp.isif_portfolio(conn, limit=limit)
    if not res.ok:
        return {"error": res.unavailable_reason}
    return {"commitments": serialize.to_records(res.data), "caveat": caveats.CORP_ISIF}


# ── Ministerial diaries (who ministers meet) ───────────────────────────────────
# Co-occurrence ACCESS record, never proof of influence: diaries are self-curated,
# non-exhaustive and quarterly-in-arrears, and a diary meeting is not a lobbying return.

_DIARY_CAVEAT = caveats.DIARY


def ministerial_diary_top_organisations(
    conn: duckdb.DuckDBPyConnection, *, limit: int = 25, outside_only: bool = True
) -> dict[str, Any]:
    """Organisations ranked by how many meetings ministers logged with them in their published diaries.
    Each row carries meetings, ministers_met, ministers_lobbied_and_met, total_lobbying_returns and
    ``corroborated`` (the org both MET and filed a lobbying return naming the same minister).
    ``outside_only`` drops state/semi-state bodies."""
    res = mdiary.org_overlap_ranked(conn, limit=limit, outside_only=outside_only)
    if not res.ok:
        return {"error": res.unavailable_reason}
    return {"organisations": serialize.to_records(res.data), "caveat": _DIARY_CAVEAT}


def ministerial_diary_organisation(conn: duckdb.DuckDBPyConnection, name: str) -> dict[str, Any] | None:
    """For ONE organisation (fuzzy name), the ministerial-access record: a summary (meetings, distinct
    ministers met, corroboration vs the lobbying register) plus the individual logged meetings (which
    minister, date, subject, source link). None if no logged meeting names the organisation."""
    summary_res = mdiary.organisation_summary(conn, name)
    if not summary_res.ok:
        return {"error": summary_res.unavailable_reason}
    summary = serialize.first_record(summary_res.data)
    if summary is None:
        return None
    return {
        "organisation": name,
        "summary": summary,
        "meetings": serialize.to_records(mdiary.organisation_meetings(conn, name).data),
        "caveat": _DIARY_CAVEAT,
    }


def ministerial_diary_meetings(
    conn: duckdb.DuckDBPyConnection, *, minister: str = "", topic: str = "", limit: int = 30
) -> dict[str, Any]:
    """Search every external meeting ministers logged in their published diaries, by minister surname
    and/or a subject keyword. Returns minister, department, date, the as-published subject and the
    source link."""
    res = mdiary.meeting_search(conn, minister=minister, topic=topic, limit=limit)
    if not res.ok:
        return {"error": res.unavailable_reason}
    return {"meetings": serialize.to_records(res.data), "caveat": _DIARY_CAVEAT}


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
            "register_of_interests": "Register of Members' Interests covers 1995–2025 (Dáil every year; Seanad missing 1996/1999/2004) — only pre-1995 divisions match no interests",
            "ted_award_winners": "TED award WINNERS are 2024+ (pre-2024 notices carry buyer + CPV + total value but no winner)",
            "money_grains": caveats.MONEY_GRAINS,
        },
    }


# ── Attendance (participation, absences, TAA compliance) ───────────────────────
# Turnout / absence model: office-holders are FLAGGED, not hidden — a low rate is
# context, not a verdict. Every response carries caveats.ATTENDANCE. Year-scoped
# lists default to the latest reporting year (echoed in head.year).


def _latest_attendance_year(conn: duckdb.DuckDBPyConnection, house: str) -> int | None:
    df = att.participation_years(conn, house).data
    return int(df.iloc[0]["year"]) if not df.empty else None


def _attendance_year_list(
    conn: duckdb.DuckDBPyConnection,
    fetch: Callable[[duckdb.DuckDBPyConnection, int, str], QueryResult],
    *,
    year: int | None,
    house: str,
    skip: int,
    limit: int,
) -> dict[str, Any]:
    y = year if year is not None else _latest_attendance_year(conn, house)
    df = fetch(conn, y, house).data if y is not None else pd.DataFrame()
    records, total, truncated = _page(df, skip, limit)
    return serialize.envelope(
        records,
        limit=limit,
        offset=skip,
        total=total,
        truncated=truncated,
        meta={"year": y, "house": house},
        caveat=caveats.ATTENDANCE,
    )


def attendance_turnout(
    conn: duckdb.DuckDBPyConnection, *, year: int | None = None, house: str = "Dáil", skip: int = 0, limit: int = 50
) -> dict[str, Any]:
    """Division turnout for a (year, house), worst-first — voted_in / missed / turnout_pct
    with each member's role flags. ``year`` defaults to the latest reporting year."""
    return _attendance_year_list(conn, att.participation_turnout, year=year, house=house, skip=skip, limit=limit)


def attendance_absences(
    conn: duckdb.DuckDBPyConnection, *, year: int | None = None, house: str = "Dáil", skip: int = 0, limit: int = 50
) -> dict[str, Any]:
    """Longest physical-absence runs for a (year, house), worst-first, with the sourced
    explanation where one exists. Excludes the chair (not voting is their role)."""
    return _attendance_year_list(conn, att.participation_absences, year=year, house=house, skip=skip, limit=limit)


def attendance_taa_compliance(
    conn: duckdb.DuckDBPyConnection, *, year: int | None = None, house: str = "Dáil", skip: int = 0, limit: int = 50
) -> dict[str, Any]:
    """Members below the statutory 120-day Travel & Accommodation Allowance threshold + the
    allowance deduction, most-docked first. Excludes office-holders (not paid TAA on this basis)."""
    return _attendance_year_list(conn, att.taa_compliance, year=year, house=house, skip=skip, limit=limit)


def attendance_missing_members(conn: duckdb.DuckDBPyConnection, *, skip: int = 0, limit: int = 100) -> dict[str, Any]:
    """Roster members with no row in the attendance record, split by ``missing_reason``
    (office_holder — documented TAA gap — vs no_record_on_file)."""
    records, total, truncated = _page(att.missing_members(conn).data, skip, limit)
    return serialize.envelope(
        records, limit=limit, offset=skip, total=total, truncated=truncated, caveat=caveats.ATTENDANCE
    )


def attendance_years(conn: duckdb.DuckDBPyConnection, *, house: str = "Dáil") -> dict[str, Any]:
    """The reporting years available for a house (newest first) — to drive the year filter."""
    df = att.participation_years(conn, house).data
    return {"house": house, "years": [int(y) for y in df["year"].tolist()] if not df.empty else []}


# ── Housing (national social-housing demand + supply) ──────────────────────────


def housing_waiting_list(
    conn: duckdb.DuckDBPyConnection, *, grain: str = "county", skip: int = 0, limit: int = 50
) -> dict[str, Any]:
    """Social-housing waiting-list league table at one grain ('county' | 'la' | 'national'):
    waiting total, YoY, long-wait %, population and waiters-per-1,000, largest first."""
    records, total, truncated = _page(hou.waiting_list_totals(conn, grain).data, skip, limit)
    return serialize.envelope(
        records, limit=limit, offset=skip, total=total, truncated=truncated, meta={"grain": grain}
    )


def housing_supply(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """National supply & affordability headline (vacancy, private rent, HAP) + the HAP profile
    + the new-dwelling completions trend — the supply-side companion to the waiting list."""
    return {
        "supply": serialize.first_record(hou.supply_national(conn).data),
        "hap": serialize.first_record(hou.hap_national(conn).data),
        "completions": serialize.to_records(hou.completions_trend(conn).data),
    }


def housing_accommodation_spend(conn: duckdb.DuckDBPyConnection, *, limit: int = 40) -> dict[str, Any]:
    """State asylum (international-protection) + Ukraine accommodation spend by year and by
    provider, from the published over-€20k purchase-order registers. Carries the spend-grain caveat."""
    return {
        "by_year": serialize.to_records(hou.accommodation_spend_by_year(conn).data),
        "providers": serialize.to_records(hou.accommodation_spend_providers(conn, limit=limit).data),
        "caveat": caveats.ACCOMMODATION_SPEND,
    }


# ── Public finance (CSO general-government series) ─────────────────────────────


def government_finance(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """National general-government revenue / expenditure / balance per year (CSO GFA01) — the
    authoritative 'share of total public spend' denominator. Carries the national-accounts caveat."""
    records = serialize.to_records(pubfin.gov_finance_annual(conn).data)
    return serialize.envelope(records, total=len(records), caveat=caveats.GOV_FINANCE)


# ── Local government (council accountability) ──────────────────────────────────
# Each council figure is its OWN reported amount beside the national benchmark — never
# apportioned, never summed across measures (caveats.COUNCIL_MONEY / NOAC_SCORECARD).


def list_councils(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """The 31-council index: each Chief Executive + the choropleth map layers + the
    one-row national accountability headline."""
    return {
        "national_summary": serialize.first_record(lg.national_summary(conn).data),
        "councils": serialize.to_records(lg.chief_executives(conn).data),
        "map_layers": serialize.to_records(lg.map_layers(conn).data),
    }


def build_council_dossier(conn: duckdb.DuckDBPyConnection, la: str) -> dict[str, Any] | None:
    """One council's accountability dossier: Chief Executive, the NOAC scorecard (+ 2022–24
    history), the co-located cash signals, collection rates, planning-overturn rate, derelict-
    sites levy, social-housing performance and the over-€20k procurement scale. None if unknown."""
    ce = lg.chief_executive(conn, la).data
    if ce.empty:
        return None
    return {
        "local_authority": la,
        "chief_executive": serialize.first_record(ce),
        "noac_scorecard": serialize.to_records(lg.noac_scorecard(conn, la).data),
        "noac_scorecard_history": serialize.to_records(lg.noac_scorecard_history(conn, la).data),
        "cash_signals": serialize.first_record(lg.cash_signals(conn, la).data),
        "collection_rates": serialize.first_record(lg.collection_rates(conn, la).data),
        "planning_overturn": serialize.first_record(lg.planning_overturn(conn, la).data),
        "derelict_sites_levy": serialize.first_record(lg.derelict_sites_levy(conn, la).data),
        "housing_performance": serialize.first_record(lg.housing_performance(conn, la).data),
        "council_money": serialize.first_record(lg.council_money(conn, la).data),
        "caveat": caveats.COUNCIL_MONEY,
    }


def council_noac_indicators(conn: duckdb.DuckDBPyConnection, la: str) -> dict[str, Any] | None:
    """Every published NOAC 2024 indicator for one council (~125 series, raw values) — the full
    reference drill-down behind the curated scorecard. None if the council name is unknown."""
    df = lg.noac_indicators(conn, la).data
    if df.empty:
        return None
    return {"local_authority": la, "indicators": serialize.to_records(df), "caveat": caveats.NOAC_SCORECARD}


# ── Constituencies (per-constituency dossier) ──────────────────────────────────


def list_constituencies(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """All 43 constituencies with demographics + current TD count (the index grid)."""
    records = serialize.to_records(cons.constituency_list(conn).data)
    return serialize.envelope(records, total=len(records))


def build_constituency_dossier(conn: duckdb.DuckDBPyConnection, name: str) -> dict[str, Any] | None:
    """One constituency's record: header (population / per-TD ratio / seats) + current Dáil TDs +
    party breakdown + the Dáil work done since GE2024 + housing context (supply + waiting list) +
    the serving councils' money (each figure stands alone). None if the constituency is unknown."""
    header = cons.constituency_header(conn, name).data
    if header.empty:
        return None
    return {
        "constituency": name,
        "header": serialize.first_record(header),
        "members": serialize.to_records(cons.constituency_members(conn, name).data),
        "party_breakdown": serialize.to_records(cons.constituency_party_breakdown(conn, name).data),
        "house_work": serialize.first_record(cons.constituency_house_work(conn, name).data),
        "housing_context": serialize.to_records(cons.constituency_housing_context_with_ssha(conn, name).data),
        "council_context": serialize.to_records(cons.constituency_council_context(conn, name).data),
        "caveat": caveats.COUNCIL_MONEY,
    }


# ── Your councillors (elected local representatives) ───────────────────────────


def list_councillor_councils(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Councils that have a published councillor roster — to drive the council picker."""
    df = yc.councils(conn).data
    return {"councils": df["local_authority"].tolist() if not df.empty else []}


def councillors_roster(
    conn: duckdb.DuckDBPyConnection, *, council: str, lea: str | None = None
) -> dict[str, Any] | None:
    """The elected-member roster for a council (or one local electoral area), plus the council's
    meeting-coverage data-state and its (unelected) Chief Executive. None if the council is unknown."""
    roster_df = (yc.roster(conn, council, lea) if lea else yc.roster_council(conn, council)).data
    if roster_df.empty and yc.coverage(conn, council).data.empty:
        return None
    return {
        "council": council,
        "lea": lea,
        "councillors": serialize.to_records(roster_df),
        "coverage": serialize.first_record(yc.coverage(conn, council).data),
        "chief_executive": serialize.first_record(yc.chief_executive(conn, council).data),
    }


def councillor_votes(conn: duckdb.DuckDBPyConnection, *, council: str, member: str) -> dict[str, Any]:
    """A councillor's recorded roll-call votes (named-vote coverage is sparse — Carlow only so far)."""
    return {
        "council": council,
        "member": member,
        "votes": serialize.to_records(yc.votes(conn, council, member).data),
    }
