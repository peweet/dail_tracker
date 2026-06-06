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
from dail_tracker_core.queries import legislation as leg
from dail_tracker_core.queries import member_overview as moq


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
        votes_cast = int(r.get("yes_count", 0) or 0) + int(r.get("no_count", 0) or 0) + int(r.get("abstained_count", 0) or 0)
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
