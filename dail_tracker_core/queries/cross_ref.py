"""Cross-reference retrieval — votes × Register of Members' Interests.

The first core query module that JOINS two domains rather than reading one. It
answers the accountability question no single-resource endpoint can: *did the
members who voted a given way on a measure also declare a relevant private
interest?*

The join key is ``member_id`` (``unique_member_code``), exposed by both
``v_vote_member_detail`` and ``v_member_interests_detail`` (the latter only since
the ``member_interests_detail.sql`` view was extended to carry it). No name
matching — the codes are exact.

Coverage caveat the callers must surface: the interests register only covers
2020–2025, so divisions before 2020 have no interests counterpart and will match
nothing. ``held_in_vote_year`` distinguishes "declared this interest in the same
calendar year as the vote" from the looser "ever declared it in the register era".

Like the other ``queries/*.py`` modules, every function takes an explicit ``conn``
and returns a ``QueryResult`` so a missing view surfaces as *unavailable* rather
than a silent empty frame.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)

# Members file a return row for EVERY category, most of them a nil "No interests
# declared". So a category-presence test over-counts wildly (≈half of Directorships
# rows are nil). This guard — the same one v_member_interests_index uses — keeps
# only rows with a real declared interest.
_NONEMPTY = (
    "interest_text IS NOT NULL AND TRIM(interest_text) <> '' AND LOWER(TRIM(interest_text)) <> 'no interests declared'"
)

# Map the public ``interest`` enum to the SQL predicate over one interests row.
# landlord / property are real pipeline-derived booleans; director / shareholder
# are read off interest_category (the dedicated flags are still hardcoded FALSE
# placeholders, TODO_PIPELINE_VIEW_REQUIRED) and so MUST carry the nil-text guard.
_INTEREST_SQL = {
    "landlord": "landlord_flag",
    "property": "property_flag",
    "director": f"(interest_category = 'Directorships' AND {_NONEMPTY})",
    "shareholder": f"(interest_category = 'Shares' AND {_NONEMPTY})",
}

_MATCH_LIMIT = 2000


def _run(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> QueryResult:
    try:
        return QueryResult.success(conn.execute(sql, params or []).df())
    except Exception as exc:  # noqa: BLE001 — any DuckDB failure is "source unavailable"
        _log.warning("cross_ref query failed: %s | params=%s | error=%s", sql[:120], params, exc)
        return QueryResult.unavailable(f"cross_ref query failed: {exc}")


def division_interest_breakdown(conn: duckdb.DuckDBPyConnection, vote_id: str) -> QueryResult:
    """One row per vote_type for a single division, counting how many of its
    voters declare each interest type (landlord / property / director / shareholder)
    and how many appear on the register at all."""
    sql = (
        "WITH intr AS ("
        "  SELECT member_id,"
        f"    BOOL_OR({_INTEREST_SQL['landlord']})    AS is_landlord,"
        f"    BOOL_OR({_INTEREST_SQL['property']})    AS is_property_owner,"
        f"    BOOL_OR({_INTEREST_SQL['director']})    AS is_director,"
        f"    BOOL_OR({_INTEREST_SQL['shareholder']}) AS is_shareholder"
        "  FROM v_member_interests_detail"
        "  WHERE member_id IS NOT NULL"
        "  GROUP BY member_id"
        ") "
        "SELECT m.vote_type,"
        "  COUNT(*)                                          AS members,"
        "  COUNT(*) FILTER (WHERE i.member_id IS NOT NULL)   AS on_register,"
        "  COUNT(*) FILTER (WHERE i.is_landlord)             AS landlords,"
        "  COUNT(*) FILTER (WHERE i.is_property_owner)       AS property_owners,"
        "  COUNT(*) FILTER (WHERE i.is_director)             AS directors,"
        "  COUNT(*) FILTER (WHERE i.is_shareholder)          AS shareholders "
        "FROM v_vote_member_detail m "
        "LEFT JOIN intr i ON i.member_id = m.member_id "
        "WHERE m.vote_id = ? AND m.member_name IS NOT NULL "
        "GROUP BY m.vote_type ORDER BY m.vote_type"
    )
    return _run(conn, sql, [vote_id])


def voting_vs_interests(
    conn: duckdb.DuckDBPyConnection,
    *,
    vote_id: str | None = None,
    keyword: str | None = None,
    vote_type: str = "Voted No",
    interest: str = "landlord",
    house: str = "Dáil",
) -> QueryResult:
    """Members who cast ``vote_type`` on a division (exact ``vote_id`` OR debate-title
    ``keyword`` ILIKE) AND declare ``interest`` somewhere in the register era.

    Returns one row per (member, matching division), with ``held_in_vote_year`` true
    when the interest was declared in the same calendar year as the vote.
    """
    expr = _INTEREST_SQL.get(interest)
    if expr is None:
        return QueryResult.unavailable(f"unknown interest '{interest}' — use one of: {', '.join(_INTEREST_SQL)}")

    clauses = ["v.house = ?", "v.member_name IS NOT NULL", "v.vote_type = ?"]
    params: list = [house, vote_type]
    if vote_id:
        clauses.append("v.vote_id = ?")
        params.append(vote_id)
    elif keyword:
        clauses.append("v.debate_title ILIKE ?")
        params.append(f"%{keyword}%")
    else:
        return QueryResult.unavailable("voting_vs_interests needs a vote_id or a keyword")
    where = " AND ".join(clauses)

    sql = (
        "WITH voters AS ("
        "  SELECT member_id, member_name, party_name, constituency, vote_type,"
        "         vote_id, debate_title, vote_date,"
        "         CAST(EXTRACT(YEAR FROM vote_date) AS INTEGER) AS vote_year"
        "  FROM v_vote_member_detail v"
        f"  WHERE {where}"
        "), "
        "holders AS ("
        f"  SELECT member_id, declaration_year, BOOL_OR({expr}) AS holds"
        "  FROM v_member_interests_detail"
        "  WHERE member_id IS NOT NULL"
        "  GROUP BY member_id, declaration_year"
        "), "
        "ever AS ("
        "  SELECT member_id, BOOL_OR(holds) AS holds_ever FROM holders GROUP BY member_id"
        ") "
        "SELECT v.vote_id, v.vote_date, v.debate_title, v.member_id, v.member_name,"
        "       v.party_name, v.constituency, v.vote_type,"
        "       COALESCE(sy.holds, FALSE) AS held_in_vote_year "
        "FROM voters v "
        "JOIN ever e ON e.member_id = v.member_id AND e.holds_ever "
        "LEFT JOIN holders sy"
        "  ON sy.member_id = v.member_id AND sy.declaration_year = v.vote_year AND sy.holds "
        "ORDER BY v.vote_date DESC, v.member_name ASC "
        f"LIMIT {_MATCH_LIMIT}"
    )
    return _run(conn, sql, params)
