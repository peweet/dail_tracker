"""Private PublicSignal procurement opportunity composition.

This module keeps the two forward opportunity lanes separate while presenting a
small, stable contract to the private API. It deliberately does not aggregate
planned estimates, awards, or payments.
"""

from __future__ import annotations

import logging
from typing import Any

import duckdb

from dail_tracker_core import caveats, serialize
from dail_tracker_core.buyer_xref import resolve_buyer
from dail_tracker_core.queries import make_runner
from dail_tracker_core.queries import procurement as proc

_run = make_runner("procurement", logging.getLogger(__name__))

_LANE_NOTES = {
    "national_live": "National eTenders live pipeline; advertised estimates are PLANNED and not sums.",
    "ted_tender": "TED competition notices; advertised estimates are PLANNED and not sums.",
}


def _value(row: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in row and row[name] is not None:
            return row[name]
    return None


def _opportunity(row: dict[str, Any], lane: str) -> dict[str, Any]:
    if lane == "national_live":
        source_id = str(row.get("resource_id") or "")
        return {
            "id": f"national:{source_id}",
            "source_identity": source_id,
            "source_lane": lane,
            "source_url": row.get("detail_url"),
            "title": row.get("title"),
            "deadline": row.get("submission_deadline"),
            "buyer_display_name": row.get("buyer"),
            "value_eur": row.get("estimated_value_eur"),
            "value_kind": row.get("value_kind") or "estimate_advertised",
            "realisation_tier": row.get("realisation_tier") or "PLANNED",
            "cpv": None,
            "cpv_division": None,
            "retrieved_at": row.get("retrieved_utc"),
        }
    source_id = str(row.get("publication_number") or "")
    return {
        "id": f"ted:{source_id}",
        "source_identity": source_id,
        "source_lane": lane,
        "source_url": row.get("notice_url"),
        "title": source_id,
        "deadline": row.get("submission_deadline"),
        "buyer_display_name": row.get("buyer_name"),
        "value_eur": row.get("estimated_value_eur"),
        "value_kind": row.get("value_kind") or "estimate_advertised",
        "realisation_tier": "PLANNED",
        "cpv": row.get("cpv_code") or row.get("cpv_division"),
        "cpv_division": row.get("cpv_division"),
        "retrieved_at": None,
    }


def opportunity_feed(
    conn: duckdb.DuckDBPyConnection,
    *,
    limit: int = 100,
    within_days: int | None = None,
    sector: str | None = None,
    source_lane: str | None = None,
) -> dict[str, Any]:
    """Return a bounded, flat opportunity list plus lane-level coverage metadata."""
    limit = max(1, min(int(limit), 200))
    lanes = [source_lane] if source_lane else ["national_live", "ted_tender"]
    opportunities: list[dict[str, Any]] = []
    coverage: list[dict[str, Any]] = []
    for lane in lanes:
        if lane == "national_live":
            result = proc.live_tenders(conn, limit=limit, within_days=within_days)
        elif lane == "ted_tender":
            result = proc.ted_tenders(conn, limit=limit, only_open=True, sector=sector, within_days=within_days)
        else:
            continue
        if not result.ok:
            coverage.append(
                {"source_lane": lane, "status": "unavailable", "retrieved_at": None, "note": "source lane unavailable"}
            )
            continue
        rows = serialize.to_records(result.data)
        mapped = [_opportunity(row, lane) for row in rows]
        if lane == "national_live" and sector:
            mapped = []  # National live snapshots do not carry CPV, so no inferred sector match.
        opportunities.extend(mapped)
        coverage.append(
            {
                "source_lane": lane,
                "status": "ok",
                "retrieved_at": next((row.get("retrieved_at") for row in mapped if row.get("retrieved_at")), None),
                "note": _LANE_NOTES[lane],
            }
        )
    opportunities.sort(key=lambda row: (row.get("deadline") is None, row.get("deadline") or "", row["id"]))
    return {
        "opportunities": opportunities[:limit],
        "coverage": coverage,
        "caveats": [caveats.MONEY_GRAINS, "Historical winners are not included in the forward opportunity list."],
    }


def _award_lane(conn: duckdb.DuckDBPyConnection, buyer_name: str) -> dict[str, Any] | None:
    summary = _run(
        conn,
        "SELECT COUNT(*) AS n_awards, COUNT(*) FILTER (WHERE value_safe_to_sum) AS n_value_safe,"
        " COALESCE(SUM(value_eur) FILTER (WHERE value_safe_to_sum), 0) AS awarded_value_safe_eur"
        " FROM v_procurement_awards WHERE contracting_authority = ?",
        [buyer_name],
    )
    rows = _run(
        conn,
        "SELECT tender_id, supplier, supplier_class, cpv_code, tender_title, award_date, value_eur, value_kind,"
        " etenders_notice_url, ted_notice_link FROM v_procurement_awards"
        " WHERE contracting_authority = ? AND COALESCE(supplier_class, '') NOT IN ('sole_trader_or_individual', 'individual')"
        " ORDER BY award_date DESC NULLS LAST LIMIT 25",
        [buyer_name],
    )
    if not summary.ok or not rows.ok:
        return None
    return {
        "source_lane": "national_awards",
        "value_kind": "awarded_contract_value",
        "summary": serialize.first_record(summary.data),
        "records": serialize.to_records(rows.data),
        "caveat": caveats.PROCUREMENT_AWARDS,
    }


def _payment_lane(conn: duckdb.DuckDBPyConnection, publisher_name: str) -> dict[str, Any] | None:
    summary = proc.payments_publisher_profile(conn, publisher_name)
    if not summary.ok or summary.data.empty:
        return None
    records: list[dict[str, Any]] = []
    for tier in ("SPENT", "COMMITTED"):
        result = proc.payments_for_publisher(conn, publisher_name, tier=tier, limit=12)
        if result.ok:
            records.extend(serialize.to_records(result.data))
    return {
        "source_lane": "public_body_payments",
        "value_kind": "spent_or_committed_disclosure",
        "summary": serialize.first_record(summary.data),
        "records": records,
        "caveat": caveats.PUBPAY,
    }


def opportunity_brief(conn: duckdb.DuckDBPyConnection, opportunity_id: str) -> dict[str, Any] | None:
    """Build one evidence brief, enriching only curated exact buyer matches."""
    try:
        lane, source_id = opportunity_id.split(":", 1)
    except ValueError:
        return None
    if lane == "national":
        result = proc.live_tender_by_id(conn, source_id)
        source_lane = "national_live"
    elif lane == "ted":
        result = proc.ted_tender_by_id(conn, source_id)
        source_lane = "ted_tender"
    else:
        return None
    if not result.ok or result.data.empty:
        return None
    row = _opportunity(serialize.first_record(result.data) or {}, source_lane)
    buyer = resolve_buyer(row.get("buyer_display_name"))
    buyer_match: dict[str, Any]
    awards = payments = None
    market = None
    if buyer and buyer.get("match_tier") == "curated_exact":
        buyer_match = {
            "state": "exact",
            "buyer_id": buyer["buyer_id"],
            "display_name": buyer["display_name"],
            "match_tier": buyer["match_tier"],
        }
        if buyer.get("registers", {}).get("etenders"):
            awards = _award_lane(conn, buyer["registers"]["etenders"])
        if buyer.get("registers", {}).get("payments"):
            payments = _payment_lane(conn, buyer["registers"]["payments"])
        if row.get("cpv_division"):
            comp = _run(
                conn,
                "SELECT cpv_division, n_notices, n_lots_with_bidcount, n_single_bid_lots,"
                " single_bid_lot_pct, n_uncompetitive_notices, n_buyers, first_year, last_year"
                " FROM v_procurement_competition_by_cpv WHERE cpv_division = ? LIMIT 1",
                [row["cpv_division"]],
            )
            if comp.ok:
                matches = comp.data.loc[comp.data["cpv_division"] == row["cpv_division"]]
                market = serialize.first_record(matches)
    else:
        buyer_match = {
            "state": "unresolved",
            "query": row.get("buyer_display_name"),
            "reason": "No curated exact buyer identity; cross-register enrichment withheld.",
        }
    return {
        "opportunity": row,
        "buyer_match": buyer_match,
        "award_lane": awards,
        "payment_lane": payments,
        "market_competition": market,
        "contract_end": None,
        "caveats": [caveats.MONEY_GRAINS, caveats.COMPETITION],
    }
