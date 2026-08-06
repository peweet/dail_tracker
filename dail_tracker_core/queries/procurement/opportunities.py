"""Private PublicSignal procurement opportunity composition.

This module keeps the two forward opportunity lanes separate while presenting a
small, stable contract to the private API. It deliberately does not aggregate
planned estimates, awards, or payments.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime
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

PUBLIC_SIGNAL_OPPORTUNITY_LIMIT = 2_000


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
    limit = max(1, min(int(limit), PUBLIC_SIGNAL_OPPORTUNITY_LIMIT))
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


def _deadline_days(value: Any, *, today: date) -> int | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except ValueError:
        return None
    return (parsed - today).days


def public_signal_market_contracts(
    conn: duckdb.DuckDBPyConnection,
    opportunity_rows: list[dict[str, Any]],
    *,
    supplier_limit: int = 250,
    today: date | None = None,
) -> dict[str, Any]:
    """Compose reviewed PublicSignal tables without crossing procurement money grains.

    Sector and buyer rows are exact roll-ups of the supplied forward-notice snapshot.
    Supplier rows come only from national award records; TED counts are attached only
    when both registers carry the same exact CRO company number. No payment values or
    TED notice values are joined or summed into these contracts.
    """
    today = today or date.today()
    sectors: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "notice_count": 0,
            "valued_notice_count": 0,
            "closing_within_30_days": 0,
            "buyers": set(),
            "source_lanes": set(),
        }
    )
    buyers: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "notice_count": 0,
            "valued_notice_count": 0,
            "closing_within_30_days": 0,
            "sectors": set(),
            "source_lanes": set(),
            "nearest_deadline": None,
            "sample_source_url": None,
        }
    )
    for row in opportunity_rows:
        sector = str(row.get("cpv_division") or "Unclassified").strip() or "Unclassified"
        buyer = str(row.get("buyer_display_name") or "Buyer not stated").strip() or "Buyer not stated"
        lane = str(row.get("source_lane") or "unknown")
        deadline = row.get("deadline")
        days = _deadline_days(deadline, today=today)
        has_value = isinstance(row.get("value_eur"), (int, float)) and row["value_eur"] > 0

        sector_row = sectors[sector]
        sector_row["notice_count"] += 1
        sector_row["valued_notice_count"] += int(has_value)
        sector_row["closing_within_30_days"] += int(days is not None and 0 <= days <= 30)
        sector_row["buyers"].add(buyer)
        sector_row["source_lanes"].add(lane)

        buyer_row = buyers[buyer]
        buyer_row["notice_count"] += 1
        buyer_row["valued_notice_count"] += int(has_value)
        buyer_row["closing_within_30_days"] += int(days is not None and 0 <= days <= 30)
        buyer_row["sectors"].add(sector)
        buyer_row["source_lanes"].add(lane)
        if deadline and (buyer_row["nearest_deadline"] is None or str(deadline) < str(buyer_row["nearest_deadline"])):
            buyer_row["nearest_deadline"] = deadline
            buyer_row["sample_source_url"] = row.get("source_url")

    sector_rows = [
        {
            "sector": name,
            "notice_count": values["notice_count"],
            "valued_notice_count": values["valued_notice_count"],
            "closing_within_30_days": values["closing_within_30_days"],
            "buyer_count": len(values["buyers"]),
            "source_lanes": sorted(values["source_lanes"]),
        }
        for name, values in sectors.items()
    ]
    sector_rows.sort(key=lambda row: (-row["notice_count"], row["sector"].casefold()))
    buyer_rows = [
        {
            "buyer": name,
            "notice_count": values["notice_count"],
            "valued_notice_count": values["valued_notice_count"],
            "closing_within_30_days": values["closing_within_30_days"],
            "sector_count": len(values["sectors"] - {"Unclassified"}),
            "source_lanes": sorted(values["source_lanes"]),
            "nearest_deadline": values["nearest_deadline"],
            "sample_source_url": values["sample_source_url"],
        }
        for name, values in buyers.items()
    ]
    buyer_rows.sort(key=lambda row: (-row["notice_count"], row["buyer"].casefold()))

    national = proc.supplier_summary(conn, limit=supplier_limit, order_by="awards")
    ted_by_company = _run(
        conn,
        "SELECT cro_company_num, COUNT(DISTINCT publication_number) AS n_ted_awards,"
        " COUNT(DISTINCT buyer_name) AS n_ted_buyers"
        " FROM v_procurement_ted_winner_history"
        " WHERE cro_company_num IS NOT NULL"
        " GROUP BY cro_company_num",
    )
    ted_rows = serialize.to_records(ted_by_company.data) if ted_by_company.ok else []
    ted_index = {str(row["cro_company_num"]): row for row in ted_rows}
    supplier_rows: list[dict[str, Any]] = []
    if national.ok:
        for row in serialize.to_records(national.data):
            exact_company = row.get("cro_match_method") == "exact_unique" and row.get("company_num") is not None
            company_number = str(row["company_num"]) if exact_company else None
            ted = ted_index.get(company_number, {}) if company_number else {}
            supplier_rows.append(
                {
                    "supplier": row.get("supplier"),
                    "supplier_normalised": row.get("supplier_norm"),
                    "national_award_count": row.get("n_awards"),
                    "national_buyer_count": row.get("n_authorities"),
                    "sum_safe_award_count": row.get("n_value_safe_awards"),
                    "sum_safe_awarded_eur": row.get("awarded_value_safe_eur"),
                    "ceiling_notice_count": row.get("n_ceiling_notices"),
                    "company_number": company_number,
                    "company_status_at_snapshot": row.get("company_status") if exact_company else None,
                    "entity_match": "exact_cro" if exact_company else "unresolved",
                    "ted_award_notice_count": ted.get("n_ted_awards") if exact_company else None,
                    "ted_buyer_count": ted.get("n_ted_buyers") if exact_company else None,
                }
            )

    return {
        "sectors": {
            "schema": "publicsignal-sector-notice-summary/1",
            "status": "reviewed",
            "grain": "one row per stated CPV division in the forward-notice snapshot",
            "source_lanes": ["national_live", "ted_tender"],
            "rows": sector_rows,
            "caveats": [
                "Counts are current advertised notices, not market size or awarded contracts.",
                "National notices without a CPV remain Unclassified; no sector is inferred.",
                "Advertised values are not summed.",
            ],
        },
        "buyers": {
            "schema": "publicsignal-buyer-notice-summary/1",
            "status": "reviewed",
            "grain": "one row per exact buyer display name in the forward-notice snapshot",
            "source_lanes": ["national_live", "ted_tender"],
            "rows": buyer_rows,
            "caveats": [
                "Names are grouped exactly as published; aliases and related bodies are not merged.",
                "Counts describe current notices, not historical spend or buyer quality.",
                "Advertised values are not summed.",
            ],
        },
        "suppliers": {
            "schema": "publicsignal-supplier-award-summary/1",
            "status": "reviewed" if national.ok else "unavailable",
            "grain": "one row per normalised company-class supplier in national award records",
            "source_lanes": ["national_awards", "ted_awards"],
            "source_status": {
                "national_awards": "ok" if national.ok else "unavailable",
                "ted_awards": "ok" if ted_by_company.ok else "unavailable",
            },
            "rows": supplier_rows,
            "caveats": [
                "National award counts and TED award-notice counts remain separate.",
                "TED activity is attached only through an exact unique CRO company-number match.",
                "Unresolved or ambiguous CRO matches are not joined across registers.",
                "Sum-safe national awarded values exclude framework and shared ceilings and are never combined with TED values or payments.",
            ],
        },
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
