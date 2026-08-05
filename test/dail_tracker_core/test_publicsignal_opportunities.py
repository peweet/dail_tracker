"""Focused tests for the private PublicSignal opportunity composition boundary."""

from __future__ import annotations

import pandas as pd

from dail_tracker_core.queries.procurement import opportunities
from dail_tracker_core.results import QueryResult


def _ted_result(buyer: str) -> QueryResult:
    return QueryResult.success(
        pd.DataFrame(
            [
                {
                    "publication_number": "ABC",
                    "notice_url": "https://ted.example/ABC",
                    "buyer_name": buyer,
                    "cpv_code": "72000000",
                    "cpv_division": "72",
                    "submission_deadline": "2026-09-01",
                    "estimated_value_eur": 1000,
                    "value_kind": "estimate_advertised",
                }
            ]
        )
    )


def test_flat_opportunity_keeps_planned_value_and_source_identity() -> None:
    row = opportunities._opportunity(
        {
            "resource_id": "R1",
            "title": "Road",
            "buyer": "Buyer",
            "detail_url": "https://e.example/R1",
            "estimated_value_eur": 4,
            "value_kind": "estimate_advertised",
            "realisation_tier": "PLANNED",
            "retrieved_utc": "2026-08-01T00:00:00Z",
        },
        "national_live",
    )
    assert row["id"] == "national:R1"
    assert row["source_url"] == "https://e.example/R1"
    assert row["value_kind"] == "estimate_advertised"
    assert row["realisation_tier"] == "PLANNED"
    assert row["retrieved_at"] == "2026-08-01T00:00:00Z"


def test_unresolved_buyer_withholds_cross_register_enrichment(monkeypatch) -> None:
    monkeypatch.setattr(opportunities.proc, "ted_tender_by_id", lambda _conn, _id: _ted_result("Uncurated Buyer"))
    brief = opportunities.opportunity_brief(object(), "ted:ABC")
    assert brief is not None
    assert brief["buyer_match"]["state"] == "unresolved"
    assert brief["award_lane"] is None
    assert brief["payment_lane"] is None


def test_exact_buyer_allows_separate_award_and_payment_lanes(monkeypatch) -> None:
    monkeypatch.setattr(opportunities.proc, "ted_tender_by_id", lambda _conn, _id: _ted_result("Exact Buyer"))
    monkeypatch.setattr(
        opportunities,
        "resolve_buyer",
        lambda _name: {
            "buyer_id": "b1",
            "display_name": "Exact Buyer",
            "match_tier": "curated_exact",
            "registers": {"etenders": "Exact Buyer", "payments": "Exact Buyer"},
        },
    )
    monkeypatch.setattr(
        opportunities,
        "_award_lane",
        lambda _conn, _name: {"source_lane": "national_awards", "value_kind": "awarded_contract_value"},
    )
    monkeypatch.setattr(
        opportunities,
        "_payment_lane",
        lambda _conn, _name: {"source_lane": "public_body_payments", "value_kind": "spent_or_committed_disclosure"},
    )
    brief = opportunities.opportunity_brief(object(), "ted:ABC")
    assert brief is not None
    assert brief["buyer_match"]["state"] == "exact"
    assert brief["award_lane"]["value_kind"] == "awarded_contract_value"
    assert brief["payment_lane"]["value_kind"] == "spent_or_committed_disclosure"
