"""Contract tests for the private award/payment shadow candidate builder."""

from __future__ import annotations

from datetime import date

import polars as pl

from extractors.procurement_award_payment_candidates import LINK_STATES, build_shadow_candidates, build_summary


def _awards(*rows: dict) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={
            "tender_id": pl.String,
            "contracting_authority": pl.String,
            "supplier_norm": pl.String,
            "award_date": pl.Date,
            "contract_duration_months": pl.Int64,
            "tender_title": pl.String,
        },
    )


def _payments(*rows: dict) -> pl.DataFrame:
    defaults = {
        "_fact_row_number": 0,
        "publisher_id": "body-1",
        "publisher_name": "Body Payments",
        "payment_supplier": "Acme Ltd",
        "supplier_normalised": "ACME",
        "cro_company_num": 123,
        "period": "2025-Q2",
        "year": 2025,
        "quarter": 2,
        "amount_eur": 25_000.0,
        "value_kind": "payment_actual",
        "realisation_tier": "SPENT",
        "value_safe_to_sum": True,
        "vat_status": "unknown",
        "po_number": "PO-1",
        "description": "Specialist bridge inspection service",
        "source_landing_url": "https://body.example/payments",
        "source_file_url": "https://body.example/q2.xlsx",
        "source_file_hash": "hash-1",
        "source_row_number": 7,
    }
    supplied_rows = rows or ({},)
    return pl.DataFrame([{**defaults, **row} for row in supplied_rows])


def _supplier_xref() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "supplier_norm": ["ACME", "AMBIGUOUS"],
            "company_num": [123, 999],
            "n_cro": [1, 2],
            "match_method": ["exact_unique", "exact_ambiguous"],
            "match_confidence": [0.9, 0.5],
        }
    )


def _buyer_xref() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "etenders_name": ["Body Awards", "Unreviewed Awards"],
            "payments_publisher_name": ["Body Payments", "Unreviewed Payments"],
            "match_tier": ["curated_exact", "single_register"],
        }
    )


def test_explicit_tender_id_wins_over_other_relationship_candidates() -> None:
    awards = _awards(
        {
            "tender_id": "123456",
            "contracting_authority": "Body Awards",
            "supplier_norm": "ACME",
            "award_date": date(2024, 1, 10),
            "contract_duration_months": 36,
            "tender_title": "Specialist bridge inspection service",
        },
        {
            "tender_id": "654321",
            "contracting_authority": "Body Awards",
            "supplier_norm": "ACME",
            "award_date": date(2024, 1, 10),
            "contract_duration_months": 36,
            "tender_title": "Other service",
        },
    )
    rows = build_shadow_candidates(
        awards,
        _payments({"description": "Payment relating to eTender 123456"}),
        _supplier_xref(),
        _buyer_xref(),
    )
    row = rows.to_dicts()[0]
    assert row["link_state"] == "explicit_reference"
    assert row["candidate_tender_id"] == "123456"
    assert row["relationship_tender_count"] == 2
    assert row["contract_attribution_permitted"] is False


def test_unique_duration_and_literal_title_match_is_review_candidate() -> None:
    rows = build_shadow_candidates(
        _awards(
            {
                "tender_id": "222222",
                "contracting_authority": "Body Awards",
                "supplier_norm": "ACME",
                "award_date": date(2024, 8, 1),
                "contract_duration_months": 24,
                "tender_title": "Specialist bridge inspection service",
            }
        ),
        _payments(),
        _supplier_xref(),
        _buyer_xref(),
    )
    row = rows.to_dicts()[0]
    assert row["link_state"] == "review_candidate"
    assert row["candidate_tender_id"] == "222222"
    assert row["review_status"] == "unreviewed"
    assert row["publication_status"] == "shadow_only"


def test_multiple_title_candidates_fail_closed_to_relationship_only() -> None:
    common = {
        "contracting_authority": "Body Awards",
        "supplier_norm": "ACME",
        "award_date": date(2024, 1, 10),
        "contract_duration_months": 36,
        "tender_title": "Specialist bridge inspection service",
    }
    rows = build_shadow_candidates(
        _awards({"tender_id": "111111", **common}, {"tender_id": "222222", **common}),
        _payments(),
        _supplier_xref(),
        _buyer_xref(),
    )
    row = rows.to_dicts()[0]
    assert rows.height == 1
    assert row["link_state"] == "relationship_only"
    assert row["candidate_tender_id"] is None
    assert row["review_candidate_count"] == 2


def test_missing_duration_or_outside_window_never_becomes_review_candidate() -> None:
    awards = _awards(
        {
            "tender_id": "333333",
            "contracting_authority": "Body Awards",
            "supplier_norm": "ACME",
            "award_date": date(2020, 1, 1),
            "contract_duration_months": None,
            "tender_title": "Specialist bridge inspection service",
        },
        {
            "tender_id": "444444",
            "contracting_authority": "Body Awards",
            "supplier_norm": "ACME",
            "award_date": date(2020, 1, 1),
            "contract_duration_months": 12,
            "tender_title": "Specialist bridge inspection service",
        },
    )
    rows = build_shadow_candidates(awards, _payments(), _supplier_xref(), _buyer_xref())
    assert rows["link_state"].item() == "relationship_only"
    assert rows["review_candidate_count"].item() == 0


def test_ambiguous_supplier_and_unreviewed_buyer_are_suppressed() -> None:
    awards = _awards(
        {
            "tender_id": "555555",
            "contracting_authority": "Body Awards",
            "supplier_norm": "AMBIGUOUS",
            "award_date": date(2024, 1, 1),
            "contract_duration_months": 12,
            "tender_title": "Specialist bridge inspection service",
        },
        {
            "tender_id": "666666",
            "contracting_authority": "Unreviewed Awards",
            "supplier_norm": "ACME",
            "award_date": date(2024, 1, 1),
            "contract_duration_months": 12,
            "tender_title": "Specialist bridge inspection service",
        },
    )
    payments = _payments(
        {"cro_company_num": 999, "supplier_normalised": "AMBIGUOUS"},
        {"_fact_row_number": 1, "publisher_name": "Unreviewed Payments"},
    )
    rows = build_shadow_candidates(awards, payments, _supplier_xref(), _buyer_xref())
    assert rows.is_empty()


def test_lifecycle_provenance_and_no_score_contract() -> None:
    rows = build_shadow_candidates(
        _awards(
            {
                "tender_id": "777777",
                "contracting_authority": "Body Awards",
                "supplier_norm": "ACME",
                "award_date": date(2024, 1, 1),
                "contract_duration_months": 36,
                "tender_title": "Specialist bridge inspection service",
            }
        ),
        _payments(
            {
                "value_kind": "po_committed",
                "realisation_tier": "COMMITTED",
                "source_file_url": "https://body.example/orders.xlsx",
            }
        ),
        _supplier_xref(),
        _buyer_xref(),
    )
    row = rows.to_dicts()[0]
    assert row["source_file_url"] == "https://body.example/orders.xlsx"
    assert "not cash paid" in row["money_caveat"]
    assert row["never_sum_with"] == "awarded|budget|ted"
    assert set(rows["link_state"]) <= LINK_STATES
    assert not any("score" in column or "probability" in column for column in rows.columns)
    summary = build_summary(rows)
    assert summary["contract_attribution_permitted"] is False
    assert summary["contains_confidence_score"] is False
