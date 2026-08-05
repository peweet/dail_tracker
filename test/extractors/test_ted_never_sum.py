"""Regression guards for the TED notice-value never-sum contract."""

from __future__ import annotations

import polars as pl

from extractors import procurement_award_spend_link, ted_enrich


def test_winner_enrichment_never_marks_ted_values_summable(tmp_path, monkeypatch):
    cro_path = tmp_path / "companies.parquet"
    pl.DataFrame(
        {
            "name_norm": ["unrelatedcompany"],
            "company_num": ["999999"],
            "company_status": ["Normal"],
        }
    ).write_parquet(cro_path)
    monkeypatch.setattr(ted_enrich, "CRO", cro_path)

    source = pl.DataFrame(
        {
            "winner_name": ["Small Co Ltd", "Large Co Ltd", "Framework Co Ltd"],
            "winner_identifier_digits": pl.Series([None, None, None], dtype=pl.String),
            "award_value_eur": [1_000_000.0, 60_000_000.0, 2_000_000.0],
            "value_kind": ["contract_award_value"] * 3,
            "is_multi_supplier_framework": [False, False, True],
            "is_pan_eu_outlier": [False, False, False],
        }
    )

    enriched = ted_enrich.enrich_winner_rows(source)

    assert not enriched["value_safe_to_sum"].any()
    assert enriched["is_large_award_review"].to_list() == [False, True, False]


def test_award_spend_link_keeps_ted_count_only(tmp_path, monkeypatch):
    api_path = tmp_path / "ted_api.parquet"
    history_path = tmp_path / "ted_history.parquet"
    pl.DataFrame(
        {
            "publication_number": ["P1", "P2"],
            "winner_name": ["Acme Ltd", "Other Ltd"],
            "winner_name_norm": ["acme", "other"],
            "award_value_eur": [1_000_000.0, 2_000_000.0],
            "value_safe_to_sum": [True, True],
        }
    ).write_parquet(api_path)
    pl.DataFrame(
        {
            "publication_number": ["P1", "P3"],
            "winner_name": ["Acme Ltd", "Acme Ltd"],
            "winner_name_norm": ["acme", "acme"],
            "award_value_eur": [1_000_000.0, 3_000_000.0],
            "value_safe_to_sum": [True, True],
        }
    ).write_parquet(history_path)
    monkeypatch.setattr(procurement_award_spend_link, "TED", api_path)
    monkeypatch.setattr(procurement_award_spend_link, "TED_WINNER_HISTORY", history_path)
    cro_map = pl.DataFrame(
        {
            "name_norm": ["acme", "other"],
            "company_num": [1, 2],
            "company_status": ["Normal", "Normal"],
        }
    )

    result = procurement_award_spend_link.load_ted(cro_map)

    assert set(result.columns) == {"entity", "ted_name", "ted_awards"}
    assert result.filter(pl.col("entity") == "CRO:1")["ted_awards"].item() == 2
    assert not any("eur" in column.lower() or "value" in column.lower() for column in result.columns)
