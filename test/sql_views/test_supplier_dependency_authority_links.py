"""Contracts for supplier-dependency authority link keys."""

import polars as pl
import pytest

from ._view_test_helpers import _view_path


def _write_awards_fixture(tmp_path):
    rows = []

    def add(supplier_norm, supplier, authority, client, n):
        rows.extend(
            {
                "supplier_norm": supplier_norm,
                "supplier": supplier,
                "supplier_class": "company",
                "name_truncated": False,
                "Contracting Authority": authority,
                "Name of Client Contracting Authority": client,
            }
            for _ in range(n)
        )

    add(
        "warrenmount",
        "Warrenmount Supplier",
        "Presentation Secondary School (Warrenmount)",
        "Presentation Primary School Warrenmount",
        5,
    )
    add("fallback", "Fallback Supplier", "Fallback Authority", None, 2)
    add("fallback", "Fallback Supplier", "Fallback Authority", "", 2)
    add("fallback", "Fallback Supplier", "Fallback Authority", "NULL", 1)
    add("ambiguous", "Ambiguous Supplier", "Ambiguous Authority", "Client A", 3)
    add("ambiguous", "Ambiguous Supplier", "Ambiguous Authority", "Client B", 2)
    add("tied", "Tie Supplier", "Zulu Authority", None, 2)
    add("tied", "Tie Supplier", "Alpha Authority", None, 2)
    add("tied", "Tie Supplier", "Other Authority", None, 1)

    path = tmp_path / "data" / "gold" / "parquet"
    path.mkdir(parents=True)
    pl.DataFrame(rows).write_parquet(path / "procurement_awards.parquet")


@pytest.mark.sql
def test_supplier_dependency_authority_profile_keys_are_safe_and_deterministic(tmp_path):
    _write_awards_fixture(tmp_path)
    sql = _view_path("procurement_supplier_dependency.sql").read_text(encoding="utf-8")
    sql = sql.replace("'data/", f"'{tmp_path.as_posix()}/data/")

    import duckdb

    con = duckdb.connect()
    con.execute(sql)
    rows = {
        row[0]: row
        for row in con.execute(
            "SELECT supplier_norm, top_authority, top_authority_profile_key, awards_from_top_authority, "
            "total_awards, n_authorities FROM v_procurement_supplier_dependency"
        ).fetchall()
    }

    assert rows["warrenmount"][1:] == (
        "Presentation Secondary School (Warrenmount)",
        "Presentation Primary School Warrenmount",
        5,
        5,
        1,
    )
    assert rows["fallback"][1:3] == ("Fallback Authority", "Fallback Authority")
    assert rows["ambiguous"][1:3] == ("Ambiguous Authority", None)
    assert rows["tied"][1:3] == ("Alpha Authority", "Alpha Authority")


def _write_itemised_awards_fixture(tmp_path):
    rows = []

    def add(tender_id, authority, client, date, supplier="Itemised Supplier"):
        rows.append(
            {
                "Tender ID": tender_id,
                "supplier": supplier,
                "supplier_norm": "itemisedsupplier",
                "supplier_class": "company",
                "name_truncated": False,
                "Contracting Authority": authority,
                "Main Cpv Code": "12345678",
                "Main Cpv Code Description": "Test works",
                "Competition Type": "Open",
                "Notice Published Date/Contract Created Date": date,
                "value_eur": 100.0,
                "value_kind": "contract_award_value",
                "is_framework_or_dps": False,
                "value_shared_across_suppliers": False,
                "value_safe_to_sum": True,
                "Parent Agreement ID": "",
                "is_call_off": False,
                "Tender/Contract Name": "Test award",
                "Spend Category": "Test",
                "Contract Type": "Services",
                "Procedure": "Open",
                "Contract Duration (Months)": "12",
                "No of Bids Received": "2",
                "No of SMEs Bids Received": "1",
                "No of Awarded SMEs": "1",
                "Additional CPV Codes on CFT": "",
                "TED Notice Link": "",
                "TED CAN Link": "",
                "estimated_value_eur": 100.0,
                "Threshold Level": "National",
                "Directive": "Classic",
                "Evaluation Type": "MEAT",
                "Name of Client Contracting Authority": client,
                "Agreement Owner": "",
                "Tender Submission Deadline": "01/01/2024",
                "Cancelled Date": "",
                "Award Published": "02/01/2024",
                "Platform": "eTenders",
            }
        )

    add("W1", "Raw Warrenmount", "Canonical Warrenmount", "02/01/2024")
    add("W2", "Raw Warrenmount", "Canonical Warrenmount", "03/01/2024")
    add("W3", "Raw Warrenmount", "Canonical Warrenmount", "04/01/2023")
    add("F1", "Fallback Authority", None, "05/01/2024")
    add("F2", "Fallback Authority", "", "06/01/2024")
    add("F3", "Fallback Authority", "NULL", "07/01/2024")

    path = tmp_path / "data" / "gold" / "parquet"
    path.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path / "procurement_awards.parquet")


@pytest.mark.sql
def test_canonical_authority_link_returns_itemised_awards(tmp_path):
    _write_itemised_awards_fixture(tmp_path)
    awards_sql = _view_path("procurement_awards.sql").read_text(encoding="utf-8")
    summary_sql = _view_path("procurement_authority_summary.sql").read_text(encoding="utf-8")
    awards_sql = awards_sql.replace("'data/", f"'{tmp_path.as_posix()}/data/")
    summary_sql = summary_sql.replace("'data/", f"'{tmp_path.as_posix()}/data/")

    import duckdb

    from dail_tracker_core.queries import procurement as procurement_queries

    con = duckdb.connect()
    con.execute(awards_sql)
    con.execute(summary_sql)

    provenance = con.execute(
        "SELECT contracting_authority, client_authority, buyer_authority FROM v_procurement_awards ORDER BY tender_id"
    ).fetchall()
    assert provenance[0] == ("Fallback Authority", None, "Fallback Authority")
    assert provenance[3] == ("Raw Warrenmount", "Canonical Warrenmount", "Canonical Warrenmount")

    summary = procurement_queries.authority_summary(con, limit=None)
    named = procurement_queries.awards_for_authority(con, "Canonical Warrenmount")
    fallback = procurement_queries.awards_for_authority(con, "Fallback Authority")
    named_2024 = procurement_queries.awards_for_authority(con, "Canonical Warrenmount", year=2024)

    assert summary.ok and named.ok and fallback.ok and named_2024.ok
    summary_counts = dict(zip(summary.data["contracting_authority"], summary.data["n_awards"], strict=True))
    assert summary_counts["Canonical Warrenmount"] == len(named.data) == 3
    assert summary_counts["Fallback Authority"] == len(fallback.data) == 3
    assert len(named_2024.data) == 2
    assert {"tender_id", "supplier", "award_date", "value_eur"}.issubset(named.data.columns)
