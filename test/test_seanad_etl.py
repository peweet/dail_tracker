"""Unit tests for the promoted Seanad-parity ETL (doc/SEANAD_PARITY_BUILD_PLAN.md §10).

Pure / fixture-driven — no network, no PDF binaries, no Streamlit. These lock the
small additive changes that gave the Dáil parsers Senator awareness:

  * payments_full_psa_etl._split_position  — the silent "Deputy" mislabel guard
  * payments_full_psa_etl.build_full_psa   — house tagging + Dáil-default parity
  * attendance._build_fact_table           — house tagging + Dáil-default parity
  * transform_votes.build_seanad_votes_silver — /seanad/ URL + schema parity
"""
from __future__ import annotations

import datetime as _dt
import sys
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

import attendance  # noqa: E402
import payments_full_psa_etl as P  # noqa: E402
import transform_votes  # noqa: E402


# ── _split_position — the highest-value guard ────────────────────────────────
@pytest.mark.parametrize(
    "cell, expected",
    [
        ("Deputy Adams, Gerry", ("Deputy", "Adams, Gerry")),            # Dáil unchanged
        ("Minister Harris, Simon", ("Minister", "Harris, Simon")),      # other roles unchanged
        ("Taoiseach Varadkar, Leo", ("Taoiseach", "Varadkar, Leo")),
        ("Senator Ahearn, Garret", ("Senator", "Ahearn, Garret")),      # the new case
        ("Senaotr Goldsboro, Imelda", ("Senator", "Goldsboro, Imelda")),  # source typo tolerated
        ("Cathaoirleach Daly, Mark", ("Cathaoirleach", "Daly, Mark")),
        ("Adams, Gerry", ("Deputy", "Adams, Gerry")),                   # bare name → Deputy default
        ("Sennett, Joe", ("Deputy", "Sennett, Joe")),                   # Sen* surname NOT clobbered
        ("", ("Deputy", "")),
    ],
)
def test_split_position(cell, expected):
    assert P._split_position(cell) == expected


# ── build_full_psa — house tagging + Dáil default unchanged ──────────────────
def _fake_rows():
    return [
        P.ExtractedRow(
            member_name="Ahearn, Garret", position="Senator", payment_kind="TAA",
            taa_band_raw="6", taa_band_label="Band 6 — 160–190 km",
            date_paid=_dt.date(2026, 2, 27), narrative="PSA February 2026",
            amount=3172.83, source_pdf="x.pdf", schema="v2020_h2_plus",
        ),
        P.ExtractedRow(
            member_name="Black, Frances", position="Senator", payment_kind="PSA_DUBLIN",
            taa_band_raw="Dublin", taa_band_label="Dublin / under 25 km",
            date_paid=_dt.date(2026, 2, 27), narrative="PSA February 2026",
            amount=1456.25, source_pdf="x.pdf", schema="v2020_h2_plus",
        ),
    ]


def test_build_full_psa_house_tag(tmp_path, monkeypatch):
    monkeypatch.setattr(P, "_iter_rows_from_pdf", lambda _p: _fake_rows())
    (tmp_path / "dummy.pdf").write_bytes(b"%PDF-1.4")  # build_full_psa globs *.pdf
    out = tmp_path / "out.parquet"
    P.build_full_psa(
        pdf_dir=tmp_path, out_parquet=out, out_csv=tmp_path / "out.csv",
        quarantine_parquet=tmp_path / "q.parquet", house="Seanad",
    )
    df = pl.read_parquet(out)
    assert "house" in df.columns
    assert set(df["house"].unique().to_list()) == {"Seanad"}
    assert df.height == 2


def test_build_full_psa_default_has_no_house_col(tmp_path, monkeypatch):
    """Dáil default path must be byte-shape-compatible: no house column."""
    monkeypatch.setattr(P, "_iter_rows_from_pdf", lambda _p: _fake_rows())
    (tmp_path / "dummy.pdf").write_bytes(b"%PDF-1.4")
    out = tmp_path / "out.parquet"
    P.build_full_psa(
        pdf_dir=tmp_path, out_parquet=out, out_csv=tmp_path / "out.csv",
        quarantine_parquet=tmp_path / "q.parquet",  # house omitted
    )
    assert "house" not in pl.read_parquet(out).columns


# ── attendance._build_fact_table — house tagging + Dáil default ──────────────
def _write_silver(tmp_path) -> Path:
    silver = tmp_path / "silver.csv"
    pd.DataFrame(
        {
            "identifier": ["Ahearn_Garret", "Ahearn_Garret"],
            "first_name": ["Garret", "Garret"],
            "last_name": ["Ahearn", "Ahearn"],
            "year": ["2025", "2025"],
            "iso_sitting_days_attendance": ["2025-01-15", "2025-01-16"],
            "iso_other_days_attendance": ["2025-01-20", None],
        }
    ).to_csv(silver, index=False)
    return silver


def test_attendance_fact_house_tag(tmp_path):
    silver = _write_silver(tmp_path)
    fact_csv = tmp_path / "fact.csv"
    attendance._build_fact_table(silver, fact_csv, tmp_path / "fact.parquet", house="Seanad")
    out = pd.read_csv(fact_csv)
    assert "house" in out.columns
    assert set(out["house"].unique()) == {"Seanad"}


def test_attendance_fact_default_no_house(tmp_path):
    silver = _write_silver(tmp_path)
    fact_csv = tmp_path / "fact.csv"
    attendance._build_fact_table(silver, fact_csv, tmp_path / "fact.parquet")  # no house
    assert "house" not in pd.read_csv(fact_csv).columns


# ── transform_votes.build_seanad_votes_silver — /seanad/ URL + schema parity ─
def _fake_division():
    return {
        "division": {
            "date": "2026-05-28",
            "outcome": "Lost",
            "voteId": "vote_3",
            "house": {"houseNo": "27", "houseCode": "seanad"},
            "subject": {"showAs": "Amendment put"},
            "debate": {"showAs": "Some Bill 2026: Committee Stage"},
            "tallies": {
                "taVotes": {"members": [
                    {"member": {"showAs": "Andrews, Chris.", "memberCode": "Chris-Andrews.D.2007-06-14",
                                "uri": "https://data.oireachtas.ie/x"}}
                ]},
                "nilVotes": {"members": [
                    {"member": {"showAs": "Black, Frances", "memberCode": "Frances-Black.S.2016-04-25",
                                "uri": "https://data.oireachtas.ie/y"}}
                ]},
            },
        }
    }


def test_build_seanad_votes_silver(tmp_path):
    out = tmp_path / "seanad_pretty_votes.csv"
    transform_votes.build_seanad_votes_silver([_fake_division()], out)
    df = pd.read_csv(out)

    # schema parity with the Dáil pretty_votes the enricher consumes
    for col in ("unique_member_code", "vote_outcome", "vote_id", "vote_type", "vote_url", "date"):
        assert col in df.columns
    # house-aware URL + globally-unique date-prefixed vote_id
    assert df["vote_url"].str.contains("/seanad/").all()
    assert (df["vote_id"] == "2026-05-28_3").all()
    # tally labels mapped
    assert set(df["vote_type"]) == {"Voted Yes", "Voted No"}
    assert "Chris-Andrews.D.2007-06-14" in set(df["unique_member_code"])
