"""Tests for the consolidated quarantine ledger reporter (tools.quarantine_report).

Proves the trace works end-to-end: a value the gate holds back is recoverable from one
ledger with enough provenance (offending value + source PDF + page) to find the source.
"""

import json
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parents[2]))

from tools.data_fidelity import fidelity_gate  # noqa: E402
from tools.quarantine_report import build_ledger  # noqa: E402


def test_build_ledger_captures_value_and_provenance(tmp_path):
    qdir = tmp_path / "quarantine"
    qdir.mkdir()
    # Simulate what a gate already wrote: full offending row + reason + provenance columns.
    pl.DataFrame(
        {
            "donor_name": ["X. Ample"],
            "value_eur": [5_000_000_000.0],
            "source_pdf": ["2024_election_donations.pdf"],
            "source_page": [12],
            "_quarantine_reason": ["value_eur"],
        }
    ).write_parquet(qdir / "sipo_donations_bronze_quarantine.parquet")
    (qdir / "sipo_donations_bronze_quarantine.json").write_text(
        json.dumps(
            {
                "generated_utc": "2026-06-27T00:00:00+00:00",
                "n_rows_total": 75,
                "frac_quarantined": 0.0133,
                "breaches": {"value_eur": {"severity": "bound"}},
            }
        ),
        encoding="utf-8",
    )

    ledger_path = tmp_path / "ledger.json"
    led = build_ledger(qdir, ledger_path, now="2026-06-27T00:00:00+00:00")

    assert ledger_path.exists()
    assert led["n_resources"] == 1 and led["n_rows_held"] == 1
    r = led["resources"]["sipo_donations_bronze"]
    assert r["n_held"] == 1
    assert "value_eur" in r["offending_columns"]
    assert {"source_pdf", "source_page"} <= set(r["provenance_columns"])
    assert r["n_rows_total"] == 75  # folded in from the sidecar summary json
    row = r["rows"][0]
    assert row["value_eur"] == 5_000_000_000.0
    assert row["source_pdf"] == "2024_election_donations.pdf" and row["source_page"] == 12


def test_empty_quarantine_dir_is_a_clean_ledger(tmp_path):
    qdir = tmp_path / "q"
    qdir.mkdir()
    led = build_ledger(qdir, tmp_path / "l.json", now="t")
    assert led["n_resources"] == 0 and led["n_rows_held"] == 0 and led["resources"] == {}


def test_gate_then_report_end_to_end(tmp_path):
    # one €5bn fat-finger among 60 real donations → gate quarantines it, reporter traces it.
    df = pl.DataFrame(
        {
            "donor_name": [f"d{i}" for i in range(60)] + ["fat_finger"],
            "value_eur": [1000.0] * 60 + [5_000_000_000.0],
            "source_pdf": ["2024_election_donations.pdf"] * 61,
            "source_page": list(range(1, 62)),
        }
    )
    clean = fidelity_gate(
        df, name="e2e_donations", bounds={"value_eur": (0, 1_000_000_000)}, quarantine_dir=tmp_path
    )
    assert clean.height == 60  # the absurd row never propagates

    led = build_ledger(tmp_path, tmp_path / "ledger.json", now="t")
    row = led["resources"]["e2e_donations"]["rows"][0]
    assert row["value_eur"] == 5_000_000_000.0
    assert row["source_pdf"] == "2024_election_donations.pdf"
    assert row["_quarantine_reason"] == "value_eur"
