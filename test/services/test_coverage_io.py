"""Tests for services/coverage_io.py — atomic coverage/provenance JSON sidecars.

What this catches:
  - The serialisation convention (UTF-8, indent, default=str for dates/Paths).
  - Atomicity: a failed dump must leave the previous good sidecar untouched
    and no .part temp behind (the save_parquet contract, applied to JSON).
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from services.coverage_io import save_coverage


def test_save_coverage_round_trips_and_returns_path(tmp_path):
    dest = tmp_path / "coverage.json"
    payload = {"rows": 85_000, "publishers": {"ie_hse": 12, "ie_tusla": 4}}

    out = save_coverage(payload, dest)

    assert out == dest
    assert json.loads(dest.read_text(encoding="utf-8")) == payload
    assert not (tmp_path / "coverage.json.part").exists()


def test_save_coverage_serialises_dates_and_paths_via_default_str(tmp_path):
    """Coverage dicts routinely carry date objects and Paths — default=str must
    keep the emit from raising mid-run (the reason many sites passed default=str)."""
    dest = tmp_path / "coverage.json"
    payload = {"run_date": date(2026, 7, 17), "source": Path("data/bronze/x.pdf")}

    save_coverage(payload, dest)

    got = json.loads(dest.read_text(encoding="utf-8"))
    assert got["run_date"] == "2026-07-17"
    assert "x.pdf" in got["source"]


def test_save_coverage_preserves_irish_accents_unescaped(tmp_path):
    dest = tmp_path / "coverage.json"
    save_coverage({"council": "Dún Laoghaire-Rathdown"}, dest)

    raw = dest.read_text(encoding="utf-8")
    assert "Dún" in raw  # ensure_ascii=False: no ú escapes


def test_save_coverage_creates_parent_dirs(tmp_path):
    dest = tmp_path / "gold" / "_meta" / "coverage.json"
    save_coverage({"ok": True}, dest)
    assert dest.exists()


def test_save_coverage_failure_leaves_previous_dest_untouched(tmp_path):
    """A payload json.dumps cannot serialise (circular ref beats default=str)
    must raise WITHOUT clobbering the previous good sidecar or leaving a .part."""
    dest = tmp_path / "coverage.json"
    save_coverage({"rows": 1}, dest)

    circular: dict = {}
    circular["self"] = circular
    with pytest.raises(ValueError):
        save_coverage(circular, dest)

    assert json.loads(dest.read_text(encoding="utf-8")) == {"rows": 1}  # untouched
    assert not (tmp_path / "coverage.json.part").exists()
