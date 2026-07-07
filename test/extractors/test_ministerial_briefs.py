"""Invariants for the incoming-minister BRIEF corpus (extractors/ministerial_briefs_extract.py).

Tests the curated registry (the source of truth) + the built frame's schema/content — so a
careless edit (missing field, bad source_type, an empty department row, a non-gov.ie URL) fails
loudly. No network: the registry is curated; _auto_key_issues is best-effort over a cache that may
be absent (returns []), so these assertions key off the curated content.
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "extractors"))

import ministerial_briefs_extract as mb  # noqa: E402

_LIST_FIELDS = ("strategic_goals", "immediate_priorities", "machinery_of_government", "key_issue_areas")
_REQUIRED = ("department", "slug", "edition", "source_type", "source_url", *_LIST_FIELDS)


def test_registry_records_well_formed():
    assert len(mb.BRIEFS) >= 10
    for b in mb.BRIEFS:
        for k in _REQUIRED:
            assert k in b, f"{b.get('department')} missing {k}"
        assert b["source_type"] in {"born-digital", "scanned"}, b["department"]
        assert b["slug"].startswith("department-of-") or b["slug"].startswith("department-"), b["slug"]
        assert "gov.ie" in b["source_url"], b["department"]
        for f in _LIST_FIELDS:
            assert isinstance(b[f], list), f"{b['department']}.{f} not a list"


def test_no_duplicate_departments():
    slugs = [b["slug"] for b in mb.BRIEFS]
    assert len(slugs) == len(set(slugs)), "duplicate department slug"


def test_every_department_has_some_agenda_content():
    """No empty rows — each dept must carry goals OR priorities OR key issue areas."""
    empty = [
        b["department"]
        for b in mb.BRIEFS
        if not (b["strategic_goals"] or b["immediate_priorities"] or b["key_issue_areas"])
    ]
    assert not empty, f"departments with no agenda content: {empty}"


def test_flagship_content_present():
    by = {b["department"]: b for b in mb.BRIEFS}
    justice = next(v for k, v in by.items() if k.startswith("Justice"))
    assert justice["machinery_of_government"], "Justice MoG transfers must be present"
    assert any("D/CEDIY" in m or "CEDIY" in m for m in justice["machinery_of_government"]), "asylum transfer expected"
    decc = next(v for k, v in by.items() if k.startswith("Climate"))
    assert len(decc["strategic_goals"]) == 6, "DECC has 6 strategic goals"


def test_built_frame_schema(tmp_path, monkeypatch):
    """main() writes a frame with the expected columns + n_* counts, without touching real gold."""
    monkeypatch.setattr(mb, "OUT_PARQUET", tmp_path / "minister_briefs.parquet")
    monkeypatch.setattr(mb, "OUT_JSON", tmp_path / "minister_briefs.json")
    monkeypatch.setattr(mb, "OUT_DIR", tmp_path)
    mb.main()
    df = pl.read_parquet(tmp_path / "minister_briefs.parquet")
    for c in [
        "department",
        "source_type",
        "strategic_goals",
        "immediate_priorities",
        "machinery_of_government",
        "key_issue_areas",
        "n_strategic_goals",
        "n_priorities",
        "n_mog_changes",
    ]:
        assert c in df.columns, c
    assert df.height == len(mb.BRIEFS)
    # n_* counts agree with the list lengths
    row = df.filter(pl.col("department").str.starts_with("Climate")).to_dicts()[0]
    assert row["n_strategic_goals"] == len(row["strategic_goals"]) == 6


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
