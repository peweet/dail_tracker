"""Runs the convention ratchet (tools/check_conventions.py) in the fast suite.

The ratchet is what keeps the 2026-07 shared-utility consolidation from
regrowing: new extractors must use http_engine/coverage_io/save_parquet/
run_extractor, pages must not re-clone ui.format helpers, and every page
entry must carry @dt_page. Grandfathered offenders live in the tool's
baselines; this test fails on any NEW occurrence.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from tools import check_conventions


def test_conventions_ratchet_holds(capsys):
    rc = check_conventions.main()
    out = capsys.readouterr().out
    assert rc == 0, f"convention violations:\n{out}"


def test_parquet_scan_skips_nested_environment_but_checks_product_source(tmp_path, monkeypatch):
    product = tmp_path / "planning" / "product"
    library = product / ".venv" / "Lib" / "site-packages" / "example.py"
    library.parent.mkdir(parents=True)
    library.write_text("frame.write_parquet('library.parquet')", encoding="utf-8")
    source = product / "core" / "example.py"
    source.parent.mkdir()
    source.write_text("frame.write_parquet('product.parquet')", encoding="utf-8")
    monkeypatch.setattr(check_conventions, "ROOT", tmp_path)
    monkeypatch.setattr(check_conventions, "EXTRACTOR_DIRS", ())
    violations = check_conventions._raw_parquet_outside_extractors()
    assert len(violations) == 1
    assert violations[0].startswith("planning/product/core/example.py")
