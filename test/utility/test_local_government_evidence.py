"""Bare-mode smoke test for the consolidated council evidence dossier."""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
for path in (ROOT, ROOT / "utility", ROOT / "utility/pages_code"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from utility.pages_code import local_government  # noqa: E402

REQUIRED = (
    ROOT / "data/silver/parquet/la_afs_capital_divisions.parquet",
    ROOT / "data/gold/parquet/council_minutes_corpus.parquet",
    ROOT / "data/gold/parquet/council_ce_reports_corpus.parquet",
    ROOT / "data/gold/parquet/council_ce_report_leads.parquet",
)


@pytest.mark.integration
@pytest.mark.skipif(not all(path.exists() for path in REQUIRED), reason="council evidence data absent")
def test_kildare_dossier_with_all_three_evidence_tabs_renders_without_exception():
    warnings.filterwarnings("ignore", message="No runtime found")
    warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
    warnings.filterwarnings("ignore", message=".*to view a Streamlit app.*")
    assert local_government._render_dossier("Kildare") is None
