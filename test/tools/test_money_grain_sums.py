"""Runs the money-grain lint (tools/check_money_grain_sums.py) in the fast suite.

The never-sum rule had metadata-level tests only (fact-card carriage, per-row
flags) — nothing checked the ACT of a cross-grain SUM/UNION landing in a new
sql_views/ file. Zero-baseline as of 2026-08-01: 250 views clean, 4 sanctioned
comparison views (side-by-side, never added) print as NOTEs.

Skips when sqlglot (mcp extra) is absent — CI's dev lane doesn't install it.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from tools import check_money_grain_sums


def test_no_cross_grain_sum_or_union(capsys):
    pytest.importorskip("sqlglot", reason="mcp extra not installed")
    rc = check_money_grain_sums.main()
    out = capsys.readouterr().out
    assert rc == 0, f"money-grain violations:\n{out}"
