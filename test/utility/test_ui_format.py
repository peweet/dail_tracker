"""Parity + behaviour tests for utility/ui/format.py — the canonical display
formatters that replaced 8+ page-local ``_eur``/``_fmt_*`` clones (2026-07-17).

Two layers:
  1. PARITY — the old page-local implementations are embedded verbatim below
     and compared against the canonical functions over a value grid, on the
     domain where they were meant to agree. This proves the migration did not
     silently change what pages render.
  2. DOCUMENTED DIVERGENCES — where the old copies disagreed WITH EACH OTHER
     (the drift that motivated consolidation), the canonical behaviour is
     asserted explicitly so the standardisation is a visible, tested decision:
       - ≥ €1bn always renders as €X.YZbn, trailing zeros stripped
         (procurement/lobbying's _eur had no bn branch → "€1234.5m";
         _eur_scale forced 1 dp → "€1.0bn" for exactly €1bn).
       - NaN always renders as — (old _eur_scale printed "€nan").
       - negative amounts scale by magnitude ("€-5.0m", not "€-5,000,000").
       - m always at 1 dp (constituency used .0m, local_government .2m); bn
         keeps up to 2 dp because 0.01bn = €10m is material (the accommodation
         page's €1.08bn headline must not round to €1.1bn).
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "utility"))
from ui.format import (  # noqa: E402
    coalesce,
    esc,
    eur,
    eur_full,
    fmt_civic_date,
    fmt_int,
    fmt_month,
    pct,
    to_int,
    truthy,
)

# ---------------------------------------------------------------------------
# Old page-local implementations, verbatim (pre-consolidation), as parity oracles
# ---------------------------------------------------------------------------


def _old_procurement_eur(val) -> str:  # procurement.py / public_payments.py / corporate._fine_eur
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    try:
        n = float(val)
    except (TypeError, ValueError):
        return "—"
    if n <= 0:
        return "—"
    if n >= 1_000_000:
        return f"€{n / 1_000_000:.1f}m"
    if n >= 1_000:
        return f"€{n / 1_000:.0f}k"
    return f"€{n:,.0f}"


def _old_eur_scale(val) -> str:  # procurement.py / public_payments.py
    try:
        n = float(val)
    except (TypeError, ValueError):
        return "—"
    if n >= 1_000_000_000:
        return f"€{n / 1_000_000_000:.1f}bn"
    if n >= 1_000_000:
        return f"€{n / 1_000_000:.1f}m"
    if n >= 1_000:
        return f"€{n / 1_000:.0f}k"
    return f"€{n:,.0f}"


def _old_lobbying_eur(v) -> str:  # lobbying_3.py
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "—"
    if abs(v) >= 1e6:
        return f"€{v / 1e6:,.1f}m"
    if abs(v) >= 1e3:
        return f"€{v / 1e3:,.0f}k"
    return f"€{v:,.0f}"


# Positive sub-billion grid: the domain every old copy was written for.
PARITY_GRID = [0.01, 1, 950, 999.4, 1_000, 1_500, 19_999, 345_000, 999_499, 1_000_000, 4_230_000, 85_000_000, 999_949_999]


@pytest.mark.parametrize("v", PARITY_GRID)
def test_eur_parity_with_procurement_eur_positive_sub_billion(v):
    assert eur(v, dash_nonpositive=True) == _old_procurement_eur(v)


@pytest.mark.parametrize("v", [*PARITY_GRID, 0, -1, -50_000, None, "not a number"])
def test_eur_dash_nonpositive_parity_including_edge_values(v):
    assert eur(v, dash_nonpositive=True) == _old_procurement_eur(v)


@pytest.mark.parametrize("v", [*PARITY_GRID, 0, 23_500_000_000, None, "x"])
def test_eur_parity_with_eur_scale_nonnegative(v):
    # sub-€1bn plus any bn value whose 2nd decimal is zero — the shared domain
    assert eur(v) == _old_eur_scale(v)


@pytest.mark.parametrize("v", [*PARITY_GRID, 0, -1, -950, None, "x"])
def test_eur_parity_with_lobbying_eur_sub_billion(v):
    # lobbying's copy formatted sub-€1k negatives in full — identical domain here
    assert eur(v) == _old_lobbying_eur(v)


# ---------------------------------------------------------------------------
# Documented divergences — the standardisation decisions, asserted explicitly
# ---------------------------------------------------------------------------


def test_billion_always_scales_to_bn_unlike_old_eur():
    assert _old_procurement_eur(2_300_000_000) == "€2300.0m"  # the drift being killed
    assert eur(2_300_000_000, dash_nonpositive=True) == "€2.3bn"


def test_nan_renders_dash_unlike_old_eur_scale():
    assert _old_eur_scale(float("nan")) == "€nan"  # the drift being killed
    assert eur(float("nan")) == "—"
    assert eur(pd.NA) == "—"
    assert eur(pd.NaT) == "—"


def test_negative_amounts_scale_by_magnitude():
    assert _old_eur_scale(-5_000_000) == "€-5,000,000"  # the drift being killed
    assert eur(-5_000_000) == "€-5.0m"  # AFS income lines stay readable
    assert eur(-1_200) == "€-1k"
    assert eur(-950) == "€-950"


def test_bn_keeps_material_precision_m_standardised_to_one_decimal():
    assert eur(1_080_000_000) == "€1.08bn"  # accommodation headline: €30m must not vanish
    assert eur(1_440_000_000) == "€1.44bn"
    assert eur(23_500_000_000) == "€23.5bn"  # trailing zero stripped
    assert eur(2_000_000_000) == "€2bn"  # old _eur_scale said €2.0bn
    # constituency showed €478m (.0f); local_government €477.60m (.2f) — now one shape:
    assert eur(477_600_000) == "€477.6m"


# ---------------------------------------------------------------------------
# eur_full / pct / fmt_int / to_int
# ---------------------------------------------------------------------------


def test_eur_full():
    assert eur_full(113_863_982) == "€113,863,982"
    assert eur_full(0) == "€0"
    assert eur_full(0, dash_zero=True) == "—"  # accommodation_spend behaviour
    assert eur_full(None) == "—"
    assert eur_full(float("nan")) == "—"


def test_pct():
    assert pct(42) == "42%"
    assert pct(3.456, dp=1) == "3.5%"
    assert pct(None) == "—"


def test_fmt_int_and_to_int():
    assert fmt_int(4958) == "4,958"
    assert fmt_int(None) == "—"
    assert fmt_int("x") == "—"
    assert to_int("7") == 7
    assert to_int(None) == 0
    assert to_int(pd.NA) == 0
    assert to_int("x") == 0


# ---------------------------------------------------------------------------
# esc / truthy / coalesce — NA-safe cell helpers
# ---------------------------------------------------------------------------


def test_esc_escapes_and_dashes_nothing():
    assert esc('<b>"x"</b>') == "&lt;b&gt;&quot;x&quot;&lt;/b&gt;"
    assert esc(None) == ""
    assert esc(float("nan")) == ""


def test_truthy_handles_pd_na_without_raising():
    assert truthy(pd.NA) is False  # bool(pd.NA) raises — the reason this exists
    assert truthy(None) is False
    assert truthy(0) is False
    assert truthy("x") is True


def test_coalesce_skips_na_and_blank():
    assert coalesce(None, pd.NA, "  ", "first", "second") == "first"
    assert coalesce(None, math.nan) == ""


# ---------------------------------------------------------------------------
# Dates
# ---------------------------------------------------------------------------


def test_fmt_civic_date():
    assert fmt_civic_date("2024-07-07") == "7 Jul 2024"  # no leading zero
    assert fmt_civic_date("2026-06-18") == "18 Jun 2026"  # committees._fmt_meeting_date parity
    assert fmt_civic_date(None) == "—"
    assert fmt_civic_date("") == "—"
    assert fmt_civic_date("already formatted") == "already formatted"


def test_fmt_month():
    assert fmt_month("2025-09-01") == "Sep 2025"  # lobbying._fmt_period parity
    assert fmt_month("2025-09") == "Sep 2025"
    assert fmt_month(None) == "—"
    assert fmt_month("None") == "—"
    assert fmt_month("") == "—"
