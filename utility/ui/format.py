"""Canonical display formatting for the UI layer — one €-formatter instead of eight.

Before this module, 8+ pages carried private near-clones of the same helpers
(``_eur``, ``_eur_scale``, ``_eur_full``, ``_esc``, ``_truthy``, ``_coalesce``…)
with silently divergent rounding and NA handling — e.g. procurement's ``_eur``
dashed ``n <= 0`` while its own ``_eur_scale`` printed ``€0``; constituency
showed ``€1.44bn`` where accommodation showed ``€1.4bn`` for the same value.
That drift is a correctness risk in a civic app: the same figure must read the
same everywhere.

Display-only, logic-firewall-clean: pure value→string functions, no queries,
no aggregation. Pages keep their private aliases (``_eur = eur``) so call
sites stay short; new pages should import from here instead of redefining.

Canonical € presentation (standardised 2026-07-17):
  ≥ €1bn   →  €1.08bn / €23.5bn / €2bn   (up to 2 dp, trailing zeros stripped —
                                          0.01bn is €10m, too material to round away)
  ≥ €1m    →  €4.2m    (1 dp)
  ≥ €1k    →  €345k    (0 dp)
  < €1k    →  €950     (comma-grouped)
  NA / unparseable → —
Negative values keep their sign and scale by magnitude (AFS income lines are
legitimately negative). Registers where a non-positive amount means "missing"
(procurement awards, CBI fines) pass ``dash_nonpositive=True``.
"""

from __future__ import annotations

import html

import pandas as pd


def _na(val) -> bool:
    """True for None/NaN/NaT — the values that must render as an em-dash."""
    if val is None:
        return True
    try:
        return bool(pd.isna(val))
    except (TypeError, ValueError):
        # pd.isna on list-likes raises/returns arrays — treat as present
        return False


def eur(val, *, dash_nonpositive: bool = False) -> str:
    """Compact euro label: €1.4bn / €4.2m / €345k / €1,234 / — .

    ``dash_nonpositive=True`` renders zero and negative as ``—`` (award/fine
    registers where a non-positive amount means "not disclosed", not "zero").
    """
    if _na(val):
        return "—"
    try:
        n = float(val)
    except (TypeError, ValueError):
        return "—"
    if dash_nonpositive and n <= 0:
        return "—"
    a = abs(n)
    if a >= 1_000_000_000:
        # up to 2 dp, trailing zeros stripped: €1.08bn / €23.5bn / €2bn.
        # 0.01bn = €10m — material at headline scale, so not rounded away.
        s = f"{n / 1_000_000_000:.2f}".rstrip("0").rstrip(".")
        return f"€{s}bn"
    if a >= 1_000_000:
        return f"€{n / 1_000_000:.1f}m"
    if a >= 1_000:
        return f"€{n / 1_000:.0f}k"
    return f"€{n:,.0f}"


def eur_full(val, *, dash_zero: bool = False) -> str:
    """Full comma-delimited euro: €113,863,982. ``dash_zero=True`` dashes 0 too."""
    if _na(val):
        return "—"
    try:
        n = float(val)
    except (TypeError, ValueError):
        return "—"
    if dash_zero and n == 0:
        return "—"
    return f"€{n:,.0f}"


def pct(val, dp: int = 0) -> str:
    """Percentage label: 42% / 3.5% / — ."""
    if _na(val):
        return "—"
    try:
        return f"{float(val):.{dp}f}%"
    except (TypeError, ValueError):
        return "—"


def fmt_int(val) -> str:
    """Comma-grouped integer for display: 4,958 / — ."""
    if _na(val):
        return "—"
    try:
        return f"{int(val):,}"
    except (TypeError, ValueError):
        return "—"


def to_int(val) -> int:
    """Coerce a possibly-NA cell to int, 0 on failure (counting contexts)."""
    if _na(val):
        return 0
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


def esc(val) -> str:
    """HTML-escape a possibly-NA cell; NA renders as '' (safe inside st.html)."""
    if _na(val):
        return ""
    return html.escape(str(val))


def truthy(val) -> bool:
    """Safe truthiness for possibly-NA pandas cells — ``bool(pd.NA)`` raises."""
    if _na(val):
        return False
    return bool(val)


def coalesce(*vals) -> str:
    """First non-NA, non-empty value as a stripped string, else ''. Avoids the
    ``pd.NA or x`` truthiness error when coalescing nullable columns."""
    for v in vals:
        if _na(v):
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def fmt_civic_date(val) -> str:
    """Civic date: ``7 Jul 2024`` (no leading zero, platform-independent).

    Canonical home of the formatter previously in ``ui.components`` (which
    re-exports it). ``—`` for None/NaN/empty; unparseable values pass through
    as ``str(val)`` so already-formatted labels don't blow up.
    """
    if _na(val) or val == "":
        return "—"
    try:
        ts = pd.Timestamp(val)
        return f"{ts.day} {ts.strftime('%b %Y')}"
    except Exception:
        return str(val)


def fmt_month(val) -> str:
    """Month-year label: ``Sep 2025`` from an ISO date/period ('2025-09-01',
    '2025-09'). ``—`` for None/NaN/empty; unparseable passes through."""
    if _na(val) or val == "" or val == "None":
        return "—"
    try:
        return pd.Timestamp(val).strftime("%b %Y")
    except Exception:
        return str(val)
