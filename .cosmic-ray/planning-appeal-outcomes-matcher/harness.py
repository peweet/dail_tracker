"""Cosmic Ray harness for _auth_key / _council_decision / _spatial_temporal_matches
(planning_appeal_outcomes.py), mirroring the siting-geometry/siting-registry-georeference pattern.

Runs against a COPY (matcher_target.py) so a killed or interrupted session can never leave the real
module mutated on disk. Plain asserts rather than pytest, because this executes once per mutant and
process startup dominates the runtime.

The _spatial_temporal_matches block is translated 1:1 from
test/planning/test_planning_appeal_outcomes_matcher.py (source of truth — refresh this translation
if it changes). The _auth_key/_council_decision blocks add direct assertions the pytest file does
not have: those two functions are only exercised INDIRECTLY there (both sides of a match use the
same _auth_key, so a mutation that stays internally consistent could still let a match through) —
without a direct check on their output, mutants inside them could survive despite the matcher tests
passing. Each _auth_key assertion is grounded in the function's own docstring (the "Cork County
Council - West Cork Section" fold is the one documented real-world case) or is a mechanical
consequence of lowercasing/stripping non-letters — none of this is invented test data.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import sys
from pathlib import Path

import polars as pl

TARGET = Path(__file__).with_name("matcher_target.py")
spec = importlib.util.spec_from_file_location("appeal_matcher_mutant", TARGET)
assert spec is not None and spec.loader is not None
m = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = m
spec.loader.exec_module(m)


# ── _auth_key ─────────────────────────────────────────────────────────────────────
assert m._auth_key("Cork County Council") == "corkcountycouncil"
assert m._auth_key("Cork County Council - West Cork Section") == m._auth_key("Cork County Council")
assert m._auth_key(None) == ""
assert m._auth_key("") == ""

# ── _council_decision ────────────────────────────────────────────────────────────
assert m._council_decision("Granted") == "GRANT"
assert m._council_decision("Granted-Conditional") == "GRANT"
assert m._council_decision("Refused") == "REFUSE"
assert m._council_decision("Withdrawn") == "OTHER"
assert m._council_decision(None) == "OTHER"


# ── _spatial_temporal_matches ────────────────────────────────────────────────────
_RESIDUAL_COLS = [
    "abp_case",
    "lon",
    "lat",
    "auth_key",
    "lodged_date",
    "abp_decision",
    "PLANINGATY",
    "CATEGORY",
    "DECIDED_ON",
]
_APPS_COLS = ["ApplicationNumber", "PlanningAuthority", "decision_normalised", "DecisionDate", "lon", "lat"]


def _residual(*, abp_case, lon, lat, lodged_date, authority="Cork County Council"):
    return pl.DataFrame(
        [[abp_case, lon, lat, m._auth_key(authority), lodged_date, "GRANT", authority, "PERMISSION", None]],
        schema=_RESIDUAL_COLS,
        orient="row",
    )


def _apps(rows):
    return pl.DataFrame(rows, schema=_APPS_COLS, orient="row")


# Real coordinates: ACP centroid for ABP-322540 vs. its true application (24/6036, ~89 m away,
# decided 2025-04-23) vs. three unrelated 2007 permissions on the same land (~52-63 m away).
residual = _residual(abp_case="322540", lon=-8.628061, lat=52.136956, lodged_date=dt.date(2025, 5, 15))
apps = _apps(
    [
        ["24/6036", "Cork County Council", "Granted-Conditional", dt.date(2025, 4, 23), -8.626905, 52.137364],
        ["07/55057", "Cork County Council", "Granted-Conditional", dt.date(2007, 9, 12), -8.627539, 52.137335],
        ["07/55006", "Cork County Council", "Granted-Conditional", None, -8.627539, 52.137335],
        ["07/55079", "Cork County Council", "Granted-Conditional", dt.date(2007, 12, 13), -8.627539, 52.137335],
    ]
)
out = m._spatial_temporal_matches(residual, apps)
assert out.height == 1
assert out["ApplicationNumber"][0] == "24/6036"
assert out["match_method"][0] == "spatial_temporal"

# Only a stale, decades-old decision sits nearby — no plausible candidate for a 2025 appeal, so the
# case should come back UNMATCHED, not silently pinned to the old permission.
residual = _residual(abp_case="999999", lon=-8.628061, lat=52.136956, lodged_date=dt.date(2025, 5, 15))
apps = _apps([["95/1", "Cork County Council", "Granted-Conditional", dt.date(1995, 1, 1), -8.628061, 52.136956]])
out = m._spatial_temporal_matches(residual, apps)
assert out.height == 0

# The common case: a genuinely close, recently-decided application still matches on the tight
# (validated) radius without needing the wide fallback.
residual = _residual(
    abp_case="321970",
    lon=-9.120162,
    lat=53.273865,
    lodged_date=dt.date(2025, 2, 25),
    authority="Galway City Council",
)
apps = _apps([["2460270", "Galway City Council", "Refused", dt.date(2025, 1, 29), -9.120500, 53.273900]])
out = m._spatial_temporal_matches(residual, apps)
assert out.height == 1
assert out["ApplicationNumber"][0] == "2460270"
