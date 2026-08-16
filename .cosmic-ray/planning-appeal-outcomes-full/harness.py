"""Cosmic Ray harness for the WHOLE planning_appeal_outcomes.py module (full-file scope, per user
request to widen from the narrow matcher-only session in .cosmic-ray/planning-appeal-outcomes-matcher/).

Runs against a COPY (outcomes_full_target.py) so a killed or interrupted session can never leave the
real module mutated on disk. Plain asserts rather than pytest, because this executes once per mutant
and process startup dominates the runtime.

The _spatial_temporal_matches block is translated 1:1 from
test/planning/test_planning_appeal_outcomes_matcher.py — the only file that calls into this module's
code. test/planning/test_planning_appeal_outcomes.py is a data-quality contract on the OUTPUT PARQUET
(skipped when that file is absent) and never calls this module, so it adds no assertions here.
_fetch_acp, _build_spine, _ms_to_date, _centroid, _ring_area, _case_status, _norm_abp and main have
NO test coverage: mutants inside them are expected to survive, and that survivor count is the real,
measured size of the gap this session widens to show.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import sys
from pathlib import Path

import polars as pl

TARGET = Path(__file__).with_name("outcomes_full_target.py")
spec = importlib.util.spec_from_file_location("appeal_outcomes_full_mutant", TARGET)
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

residual = _residual(abp_case="999999", lon=-8.628061, lat=52.136956, lodged_date=dt.date(2025, 5, 15))
apps = _apps([["95/1", "Cork County Council", "Granted-Conditional", dt.date(1995, 1, 1), -8.628061, 52.136956]])
out = m._spatial_temporal_matches(residual, apps)
assert out.height == 0

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
