"""Cosmic Ray harness for planning_appeal_vector.py, mirroring the siting-geometry /
siting-registry-georeference pattern.

Runs against a COPY (vector_target.py) so a killed or interrupted session can never leave the real
module mutated on disk. Plain asserts rather than pytest, because this executes once per mutant and
process startup dominates the runtime. Each assertion is translated 1:1 from
test/planning/test_planning_appeal_vector.py (the four scalar-expression tests) and
test/planning/test_planning_appeal_outcomes_matcher.py (the 13 spatial_temporal_matches cases) --
those files are the source of truth; refresh this translation if they change. Expected values are
hardcoded rather than computed via the live extractor's scalar helpers, so the harness does not
depend on any unmutated import outside the copy under test.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import re
import sys
import unicodedata
from pathlib import Path

import polars as pl

TARGET = Path(__file__).with_name("vector_target.py")
spec = importlib.util.spec_from_file_location("planning_appeal_vector_mutant", TARGET)
assert spec is not None and spec.loader is not None
m = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = m
spec.loader.exec_module(m)


# ── fixture-only helper (not part of the target; mirrors _auth_key in planning_appeal_outcomes.py,
#    which the real test file imports to build residual rows ahead of the spatial join) ──────────
def _auth_key(name: str | None) -> str:
    s = re.sub(r"\s*-\s*.*$", "", name or "")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z]", "", s.lower())


assert m.OUT_COLS == [
    "ApplicationNumber",
    "PlanningAuthority",
    "decision_normalised",
    "AppealRefNumber",
    "abp_case",
    "council_decision",
    "abp_decision",
    "PLANINGATY",
    "CATEGORY",
    "DECIDED_ON",
    "match_method",
]

# ── scalar expressions (test_planning_appeal_vector.py) ─────────────────────────────────────────
rows = pl.DataFrame(
    {
        "authority": ["Cork County Council - West Cork Section", "Dún Laoghaire Council", None, "Test"],
        "decision": ["Granted-Conditional", "Refused", "Other", "Zed"],
        "appeal": ["ABP-312345-22", "no case", None, "123456"],
        "ms": [0, 1_709_164_800_000, None, 86_400_000],
    }
)
out = rows.select(
    m.authority_key_expr("authority").alias("authority"),
    m.council_decision_expr("decision").alias("decision"),
    m.appeal_case_expr("appeal").alias("appeal"),
    m.epoch_ms_date_expr("ms").alias("date"),
)
assert out["authority"].to_list() == ["corkcountycouncil", "dunlaoghairecouncil", "", "test"]
assert out["decision"].to_list() == ["GRANT", "REFUSE", "OTHER", "OTHER"]
assert out["appeal"].to_list() == ["312345", None, None, "123456"]
assert out["date"].to_list() == [dt.date(1970, 1, 1), dt.date(2024, 2, 29), None, dt.date(1970, 1, 2)]

status_frame = pl.DataFrame(
    {
        "decision": ["Case is due to be decided by 01/01/2020", "Permission granted", None],
        "decided": [1_577_836_800_000, 1_577_836_800_000, None],
    }
)
assert status_frame.select(m.case_status_expr("decision", "decided").alias("status"))["status"].to_list() == [
    "live",
    "decided",
    "live",
]

epoch_frame = pl.DataFrame({"ms": [-1, 0, 86_400_000, None]}, schema={"ms": pl.Int64})
assert epoch_frame.select(m.epoch_ms_date_expr("ms"))["ms"].to_list() == [
    dt.date(1969, 12, 31),
    dt.date(1970, 1, 1),
    dt.date(1970, 1, 2),
    None,
]


# ── spatial_temporal_matches (test_planning_appeal_outcomes_matcher.py) ─────────────────────────
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
        [[abp_case, lon, lat, _auth_key(authority), lodged_date, "GRANT", authority, "PERMISSION", None]],
        schema=_RESIDUAL_COLS,
        orient="row",
    )


def _apps(row_list):
    return pl.DataFrame(row_list, schema=_APPS_COLS, orient="row")


# prefers a wider but recent match over a close but ancient one
residual = _residual(abp_case="322540", lon=-8.628061, lat=52.136956, lodged_date=dt.date(2025, 5, 15))
apps = _apps(
    [
        ["24/6036", "Cork County Council", "Granted-Conditional", dt.date(2025, 4, 23), -8.626905, 52.137364],
        ["07/55057", "Cork County Council", "Granted-Conditional", dt.date(2007, 9, 12), -8.627539, 52.137335],
        ["07/55006", "Cork County Council", "Granted-Conditional", None, -8.627539, 52.137335],
        ["07/55079", "Cork County Council", "Granted-Conditional", dt.date(2007, 12, 13), -8.627539, 52.137335],
    ]
)
out = m.spatial_temporal_matches(residual, apps)
assert out.height == 1
assert out["ApplicationNumber"][0] == "24/6036"
assert out["match_method"][0] == "spatial_temporal"

# returns no match rather than an implausibly old one
residual = _residual(abp_case="999999", lon=-8.628061, lat=52.136956, lodged_date=dt.date(2025, 5, 15))
apps = _apps([["95/1", "Cork County Council", "Granted-Conditional", dt.date(1995, 1, 1), -8.628061, 52.136956]])
assert m.spatial_temporal_matches(residual, apps).height == 0

# still matches within the tight radius when recent
residual = _residual(
    abp_case="321970", lon=-9.120162, lat=53.273865, lodged_date=dt.date(2025, 2, 25), authority="Galway City Council"
)
apps = _apps([["2460270", "Galway City Council", "Refused", dt.date(2025, 1, 29), -9.120500, 53.273900]])
out = m.spatial_temporal_matches(residual, apps)
assert out.height == 1
assert out["ApplicationNumber"][0] == "2460270"

# tight candidate wins before a newer wide candidate
residual = _residual(abp_case="400001", lon=-8.0, lat=53.0, lodged_date=dt.date(2025, 6, 1))
apps = _apps(
    [
        ["TIGHT", "Cork County Council", "Refused", dt.date(2025, 4, 1), -8.0003, 53.0003],
        ["WIDE", "Cork County Council", "Granted", dt.date(2025, 5, 20), -8.0010, 53.0010],
    ]
)
assert m.spatial_temporal_matches(residual, apps)["ApplicationNumber"].item() == "TIGHT"

# same-date tie keeps legacy neighbour-then-source order
residual = _residual(abp_case="400002", lon=-8.0008, lat=53.0008, lodged_date=dt.date(2025, 6, 1))
apps = _apps(
    [
        ["CENTER", "Cork County Council", "Granted", dt.date(2025, 5, 1), -8.0007, 53.0007],
        ["NEIGHBOUR", "Cork County Council", "Refused", dt.date(2025, 5, 1), -8.0013, 53.0003],
    ]
)
assert m.spatial_temporal_matches(residual, apps)["ApplicationNumber"].item() == "NEIGHBOUR"

# different authority and post-lodgement rows are excluded
residual = _residual(abp_case="400003", lon=-8.0, lat=53.0, lodged_date=dt.date(2025, 6, 1))
apps = _apps(
    [
        ["WRONG-AUTH", "Galway City Council", "Granted", dt.date(2025, 5, 1), -8.0, 53.0],
        ["TOO-LATE", "Cork County Council", "Granted", dt.date(2025, 6, 2), -8.0, 53.0],
    ]
)
assert m.spatial_temporal_matches(residual, apps).is_empty()

# undated appeal selects the geometrically nearest tight candidate
residual = _residual(abp_case="400004", lon=-8.0, lat=53.0, lodged_date=None)
apps = _apps(
    [
        ["FARTHER", "Cork County Council", "Granted", dt.date(2025, 5, 1), -8.0004, 53.0004],
        ["NEAREST", "Cork County Council", "Refused", dt.date(1990, 1, 1), -8.0001, 53.0001],
    ]
)
assert m.spatial_temporal_matches(residual, apps)["ApplicationNumber"].item() == "NEAREST"

# exact date and radius boundaries are inclusive
lodged = dt.date(2025, 6, 1)
cutoff = lodged - dt.timedelta(days=365 * 5)
residual = _residual(abp_case="400005", lon=0.0, lat=0.0, lodged_date=lodged)
apps_at_cutoff = _apps([["AT-LIMITS", "Cork County Council", "Granted", cutoff, 0.0015, 0.0015]])
assert m.spatial_temporal_matches(residual, apps_at_cutoff)["ApplicationNumber"].item() == "AT-LIMITS"
apps_on_lodgement = _apps([["ON-LODGEMENT", "Cork County Council", "Refused", lodged, 0.0006, 0.0006]])
assert m.spatial_temporal_matches(residual, apps_on_lodgement)["ApplicationNumber"].item() == "ON-LODGEMENT"

# one step outside the date or wide radius is excluded
lodged = dt.date(2025, 6, 1)
residual = _residual(abp_case="400006", lon=0.0, lat=0.0, lodged_date=lodged)
rows2 = _apps(
    [
        ["TOO-OLD", "Cork County Council", "Granted", lodged - dt.timedelta(days=365 * 5 + 1), 0.0, 0.0],
        ["TOO-WIDE", "Cork County Council", "Granted", lodged, 0.001501, 0.0],
    ]
)
assert m.spatial_temporal_matches(residual, rows2).is_empty()

# undated nearest uses both coordinate deltas
residual = _residual(abp_case="400007", lon=-8.0, lat=53.0, lodged_date=None)
rows2 = _apps(
    [
        ["LON-HEAVY", "Cork County Council", "Granted", dt.date(2025, 1, 1), -8.0005, 53.0001],
        ["BALANCED", "Cork County Council", "Granted", dt.date(2025, 1, 1), -8.0003, 53.0003],
    ]
)
assert m.spatial_temporal_matches(residual, rows2)["ApplicationNumber"].item() == "BALANCED"

# exact tight boundary stays in the tight band, and is included when undated
lodged = dt.date(2025, 6, 1)
dated = m.spatial_temporal_matches(
    _residual(abp_case="400008", lon=0.0, lat=0.0, lodged_date=lodged),
    _apps(
        [
            ["TIGHT-EXACT", "Cork County Council", "Refused", dt.date(2025, 4, 1), 0.0006, 0.0006],
            ["WIDE-NEWER", "Cork County Council", "Granted", dt.date(2025, 5, 20), 0.0010, 0.0010],
        ]
    ),
)
assert dated["ApplicationNumber"].item() == "TIGHT-EXACT"
undated = m.spatial_temporal_matches(
    _residual(abp_case="400009", lon=0.0, lat=0.0, lodged_date=None),
    _apps([["UNDATED-EXACT", "Cork County Council", "Refused", None, 0.0006, 0.0006]]),
)
assert undated["ApplicationNumber"].item() == "UNDATED-EXACT"

# either empty input returns a typed empty output
residual = _residual(abp_case="400010", lon=0.0, lat=0.0, lodged_date=dt.date(2025, 6, 1))
application = _apps([["A", "Cork County Council", "Granted", dt.date(2025, 5, 1), 0.0, 0.0]])
assert m.spatial_temporal_matches(residual.head(0), application).is_empty()
assert m.spatial_temporal_matches(residual, application.head(0)).is_empty()

# candidate in each of the 8 neighbouring grid cells is still found (GRID=0.002, boundary at 0.001,
# each offset below crosses it while staying inside SPATIAL_DEG_WIDE=0.0015)
for direction, dlat, dlon in [
    ("N", 0.0012, 0.0),
    ("S", -0.0012, 0.0),
    ("E", 0.0, 0.0012),
    ("W", 0.0, -0.0012),
    ("NE", 0.00105, 0.00105),
    ("NW", 0.00105, -0.00105),
    ("SE", -0.00105, 0.00105),
    ("SW", -0.00105, -0.00105),
]:
    residual = _residual(abp_case=f"grid-{direction}", lon=0.0, lat=0.0, lodged_date=dt.date(2025, 6, 1))
    apps = _apps([[f"APP-{direction}", "Cork County Council", "Granted", dt.date(2025, 5, 1), dlon, dlat]])
    out = m.spatial_temporal_matches(residual, apps)
    assert out["ApplicationNumber"].to_list() == [f"APP-{direction}"]

# most recent pre-dating decision wins within the same radius band
residual = _residual(abp_case="date-recency", lon=-8.0, lat=53.0, lodged_date=dt.date(2025, 6, 1))
apps = _apps(
    [
        ["OLDER", "Cork County Council", "Granted", dt.date(2024, 1, 1), -8.0001, 53.0001],
        ["NEWER", "Cork County Council", "Granted", dt.date(2025, 1, 1), -8.0002, 53.0002],
    ]
)
assert m.spatial_temporal_matches(residual, apps)["ApplicationNumber"].item() == "NEWER"

# full tie (same date, same band, same neighbour cell) falls back to source application order
residual = _residual(abp_case="app-order", lon=-8.0, lat=53.0, lodged_date=dt.date(2025, 6, 1))
apps = _apps(
    [
        ["FIRST", "Cork County Council", "Granted", dt.date(2025, 5, 1), -8.0001, 53.0001],
        ["SECOND", "Cork County Council", "Granted", dt.date(2025, 5, 1), -8.0001, 53.0001],
    ]
)
assert m.spatial_temporal_matches(residual, apps)["ApplicationNumber"].item() == "FIRST"
