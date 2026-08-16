# Known stale as of 2026-08-14: _spatial_temporal_matches is now a one-line wrapper around
# planning_appeal_vector.spatial_temporal_matches; this copy still carries its old body. The
# all-zero hash never matches a real file, so assert_fresh() always refuses this session until
# it's genuinely refreshed and rehashed with
# `python tools/mutation_session.py --rehash planning-appeal-outcomes-matcher`.
# COPIED-FROM: planning/civic/extractors/planning_appeal_outcomes.py @ sha256:0000000000000000000000000000000000000000000000000000000000000000
"""Cosmic Ray mutation target: _auth_key, _council_decision and _spatial_temporal_matches from
planning/civic/extractors/planning_appeal_outcomes.py.

A verbatim copy of just the tested seam — not the whole 519-line module. _fetch_acp, _build_spine
and main() do ArcGIS/filesystem I/O and have no unit test; the only file that tests real behaviour
is test/planning/test_planning_appeal_outcomes_matcher.py, which exercises _spatial_temporal_matches
(and, through it, _council_decision and _auth_key). test/planning/test_planning_appeal_outcomes.py
is a data-quality contract on the OUTPUT PARQUET, skipped when that file is absent — it never calls
into this module's code, so it cannot kill a mutant here. Refresh this copy from the real module
before any re-run.
"""

from __future__ import annotations

import datetime as dt
import re
import unicodedata

import polars as pl

_SPATIAL_DEG = 0.0006  # ~55 m at 53°N — primary radius (validated 98.4% vs Kerry ground truth)
_SPATIAL_DEG_WIDE = 0.0015  # ~150 m — tried only if the primary radius finds no plausibly-dated candidate
_GRID = 0.002  # grid-cell size for the spatial index (must be >= _SPATIAL_DEG_WIDE)
_MAX_LOOKBACK_YEARS = 5  # a decision this much older than the appeal lodgement cannot plausibly be
# the application under appeal — PDA 2000's default permission lifespan is 5 years.


def _auth_key(name: str | None) -> str:
    """Normalise a planning-authority name to a join key: drop any ' - … Section' suffix
    (only 'Cork County Council - West Cork Section' exists), strip accents/punctuation.
    Maps ACP's PLANINGATY onto the application feed's PlanningAuthority (e.g. West Cork
    folds into Cork County) so the spatial fallback only matches within the same council."""
    s = re.sub(r"\s*-\s*.*$", "", name or "")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z]", "", s.lower())


def _council_decision(d: str | None) -> str:
    if d in ("Granted", "Granted-Conditional"):
        return "GRANT"
    return "REFUSE" if d == "Refused" else "OTHER"


def _spatial_temporal_matches(residual: pl.DataFrame, apps: pl.DataFrame) -> pl.DataFrame:
    """For ACP cases the ref join missed, match each to the nearest application of the SAME
    authority whose decision pre-dates the appeal lodgement (most recent such). Returns rows
    in the _OUT_COLS schema, tagged match_method='spatial_temporal'."""
    cand = apps.filter(pl.col("lon").is_not_null() & pl.col("lat").is_not_null()).with_columns(
        pl.col("PlanningAuthority").map_elements(_auth_key, return_dtype=pl.Utf8).alias("auth_key")
    )
    # spatial index: (auth_key, rounded lat, rounded lon) → list of candidate application rows
    grid: dict[tuple, list[dict]] = {}
    cols = {
        c: cand[c].to_list()
        for c in (
            "auth_key",
            "lat",
            "lon",
            "ApplicationNumber",
            "PlanningAuthority",
            "decision_normalised",
            "DecisionDate",
        )
    }
    for i in range(cand.height):
        key = (cols["auth_key"][i], round(cols["lat"][i] / _GRID), round(cols["lon"][i] / _GRID))
        grid.setdefault(key, []).append({c: cols[c][i] for c in cols})

    def _best_within(cell_pool: list[dict], lat: float, lon: float, lodged: dt.date, radius: float) -> dict | None:
        trimmed = [a for a in cell_pool if abs(a["lat"] - lat) <= radius and abs(a["lon"] - lon) <= radius]
        cutoff = lodged - dt.timedelta(days=365 * _MAX_LOOKBACK_YEARS)
        before = [a for a in trimmed if a["DecisionDate"] is not None and cutoff <= a["DecisionDate"] <= lodged]
        return max(before, key=lambda a: a["DecisionDate"]) if before else None

    def nearest(ak: str, lat: float, lon: float, lodged: dt.date | None) -> dict | None:
        gl, go = round(lat / _GRID), round(lon / _GRID)
        cell_pool = [a for dl in (-1, 0, 1) for dd in (-1, 0, 1) for a in grid.get((ak, gl + dl, go + dd), [])]
        if not cell_pool:
            return None
        if lodged is not None:
            # tight radius first (the validated rule) — only widen if it finds no plausible candidate.
            return _best_within(cell_pool, lat, lon, lodged, _SPATIAL_DEG) or _best_within(
                cell_pool, lat, lon, lodged, _SPATIAL_DEG_WIDE
            )
        pool = [a for a in cell_pool if abs(a["lat"] - lat) <= _SPATIAL_DEG and abs(a["lon"] - lon) <= _SPATIAL_DEG]
        return min(pool, key=lambda a: (a["lat"] - lat) ** 2 + (a["lon"] - lon) ** 2) if pool else None

    out = []
    rc = {
        c: residual[c].to_list()
        for c in (
            "auth_key",
            "lat",
            "lon",
            "lodged_date",
            "abp_case",
            "abp_decision",
            "PLANINGATY",
            "CATEGORY",
            "DECIDED_ON",
        )
    }
    for i in range(residual.height):
        m = nearest(rc["auth_key"][i], rc["lat"][i], rc["lon"][i], rc["lodged_date"][i])
        if not m:
            continue
        out.append(
            {
                "ApplicationNumber": m["ApplicationNumber"],
                "PlanningAuthority": m["PlanningAuthority"],
                "decision_normalised": m["decision_normalised"],
                "AppealRefNumber": None,
                "abp_case": rc["abp_case"][i],
                "council_decision": _council_decision(m["decision_normalised"]),
                "abp_decision": rc["abp_decision"][i],
                "PLANINGATY": rc["PLANINGATY"][i],
                "CATEGORY": rc["CATEGORY"][i],
                "DECIDED_ON": rc["DECIDED_ON"][i],
                "match_method": "spatial_temporal",
            }
        )
    schema = {
        "ApplicationNumber": pl.Utf8,
        "PlanningAuthority": pl.Utf8,
        "decision_normalised": pl.Utf8,
        "AppealRefNumber": pl.Utf8,
        "abp_case": pl.Utf8,
        "council_decision": pl.Utf8,
        "abp_decision": pl.Utf8,
        "PLANINGATY": pl.Utf8,
        "CATEGORY": pl.Utf8,
        "DECIDED_ON": pl.Int64,
        "match_method": pl.Utf8,
    }
    return pl.DataFrame(out, schema=schema) if out else pl.DataFrame(schema=schema)
