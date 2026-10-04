"""Build-logic tests for planning_decision_profiles.py — no network, no built parquet needed.

Guards the three completeness fixes of 2026-09-29: a short ArcGIS page no longer ends a
designation pull, a pull that doesn't reconcile to returnCountOnly stops the build, and the
receipt's per-council depth and impossible-latency counts are read straight from the register.
"""

from __future__ import annotations

import datetime as dt

import pytest

pl = pytest.importorskip("polars")
pytest.importorskip("shapely")

from planning.civic.extractors import planning_decision_profiles as profiles  # noqa: E402

INSIDE = {"type": "Polygon", "coordinates": [[[-8.0, 53.0], [-7.9, 53.0], [-7.9, 53.1], [-8.0, 53.1], [-8.0, 53.0]]]}
OUTSIDE = {"type": "Polygon", "coordinates": [[[20.0, 53.0], [20.1, 53.0], [20.1, 53.1], [20.0, 53.1], [20.0, 53.0]]]}


def _feature(geometry=INSIDE):
    return {"type": "Feature", "geometry": geometry, "properties": {}}


def _fake_fetch(count, pages_by_offset, *, ignore_offset=False):
    """ArcGIS stand-in: returnCountOnly -> count; otherwise the page stored at resultOffset."""

    def fake(url, timeout=None, *, params=None, **_):
        if params.get("returnCountOnly") == "true":
            return {"count": count}, 200
        offset = 0 if ignore_offset else params["resultOffset"]
        return {"type": "FeatureCollection", "features": pages_by_offset.get(offset, [])}, 200

    return fake


def test_short_page_is_not_the_end_of_the_layer(monkeypatch):
    # The old loop stopped on len(page) < 2000 and would have kept only the first 2 of 3.
    pages = {0: [_feature(), _feature()], 2: [_feature()]}
    monkeypatch.setattr(profiles, "fetch_json", _fake_fetch(3, pages))
    polys, stats = profiles._fetch_polys("layer")
    assert len(polys) == 3
    assert stats == {"expected": 3, "fetched": 3, "no_geometry": 0, "out_of_bounds": 0, "kept": 3}


def test_pull_short_of_the_layer_count_stops_the_build(monkeypatch):
    monkeypatch.setattr(profiles, "fetch_json", _fake_fetch(5, {0: [_feature(), _feature()]}))
    with pytest.raises(profiles.SourceCountError, match="fetched 2 features, layer reports 5"):
        profiles._fetch_polys("layer")


def test_server_ignoring_offset_fails_instead_of_looping(monkeypatch):
    fake = _fake_fetch(3, {0: [_feature(), _feature()]}, ignore_offset=True)
    monkeypatch.setattr(profiles, "fetch_json", fake)
    with pytest.raises(profiles.SourceCountError):
        profiles._fetch_polys("layer")


def test_dropped_geometry_is_counted_not_hidden(monkeypatch):
    pages = {0: [_feature(), _feature(None), _feature(OUTSIDE)]}
    monkeypatch.setattr(profiles, "fetch_json", _fake_fetch(3, pages))
    polys, stats = profiles._fetch_polys("layer")
    assert len(polys) == 1
    assert stats == {"expected": 3, "fetched": 3, "no_geometry": 1, "out_of_bounds": 1, "kept": 1}


def test_arcgis_error_on_count_raises(monkeypatch):
    monkeypatch.setattr(profiles, "fetch_json", lambda *a, **k: ({"error": {"code": 400}}, 200))
    with pytest.raises(profiles.SourceCountError):
        profiles._layer_count("layer")


def _register(rows):
    return pl.DataFrame(
        rows,
        schema={
            "PlanningAuthority": pl.String,
            "decision_normalised": pl.String,
            "ReceivedDate": pl.Date,
            "DecisionDate": pl.Date,
            "FIRequestDate": pl.Date,
            "AppealDecision": pl.String,
        },
        orient="row",
    )


D = dt.date


def test_decision_before_receipt_is_nulled_and_flagged():
    df = profiles._decision_fields(
        _register(
            [
                ("A", "Granted", D(2020, 1, 1), D(2020, 1, 11), None, ""),
                ("A", "Refused", D(2020, 3, 1), D(2020, 2, 25), None, ""),
                ("A", "Undecided/None", D(2020, 5, 1), None, None, None),
            ]
        )
    )
    assert df["decision_latency_days"].to_list() == [10, None, None]
    assert df["_negative_latency"].to_list() == [False, True, False]
    assert df["decided"].to_list() == [True, True, False]


def test_council_rows_leave_rfi_and_appeal_unknown_not_false():
    df = _register(
        [
            ("Dublin City Council", "Granted", D(2026, 8, 3), D(2026, 9, 1), None, ""),
            ("Dublin City Council", "Granted", D(2026, 5, 3), D(2026, 6, 1), D(2026, 5, 20), "Refused"),
        ]
    ).with_columns(pl.Series("register_source", ["council_agile", "national_arcgis"]))
    out = profiles._decision_fields(df)
    assert out["had_rfi"].to_list() == [None, True]
    assert out["appealed"].to_list() == [None, True]
    assert out["decided"].to_list() == [True, True]


def test_authority_completeness_reports_depth_without_a_threshold():
    df = profiles._decision_fields(
        _register(
            [
                ("Cork City Council", "Granted", D(2012, 6, 1), D(2012, 8, 1), None, ""),
                ("Cork City Council", "Refused", D(2019, 2, 1), D(2019, 1, 1), None, ""),
                ("Cork City Council", "Undecided/None", None, None, None, ""),
                ("Leitrim County Council", "Granted", D(2024, 1, 5), D(2024, 3, 1), D(2024, 2, 1), ""),
            ]
        )
    )
    cork, leitrim = profiles._authority_completeness(df)
    assert cork == {
        "planning_authority": "Cork City Council",
        "n_applications": 3,
        "n_decided": 2,
        "n_received_date_null": 1,
        "n_negative_latency_nulled": 1,
        "n_council_supplement": 0,
        "received_first": "2012-06-01",
        "received_last": "2019-02-01",
        "decision_last": "2019-01-01",
        "received_year_counts": {"2012": 1, "2019": 1},
    }
    assert leitrim["received_year_counts"] == {"2024": 1}
    assert leitrim["n_decided"] == 1


def test_authority_completeness_refuses_oversize_receipt_summary(monkeypatch):
    df = profiles._decision_fields(
        _register(
            [
                ("Cork City Council", "Granted", D(2012, 6, 1), D(2012, 8, 1), None, ""),
                ("Leitrim County Council", "Granted", D(2024, 1, 5), D(2024, 3, 1), None, ""),
            ]
        )
    )
    monkeypatch.setattr(profiles, "RECEIPT_ROW_LIMIT", 1)

    with pytest.raises(ValueError, match="receipt summary has 2 rows; limit is 1"):
        profiles._authority_completeness(df)
