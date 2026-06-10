"""Tests for the QS benchmark valuation (inference; experimental surface only)."""

from __future__ import annotations

from dail_tracker_core import qs_valuation as qs


def test_tpi_year_is_mean_of_halves():
    # 2019 H1=152.7, H2=157.0 -> mean 154.85
    assert qs.tpi_for_year(2019) == (152.7 + 157.0) / 2
    assert qs.tpi_for_year(2025) == 220.4  # only H1 published
    assert qs.tpi_for_year(1700) is None


def test_per_m2_requires_area():
    e = qs.estimate("semi_detached_house", units=3)
    assert not e.ok and "area" in e.message.lower()


def test_year_adjustment_deflates_older_awards():
    base = qs.estimate("semi_detached_house", units=1, area_m2=95)
    y2019 = qs.estimate("semi_detached_house", units=1, area_m2=95, award_year=2019)
    assert base.ok and y2019.ok
    # 2019 costs were below the 2025 basis -> the adjusted value must be lower.
    assert y2019.payload["value_eur"]["mid"] < base.payload["value_eur"]["mid"]
    # and the factor is the TPI ratio
    assert "×0.703" in y2019.payload["year_adjustment"]


def test_value_scales_with_units_and_area():
    one = qs.estimate("semi_detached_house", units=1, area_m2=100).payload["value_eur"]["mid"]
    ten = qs.estimate("semi_detached_house", units=10, area_m2=100).payload["value_eur"]["mid"]
    assert ten == 10 * one


def test_per_unit_type_ignores_area():
    e = qs.estimate("three_star", units=120)  # hotel, per_key
    assert e.ok
    assert e.payload["value_eur"]["mid"] == (160000 + 180000) / 2 * 120


def test_framework_ceiling_gap():
    e = qs.estimate("semi_detached_house", units=12, area_m2=95, framework_ceiling_eur=30_000_000)
    assert e.payload["ceiling_vs_estimate_multiple"] > 1
    assert "legal headroom" in e.payload["ceiling_reading"]


def test_inference_is_labelled_everywhere():
    e = qs.estimate("apartment", units=20, area_m2=75, award_year=2022)
    assert "inference" in e.payload["estimate_kind"].lower()
    assert "never as the contract" in e.payload["caveat"]
    assert any("Tender Price Index" in s for s in e.payload["sources"])


def test_unknown_subtype_fails_cleanly():
    assert not qs.estimate("not_a_real_type", units=1, area_m2=10).ok
