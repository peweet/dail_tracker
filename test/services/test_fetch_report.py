"""Tests for services/fetch_report — fetch-failure classifier, breaker, report writer.

Proves the three behaviours the extractors depend on:
  - the classifier maps exceptions/bodies to the stable error_class vocabulary
    (bot-challenge interstitials are recognised, magic-byte mismatches flagged);
  - the breaker trips after N CONSECUTIVE failures and a success resets it;
  - FetchReport.write() merges per-extractor sections (la_payments and
    public_body never clobber each other) and enriches records from gold.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import services.fetch_report as fr
from services.fetch_report import Breaker, FetchReport, classify_body, classify_exception


# ----------------------------------------------------------------- classifier
def test_classify_http_error():
    resp = requests.models.Response()
    resp.status_code = 403
    exc = requests.exceptions.HTTPError(response=resp)
    assert classify_exception(exc) == ("http_403", 403)


def test_classify_malformed_url():
    assert classify_exception(requests.exceptions.InvalidURL())[0] == "malformed_url"


def test_classify_timeout_and_connection():
    assert classify_exception(requests.exceptions.ReadTimeout())[0] == "timeout"
    assert classify_exception(requests.exceptions.ConnectionError())[0] == "connection_error"


def test_classify_body_bot_challenge():
    body = b"<html><head><title>One Moment, Please...</title></head></html>" + b"x" * 100
    assert classify_body(body, b"%PDF") == "bot_challenge"


def test_classify_body_magic_mismatch_and_ok():
    assert classify_body(b"<html>plain 404 page</html>", b"%PDF") == "not_expected_filetype"
    assert classify_body(b"%PDF-1.7 ...", b"%PDF") is None
    assert classify_body(b"anything", None) is None  # no magic expected -> body ok


# -------------------------------------------------------------------- breaker
def test_breaker_trips_on_consecutive_failures():
    b = Breaker(threshold=3)
    b.record(False)
    b.record(False)
    assert not b.tripped
    b.record(False)
    assert b.tripped


def test_breaker_reset_by_success():
    b = Breaker(threshold=3)
    b.record(False)
    b.record(False)
    b.record(True)  # success resets the streak
    b.record(False)
    b.record(False)
    assert not b.tripped


# --------------------------------------------------------------------- report
def test_report_merges_extractor_sections(tmp_path, monkeypatch):
    monkeypatch.setattr(fr, "OUT_PATH", tmp_path / "fetch_failures.json")
    monkeypatch.setattr(fr, "GOLD_FACT", tmp_path / "missing.parquet")  # no gold -> no enrichment

    r1 = FetchReport("public_body")
    r1.record_failure(publisher_id="ie_courts", publisher_name="Courts", url="u1", error_class="http_403")
    r1.write()

    r2 = FetchReport("la_payments")
    r2.record_zero_harvest(publisher_id="wicklow", publisher_name="Wicklow", listing_url="lw")
    r2.record_breaker_trip(publisher_id="sligo", publisher_name="Sligo", files_skipped=5)
    path = r2.write()

    doc = json.loads(path.read_text(encoding="utf-8"))
    assert set(doc["extractors"]) == {"public_body", "la_payments"}  # merged, not clobbered
    assert doc["extractors"]["public_body"]["failures"][0]["error_class"] == "http_403"
    assert doc["extractors"]["la_payments"]["zero_harvest"][0]["publisher_id"] == "wicklow"
    assert doc["extractors"]["la_payments"]["breaker_trips"][0]["files_skipped"] == 5


def test_report_gold_enrichment(tmp_path, monkeypatch):
    import polars as pl

    gold = tmp_path / "gold.parquet"
    pl.DataFrame(
        {"publisher_id": ["wicklow"] * 3 + ["other"], "period": ["2020-Q1", "2021-Q3", "2019-Q4", "2024-Q1"]}
    ).write_parquet(gold)
    monkeypatch.setattr(fr, "OUT_PATH", tmp_path / "fetch_failures.json")
    monkeypatch.setattr(fr, "GOLD_FACT", gold)

    r = FetchReport("la_payments")
    r.record_zero_harvest(publisher_id="wicklow", publisher_name="Wicklow", listing_url="lw")
    path = r.write()

    rec = json.loads(path.read_text(encoding="utf-8"))["extractors"]["la_payments"]["zero_harvest"][0]
    assert rec["rows_in_gold"] == 3
    assert rec["last_period_in_gold"] == "2021-Q3"


def test_report_survives_corrupt_existing_file(tmp_path, monkeypatch):
    out = tmp_path / "fetch_failures.json"
    out.write_text("{not json", encoding="utf-8")
    monkeypatch.setattr(fr, "OUT_PATH", out)
    monkeypatch.setattr(fr, "GOLD_FACT", tmp_path / "missing.parquet")

    r = FetchReport("public_body")
    r.record_failure(publisher_id="x", publisher_name="X", url="u", error_class="timeout")
    doc = json.loads(r.write().read_text(encoding="utf-8"))
    assert doc["extractors"]["public_body"]["n_failures"] == 1
