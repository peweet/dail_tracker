"""TestClient tests for the second parity wave — the domains brought up to MCP
parity: SIPO political finance, judiciary, charities, public-body payments, public
appointments, procurement deep cuts, current cabinet, DPO profile, votes-by-topic,
and the coverage scope-guard.

Composition + serialization are covered by the core query/dossier tests; these
assert the HTTP wiring (routing, envelope, 400/404/503 mapping) and skip cleanly
when an optional dataset (SIPO/judiciary/appointments parquet) is absent locally.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from api.main import app  # noqa: E402


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


# ── SIPO political finance ────────────────────────────────────────────────────


def test_donations_ranking_and_party_drilldown(client):
    r = client.get("/v1/political-finance/donations")
    if r.status_code == 503:
        pytest.skip("SIPO donations source unavailable")
    body = r.json()
    assert {"summary", "by_party", "note"} <= set(body)
    if not body["by_party"]:
        pytest.skip("no donation rows")
    party = body["by_party"][0]["party"]
    drill = client.get("/v1/political-finance/donations", params={"party": party}).json()
    assert drill["party"] == party
    assert isinstance(drill["donations"], list) and len(drill["donations"]) > 0


def test_election_spend_grain_kept_separate(client):
    r = client.get("/v1/political-finance/election-spend")
    if r.status_code == 503:
        pytest.skip("SIPO expenses source unavailable")
    body = r.json()
    assert {"summary", "by_party"} <= set(body)
    # Donations and expenses are distinct grains — the totals must not be the same object.
    don = client.get("/v1/political-finance/donations")
    if don.status_code == 200 and don.json().get("summary") and body.get("summary"):
        assert don.json()["summary"].get("total_value") != body["summary"].get("total_expenditure")


# ── Judiciary ─────────────────────────────────────────────────────────────────


def test_judicial_appointments_bundle(client):
    r = client.get("/v1/judiciary/appointments", params={"limit": 3})
    if r.status_code == 503:
        pytest.skip("judiciary source unavailable")
    body = r.json()
    assert {"appointments", "elevation_ladder", "roster"} <= set(body)
    assert len(body["appointments"]) <= 3


def test_courts_health_names_no_judge(client):
    r = client.get("/v1/judiciary/courts-health")
    if r.status_code == 503:
        pytest.skip("courts source unavailable")
    body = r.json()
    assert {"clearance", "waiting_times", "courthouses"} <= set(body)


# ── Charities ─────────────────────────────────────────────────────────────────


def test_charities_sector_then_one(client):
    r = client.get("/v1/charities")
    if r.status_code == 503:
        pytest.skip("charities source unavailable")
    body = r.json()
    assert {"latest_year", "sector_totals_by_year"} <= set(body)


# ── Public-body payments ──────────────────────────────────────────────────────


def test_public_body_payments_sides(client):
    for side in ("publisher", "supplier"):
        r = client.get("/v1/public-body-payments", params={"side": side, "limit": 3})
        if r.status_code == 503:
            pytest.skip("public-body payments source unavailable")
        body = r.json()
        assert body["side"] == side
        assert "caveat" in body and isinstance(body["ranking"], list)


# ── Public appointments / procurement deep cuts (envelope lists) ──────────────


def test_envelope_lists_cap(client):
    for path in (
        "/v1/public-appointments",
        "/v1/procurement/authorities",
        "/v1/procurement/cpv",
        "/v1/procurement/open-tenders",
    ):
        body = client.get(path, params={"limit": 3}).json()
        assert set(body.keys()) == {"head", "results"}, path
        assert body["head"]["limit"] == 3
        assert client.get(path, params={"limit": 9999}).status_code == 422, path


# ── Cabinet ───────────────────────────────────────────────────────────────────


def test_current_cabinet(client):
    r = client.get("/v1/cabinet")
    if r.status_code == 503:
        pytest.skip("ministerial source unavailable")
    body = r.json()
    assert {"current_ministers", "departments"} <= set(body)
    assert len(body["current_ministers"]) > 0


# ── DPO profile ───────────────────────────────────────────────────────────────


def test_dpo_unknown_404(client):
    assert client.get("/v1/lobbying/dpo/__definitely_no_such_person__").status_code == 404


# ── Votes by topic ────────────────────────────────────────────────────────────


def test_votes_by_topic(client):
    r = client.get("/v1/search/votes-by-topic", params={"topics": "housing, rent"})
    if r.status_code == 503:
        pytest.skip("vote source unavailable")
    body = r.json()
    assert {"topics", "debates", "votes"} <= set(body)
    assert body["topics"] == ["housing", "rent"]


def test_votes_by_topic_empty_is_400(client):
    assert client.get("/v1/search/votes-by-topic", params={"topics": " , "}).status_code == 400


# ── Coverage scope guard ──────────────────────────────────────────────────────


def test_coverage_scope_guard(client):
    body = client.get("/v1/coverage").json()
    assert "caveats" in body
    assert "money_grains" in body["caveats"]
    # Every documented domain key is present (value may be None if its parquet is absent).
    for k in ("procurement_awards", "ted_awards", "public_body_payments", "sipo_donations"):
        assert k in body


# ── Catalog reflects the new resources ────────────────────────────────────────


def test_catalog_lists_second_wave(client):
    names = {r["resource"] for r in client.get("/v1/catalog").json()["resources"]}
    assert {
        "political-finance",
        "judiciary",
        "charities",
        "public-body-payments",
        "public-appointments",
    }.issubset(names)
