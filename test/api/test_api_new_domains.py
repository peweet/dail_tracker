"""TestClient tests for the six domains added to close the API↔core parity gap:
attendance, housing, public-finance, local-government, constituencies, councillors.

Retrieval/composition is exercised by the core query tests; these assert the HTTP
wiring (routing, envelope, the lifted caveat in head/body, 404/422 mapping) and skip
cleanly when a dataset is absent on the machine running them.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from api.main import app  # noqa: E402
from dail_tracker_core import caveats  # noqa: E402


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


# ── Attendance ────────────────────────────────────────────────────────────────


def test_attendance_turnout_envelope_year_and_caveat(client):
    body = client.get("/v1/attendance/turnout", params={"limit": 3}).json()
    assert set(body.keys()) == {"head", "results"}
    assert body["head"]["limit"] == 3
    assert "house" in body["head"]
    # Year defaults to the latest reporting year (echoed in head) when data exists.
    if body["results"]:
        assert body["head"]["year"] is not None
        assert body["head"]["caveat"] == caveats.ATTENDANCE


def test_attendance_turnout_cap(client):
    assert client.get("/v1/attendance/turnout", params={"limit": 9999}).status_code == 422


def test_attendance_other_endpoints(client):
    for path in ("/v1/attendance/absences", "/v1/attendance/taa-compliance"):
        body = client.get(path, params={"limit": 2}).json()
        assert set(body.keys()) == {"head", "results"}
    missing = client.get("/v1/attendance/missing-members", params={"limit": 5}).json()
    assert "results" in missing
    years = client.get("/v1/attendance/years", params={"house": "Dáil"}).json()
    assert years["house"] == "Dáil" and isinstance(years["years"], list)


# ── Housing ──────────────────────────────────────────────────────────────────


def test_housing_waiting_list_grain(client):
    body = client.get("/v1/housing/waiting-list", params={"grain": "county", "limit": 4}).json()
    assert body["head"]["grain"] == "county"
    assert isinstance(body["results"], list)


def test_housing_supply_shape(client):
    body = client.get("/v1/housing/supply").json()
    assert set(body.keys()) == {"supply", "hap", "completions"}
    assert isinstance(body["completions"], list)


def test_housing_accommodation_spend_caveat(client):
    body = client.get("/v1/housing/accommodation-spend", params={"limit": 5}).json()
    assert "by_year" in body and "providers" in body
    assert body["caveat"] == caveats.ACCOMMODATION_SPEND


# ── Public finance ───────────────────────────────────────────────────────────


def test_government_finance_envelope_and_caveat(client):
    body = client.get("/v1/public-finance/government-finance").json()
    assert set(body.keys()) == {"head", "results"}
    if body["results"]:
        assert body["head"]["caveat"] == caveats.GOV_FINANCE
        assert "year" in body["results"][0]


# ── Local government ─────────────────────────────────────────────────────────


def test_councils_index_and_dossier_404(client):
    idx = client.get("/v1/local-government/councils").json()
    assert {"national_summary", "councils", "map_layers"} <= set(idx.keys())
    if not idx["councils"]:
        pytest.skip("no local-government data")
    la = idx["councils"][0]["local_authority"]
    d = client.get(f"/v1/local-government/councils/{la}").json()
    assert d["local_authority"] == la
    assert d["caveat"] == caveats.COUNCIL_MONEY
    assert isinstance(d["noac_scorecard"], list)
    assert client.get("/v1/local-government/councils/__nope__").status_code == 404


def test_council_noac_indicators(client):
    idx = client.get("/v1/local-government/councils").json()
    if not idx["councils"]:
        pytest.skip("no local-government data")
    la = idx["councils"][0]["local_authority"]
    body = client.get(f"/v1/local-government/councils/{la}/noac-indicators").json()
    assert body["local_authority"] == la and isinstance(body["indicators"], list)
    assert client.get("/v1/local-government/councils/__nope__/noac-indicators").status_code == 404


# ── Constituencies ───────────────────────────────────────────────────────────


def test_constituencies_index_and_dossier_404(client):
    idx = client.get("/v1/constituencies").json()
    assert set(idx.keys()) == {"head", "results"}
    if not idx["results"]:
        pytest.skip("no constituency data")
    name = idx["results"][0]["constituency_name"]
    d = client.get(f"/v1/constituencies/{name}/dossier").json()
    assert d["constituency"] == name
    assert isinstance(d["members"], list)
    assert d["caveat"] == caveats.COUNCIL_MONEY
    assert client.get("/v1/constituencies/__nope__/dossier").status_code == 404


# ── Councillors ──────────────────────────────────────────────────────────────


def test_councillors_roster_and_404(client):
    councils = client.get("/v1/councillors/councils").json()["councils"]
    if not councils:
        pytest.skip("no councillor data")
    la = councils[0]
    body = client.get("/v1/councillors", params={"council": la}).json()
    assert body["council"] == la and isinstance(body["councillors"], list)
    assert client.get("/v1/councillors", params={"council": "__nope__"}).status_code == 404


def test_councillors_requires_council(client):
    assert client.get("/v1/councillors").status_code == 422


# ── Catalog reflects the six new resources ───────────────────────────────────


def test_catalog_lists_new_domains(client):
    names = {r["resource"] for r in client.get("/v1/catalog").json()["resources"]}
    assert {
        "attendance",
        "housing",
        "public-finance",
        "local-government",
        "constituencies",
        "councillors",
    }.issubset(names)
