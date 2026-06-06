"""TestClient tests for the legislation + statutory-instruments + catalog resources."""

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


@pytest.fixture(scope="module")
def has_bills(client) -> bool:
    return client.get("/v1/legislation", params={"limit": 1}).json()["head"].get("total", 0) > 0


def test_catalog(client):
    r = client.get("/v1/catalog")
    assert r.status_code == 200
    body = r.json()
    assert body["licence"] == "CC-BY-4.0"
    names = {res["resource"] for res in body["resources"]}
    assert {"members", "legislation", "statutory-instruments"}.issubset(names)


def test_legislation_list_envelope(client, has_bills):
    r = client.get("/v1/legislation", params={"limit": 5})
    assert r.status_code == 200
    body = r.json()
    assert set(body.keys()) == {"head", "results"}
    assert body["head"]["limit"] == 5
    if not has_bills:
        pytest.skip("no legislation data")
    assert len(body["results"]) <= 5
    assert "bill_id" in body["results"][0]


def test_legislation_status_filter(client, has_bills):
    if not has_bills:
        pytest.skip("no legislation data")
    r = client.get("/v1/legislation", params={"status": "Current", "limit": 50})
    assert r.status_code == 200
    rows = r.json()["results"]
    if rows:
        assert all(row["bill_status"] == "Current" for row in rows)


def test_bill_dossier_roundtrip(client, has_bills):
    if not has_bills:
        pytest.skip("no legislation data")
    bill_id = client.get("/v1/legislation", params={"limit": 1}).json()["results"][0]["bill_id"]
    r = client.get(f"/v1/legislation/{bill_id}")
    assert r.status_code == 200
    d = r.json()
    assert isinstance(d["bill"], dict)
    for section in ("timeline", "pdfs", "debates", "statutory_instruments", "si_composition"):
        assert isinstance(d[section], list)


def test_bill_dossier_unknown_404(client):
    r = client.get("/v1/legislation/not_a_real_bill_id/dossier" if False else "/v1/legislation/9999_9999")
    assert r.status_code == 404


def test_si_list_and_eu_filter(client):
    r = client.get("/v1/statutory-instruments", params={"limit": 5})
    assert r.status_code == 200
    body = r.json()
    assert set(body.keys()) == {"head", "results"}
    if body["head"].get("total", 0) == 0:
        pytest.skip("no SI data")
    assert "si_id" in body["results"][0]
    eu = client.get("/v1/statutory-instruments", params={"eu_only": True, "limit": 500})
    assert eu.status_code == 200
    eu_rows = eu.json()["results"]
    if eu_rows:
        assert all(bool(row.get("si_is_eu")) for row in eu_rows)


def test_si_limit_cap_rejected(client):
    assert client.get("/v1/statutory-instruments", params={"limit": 9999}).status_code == 422
