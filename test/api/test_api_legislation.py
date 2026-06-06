"""TestClient tests for legislation + statutory-instruments + catalog."""

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
    body = client.get("/v1/catalog").json()
    assert body["licence"] == "CC-BY-4.0"
    names = {res["resource"] for res in body["resources"]}
    assert {"members", "legislation", "statutory-instruments"}.issubset(names)


def test_legislation_list_envelope(client, has_bills):
    body = client.get("/v1/legislation", params={"limit": 5}).json()
    assert set(body.keys()) == {"head", "results"}
    assert body["head"]["limit"] == 5
    if not has_bills:
        pytest.skip("no legislation data")
    assert "bill_id" in body["results"][0]


def test_legislation_status_filter(client, has_bills):
    if not has_bills:
        pytest.skip("no legislation data")
    rows = client.get("/v1/legislation", params={"status": "Current", "limit": 50}).json()["results"]
    if rows:
        assert all(r["bill_status"] == "Current" for r in rows)


def test_bill_dossier_roundtrip(client, has_bills):
    if not has_bills:
        pytest.skip("no legislation data")
    bill_id = client.get("/v1/legislation", params={"limit": 1}).json()["results"][0]["bill_id"]
    d = client.get(f"/v1/legislation/{bill_id}").json()
    assert isinstance(d["bill"], dict)
    for section in ("timeline", "pdfs", "debates", "statutory_instruments", "si_composition"):
        assert isinstance(d[section], list)


def test_bill_dossier_unknown_404(client):
    assert client.get("/v1/legislation/9999_9999").status_code == 404


def test_si_list_and_eu_filter(client):
    body = client.get("/v1/statutory-instruments", params={"limit": 5}).json()
    assert set(body.keys()) == {"head", "results"}
    if body["head"].get("total", 0) == 0:
        pytest.skip("no SI data")
    assert "si_id" in body["results"][0]
    eu_rows = client.get("/v1/statutory-instruments", params={"eu_only": True, "limit": 500}).json()["results"]
    if eu_rows:
        assert all(bool(r.get("si_is_eu")) for r in eu_rows)


def test_si_limit_cap_rejected(client):
    assert client.get("/v1/statutory-instruments", params={"limit": 9999}).status_code == 422
