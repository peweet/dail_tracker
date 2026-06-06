"""TestClient tests for the votes / payments / lobbying resources."""

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


def _has(client, path, **params) -> bool:
    return client.get(path, params={**params, "limit": 1}).json()["head"].get("total", 0) > 0


def test_envelope_everywhere(client):
    for path in ("/v1/votes", "/v1/payments", "/v1/lobbying/organisations", "/v1/lobbying/revolving-door"):
        body = client.get(path, params={"limit": 3}).json()
        assert set(body.keys()) == {"head", "results"}, path
        assert body["head"]["limit"] == 3


def test_limit_caps(client):
    for path in ("/v1/votes", "/v1/payments", "/v1/lobbying/organisations"):
        assert client.get(path, params={"limit": 9999}).status_code == 422, path


def test_votes_list_and_division_dossier(client):
    if not _has(client, "/v1/votes"):
        pytest.skip("no vote data")
    rows = client.get("/v1/votes", params={"limit": 1}).json()["results"]
    assert "vote_id" in rows[0]
    vid = rows[0]["vote_id"]
    d = client.get(f"/v1/votes/{vid}").json()
    assert isinstance(d["division"], dict)
    assert isinstance(d["party_breakdown"], list)
    assert isinstance(d["members"], list)


def test_division_unknown_404(client):
    assert client.get("/v1/votes/9999-99-99_999").status_code == 404


def test_payments_ranking(client):
    if not _has(client, "/v1/payments"):
        pytest.skip("no payments data")
    rows = client.get("/v1/payments", params={"limit": 5}).json()["results"]
    assert len(rows) <= 5


def test_lobbying_org_name_filter(client):
    if not _has(client, "/v1/lobbying/organisations"):
        pytest.skip("no lobbying data")
    full = client.get("/v1/lobbying/organisations", params={"limit": 1}).json()["results"][0]
    name_col = next((k for k in full if "name" in k.lower()), None)
    assert name_col is not None
    frag = str(full[name_col])[:4]
    sub = client.get("/v1/lobbying/organisations", params={"name": frag, "limit": 500}).json()
    assert sub["head"]["total"] <= client.get("/v1/lobbying/organisations", params={"limit": 1}).json()["head"]["total"]


def test_catalog_lists_all_resources(client):
    names = {r["resource"] for r in client.get("/v1/catalog").json()["resources"]}
    assert {"members", "legislation", "statutory-instruments", "votes", "payments", "lobbying"}.issubset(names)
