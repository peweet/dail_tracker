"""TestClient tests for the third parity wave — the domains brought up to the
Streamlit/MCP surface: corporate notices (Iris Oifigiúil) and ministerial diaries
(who ministers meet).

Composition + serialization are covered by the core query/dossier tests; these
assert the HTTP wiring (routing, caveat carry-through, 404/422/503 mapping) and skip
cleanly when an optional dataset (corporate notices / diary gold parquet) is absent
locally.
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


# ── Corporate notices ─────────────────────────────────────────────────────────


def test_corporate_notices_filters_and_caveat(client):
    r = client.get("/v1/corporate/notices", params={"limit": 3})
    if r.status_code == 503:
        pytest.skip("corporate notices source unavailable")
    body = r.json()
    assert {"count", "notices", "caveat"} <= set(body)
    assert body["count"] <= 3
    # The bulky OCR scratch columns must never leak through the API.
    for n in body["notices"]:
        assert "raw_text" not in n and "title" not in n
    # MVL-is-not-distress disclaimer is carried verbatim.
    assert "Members' Voluntary Liquidation" in body["caveat"]


def test_corporate_notices_limit_cap(client):
    assert client.get("/v1/corporate/notices", params={"limit": 9999}).status_code == 422


def test_corporate_repeat_distress(client):
    r = client.get("/v1/corporate/repeat-distress", params={"limit": 3})
    if r.status_code == 503:
        pytest.skip("corporate repeat-distress source unavailable")
    body = r.json()
    assert {"firms", "caveat"} <= set(body)
    assert isinstance(body["firms"], list) and len(body["firms"]) <= 3


def test_corporate_receivers_bundle(client):
    r = client.get("/v1/corporate/receivers", params={"limit": 5})
    if r.status_code == 503:
        pytest.skip("corporate receiver gold unavailable")
    body = r.json()
    assert {"summary", "appointers", "firms", "appointer_type_mix", "notices_by_year", "caveat"} <= set(body)
    assert len(body["appointers"]) <= 5 and len(body["firms"]) <= 5


# ── Ministerial diaries ───────────────────────────────────────────────────────


def test_diary_top_organisations_and_one(client):
    r = client.get("/v1/ministerial/diary/organisations", params={"limit": 3})
    if r.status_code == 503:
        pytest.skip("ministerial diary source unavailable")
    body = r.json()
    assert {"organisations", "caveat"} <= set(body)
    assert len(body["organisations"]) <= 3
    if not body["organisations"]:
        pytest.skip("no diary org rows")
    name = body["organisations"][0]["organisation"]
    one = client.get(f"/v1/ministerial/diary/organisations/{name}")
    assert one.status_code == 200
    assert {"organisation", "summary", "meetings", "caveat"} <= set(one.json())


def test_diary_organisation_unknown_404(client):
    if client.get("/v1/ministerial/diary/organisations").status_code == 503:
        pytest.skip("ministerial diary source unavailable")
    assert client.get("/v1/ministerial/diary/organisations/__no_such_org__").status_code == 404


def test_diary_meeting_search(client):
    r = client.get("/v1/ministerial/diary/meetings", params={"topic": "energy", "limit": 3})
    if r.status_code == 503:
        pytest.skip("ministerial diary source unavailable")
    body = r.json()
    assert {"meetings", "caveat"} <= set(body)
    assert isinstance(body["meetings"], list) and len(body["meetings"]) <= 3


# ── Catalog reflects the third wave ───────────────────────────────────────────


def test_catalog_lists_third_wave(client):
    names = {r["resource"] for r in client.get("/v1/catalog").json()["resources"]}
    assert {"corporate", "ministerial-diaries"}.issubset(names)
