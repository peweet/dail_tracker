"""Contract tests for the isolated PublicSignal procurement snapshot service."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from fastapi.testclient import TestClient


def _module(snapshot_path: Path, monkeypatch):
    monkeypatch.setenv("PUBLIC_SIGNAL_SNAPSHOT_PATH", str(snapshot_path))
    monkeypatch.setenv("PUBLIC_SIGNAL_FEED_TOKEN", "private-token")
    source = Path("apps/public-signal/private-api/app.py")
    spec = importlib.util.spec_from_file_location("publicsignal_private_api_test", source)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _app(snapshot_path: Path, monkeypatch):
    return TestClient(_module(snapshot_path, monkeypatch).app)


def test_private_snapshot_feed_requires_token_and_filters(tmp_path: Path, monkeypatch) -> None:
    snapshot = {
        "schema": "publicsignal-procurement-snapshot/1",
        "built_at": "2026-08-05T12:00:00+00:00",
        "feed": {
            "opportunities": [
                {"id": "ted:A", "deadline": "2099-01-01", "cpv_division": "72", "source_lane": "ted_tender"},
                {"id": "national:B", "deadline": "2099-01-01", "cpv_division": None, "source_lane": "national_live"},
            ],
            "coverage": [{"source_lane": "ted_tender", "status": "ok"}],
            "caveats": ["planned values"],
        },
        "briefs": {"ted:A": {"opportunity": {"id": "ted:A"}}},
        "contracts": {
            "sectors": {"status": "reviewed", "rows": [{"sector": "72"}]},
            "buyers": {"status": "reviewed", "rows": []},
            "suppliers": {"status": "reviewed", "rows": []},
        },
    }
    path = tmp_path / "snapshot.json"
    path.write_text(json.dumps(snapshot), encoding="utf-8")
    client = _app(path, monkeypatch)

    assert client.get("/v1/procurement/opportunities").status_code == 401
    response = client.get("/v1/procurement/opportunities?sector=72", headers={"authorization": "Bearer private-token"})
    assert response.status_code == 200
    assert [row["id"] for row in response.json()["opportunities"]] == ["ted:A"]
    assert (
        client.get(
            "/v1/procurement/opportunities/ted:A/brief", headers={"authorization": "Bearer private-token"}
        ).status_code
        == 200
    )
    contract = client.get("/v1/procurement/contracts/sectors", headers={"authorization": "Bearer private-token"})
    assert contract.status_code == 200
    assert contract.json()["rows"] == [{"sector": "72"}]
    assert (
        client.get("/v1/procurement/contracts/unknown", headers={"authorization": "Bearer private-token"}).status_code
        == 404
    )


def test_private_snapshot_cache_reloads_when_snapshot_is_replaced(tmp_path: Path, monkeypatch) -> None:
    snapshot = {
        "schema": "publicsignal-procurement-snapshot/1",
        "built_at": "before",
        "feed": {"opportunities": []},
        "briefs": {},
    }
    path = tmp_path / "snapshot.json"
    path.write_text(json.dumps(snapshot), encoding="utf-8")
    module = _module(path, monkeypatch)

    first = module._snapshot()
    assert module._snapshot() is first

    snapshot["built_at"] = "after-a-replaced-snapshot"
    path.write_text(json.dumps(snapshot), encoding="utf-8")
    assert module._snapshot()["built_at"] == "after-a-replaced-snapshot"
