"""Private, read-only API over a materialised PublicSignal procurement snapshot."""

from __future__ import annotations

import hmac
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query

SNAPSHOT_PATH = Path(os.environ.get("PUBLIC_SIGNAL_SNAPSHOT_PATH", "/app/snapshot.json"))
app = FastAPI(title="PublicSignal private procurement API", docs_url=None, redoc_url=None, openapi_url=None)
_snapshot_cache: tuple[int, int, dict[str, Any]] | None = None


def _snapshot() -> dict[str, Any]:
    """Read the immutable build snapshot once, reloading only when it is replaced."""
    global _snapshot_cache
    try:
        stat = SNAPSHOT_PATH.stat()
        cache_key = (stat.st_mtime_ns, stat.st_size)
        if _snapshot_cache and _snapshot_cache[:2] == cache_key:
            return _snapshot_cache[2]
        payload = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=503, detail="procurement snapshot unavailable") from exc
    if payload.get("schema") != "publicsignal-procurement-snapshot/1":
        raise HTTPException(status_code=503, detail="procurement snapshot is incompatible")
    _snapshot_cache = (*cache_key, payload)
    return payload


def require_token(authorization: str | None = Header(None)) -> None:
    expected = os.environ.get("PUBLIC_SIGNAL_FEED_TOKEN", "").strip()
    scheme, _, supplied = (authorization or "").partition(" ")
    if not expected:
        raise HTTPException(status_code=503, detail="private procurement API is not configured")
    if scheme.lower() != "bearer" or not supplied or not hmac.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="private procurement API token required")


@app.get("/v1/health")
def health() -> dict[str, Any]:
    snapshot = _snapshot()
    return {"ok": True, "schema": snapshot["schema"], "built_at": snapshot["built_at"]}


@app.get("/v1/procurement/opportunities", dependencies=[Depends(require_token)])
def opportunities(
    within_days: int | None = Query(None, ge=0, le=1095),
    sector: str | None = None,
    source_lane: str | None = Query(None, pattern="^(national_live|ted_tender)$"),
    limit: int = Query(100, ge=1, le=200),
) -> dict[str, Any]:
    snapshot = _snapshot()
    rows = snapshot["feed"]["opportunities"]
    if within_days is not None:
        rows = [row for row in rows if row.get("deadline") and _days_until(row["deadline"]) <= within_days]
    if sector:
        rows = [row for row in rows if row.get("cpv_division") == sector]
    if source_lane:
        rows = [row for row in rows if row.get("source_lane") == source_lane]
    return {**snapshot["feed"], "opportunities": rows[:limit], "snapshot_built_at": snapshot["built_at"]}


@app.get("/v1/procurement/opportunities/{opportunity_id}/brief", dependencies=[Depends(require_token)])
def brief(opportunity_id: str) -> dict[str, Any]:
    value = _snapshot()["briefs"].get(opportunity_id)
    if value is None:
        raise HTTPException(status_code=404, detail="opportunity not found in this snapshot")
    return value


@app.get("/v1/procurement/contracts/{contract_name}", dependencies=[Depends(require_token)])
def contract(contract_name: str) -> dict[str, Any]:
    if contract_name not in {"sectors", "buyers", "suppliers"}:
        raise HTTPException(status_code=404, detail="procurement contract not found")
    value = _snapshot().get("contracts", {}).get(contract_name)
    if value is None:
        raise HTTPException(status_code=503, detail="procurement contract unavailable")
    return value


def _days_until(value: str) -> int:
    try:
        deadline = datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except (TypeError, ValueError):
        return 1096
    return (deadline - datetime.now(UTC).date()).days
