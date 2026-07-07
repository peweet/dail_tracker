"""Shared harness for the NEW_SOURCE_INGESTION_PLAN sandbox ingests.

ISOLATED + SANDBOX ONLY. Nothing here writes to data/gold, edits pipeline.py,
or promotes anything. Raw caches + silver outputs land under
``c:/tmp/dail_new_sources/`` so the repo data tree is untouched.

Provenance is attached to every row per the plan's schema:
source_url, source_document_hash, fetched_at, source_published_date,
source_last_modified, extraction_method, confidence, privacy_tier.
"""
from __future__ import annotations

import hashlib
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path("c:/tmp/dail_new_sources")
BRONZE = ROOT / "bronze"
SILVER = ROOT / "silver"
for _d in (BRONZE, SILVER):
    _d.mkdir(parents=True, exist_ok=True)

UA = "Mozilla/5.0 (DailTracker new-source scan; research/non-commercial; +contact via repo)"
_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": UA, "Accept-Language": "en-IE,en;q=0.9"})

POLITE_DELAY_S = 0.4


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def fetch(url: str, params: dict | None = None, binary: bool = False, timeout: int = 45):
    """GET with polite delay. Returns (payload, meta). Raises on HTTP error."""
    time.sleep(POLITE_DELAY_S)
    r = _SESSION.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    payload = r.content if binary else r.text
    raw = r.content
    meta = {
        "source_url": r.url,
        "status": r.status_code,
        "content_type": r.headers.get("content-type", ""),
        "source_last_modified": r.headers.get("last-modified"),
        "etag": r.headers.get("etag"),
        "source_document_hash": sha256_bytes(raw),
        "fetched_at": now_iso(),
        "bytes": len(raw),
    }
    return payload, meta


def cache_raw(source: str, name: str, content: bytes) -> tuple[Path, str]:
    d = BRONZE / source
    d.mkdir(parents=True, exist_ok=True)
    p = d / name
    p.write_bytes(content)
    return p, sha256_bytes(content)


def write_silver(source: str, df) -> Path:
    """Write a polars DataFrame to silver parquet (zstd). Refuses an empty frame."""
    if df.is_empty():
        raise ValueError(f"{source}: refusing to write an empty silver frame")
    SILVER.mkdir(parents=True, exist_ok=True)
    p = SILVER / f"{source}.parquet"
    df.write_parquet(p, compression="zstd", statistics=True)
    return p
