"""pdf_fingerprint.py — content fingerprints to catch SUPERSEDED source files.

The pollers skip a file when its FILENAME already exists on disk. That misses
DAIL-162: a publisher replacing a PDF *in place* (same URL / same filename, new
bytes) — a silent, permanent drift between our gold layer and the source, and the
single failure mode hardest to notice ("stale data" with a green pipeline).

This module records a sha256 + byte-size fingerprint per known file and offers a
pure decision (``compare``) over a cheap signal (the HTTP ``Content-Length`` from a
HEAD request), so a poller can flag "the source replaced a file you already hold"
WITHOUT re-downloading every historical file on every run.

The decision is deliberately conservative — it never raises a false alarm:
  * a file we have never fingerprinted -> ``NEW_BASELINE`` (record it, no network);
  * the server hid the size           -> ``UNKNOWN`` (can't tell; stay silent);
  * size matches the stored baseline   -> ``UNCHANGED``;
  * size differs                       -> ``SUPERSEDED`` (flag for a human).

A same-size in-place edit is not detected by ``Content-Length`` alone — that is a
rare case and the honest limit of a cheap check; a periodic full re-hash (not done
here) would be the only way to catch it. Detection is FLAG-ONLY: the caller must not
auto-download the replacement into an ETL-globbed directory (it would double-count).
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

NEW_BASELINE = "new_baseline"
UNCHANGED = "unchanged"
SUPERSEDED = "superseded"
UNKNOWN = "unknown"


def sha256_bytes(blob: bytes) -> str:
    return hashlib.sha256(blob).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def compare(stored_bytes: int | None, remote_bytes: int | None) -> str:
    """Pure supersession verdict from a stored baseline size and a freshly-observed
    remote size. See module docstring for the four outcomes."""
    if stored_bytes is None:
        return NEW_BASELINE
    if remote_bytes is None:
        return UNKNOWN
    return UNCHANGED if int(remote_bytes) == int(stored_bytes) else SUPERSEDED


def load_index(path: Path) -> dict:
    """Load the per-source fingerprint index; a missing/corrupt file is an empty
    index (the next run rebuilds baselines, never crashes the poller)."""
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_index(path: Path, index: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(index, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
