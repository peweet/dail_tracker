"""Data-freshness access — reads the committed ``data/_meta/freshness.json``.

The pipeline's final chain (tools/check_freshness.py) writes one entry per
dataset recording the newest record date / fetch timestamp we hold. The UI
only ever READS that committed JSON — never parquet — so this module is the
single place a page gets its "Data updated …" provenance line from.

The signal is the age of the data we already hold (a pipeline canary), not
proof that no newer upstream data exists — wording below stays factual
("newest record", "pipeline last ran") and never claims completeness.
"""

from __future__ import annotations

import json
from datetime import date, datetime

import streamlit as st

from config import FRESHNESS_JSON


@st.cache_data(ttl=600)
def _load_freshness() -> dict:
    """The parsed freshness.json payload, or {} when missing/unreadable."""
    try:
        return json.loads(FRESHNESS_JSON.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _fmt_iso_date(iso: str | None) -> str:
    """'2026-06-05' or '2026-06-07T15:27:46Z' → '5 June 2026' ('' when absent)."""
    if not iso:
        return ""
    try:
        d = datetime.fromisoformat(iso.replace("Z", "+00:00")).date() if "T" in iso else date.fromisoformat(iso)
    except ValueError:
        return ""
    return f"{d.day} {d:%B %Y}"


def freshness_line(dataset: str) -> str:
    """One factual provenance sentence for a dataset, or '' when unavailable.

    e.g. "Newest record held: 5 June 2026 · pipeline last ran 7 June 2026."
    Datasets are the keys written by tools/check_freshness.py (votes,
    questions, lobbying, iris, corporate, statutory_instruments, procurement,
    members). An empty string means the page renders no freshness line —
    never a placeholder like "None → None".
    """
    payload = _load_freshness()
    entry = (payload.get("datasets") or {}).get(dataset) or {}
    generated = _fmt_iso_date(payload.get("generated_at"))

    parts: list[str] = []
    record = _fmt_iso_date(entry.get("latest_record_date"))
    if record:
        parts.append(f"Newest record held: {record}")
    elif entry.get("period_label"):
        parts.append(f"Latest period held: {entry['period_label']}")
    else:
        fetched = _fmt_iso_date(entry.get("latest_fetch_at") or entry.get("fetched_at"))
        if fetched:
            parts.append(f"Last fetched: {fetched}")
    if not parts:
        # Unknown dataset key or empty entry — render nothing rather than a
        # bare pipeline date that says nothing about THIS page's data.
        return ""
    if generated:
        parts.append(f"pipeline last ran {generated}")
    return " · ".join(parts) + "."
