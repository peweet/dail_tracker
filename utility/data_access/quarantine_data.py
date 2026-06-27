"""Quarantine ledger access — reads the committed ``data/_meta/quarantine_ledger.json``.

The fidelity / contract gates hold back values that fall outside plausible bounds (a
fat-fingered or mis-OCR'd figure — e.g. a donation larger than physically possible) so
they can never reach a published total. ``tools/quarantine_report.py`` folds the
per-resource evidence into that one ledger; this module is the single place the app READS
it (never the parquet). An empty ledger means nothing was held back.

Copy here stays strictly factual (no inference): we state what was held and why it was
flagged, not any judgement about the underlying figure.
"""

from __future__ import annotations

import json

import streamlit as st

from config import QUARANTINE_LEDGER_JSON


@st.cache_data(ttl=600)
def _load_ledger() -> dict:
    """Parsed quarantine_ledger.json, or {} when missing/unreadable (the healthy default)."""
    try:
        return json.loads(QUARANTINE_LEDGER_JSON.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def held_summary() -> dict:
    """Small summary: ``{n_rows_held, n_resources, resources}``."""
    led = _load_ledger()
    return {
        "n_rows_held": int(led.get("n_rows_held", 0) or 0),
        "n_resources": int(led.get("n_resources", 0) or 0),
        "resources": led.get("resources", {}) or {},
    }


def render_data_integrity_panel() -> None:
    """Render the 'values held for review' panel. Factual only — for any meta/reference page."""
    summary = held_summary()
    st.html('<h2 class="section-heading">Data integrity — values held for review</h2>')

    if not summary["n_rows_held"]:
        st.caption(
            "No values are currently held back. Every figure on the site passed the automated "
            "plausibility checks — for example, no amount larger than is physically possible has "
            "been allowed through to a published total."
        )
        return

    st.caption(
        f"{summary['n_rows_held']} value(s) across {summary['n_resources']} source(s) were "
        "automatically held back because they fell outside plausible bounds, and are excluded "
        "from the site pending manual review. They are listed here for transparency."
    )
    for name, entry in sorted(summary["resources"].items()):
        rows = entry.get("rows", [])
        if not rows:
            continue
        offending = entry.get("offending_columns", [])
        with st.expander(f"{name} — {entry.get('n_held', len(rows))} held back"):
            for row in rows[:20]:
                value = "; ".join(f"{c} = {row.get(c)}" for c in offending if c in row)
                src, page = row.get("source_pdf"), row.get("source_page")
                where = f"  ·  {src} p.{page}" if src else (f"  ·  {src}" if src else "")
                st.write(f"- {value or '(value)'}{where}")
