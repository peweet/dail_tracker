"""Official source link rendering for Dáil Tracker pages."""

from __future__ import annotations

from html import escape
from urllib.parse import urlparse

import pandas as pd
import streamlit as st
from ui.components import todo_callout

_APPROVED: frozenset[str] = frozenset(
    {
        "source_url",
        "source_document_url",
        "official_pdf_url",
        "oireachtas_url",
        "legislation_url",
    }
)

_LABELS: dict[str, str] = {
    "source_url": "Source",
    "source_document_url": "Source document",
    "official_pdf_url": "Official PDF",
    "oireachtas_url": "Oireachtas.ie",
    "legislation_url": "Legislation.gov.ie",
}


def render_source_links(df: pd.DataFrame) -> None:
    if df.empty:
        st.caption("No official source links available.")
        return

    present_cols = [c for c in _APPROVED if c in df.columns]
    links_html = ""

    for _, row in df.iterrows():
        for col in present_cols:
            raw_url = row.get(col)
            if not isinstance(raw_url, str):
                continue
            url = raw_url.strip()
            parsed = urlparse(url)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                continue
            raw_label = row.get("source_label")
            label_text = (
                raw_label.strip() if isinstance(raw_label, str) and raw_label.strip() else _LABELS.get(col, col)
            )
            safe_url = escape(url, quote=True)
            safe_label = escape(label_text)
            links_html += (
                f'<a href="{safe_url}" target="_blank" rel="noopener noreferrer"'
                f' style="display:inline-flex;align-items:center;gap:0.3rem;'
                f"color:var(--accent);font-weight:600;font-size:0.85rem;"
                f"text-decoration:none;border:1px solid var(--border);"
                f'border-radius:2px;padding:0.25rem 0.65rem;background:var(--surface)">'
                f"{safe_label} ↗</a>"
            )

    if links_html:
        st.markdown(
            f'<div style="display:flex;flex-wrap:wrap;gap:0.5rem;margin:0.25rem 0">{links_html}</div>',
            unsafe_allow_html=True,
        )
    else:
        todo_callout("source_url column on v_vote_sources")
