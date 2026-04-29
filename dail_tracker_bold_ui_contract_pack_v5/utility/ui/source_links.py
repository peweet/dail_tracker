from __future__ import annotations

from html import escape
from urllib.parse import urlparse

import pandas as pd
import streamlit as st


APPROVED_URL_COLUMNS = {
    "source_url",
    "source_document_url",
    "official_pdf_url",
    "oireachtas_url",
    "legislation_url",
}


def is_probably_safe_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def render_source_links(row: pd.Series, *, title: str = "Official sources") -> None:
    links = []
    for col in APPROVED_URL_COLUMNS:
        if col in row and pd.notna(row[col]) and str(row[col]).strip():
            url = str(row[col]).strip()
            if is_probably_safe_url(url):
                label = {
                    "official_pdf_url": "Official PDF",
                    "oireachtas_url": "Oireachtas record",
                    "legislation_url": "Legislation record",
                    "source_document_url": "Source document",
                    "source_url": "Source",
                }.get(col, "Source")
                links.append((label, url))

    if not links:
        st.caption("No official source links are available for this record.")
        return

    items = "".join(
        f"<li><a href='{escape(url)}' target='_blank' rel='noopener noreferrer'>{escape(label)}</a></li>"
        for label, url in links
    )
    st.html(f"<section class='dt-source-list'><strong>{escape(title)}</strong><ul>{items}</ul></section>")
