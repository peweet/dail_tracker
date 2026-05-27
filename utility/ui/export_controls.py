"""Consistent CSV export button for Dáil Tracker pages."""

from __future__ import annotations

import pandas as pd
import streamlit as st


def export_button(df: pd.DataFrame, label: str, filename: str, key: str) -> None:
    """Download button placed directly below its table; disabled when df is empty.

    The CSV encoding is deferred: ``data`` is a zero-arg callable that
    ``st.download_button`` invokes only when the user clicks, so every page
    rerun no longer serialises the full frame. For a 5000-row votes export
    or the full lobbying corpus this saves the full ``df.to_csv().encode()``
    on every unrelated widget interaction. (Requires Streamlit ≥1.30, which
    accepts a callable for ``data``.)
    """
    st.download_button(
        label=label,
        data=lambda: df.to_csv(index=False).encode("utf-8"),
        file_name=filename,
        mime="text/csv",
        disabled=df.empty,
        key=key,
    )
