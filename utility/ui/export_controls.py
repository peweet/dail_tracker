"""Consistent CSV export button for Dáil Tracker pages."""
from __future__ import annotations
import pandas as pd
import streamlit as st


def export_button(df: pd.DataFrame, label: str, filename: str, key: str) -> None:
    """Download button placed directly below its table; disabled when df is empty."""
    st.download_button(
        label=label,
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=filename,
        mime="text/csv",
        disabled=df.empty,
        key=key,
    )
