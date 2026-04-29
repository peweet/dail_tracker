from __future__ import annotations

from typing import Iterable

import pandas as pd
import streamlit as st


def humanise_label(name: str) -> str:
    return name.replace("_", " ").strip().title()


def default_column_config(df: pd.DataFrame, *, url_columns: Iterable[str] = ()):
    config = {}
    url_set = set(url_columns)
    for col in df.columns:
        label = humanise_label(col)
        if col in url_set or col.endswith("_url"):
            config[col] = st.column_config.LinkColumn(label, display_text="Open source")
        elif pd.api.types.is_numeric_dtype(df[col]):
            config[col] = st.column_config.NumberColumn(label)
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            config[col] = st.column_config.DateColumn(label, format="DD MMM YYYY")
        else:
            config[col] = st.column_config.TextColumn(label)
    return config


def render_evidence_table(
    df: pd.DataFrame,
    *,
    url_columns: Iterable[str] = (),
    key: str,
    selection_mode: str | None = "single-row",
):
    if df.empty:
        st.info("No rows match the current filters.")
        return None

    return st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config=default_column_config(df, url_columns=url_columns),
        selection_mode=selection_mode,
        on_select="rerun" if selection_mode else "ignore",
        key=key,
    )
