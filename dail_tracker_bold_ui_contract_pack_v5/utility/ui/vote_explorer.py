from __future__ import annotations

import pandas as pd
import streamlit as st

from utility.ui.table_config import render_evidence_table


def render_vote_index(df: pd.DataFrame):
    """Render a searchable vote index.

    This helper displays already-shaped vote index rows. It must not calculate vote results.
    """
    if df.empty:
        st.info("No votes match the current filters.")
        return None
    return render_evidence_table(
        df,
        url_columns=("oireachtas_url", "official_pdf_url", "source_url"),
        key="vote_index_table",
        selection_mode="single-row",
    )


def render_vote_result_summary(summary: pd.DataFrame) -> None:
    if summary.empty:
        st.caption("No vote result summary is available for the selected vote.")
        return
    st.dataframe(summary, use_container_width=True, hide_index=True)
