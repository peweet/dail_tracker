from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import streamlit as st


def download_current_view_csv(
    df: pd.DataFrame,
    *,
    page_id: str,
    label: str = "Download current view as CSV",
    help_text: str | None = "Exports the rows currently displayed after filters.",
) -> None:
    if df.empty:
        st.download_button(
            label,
            data="",
            file_name=f"{page_id}_empty.csv",
            mime="text/csv",
            disabled=True,
            help="No rows to export.",
        )
        return

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_name = f"{page_id}_{timestamp}.csv"

    st.download_button(
        label,
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=file_name,
        mime="text/csv",
        help=help_text,
    )
