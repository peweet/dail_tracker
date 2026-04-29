from __future__ import annotations

import pandas as pd
import streamlit as st

from utility.ui.components import empty_state


def render_member_identity_strip(row: pd.Series) -> None:
    member = row.get("member_name", "Selected member")
    party = row.get("party_name", "Unknown party")
    constituency = row.get("constituency", "Unknown constituency")

    st.html(
        f"""
        <section class="dt-member-strip">
          <div class="dt-kicker">Selected member</div>
          <h2>{member}</h2>
          <p>{party} · {constituency}</p>
        </section>
        """
    )


def render_member_detail_table(df: pd.DataFrame, *, title: str = "Member records") -> None:
    st.subheader(title)
    if df.empty:
        empty_state("No records for this member", "The selected member has no records in the current filtered view.")
        return
    st.dataframe(df, use_container_width=True, hide_index=True)
