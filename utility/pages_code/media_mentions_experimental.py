"""EXPERIMENTAL page — preview of the per-member 'recent media mentions' section.

Isolated sandbox so it can be shown locally and then deleted or promoted:
  DELETE  : remove this file, ``ui/media_mentions_experimental.py`` and the nav
            entry in ``utility/app.py`` (search "media-mentions").
  PROMOTE : move :func:`render_media_mentions_section` into the Interests area of
            ``pages_code/member_overview.py`` and swap the sandbox parquet for a
            registered ``v_member_news_mentions`` view.

Reads the sandbox parquet written by ``pipeline_sandbox/news_mentions/extract.py``.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

_ROOT = Path(__file__).resolve().parents[2]
_HISTORIC = _ROOT / "pipeline_sandbox" / "historic_members" / "_out" / "member_roster_wide.parquet"
_DAIL = _ROOT / "data" / "silver" / "parquet" / "flattened_members.parquet"
_SEANAD = _ROOT / "data" / "silver" / "parquet" / "flattened_seanad_members.parquet"


@st.cache_data(show_spinner=False)
def _roster() -> pd.DataFrame:
    """Pickable members (current + former if the historic roster exists)."""
    cols = ["unique_member_code", "full_name", "party", "constituency_name"]
    src = _HISTORIC if _HISTORIC.exists() else _DAIL
    df = pd.read_parquet(src)
    df = df[[c for c in cols if c in df.columns]].drop_duplicates()
    df = df.dropna(subset=["full_name"]).sort_values("full_name")
    # house lookup for the interests panel: Seanad codes else Dáil
    seanad_codes: set = set()
    if _SEANAD.exists():
        seanad_codes = set(pd.read_parquet(_SEANAD)["unique_member_code"].tolist())
    df["house"] = df["unique_member_code"].apply(lambda c: "Seanad" if c in seanad_codes else "Dáil")
    return df.reset_index(drop=True)


def media_mentions_experimental_page() -> None:
    from ui.components import hide_sidebar
    from ui.media_mentions_experimental import load_media_mentions, render_media_mentions_section

    hide_sidebar()

    st.markdown("## 📰 Media mentions — experimental")
    st.caption(
        "Sandbox preview of a per-member media-mentions section (not promoted). Source: "
        "`pipeline_sandbox/news_mentions` — name-matched RSS from ~41 Irish national & local outlets."
    )

    roster = _roster()
    mentions = load_media_mentions()

    # default to the best-covered member so the preview lands on content
    default_idx = 0
    if not mentions.empty:
        from collections import Counter

        top_code = Counter(mentions["unique_member_code"]).most_common(1)[0][0]
        hit = roster.index[roster["unique_member_code"] == top_code].tolist()
        if hit:
            default_idx = int(hit[0])

    names = roster["full_name"].tolist()
    pick = st.selectbox("Member", names, index=default_idx)
    row = roster.iloc[names.index(pick)]
    code, house = row["unique_member_code"], row["house"]

    st.markdown(f"### {pick}")
    st.caption(f"{row.get('party') or '—'} · {row.get('constituency_name') or '—'}")

    # Real interests panel for context (defensive: never let it break the preview).
    with st.expander("Declared interests", expanded=False):
        try:
            from ui.interests_panel import render_member_interests

            render_member_interests(house, pick, show_member_header=False, year_pill_key="mm_int_year")
        except Exception as exc:  # noqa: BLE001 — preview robustness only
            st.caption(f"(interests panel unavailable in this preview: {type(exc).__name__})")

    # The feature under preview.
    render_media_mentions_section(code, expanded_default=True)
