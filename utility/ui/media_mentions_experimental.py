"""EXPERIMENTAL — sandbox 'recent media mentions' section for the member page.

Self-contained and easy to remove. Reads the sandbox parquet produced by
``pipeline_sandbox/news_mentions/extract.py`` (NOT a registered SQL view yet).

PROMOTE: replace :func:`load_media_mentions` with a registered view
(``v_member_news_mentions``) and call :func:`render_media_mentions_section`
from ``ui/interests_panel`` / ``pages_code/member_overview``.

DELETE: remove this file, ``pages_code/media_mentions_experimental.py``, and the
nav entry in ``utility/app.py``. Nothing else imports it.

Data shaping here is presentation-only (filter to one member, newest-first,
client-side pagination) — the member-matching logic that decides *who* an
article is about lives upstream in the extractor, keeping the logic firewall
intact.
"""
from __future__ import annotations

from html import escape as _h
from pathlib import Path

import pandas as pd
import streamlit as st

_SANDBOX_PARQUET = (
    Path(__file__).resolve().parents[2]
    / "pipeline_sandbox" / "news_mentions" / "news_mentions_sandbox.parquet"
)

PAGE_SIZE = 6
# tier -> (badge colour, label)
TIER_STYLE = {
    "national": ("#1d4ed8", "National"),
    "specialist": ("#0f766e", "Specialist"),
    "local_paper": ("#b45309", "Local paper"),
    "local_radio": ("#7c3aed", "Local radio"),
    "partisan": ("#9f1239", "Partisan"),
}
_DISCLAIMER = (
    "Name-matched from public RSS feeds of Irish news outlets. A mention is not an "
    "assertion by this site and does not imply involvement. Headlines link to the publisher."
)


# ── pure helpers (unit-tested; no Streamlit, no I/O) ─────────────────────────
def shape_member_mentions(df: pd.DataFrame, member_code: str) -> pd.DataFrame:
    """Filter to one member's mentions, newest-first (undated last). Display-only."""
    if df is None or len(df) == 0 or "unique_member_code" not in df.columns:
        return pd.DataFrame(columns=getattr(df, "columns", None))
    m = df[df["unique_member_code"] == member_code].copy()
    if m.empty:
        return m
    m["_pub"] = pd.to_datetime(m["published_at"], errors="coerce")
    m = m.sort_values("_pub", ascending=False, na_position="last").drop(columns="_pub")
    return m.reset_index(drop=True)


def month_label(ts) -> str:
    """'June 2026' for a timestamp; 'Undated' for null/unparseable."""
    ts = pd.to_datetime(ts, errors="coerce")
    return "Undated" if pd.isna(ts) else ts.strftime("%B %Y")


def _day_label(ts) -> str:
    ts = pd.to_datetime(ts, errors="coerce")
    return "" if pd.isna(ts) else ts.strftime("%d %b")


# ── data access ──────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def load_media_mentions() -> pd.DataFrame:
    if not _SANDBOX_PARQUET.exists():
        return pd.DataFrame()
    return pd.read_parquet(_SANDBOX_PARQUET)


# ── render ───────────────────────────────────────────────────────────────────
def _inject_css() -> None:
    if st.session_state.get("_mm_css"):
        return
    st.session_state["_mm_css"] = True
    st.markdown(
        """
<style>
.mm-card {background:#ffffff;border:1px solid #e7e2d8;border-radius:10px;padding:14px 16px;margin-bottom:10px;}
.mm-card a {color:#111827;text-decoration:none;font-weight:600;font-size:1.02rem;line-height:1.35;}
.mm-card a:hover {text-decoration:underline;}
.mm-meta {margin-top:7px;font-size:0.82rem;color:#6b7280;display:flex;gap:8px;align-items:center;flex-wrap:wrap;}
.mm-badge {color:#fff;border-radius:999px;padding:1px 9px;font-size:0.7rem;font-weight:600;}
.mm-body-note {color:#9ca3af;font-style:italic;}
.mm-month {font-size:0.78rem;letter-spacing:.06em;text-transform:uppercase;color:#9a8f7a;font-weight:700;margin:14px 0 8px;}
.mm-disc {font-size:0.76rem;color:#9ca3af;border-top:1px solid #eee;margin-top:10px;padding-top:8px;}
</style>
""",
        unsafe_allow_html=True,
    )


def _safe_url(url: str) -> str:
    url = (url or "").strip()
    return _h(url) if url[:8].lower() in ("https://", "http://") or url.startswith("https") else "#"


def render_media_mentions_section(member_code: str, *, expanded_default: bool = False) -> None:
    """Collapsible 'Recent media mentions' section for one member (display-only)."""
    _inject_css()
    m = shape_member_mentions(load_media_mentions(), member_code)
    n = len(m)
    with st.expander(f"📰 Recent media mentions ({n})", expanded=expanded_default and n > 0):
        if n == 0:
            st.caption("No recent mentions matched this member in the sampled local & national feeds.")
            return

        key = f"mm_shown_{member_code}"
        shown = int(st.session_state.get(key, PAGE_SIZE))
        last_month = None
        for r in m.head(shown).to_dict("records"):  # display-only iteration (no logic)
            label_month = month_label(r.get("published_at"))
            if label_month != last_month:
                st.markdown(f"<div class='mm-month'>{label_month}</div>", unsafe_allow_html=True)
                last_month = label_month
            colour, tier_label = TIER_STYLE.get(r.get("outlet_tier"), ("#6b7280", str(r.get("outlet_tier"))))
            body_note = "" if r.get("match_in_title") else "<span class='mm-body-note'>· named in article body</span>"
            st.markdown(
                f"<div class='mm-card'>"
                f"<a href='{_safe_url(r.get('article_url'))}' target='_blank' rel='noopener'>"
                f"{_h(str(r.get('article_title') or ''))}</a>"
                f"<div class='mm-meta'>"
                f"<span class='mm-badge' style='background:{colour}'>{_h(tier_label)}</span>"
                f"<span>{_h(str(r.get('outlet') or ''))}</span>"
                f"<span>· {_day_label(r.get('published_at'))}</span>{body_note}"
                f"</div></div>",
                unsafe_allow_html=True,
            )

        if shown < n:
            if st.button(f"Show {min(PAGE_SIZE, n - shown)} more  ·  {shown}/{n}", key=f"mm_more_{member_code}"):
                st.session_state[key] = shown + PAGE_SIZE
                st.rerun()

        st.markdown(f"<div class='mm-disc'>{_DISCLAIMER}</div>", unsafe_allow_html=True)
