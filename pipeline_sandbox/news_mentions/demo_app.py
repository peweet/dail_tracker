"""Standalone sandbox demo for the 'recent media mentions' member-page section.

Renders the collapsible section as it would appear on a member profile, with a
member picker so you can flip between a well-covered minister and a quiet
backbencher (empty state). Does NOT touch production pages.

Run:  streamlit run pipeline_sandbox/news_mentions/demo_app.py
"""
from pathlib import Path
import polars as pl
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
DATA = Path(__file__).resolve().parent / "news_mentions_sandbox.parquet"
MEMBERS = ROOT / "data/silver/parquet/flattened_members.parquet"

PAGE_SIZE = 6
TIER_STYLE = {
    "national":    ("#1d4ed8", "National"),
    "specialist":  ("#0f766e", "Specialist"),
    "local_paper": ("#b45309", "Local paper"),
    "local_radio": ("#7c3aed", "Local radio"),
    "partisan":    ("#9f1239", "Partisan"),
}

st.set_page_config(page_title="Media mentions — sandbox", page_icon="📰", layout="centered")

CSS = """
<style>
.block-container {max-width: 760px;}
.mm-card {background:#ffffff;border:1px solid #e7e2d8;border-radius:10px;
          padding:14px 16px;margin-bottom:10px;}
.mm-card a {color:#111827;text-decoration:none;font-weight:600;font-size:1.02rem;line-height:1.35;}
.mm-card a:hover {text-decoration:underline;}
.mm-meta {margin-top:7px;font-size:0.82rem;color:#6b7280;display:flex;gap:8px;align-items:center;flex-wrap:wrap;}
.mm-badge {color:#fff;border-radius:999px;padding:1px 9px;font-size:0.7rem;font-weight:600;}
.mm-body-note {color:#9ca3af;font-style:italic;}
.mm-month {font-size:0.78rem;letter-spacing:.06em;text-transform:uppercase;
           color:#9a8f7a;font-weight:700;margin:14px 0 8px;}
.mm-id {font-size:0.92rem;color:#6b7280;margin-top:-6px;}
.mm-disc {font-size:0.76rem;color:#9ca3af;border-top:1px solid #eee;margin-top:10px;padding-top:8px;}
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)


@st.cache_data
def load():
    df = pl.read_parquet(DATA)
    mem = (pl.read_parquet(MEMBERS)
           .select(["unique_member_code", "full_name", "party", "constituency_name"])
           .unique().drop_nulls(subset=["full_name"]).sort("full_name"))
    return df, mem


df, mem = load()

st.markdown("#### 📰 Media mentions — sandbox preview")
st.caption("Standalone demo of the collapsible member-page section. Switch members to see a busy "
           "minister vs. a quiet backbencher.")

# member picker — default to a well-covered member
covered = set(df["unique_member_code"].to_list())
names = mem["full_name"].to_list()
default_name = "Martin Heydon" if "Martin Heydon" in names else names[0]
pick = st.selectbox("Member", names, index=names.index(default_name))
row = mem.filter(pl.col("full_name") == pick).row(0, named=True)
code = row["unique_member_code"]

# ----- mock member-profile header (context for how the section sits) -----
st.markdown(f"### {pick}")
st.markdown(f"<div class='mm-id'>{row['party'] or '—'} · {row['constituency_name'] or '—'}</div>",
            unsafe_allow_html=True)
st.markdown("&nbsp;", unsafe_allow_html=True)

# ----- the media-mentions section (collapsible, count in label) -----
m = (df.filter(pl.col("unique_member_code") == code)
       .sort("published_at", descending=True, nulls_last=True))
n = m.height

with st.expander(f"📰 Recent media mentions ({n})", expanded=(n > 0)):
    if n == 0:
        st.caption("No recent mentions matched this member in the sampled local & national feeds.")
    else:
        # pagination state per member
        key = f"mm_shown_{code}"
        shown = st.session_state.get(key, PAGE_SIZE)
        page = m.head(shown)

        last_month = None
        for r in page.iter_rows(named=True):
            dt = r["published_at"]
            month = dt.strftime("%B %Y") if dt else "Undated"
            if month != last_month:
                st.markdown(f"<div class='mm-month'>{month}</div>", unsafe_allow_html=True)
                last_month = month
            colour, label = TIER_STYLE.get(r["outlet_tier"], ("#6b7280", r["outlet_tier"]))
            date_str = dt.strftime("%d %b") if dt else ""
            body_note = "" if r["match_in_title"] else "<span class='mm-body-note'>· named in article body</span>"
            url = r["article_url"] or "#"
            st.markdown(
                f"<div class='mm-card'>"
                f"<a href='{url}' target='_blank'>{r['article_title']}</a>"
                f"<div class='mm-meta'>"
                f"<span class='mm-badge' style='background:{colour}'>{label}</span>"
                f"<span>{r['outlet']}</span><span>· {date_str}</span>{body_note}"
                f"</div></div>",
                unsafe_allow_html=True,
            )

        if shown < n:
            if st.button(f"Show {min(PAGE_SIZE, n - shown)} more  ·  {shown}/{n}", key=f"more_{code}"):
                st.session_state[key] = shown + PAGE_SIZE
                st.rerun()

        st.markdown(
            "<div class='mm-disc'>Name-matched from public RSS feeds of Irish news outlets. "
            "A mention is not an assertion by this site and does not imply involvement. "
            "Headlines link to the publisher.</div>", unsafe_allow_html=True)

# ----- sidebar: coverage at a glance -----
with st.sidebar:
    st.markdown("**Sandbox coverage**")
    st.metric("Mentions", df.height)
    st.metric("Members matched", df["matched_name"].n_unique())
    st.metric("Articles", df["article_id"].n_unique())
    st.markdown("**Best-covered (try these):**")
    top = df.group_by("matched_name").len().sort("len", descending=True).head(8)
    for name, c in top.iter_rows():
        st.caption(f"{name} — {c}")
