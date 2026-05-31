"""Locality housing PoC — NOT a published feature.

Standalone Streamlit sketch that renders the three housing parquets for a
selected Local Authority. Lives in pipeline_sandbox/ deliberately:
  * sibling-app posture (not registered in utility/app.py)
  * no import from utility/* (mini-app must stand alone)
  * explicit "PoC, not for publication" banner at top of the page

Run:  streamlit run pipeline_sandbox/housing_locality_poc_experimental.py

Reads (read-only):
  data/gold/parquet/housing_la_master.parquet
  data/gold/parquet/housing_la_year_series.parquet
  data/gold/parquet/housing_national_year_series.parquet
"""
from __future__ import annotations

from pathlib import Path

import altair as alt
import pandas as pd
import polars as pl
import streamlit as st

_ROOT = Path(__file__).resolve().parents[1]
_PARQ = _ROOT / "data" / "gold" / "parquet"

st.set_page_config(
    page_title="Housing — Locality PoC",
    page_icon=":material/home_work:",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# ── data ─────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def load_master() -> pd.DataFrame:
    return pl.read_parquet(_PARQ / "housing_la_master.parquet").to_pandas()


@st.cache_data(show_spinner=False)
def load_year_series() -> pd.DataFrame:
    return pl.read_parquet(_PARQ / "housing_la_year_series.parquet").to_pandas()


@st.cache_data(show_spinner=False)
def load_national() -> pd.DataFrame:
    return pl.read_parquet(_PARQ / "housing_national_year_series.parquet").to_pandas()


# ── styling ─────────────────────────────────────────────────────────────────
st.html(
    """
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Zilla+Slab:wght@400;600;700&family=Epilogue:wght@400;500;600&display=swap');

      header[data-testid="stHeader"] { display: none; }
      .block-container { padding-top: 1rem; max-width: 1280px; }

      .poc-banner {
        background: #fef3c7;
        border: 2px solid #d97706;
        border-radius: 6px;
        padding: 0.75rem 1.25rem;
        margin-bottom: 1.25rem;
        font-family: 'Epilogue', sans-serif;
        font-size: 0.86rem;
        color: #78350f;
      }
      .poc-banner strong { color: #92400e; }

      .hero {
        background: #111827;
        color: #fff;
        padding: 1.5rem 1.75rem;
        border-radius: 6px;
        margin-bottom: 1.25rem;
      }
      .hero h1 {
        font-family: 'Zilla Slab', Georgia, serif;
        font-size: 1.6rem;
        font-weight: 700;
        letter-spacing: -0.02em;
        margin: 0;
        color: #fff;
      }
      .hero .sub {
        font-family: 'Epilogue', sans-serif;
        font-size: 0.86rem;
        color: rgba(255,255,255,0.65);
        margin-top: 0.3rem;
      }

      .section-h {
        font-family: 'Zilla Slab', Georgia, serif;
        font-size: 1.15rem;
        font-weight: 600;
        color: #1f2937;
        border-bottom: 1px solid #e5e7eb;
        padding-bottom: 0.3rem;
        margin: 1.5rem 0 0.75rem 0;
      }

      .card {
        background: #ffffff;
        border: 1px solid #e5e7eb;
        border-radius: 6px;
        padding: 0.95rem 1.1rem;
        height: 100%;
      }
      .card .label {
        font-family: 'Epilogue', sans-serif;
        font-size: 0.74rem;
        font-weight: 500;
        color: #6b7280;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        margin-bottom: 0.4rem;
      }
      .card .figure {
        font-family: 'Zilla Slab', Georgia, serif;
        font-size: 1.55rem;
        font-weight: 700;
        color: #111827;
        line-height: 1.1;
      }
      .card .sub {
        font-family: 'Epilogue', sans-serif;
        font-size: 0.78rem;
        color: #6b7280;
        margin-top: 0.3rem;
      }
      .card .prov {
        font-family: 'Epilogue', sans-serif;
        font-size: 0.68rem;
        color: #9ca3af;
        margin-top: 0.55rem;
        font-style: italic;
      }
    </style>
    """
)

# ── header / banner ──────────────────────────────────────────────────────────
st.html(
    """
    <div class="poc-banner">
      <strong>PROOF-OF-CONCEPT</strong> &nbsp;·&nbsp; Not a Dáil Tracker feature.
      Sibling-app sketch for LA-axis housing data. Will not be published
      as a feature of the main app — exists only to test whether the data
      deserves its own standalone mini-app.
    </div>
    """
)

# ── controls ─────────────────────────────────────────────────────────────────
master = load_master()
ys = load_year_series()
nat = load_national()

las = sorted(master["la"].dropna().unique().tolist())
default_ix = las.index("Dublin City") if "Dublin City" in las else 0
la = st.selectbox(
    "Local Authority",
    las,
    index=default_ix,
    label_visibility="collapsed",
)

# ── hero ─────────────────────────────────────────────────────────────────────
row = master[master["la"] == la].iloc[0].to_dict()
pop_year = row.get("__pop_vintage_year")
pop_est = row.get("population_2025_est") or row.get("population_2022_census")
st.html(
    f"""
    <div class="hero">
      <h1>{la}</h1>
      <div class="sub">Population {int(pop_est):,} ({pop_year}) &nbsp;·&nbsp;
        Local Authority housing snapshot</div>
    </div>
    """
)

# ── snapshot cards (from master) ────────────────────────────────────────────
st.html('<div class="section-h">Snapshot</div>')


def _fmt(v, fmt="{:,.0f}", fallback="—"):
    if v is None or pd.isna(v):
        return fallback
    return fmt.format(v)


def card(label, figure, sub, prov):
    return f"""
    <div class="card">
      <div class="label">{label}</div>
      <div class="figure">{figure}</div>
      <div class="sub">{sub}</div>
      <div class="prov">{prov}</div>
    </div>
    """


cards_row1 = [
    card(
        "Social housing waiting list",
        _fmt(row.get("ssha_waiting_list_2025")),
        f"{_fmt(row.get('ssha_non_eea_2025'))} non-EEA · {_fmt(row.get('ssha_irish_2025'))} Irish",
        row.get("__ssha_source", ""),
    ),
    card(
        "Active vacancy rate",
        f"{_fmt(row.get('vacancy_rate_active_pct'), '{:.1f}%')}",
        f"Census 2022 incl. holiday homes: {_fmt(row.get('vacancy_rate_census_pct'), '{:.1f}%')}",
        row.get("__vacancy_active_source", ""),
    ),
    card(
        "Construction pipeline",
        _fmt(row.get("pipeline_units_q4_2025")),
        f"{_fmt(row.get('pipeline_schemes_q4_2025'))} schemes",
        row.get("__pipeline_source", ""),
    ),
]
cols = st.columns(3, gap="small")
for c, html in zip(cols, cards_row1):
    with c:
        st.html(html)

cards_row2 = [
    card(
        "Weekly rent — LA tenants",
        f"€{_fmt(row.get('weekly_rent_local_authority_eur'), '{:.0f}')}",
        f"Private landlord: €{_fmt(row.get('weekly_rent_private_landlord_eur'), '{:.0f}')}",
        row.get("__rent_source", ""),
    ),
    card(
        "HAP ceiling — 2 children",
        f"€{_fmt(row.get('hap_ceiling_2children_eur'), '{:.0f}')} / mo",
        f"1 adult: €{_fmt(row.get('hap_ceiling_1adult_eur'), '{:.0f}')} · "
        f"couple: €{_fmt(row.get('hap_ceiling_couple_eur'), '{:.0f}')}",
        row.get("__hap_limit_source", ""),
    ),
    card(
        "Housing Commission supply target to 2050",
        _fmt(row.get("supply_needed_2050_scen_a")),
        f"Scenario A · Scenario B: {_fmt(row.get('supply_needed_2050_scen_b'))}",
        row.get("__supply_source", ""),
    ),
]
cols = st.columns(3, gap="small")
for c, html in zip(cols, cards_row2):
    with c:
        st.html(html)


# ── trends (from year_series) ───────────────────────────────────────────────
st.html('<div class="section-h">Trends</div>')
ys_la = ys[ys["la"] == la].copy()


def line_chart(df, y, title, y_fmt=",d"):
    base = (
        alt.Chart(df.dropna(subset=[y]))
        .mark_line(point=alt.OverlayMarkDef(filled=True, size=55), strokeWidth=2.5, color="#1e40af")
        .encode(
            x=alt.X("year:O", title=None, axis=alt.Axis(labelFontSize=11)),
            y=alt.Y(f"{y}:Q", title=title, axis=alt.Axis(format=y_fmt, labelFontSize=11)),
            tooltip=["year", alt.Tooltip(f"{y}:Q", format=y_fmt)],
        )
        .properties(height=200, title=alt.TitleParams(text=title, fontSize=12, anchor="start", color="#374151"))
    )
    return base


trend_cols = st.columns(2, gap="medium")
with trend_cols[0]:
    st.altair_chart(
        line_chart(ys_la, "population_est", "Population (est.)", y_fmt=",d"),
        use_container_width=True,
    )
with trend_cols[1]:
    st.altair_chart(
        line_chart(ys_la, "hap_starts", "HAP starts per year", y_fmt=",d"),
        use_container_width=True,
    )

trend_cols2 = st.columns(2, gap="medium")
with trend_cols2[0]:
    st.altair_chart(
        line_chart(ys_la, "vacancy_rate_pct", "Active vacancy rate %", y_fmt=".1f"),
        use_container_width=True,
    )
with trend_cols2[1]:
    st.altair_chart(
        line_chart(ys_la, "hc_la_grants_eur", "Housing Commission grants (€)", y_fmt=",.0f"),
        use_container_width=True,
    )

# ── national context ─────────────────────────────────────────────────────────
st.html('<div class="section-h">National context</div>')

delivery = nat[(nat["metric_family"] == "social_housing_delivery") &
               (nat["category"] == "Build")].copy()
chart = (
    alt.Chart(delivery)
    .mark_bar()
    .encode(
        x=alt.X("year:O", title=None),
        y=alt.Y("value:Q", title="Build units (national)"),
        color=alt.Color(
            "metric:N",
            scale=alt.Scale(domain=["target", "output"], range=["#9ca3af", "#1e40af"]),
            legend=alt.Legend(title=None, orient="top-left"),
        ),
        xOffset="metric:N",
        tooltip=["year", "metric", alt.Tooltip("value:Q", format=",d")],
    )
    .properties(
        height=230,
        title=alt.TitleParams(
            text="Social housing — Build target vs output (national, Rebuilding Ireland)",
            fontSize=12, anchor="start", color="#374151",
        ),
    )
)
st.altair_chart(chart, use_container_width=True)

# ── provenance footer ───────────────────────────────────────────────────────
st.html('<div class="section-h">Sources</div>')
prov_cols = [c for c in row.keys() if c.startswith("__") and "source" in c]
srcs = sorted({row[c] for c in prov_cols if isinstance(row.get(c), str) and row.get(c)})
src_html = "<ul style='font-family: Epilogue, sans-serif; font-size: 0.78rem; color: #6b7280;'>"
for s in srcs:
    src_html += f"<li style='margin: 0.2rem 0;'>{s}</li>"
src_html += "</ul>"
st.html(src_html)

st.caption(
    f"PoC built {pd.Timestamp.now():%Y-%m-%d %H:%M} from "
    f"housing_la_master.parquet · housing_la_year_series.parquet · "
    f"housing_national_year_series.parquet"
)
