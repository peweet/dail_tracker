"""
Statutory Instruments — standalone browser page.

Sources from the registered DuckDB view v_statutory_instruments
(sql_views/legislation_si_index.sql), which reads
data/gold/parquet/statutory_instruments.parquet — produced by
si_entity_enrichment.py. The SI is treated as a first-class entity: the
full ~5,900-SI corpus (2016+), NOT gated on a bill match. No raw parquet
read here; filtering/facets/KPIs happen in pandas off the single
registered frame.

Features:
  1. Editorial hero + KPI strip (totals, top domain, top department, EU share)
  2. Trends — domain × year heatmap, department activity, operation breakdown
  3. Filterable SI index — year / domain / department / operation / EU-only /
     title search, paginated cards
  4. SI detail panel — full taxonomy, irishstatutebook.ie link, Iris source
  5. Cross-link to legislation — the enabling Act, where a confident match
     exists (~30% of SIs).
"""
from __future__ import annotations

import html
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.legislation_data import fetch_si_entity_index
from shared_css import inject_css
from ui.components import (
    back_button,
    empty_state,
    hero_banner,
    paginate,
    pagination_controls,
    render_stat_strip,
    sidebar_page_header,
    stat_item,
)
from ui.entity_links import member_profile_url, source_link_html

# ── Page config ────────────────────────────────────────────────────────────────
PAGE_SIZE = 10


# ──────────────────────────────────────────────────────────────────────────────
# Page-local CSS. Reuses dt-* tokens but lives here rather than shared_css.py
# so the canonical class set is not polluted by this page's si-* classes.
# ──────────────────────────────────────────────────────────────────────────────
def _inject_si_css() -> None:
    st.html(
        """
        <style>
        .si-stat-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(170px,1fr));
            gap: 0.85rem; margin: 1.1rem 0 1.6rem; }
        .si-stat { background:#ffffff; border:1px solid #e5e2db; border-radius:8px; padding:0.95rem 1.1rem; }
        .si-stat-num { font-family: ui-serif, Georgia, serif; font-size:1.7rem; font-weight:700;
            line-height:1.1; color:#14232b; }
        .si-stat-label { font-size:0.7rem; text-transform:uppercase; letter-spacing:0.07em;
            color:#5b6b73; margin-top:0.35rem; }
        .si-stat-sub { font-size:0.75rem; color:#5b6b73; margin-top:0.15rem; }

        .si-card { background:#ffffff; border:1px solid #e5e2db; border-radius:8px;
            padding:1rem 1.15rem; margin-bottom:0.55rem; }
        .si-card-head { display:flex; align-items:baseline; gap:0.6rem; flex-wrap:wrap; }
        .si-card-ref { font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size:0.78rem;
            color:#5b6b73; }
        .si-card-date { margin-left:auto; font-size:0.76rem; color:#5b6b73; }
        .si-card-title { font-family: ui-serif, Georgia, serif; font-size:1.02rem; line-height:1.4;
            color:#14232b; margin: 0.35rem 0 0.55rem; }
        .si-card-foot { display:flex; gap:0.4rem; flex-wrap:wrap; align-items:center; }

        .si-pill { background:#f5f1ea; border:1px solid #e5e2db; border-radius:999px;
            padding:0.16rem 0.6rem; font-size:0.7rem; color:#14232b; line-height:1.4;
            white-space:nowrap; }
        .si-pill-domain { background:#ecf2f6; border-color:#cfdde6; }
        .si-pill-op     { background:#f6f0e6; border-color:#e6d9c2; }
        .si-pill-eu     { background:#fff7e6; border-color:#f0d99b; color:#7a5a00; }
        .si-pill-act    { background:#e8efe6; border-color:#bcd1b3; color:#2c4a23; }
        .si-pill-dept   { background:#ffffff; border-color:#dfd9cf; color:#5b6b73; }

        .si-detail { background:#ffffff; border:1px solid #e5e2db; border-radius:10px;
            padding:1.4rem 1.55rem; margin-top:0.5rem; }
        .si-detail-ref { font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size:0.82rem;
            color:#5b6b73; }
        .si-detail-title { font-family: ui-serif, Georgia, serif; font-size:1.4rem; line-height:1.3;
            color:#14232b; margin: 0.4rem 0 1rem; }
        .si-detail-row { display:flex; gap:0.85rem; padding:0.55rem 0;
            border-top:1px solid #f0ece5; align-items:flex-start; }
        .si-detail-row:first-of-type { border-top:none; padding-top:0; }
        .si-detail-label { width:170px; flex-shrink:0; font-size:0.72rem; text-transform:uppercase;
            letter-spacing:0.06em; color:#5b6b73; padding-top:0.18rem; }
        .si-detail-val { flex:1; font-size:0.93rem; color:#14232b; line-height:1.5; }
        .si-detail-val .si-pill { margin-right:0.25rem; }

        .si-billlink { background:#fbfcf9; border:1px solid #c9d6c0; border-radius:8px;
            padding:1.1rem 1.25rem; margin-top:1.1rem; }
        .si-billlink-kicker { font-size:0.7rem; text-transform:uppercase; letter-spacing:0.07em;
            color:#2c4a23; font-weight:600; }
        .si-billlink-title { font-family: ui-serif, Georgia, serif; font-size:1.1rem; margin:0.35rem 0 0.45rem;
            color:#14232b; line-height:1.35; }
        .si-billlink-meta { font-size:0.82rem; color:#5b6b73; margin-bottom:0.55rem; }

        .si-section-h { font-family: ui-serif, Georgia, serif; font-size:1.05rem; margin: 1.5rem 0 0.55rem;
            color:#14232b; }

        .si-trend-card-h { font-size:0.78rem; text-transform:uppercase; letter-spacing:0.07em;
            color:#5b6b73; margin-bottom:0.35rem; }

        .si-note { font-size:0.78rem; color:#5b6b73; font-style:italic; margin: 0.4rem 0 1rem; }

        /* Active-filter scope bar — read-only chips that show what's
           currently filtered. Modify filters via the facet tabs below. */
        .si-active-bar { display:flex; flex-wrap:wrap; gap:0.4rem; align-items:center;
            padding:0.5rem 0.75rem; background:#f5f1ea; border:1px solid #e5e2db;
            border-radius:6px; margin:0.4rem 0 0.6rem; }
        .si-active-label { font-size:0.7rem; text-transform:uppercase;
            letter-spacing:0.07em; color:#5b6b73; margin-right:0.3rem; }
        .si-active-chip { background:#ffffff; border:1px solid #cfdde6;
            border-radius:999px; padding:0.18rem 0.7rem; font-size:0.78rem;
            color:#14232b; line-height:1.4; white-space:nowrap; }

        </style>
        """
    )


# ──────────────────────────────────────────────────────────────────────────────
# Data loading
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Loading Statutory Instruments…")
def load_si() -> pd.DataFrame:
    """The full SI entity table via the registered v_statutory_instruments
    view. Year floor, taxonomy-confidence, quarantine and category filters are
    already applied upstream by si_entity_enrichment.py."""
    df = fetch_si_entity_index()
    if df.empty:
        return df
    df["si_signed_date"] = pd.to_datetime(df["si_signed_date"], errors="coerce")
    # Defensive: drop any mojibake-bearing titles.
    df = df[~df["si_title"].astype(str).str.contains("�", na=False)]
    return df.reset_index(drop=True)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _eisb_url(row: pd.Series) -> str:
    """Prefer the extracted eisb_url; fall back to the canonical eli pattern."""
    eisb = row.get("eisb_url")
    if isinstance(eisb, str) and eisb.startswith("http"):
        return eisb
    yr, no = row.get("si_year"), row.get("si_number")
    if pd.notna(yr) and pd.notna(no):
        return f"https://www.irishstatutebook.ie/eli/{int(yr)}/si/{int(no)}/made/en/print"
    return ""


def _safe(v) -> str:
    """Coerce a possibly-NaN/None cell to a string. NaN is truthy, so
    `row.get(x) or ''` does not guard missing values — anything heading for
    html.escape or string ops goes through this."""
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    return str(v)


def _fmt_date(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    try:
        ts = pd.Timestamp(val)
        return f"{ts.day} {ts.strftime('%b %Y')}"
    except Exception:
        return str(val)


def _pretty_token(s: str) -> str:
    """snake_case → sentence case; leaves human strings (with spaces/caps)
    alone. Pill rows read calmer with sentence case than Title Case."""
    if not isinstance(s, str) or not s:
        return ""
    if "_" in s and s.lower() == s:
        return s.replace("_", " ").capitalize()
    return s


def _split_multi(s, sep="|"):
    if not isinstance(s, str):
        return []
    return [p.strip() for p in s.split(sep) if p.strip()]


# ──────────────────────────────────────────────────────────────────────────────
# Filters
# ──────────────────────────────────────────────────────────────────────────────
# The Seanad Committee on EU Scrutiny & Transparency was formally established
# in December 2025. Statutory instruments signed on/after this date are the
# population the committee was meant to scrutinise — and per its chair (Irish
# Times, Feb 2026), zero have been received for prior review.
_COMMITTEE_FORMED = pd.Timestamp("2025-12-01")


def _apply_filters(df, years, domain, op, department, minister, eu_only, search,
                   post_committee=False) -> pd.DataFrame:
    out = df
    if years:
        out = out[out["si_year"].isin(years)]
    if domain and domain != "All":
        out = out[out["si_policy_domain"] == domain]
    if op and op != "All":
        out = out[out["si_operation"] == op]
    if department and department != "All":
        out = out[out["si_department_label"] == department]
    if minister and minister != "All":
        out = out[out["si_minister_name"] == minister]
    if eu_only:
        out = out[out["si_is_eu"].fillna(False).astype(bool)]
    if post_committee:
        signed = pd.to_datetime(out["si_signed_date"], errors="coerce")
        out = out[signed >= _COMMITTEE_FORMED]
    if search:
        s = search.strip().lower()
        out = out[out["si_title"].astype(str).str.lower().str.contains(s, na=False)]
    return out


# ──────────────────────────────────────────────────────────────────────────────
# View 1 — KPI strip
# ──────────────────────────────────────────────────────────────────────────────
def _render_kpi_strip(df: pd.DataFrame) -> None:
    total = len(df)
    if total == 0:
        return
    top_domain = df["si_policy_domain"].dropna().value_counts().head(1)
    top_dept   = df["si_department_label"].dropna().value_counts().head(1)
    eu_count   = int(df["si_is_eu"].fillna(False).astype(bool).sum())
    eu_share   = (eu_count / total * 100) if total else 0
    yrs        = sorted(int(y) for y in df["si_year"].dropna().unique())
    yr_span    = f"{yrs[0]}–{yrs[-1]}" if len(yrs) >= 2 else (str(yrs[0]) if yrs else "—")

    td   = top_domain.index[0] if not top_domain.empty else "—"
    tdc  = int(top_domain.iloc[0]) if not top_domain.empty else 0
    tdep = top_dept.index[0] if not top_dept.empty else "—"
    tdepc = int(top_dept.iloc[0]) if not top_dept.empty else 0

    st.html(f"""
    <div class="si-stat-grid">
      <div class="si-stat">
        <div class="si-stat-num">{total:,}</div>
        <div class="si-stat-label">Statutory Instruments</div>
        <div class="si-stat-sub">{html.escape(yr_span)}</div>
      </div>
      <div class="si-stat">
        <div class="si-stat-num">{html.escape(_pretty_token(td))}</div>
        <div class="si-stat-label">Top policy domain</div>
        <div class="si-stat-sub">{tdc:,} SIs</div>
      </div>
      <div class="si-stat">
        <div class="si-stat-num">{html.escape(_pretty_token(tdep))}</div>
        <div class="si-stat-label">Most active department</div>
        <div class="si-stat-sub">{tdepc:,} SIs</div>
      </div>
      <div class="si-stat">
        <div class="si-stat-num">{eu_count:,}</div>
        <div class="si-stat-label">EU-derived</div>
        <div class="si-stat-sub">{eu_share:.0f}% of scope</div>
      </div>
    </div>
    """)


# ──────────────────────────────────────────────────────────────────────────────
# View 2 — Facet pills (replaces the old static heatmaps + bar charts)
#
# Every chip is the filter for its facet — clicking it sets the corresponding
# session_state key and the SI list below refreshes. Counts come from the
# full corpus so chip widths stay stable across interactions; the index's
# heading reports the post-filter total. ~50 ministers is too many for a
# pill row, so that one falls back to a native selectbox.
# ──────────────────────────────────────────────────────────────────────────────
def _clear_all_filters() -> None:
    """Reset every facet to its 'no filter' state. The chip bar's Clear All
    button calls this; widgets that own these keys pick the cleared values
    up on the next rerun."""
    st.session_state.si_year_filter            = []
    st.session_state.si_dept_filter            = "All"
    st.session_state.si_op_filter              = "All"
    st.session_state.si_domain_filter          = "All"
    st.session_state.si_minister_filter        = "All"
    st.session_state.si_eu_filter              = False
    st.session_state.si_post_committee_filter  = False
    st.session_state.si_title_search           = ""


def _set_eu_scrutiny_scope() -> None:
    """The 'Show these SIs' button on the callout / tab. Clears everything
    else and sets EU-derived ON + the precise post-Dec-2025 date filter, so
    the list below matches the scrutiny-gap count exactly."""
    _clear_all_filters()
    st.session_state.si_eu_filter             = True
    st.session_state.si_post_committee_filter = True


def _eu_scrutiny_stats(full_df: pd.DataFrame) -> dict:
    eu_mask = full_df["si_is_eu"].fillna(False).astype(bool) & (
        pd.to_datetime(full_df["si_signed_date"], errors="coerce") >= _COMMITTEE_FORMED
    )
    eu_df = full_df[eu_mask]
    return {
        "count":     int(len(eu_df)),
        "top_depts": eu_df["si_department_label"].dropna().value_counts().head(5).to_dict(),
        "eu_df":     eu_df,
    }


_ARTICLE_URL = (
    "https://www.irishtimes.com/politics/2026/02/18/"
    "eu-directives-not-being-passed-to-special-committee-before-being-signed-into-law/"
)


def _render_eu_scrutiny_tab(full_df: pd.DataFrame) -> None:
    """Expanded 'EU scrutiny' tab — same data as the callout plus more
    framing and a fuller department breakdown."""
    s = _eu_scrutiny_stats(full_df)
    n = s["count"]
    if n == 0:
        st.caption("No EU-derived SIs have been signed since the committee was established (1 December 2025).")
        return

    st.html(f"""
    <p style="font-size:0.95rem; line-height:1.55; margin: 0.6rem 0 0.7rem;">
      A new <strong>Seanad Committee on EU Scrutiny &amp; Transparency</strong>,
      chaired by Cathaoirleach Mark Daly, was established in December 2025 to
      examine the draft statutory instruments that transpose EU directives
      <em>before</em> they become Irish law. The Taoiseach committed in writing
      that all such instruments would be sent to the committee at least six
      months before their transposition deadlines.
    </p>
    <p style="font-size:0.95rem; line-height:1.55; margin: 0 0 0.8rem;">
      In February 2026, the chair reported that
      <strong>not one draft EU law had been received.</strong> The State has
      also paid a <strong>€1.54&nbsp;m</strong> fine for failing to transpose
      the EU work-life balance directive on time.
    </p>
    """)

    c1, c2 = st.columns(2)
    c1.metric("EU SIs signed since 1 Dec 2025", f"{n:,}")
    c2.metric("Departments transposing", f"{len(s['top_depts'])}+")

    st.markdown("**By department**")
    for d, c in s["top_depts"].items():
        st.html(
            f'<div style="margin:0.18rem 0; font-size:0.9rem;">'
            f'<strong>{c:,}</strong> &nbsp; {html.escape(d)}</div>'
        )

    st.html('<div style="margin:0.9rem 0;"></div>')

    bc1, bc2 = st.columns(2)
    with bc1:
        # on_click — the button lives inside the tab (after the year-pill
        # widget in script order), so this must mutate state pre-render.
        st.button(f"Show these {n} SIs in the list below →",
                  key="si_eu_tab_show",
                  type="primary",
                  on_click=_set_eu_scrutiny_scope)
    with bc2:
        st.markdown(f"[Read the article ↗]({_ARTICLE_URL})")


def _active_filter_labels(full_df: pd.DataFrame) -> list[str]:
    """Human-readable labels for every facet that's currently filtering. Year
    is collapsed when many are selected so the bar stays readable."""
    labels: list[str] = []
    all_yrs = set(int(y) for y in full_df["si_year"].dropna().unique())
    yrs     = st.session_state.get("si_year_filter") or []
    if yrs and set(yrs) != all_yrs:
        if len(yrs) <= 3:
            for y in sorted(yrs, reverse=True):
                labels.append(str(int(y)))
        else:
            labels.append(f"Years ({len(yrs)})")
    if (d := st.session_state.get("si_dept_filter")) and d != "All":
        labels.append(d)
    if (op := st.session_state.get("si_op_filter")) and op != "All":
        labels.append(_pretty_token(op))
    if (dom := st.session_state.get("si_domain_filter")) and dom != "All":
        labels.append(_pretty_token(dom))
    if (m := st.session_state.get("si_minister_filter")) and m != "All":
        labels.append(m)
    if st.session_state.get("si_eu_filter"):
        labels.append("EU-derived")
    if st.session_state.get("si_post_committee_filter"):
        labels.append("Since Dec 2025")
    s = (st.session_state.get("si_title_search") or "").strip()
    if s:
        labels.append(f'"{s}"')
    return labels


def _tab_label(base: str, active_value: str | None) -> str:
    """A tab label that carries its currently-selected value (truncated when
    long). Plain `base` when the facet is unfiltered."""
    if not active_value:
        return base
    val = str(active_value)
    if len(val) > 22:
        val = val[:20] + "…"
    return f"{base}: {val}"


def _render_facets(full_df: pd.DataFrame) -> None:
    if full_df.empty:
        return

    # ── Row 1: search + EU toggle ─────────────────────────────────────────
    c1, c2 = st.columns([3, 1])
    with c1:
        st.text_input(
            "Search title",
            placeholder="Search SI titles — e.g. fisheries, vehicles, sanctions, COVID…",
            key="si_title_search",
            label_visibility="collapsed",
        )
    with c2:
        st.toggle("EU-derived only", key="si_eu_filter")

    # ── Row 2: active-filter scope bar (only when something is filtered) ──
    active = _active_filter_labels(full_df)
    if active:
        chips = "".join(
            f'<span class="si-active-chip">{html.escape(lbl)}</span>'
            for lbl in active
        )
        bar_col, btn_col = st.columns([6, 1])
        with bar_col:
            st.html(
                '<div class="si-active-bar">'
                '<span class="si-active-label">Filtered by</span>'
                f' {chips}</div>'
            )
        with btn_col:
            # on_click for the same reason — Clear all mutates every widget
            # key, so the mutation must happen pre-render.
            st.button("Clear all", type="tertiary", key="si_clear_all",
                      on_click=_clear_all_filters)

    # ── Row 3: Year — always visible, single line, multi-select ──────────
    yrs       = sorted((int(y) for y in full_df["si_year"].dropna().unique()), reverse=True)
    yr_counts = full_df["si_year"].astype("Int64").value_counts().to_dict()
    st.pills(
        "Year",
        yrs,
        default=yrs[:3] if len(yrs) >= 3 else yrs,
        selection_mode="multi",
        key="si_year_filter",
        format_func=lambda y: f"{y}  · {yr_counts.get(y, 0):,}",
    )

    # ── Row 4: tabbed primary facets — only one set of pills shows at a
    # time, so the page stays short. The tab label carries its selected
    # value once filtered (e.g. "Department: Justice").
    dept_sel = st.session_state.get("si_dept_filter")
    op_sel   = st.session_state.get("si_op_filter")
    dom_sel  = st.session_state.get("si_domain_filter")
    min_sel  = st.session_state.get("si_minister_filter")
    tabs = st.tabs([
        _tab_label("Department",  dept_sel if dept_sel and dept_sel != "All" else None),
        _tab_label("What it does", _pretty_token(op_sel) if op_sel and op_sel != "All" else None),
        _tab_label("Policy area", _pretty_token(dom_sel) if dom_sel and dom_sel != "All" else None),
        _tab_label("Minister",    min_sel if min_sel and min_sel != "All" else None),
        "⚠ EU scrutiny",
    ])

    with tabs[0]:
        dept_counts = full_df["si_department_label"].dropna().value_counts().to_dict()
        dept_opts   = ["All"] + sorted(dept_counts, key=dept_counts.get, reverse=True)
        st.pills(
            "Department",
            dept_opts,
            default="All",
            key="si_dept_filter",
            label_visibility="collapsed",
            format_func=lambda x: "All departments"
                                  if x == "All"
                                  else f"{x}  · {dept_counts.get(x, 0):,}",
        )

    with tabs[1]:
        op_counts = full_df["si_operation"].dropna().value_counts().to_dict()
        op_opts   = ["All"] + sorted(op_counts, key=op_counts.get, reverse=True)
        st.pills(
            "Operation",
            op_opts,
            default="All",
            key="si_op_filter",
            label_visibility="collapsed",
            format_func=lambda x: "All operations"
                                  if x == "All"
                                  else f"{_pretty_token(x)}  · {op_counts.get(x, 0):,}",
        )

    with tabs[2]:
        dom_counts = full_df["si_policy_domain"].dropna().value_counts().to_dict()
        dom_opts   = ["All"] + sorted(dom_counts, key=dom_counts.get, reverse=True)
        st.pills(
            "Policy area",
            dom_opts,
            default="All",
            key="si_domain_filter",
            label_visibility="collapsed",
            format_func=lambda x: "All policy areas"
                                  if x == "All"
                                  else f"{_pretty_token(x)}  · {dom_counts.get(x, 0):,}",
        )

    with tabs[3]:
        min_counts = full_df["si_minister_name"].dropna().value_counts().to_dict()
        min_opts   = ["All"] + sorted(min_counts, key=min_counts.get, reverse=True)
        st.selectbox(
            "Minister",
            min_opts,
            index=0,
            key="si_minister_filter",
            label_visibility="collapsed",
            format_func=lambda x: "All ministers"
                                  if x == "All"
                                  else f"{x}  · {min_counts.get(x, 0):,}",
        )

    with tabs[4]:
        _render_eu_scrutiny_tab(full_df)


# ──────────────────────────────────────────────────────────────────────────────
# View 3 — SI index (cards + pagination)
# ──────────────────────────────────────────────────────────────────────────────
def _render_si_card(row: pd.Series) -> str:
    si_id    = html.escape(_safe(row.get("si_id")) or "—")
    title    = html.escape(_safe(row.get("si_title")) or "—")
    date_str = html.escape(_fmt_date(row.get("si_signed_date")))
    domain   = _pretty_token(_safe(row.get("si_policy_domain")))
    op       = _pretty_token(_safe(row.get("si_operation")))
    dept     = _safe(row.get("si_department_label"))
    bill     = _safe(row.get("bill_short_title"))

    pills = []
    if domain:
        pills.append(f'<span class="si-pill si-pill-domain">{html.escape(domain)}</span>')
    if op:
        pills.append(f'<span class="si-pill si-pill-op">{html.escape(op)}</span>')
    if bool(row.get("si_is_eu")):
        pills.append('<span class="si-pill si-pill-eu">EU-derived</span>')
    if bill:
        pills.append(f'<span class="si-pill si-pill-act">↪ Made under {html.escape(bill)}</span>')
    if dept:
        pills.append(f'<span class="si-pill si-pill-dept">{html.escape(dept)}</span>')

    return (
        '<div class="si-card">'
        '<div class="si-card-head">'
        f'<span class="si-card-ref">SI No. {si_id}</span>'
        f'<span class="si-card-date">{date_str}</span>'
        '</div>'
        f'<div class="si-card-title">{title}</div>'
        f'<div class="si-card-foot">{"".join(pills)}</div>'
        '</div>'
    )


def _render_si_index(df: pd.DataFrame) -> None:
    if df.empty:
        empty_state("No SIs in scope",
                    "Adjust the filters to widen the year, domain, department or operation.")
        return

    total = len(df)
    st.html(
        f'<div class="si-section-h">{total:,} statutory instrument'
        f'{"s" if total != 1 else ""} match the current filters</div>'
    )

    page_idx = paginate(total, key_prefix="si_idx", page_size=PAGE_SIZE)
    visible = df.iloc[page_idx * PAGE_SIZE : (page_idx + 1) * PAGE_SIZE]

    for _, row in visible.iterrows():
        st.html(_render_si_card(row))
        if st.button("View detail →", key=f"si_open_{row['si_id']}", type="tertiary"):
            st.session_state["si_selected_id"] = row["si_id"]
            st.query_params["si"] = row["si_id"]
            st.rerun()

    pagination_controls(
        total,
        key_prefix="si_idx",
        page_sizes=(PAGE_SIZE,),
        default_page_size=PAGE_SIZE,
        label="SIs",
    )


# ──────────────────────────────────────────────────────────────────────────────
# View 4 — SI detail (+ View 5 cross-link, inline)
# ──────────────────────────────────────────────────────────────────────────────
def _render_si_detail(row: pd.Series) -> None:
    if back_button("← Back to SI Index", key="si_detail"):
        st.session_state.pop("si_selected_id", None)
        st.query_params.clear()
        st.rerun()

    si_id    = html.escape(_safe(row.get("si_id")) or "—")
    title    = html.escape(_safe(row.get("si_title")) or "—")
    domain   = _safe(row.get("si_policy_domain"))
    op       = _safe(row.get("si_operation"))
    dept     = _safe(row.get("si_department_label"))
    actor    = _safe(row.get("si_responsible_actor"))
    min_name = _safe(row.get("si_minister_name"))
    min_code = _safe(row.get("si_minister_member_code"))
    si_form  = _safe(row.get("si_form"))
    eu_rel   = _safe(row.get("si_eu_relationship"))
    parent   = _safe(row.get("si_parent_legislation"))
    op_flags = _split_multi(_safe(row.get("si_operation_flags")))
    domains  = _split_multi(_safe(row.get("si_policy_domains_all")))
    confidence = row.get("si_taxonomy_confidence")

    eisb = _eisb_url(row)
    bill_id    = _safe(row.get("bill_id"))
    bill_title = _safe(row.get("bill_short_title"))

    st.html(f"""
    <div class="si-detail">
      <div class="si-detail-ref">Statutory Instrument No. {si_id}</div>
      <div class="si-detail-title">{title}</div>
    </div>
    """)

    render_stat_strip(
        stat_item(_fmt_date(row.get("si_signed_date")), "Issued"),
        stat_item(_pretty_token(op) or "—", "Operation"),
        stat_item(_pretty_token(domain) or "—", "Policy domain"),
        stat_item(dept or "—", "Department"),
    )

    rows_html: list[str] = []

    def _row(label: str, val_html: str) -> None:
        rows_html.append(
            f'<div class="si-detail-row">'
            f'<div class="si-detail-label">{html.escape(label)}</div>'
            f'<div class="si-detail-val">{val_html}</div>'
            f'</div>'
        )

    _row("SI form", html.escape(_pretty_token(si_form)) or "—")
    _row("Operation flags", " ".join(
        f'<span class="si-pill si-pill-op">{html.escape(_pretty_token(f))}</span>'
        for f in op_flags) if op_flags else "—")
    _row("Policy domains", " ".join(
        f'<span class="si-pill si-pill-domain">{html.escape(_pretty_token(d))}</span>'
        for d in domains) if domains else "—")
    if eu_rel and eu_rel != "none_detected":
        _row("EU relationship", " ".join(
            f'<span class="si-pill si-pill-eu">{html.escape(_pretty_token(e))}</span>'
            for e in _split_multi(eu_rel)))
    if min_name:
        # Section anchor lands the user in /member-overview's Legislation
        # expander — that's where SIs signed by ministers live as a
        # sub-section. Matches the cross-page contract from Phases 3–8.
        person_html = (
            f'<a class="dt-source-link" '
            f'href="{html.escape(member_profile_url(min_code, section="legislation"), quote=True)}" '
            f'target="_self">{html.escape(min_name)}</a>'
        ) if min_code else html.escape(min_name)
        _row("Minister", person_html)
    if actor:
        _row("Responsible actor (as signed)", html.escape(actor))
    if parent.strip():
        # Pipe-separated Act names — rendered as plain text now that the
        # /legislation-poc target has been retired.
        pieces = [p.strip(" .,;") for p in parent.split("|") if p.strip(" .,;")]
        _row("Parent legislation",
             " &nbsp;·&nbsp; ".join(html.escape(p) for p in pieces)
             if pieces else html.escape(parent))
    if isinstance(confidence, (int, float)) and pd.notna(confidence):
        _row("Taxonomy confidence", f"{float(confidence):.2f}")

    eisb_html = source_link_html(
        eisb, "View on irishstatutebook.ie",
        aria_label="Open this SI on the Electronic Irish Statute Book",
    ) if eisb else ""
    src_iris = _safe(row.get("iris_source_pdf"))
    src_links = []
    if eisb_html:
        src_links.append(eisb_html)
    if src_iris:
        src_links.append(
            f'<span style="color:#5b6b73;font-size:0.85rem;">'
            f'Iris Oifigiúil source: {html.escape(src_iris)}</span>'
        )
    if src_links:
        _row("Official sources", " &nbsp; · &nbsp; ".join(src_links))

    st.html('<div class="si-detail">' + "".join(rows_html) + "</div>")

    # ── Cross-link panel — the enabling Act ───────────────────────────────────
    if bill_id:
        is_pre2014 = bill_id.startswith("act_")
        ref_label  = "Act" if is_pre2014 else "Bill"
        kicker = (
            "↪ Made under (pre-2014 primary Act, curated)"
            if is_pre2014
            else "↪ Made under (matched Act in the Oireachtas index)"
        )
        local_link = (
            f'<a class="dt-source-link" href="/legislation?bill={html.escape(bill_id)}" '
            f'target="_self">View {ref_label} detail →</a>'
        )
        st.html(f"""
        <div class="si-billlink">
          <div class="si-billlink-kicker">{kicker}</div>
          <div class="si-billlink-title">{html.escape(bill_title) or "—"}</div>
          <div class="si-billlink-meta">{ref_label} {html.escape(bill_id)}</div>
          {local_link}
        </div>
        """)
    elif parent.strip():
        st.html(f"""
        <div class="si-billlink" style="background:#fbf6ed; border-color:#e9dab3;">
          <div class="si-billlink-kicker" style="color:#7a5a00;">Parent legislation (unmatched)</div>
          <div class="si-billlink-title">{html.escape(parent)}</div>
          <div class="si-billlink-meta">
            No confident match against the Oireachtas bills index or the curated
            pre-2014 Acts table. Many SIs are made under framework Acts that
            predate the bills database (2014).
          </div>
        </div>
        """)


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────
def statutory_instruments_page() -> None:
    inject_css()
    _inject_si_css()

    si_df = load_si()

    url_si = st.query_params.get("si")
    if url_si:
        st.session_state["si_selected_id"] = url_si
    selected = st.session_state.get("si_selected_id")

    # ── Sidebar ───────────────────────────────────────────────────────────────
    # All filters live in the main panel now (see _render_facets), so the
    # sidebar is just the page header — the facet pills are more discoverable
    # in the main flow than tucked into a sidebar selectbox stack.
    with st.sidebar:
        sidebar_page_header("Statutory Instruments")
        st.html(
            '<div class="page-subtitle">'
            + ('SI detail' if selected else 'Secondary legislation · Iris Oifigiúil')
            + '</div>'
        )

    # ── Detail view ───────────────────────────────────────────────────────────
    if selected:
        match = si_df[si_df["si_id"] == selected]
        if match.empty:
            st.warning(f"SI '{selected}' not found.")
            if back_button("← Back to SI Index", key="si_detail_nf"):
                st.session_state.pop("si_selected_id", None)
                st.query_params.clear()
                st.rerun()
            return
        _render_si_detail(match.iloc[0])
        return

    # ── Index view ────────────────────────────────────────────────────────────
    hero_banner(
        kicker="Iris Oifigiúil · Secondary legislation",
        title="Statutory Instruments",
        dek=(
            "The regulations, orders and commencement instruments that put Acts "
            "into force and fill in their detail — far more of Irish law happens "
            "here than in primary legislation. Browse the full corpus by policy "
            "domain, responsible department, operation type, or EU origin; every "
            "instrument links to its authoritative text on irishstatutebook.ie."
        ),
    )

    # Facet pills first — they are the interaction surface that replaces the
    # old static heatmaps + bar charts. Each chip is its own click-to-filter
    # control, with the SI count baked into the label.
    _render_facets(si_df)

    filtered = _apply_filters(
        si_df,
        years=st.session_state.get("si_year_filter") or [],
        domain=st.session_state.get("si_domain_filter"),
        op=st.session_state.get("si_op_filter"),
        department=st.session_state.get("si_dept_filter"),
        minister=st.session_state.get("si_minister_filter"),
        eu_only=st.session_state.get("si_eu_filter", False),
        post_committee=st.session_state.get("si_post_committee_filter", False),
        search=st.session_state.get("si_title_search"),
    )

    _render_kpi_strip(filtered)
    _render_si_index(filtered)


if __name__ == "__main__":
    statutory_instruments_page()
