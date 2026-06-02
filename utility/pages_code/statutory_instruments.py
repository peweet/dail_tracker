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

import datetime
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
    fmt_civic_date as _fmt_date,
    hero_banner,
    hide_sidebar,
    paginate,
    pagination_controls,
    render_stat_strip,
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
    # Use st.markdown with unsafe_allow_html so styles inject into the
    # document head (shared_css.inject_css does the same). st.html()
    # renders inside a sandboxed iframe in recent Streamlit versions,
    # which silently scopes any <style> block to that iframe only and
    # leaves the rest of the page unstyled.
    st.markdown(
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

        /* Whole card is wrapped in <a class="si-card-link"> — strip the
           default link colour/underline and give it a subtle hover lift so
           the affordance reads as clickable without screaming. */
        .si-card-link { display:block; text-decoration:none; color:inherit;
            margin-bottom:0.55rem; }
        .si-card-link:focus-visible { outline:2px solid #14232b; outline-offset:2px; }
        .si-card-link:hover .si-card { border-color:#c9cfd3;
            box-shadow: 0 1px 3px rgba(20,35,43,0.08); }
        .si-card-link:hover .si-card-title { color:#0a1418; }

        .si-card { background:#ffffff; border:1px solid #e5e2db; border-radius:8px;
            padding:1rem 1.15rem;
            transition: border-color 120ms ease-out, box-shadow 120ms ease-out; }
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

        /* Active-filter scope bar — read-only chips that show what's
           currently filtered. Modify filters via the facet tabs below. */
        .si-active-bar { display:flex; flex-wrap:wrap; gap:0.4rem; align-items:center;
            padding:0.5rem 0.75rem; background:#f5f1ea; border:1px solid #e5e2db;
            border-radius:6px; margin:0.4rem 0 0.6rem; }
        .si-active-label { font-size:0.7rem; text-transform:uppercase;
            letter-spacing:0.07em; color:#5b6b73; margin-right:0.3rem; }
        .si-active-chip { display:inline-flex; align-items:center; gap:0.3rem;
            background:#ffffff; border:1px solid #cfdde6; border-radius:999px;
            padding:0.18rem 0.45rem 0.18rem 0.7rem; font-size:0.78rem;
            color:#14232b; line-height:1.4; white-space:nowrap;
            text-decoration:none;
            transition: background 120ms ease-out, border-color 120ms ease-out; }
        .si-active-chip:hover { background:#fef2f2; border-color:#fca5a5;
            color:#7f1d1d; }
        .si-active-chip:focus-visible { outline:2px solid #14232b; outline-offset:1px; }
        .si-active-chip-x { font-size:0.95rem; line-height:1; color:#5b6b73;
            font-weight:400; }
        .si-active-chip:hover .si-active-chip-x { color:#7f1d1d; }
        .si-active-chip-all { background:transparent; border-color:#5b6b73;
            color:#5b6b73; padding-right:0.7rem; }
        .si-active-chip-all:hover { background:#14232b; border-color:#14232b;
            color:#ffffff; }

        /* Mobile hero tightening (≤640px). Audit P1-2: at 390×844 the
           hero+dek pushed all data below the fold. The kicker
           ("Iris Oifigiúil · Secondary legislation") is fully redundant
           on mobile — the page title and sidebar already establish
           context — so hide it. Clamp the dek to 2 lines so the search
           input and first KPI/card land above the fold. Page-scoped via
           _inject_si_css so other heroes are unaffected. */
        @media (max-width: 640px) {
            .dt-hero .dt-kicker { display: none; }
            .dt-hero .dt-dek {
                display: -webkit-box;
                -webkit-line-clamp: 2;
                -webkit-box-orient: vertical;
                overflow: hidden;
                font-size: 0.82rem;
                margin-top: 0.25rem;
            }
            .dt-hero { padding: 0.7rem 0.95rem 0.65rem !important; }
            .dt-hero h1 { font-size: 1.2rem !important; }
        }

        </style>
        """,
        unsafe_allow_html=True,
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


def _pretty_token(s: str) -> str:
    """snake_case / lowercase taxonomy values → sentence case; mixed-case
    human strings pass through. Pill rows read calmer with sentence case
    than Title Case. Two-axis decision tree:
      - underscores OR plain-lowercase → strip underscores + capitalise
      - already mixed-case → leave alone
    Special-case: tokens beginning with 'eu_'/'eu ' keep the EU prefix
    upper ('Eu instrument referenced' → 'EU instrument referenced')."""
    if not isinstance(s, str) or not s:
        return ""
    if "_" in s and s.lower() == s:
        out = s.replace("_", " ").capitalize()
    elif s == s.lower():
        out = s.capitalize()
    else:
        out = s
    # Preserve the EU acronym at word boundaries (handles 'Eu full effect',
    # 'Eu instrument referenced', and the underscore-converted 'Eu derived').
    if out.lower().startswith("eu "):
        out = "EU" + out[2:]
    return out


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


def _apply_filters(df, years, domain, op, department, minister, eu_only, search, post_committee=False) -> pd.DataFrame:
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
    """Four cells: total + year span, most active department, EU-derived
    share, enabling-Act link rate. The previous version repeated 'Finance'
    in two adjacent cells (policy-domain top almost always == dept top);
    dropped the domain cell and added the enabling-Act stat — that's the
    bridge to /legislation and the cleanest editorial fact about SI
    provenance."""
    total = len(df)
    if total == 0:
        return
    # logic_firewall: display_only — KPI strip operates on the active filter
    # set (df is post-filter). View-side rollup would need the same filter
    # parameters passed to a registered view; render-time aggregation on a
    # ≤6k-row frame is the simpler call.
    top_dept = df["si_department_label"].dropna().value_counts().head(1)
    eu_count = int(df["si_is_eu"].fillna(False).astype(bool).sum())
    eu_share = (eu_count / total * 100) if total else 0
    bill_count = int(df["bill_id"].notna().sum()) if "bill_id" in df.columns else 0
    bill_share = (bill_count / total * 100) if total else 0
    yrs = sorted(int(y) for y in df["si_year"].dropna().unique())
    yr_span = f"{yrs[0]}–{yrs[-1]}" if len(yrs) >= 2 else (str(yrs[0]) if yrs else "—")

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
        <div class="si-stat-num">{html.escape(_pretty_token(tdep))}</div>
        <div class="si-stat-label">Most active department</div>
        <div class="si-stat-sub">{tdepc:,} SIs</div>
      </div>
      <div class="si-stat">
        <div class="si-stat-num">{eu_count:,}</div>
        <div class="si-stat-label">EU-derived</div>
        <div class="si-stat-sub">{eu_share:.0f}% of scope</div>
      </div>
      <div class="si-stat">
        <div class="si-stat-num">{bill_share:.0f}%</div>
        <div class="si-stat-label">Linked to enabling Act</div>
        <div class="si-stat-sub">{bill_count:,} of {total:,} SIs</div>
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
    st.session_state.si_year_filter = []
    st.session_state.si_dept_filter = "All"
    st.session_state.si_op_filter = "All"
    st.session_state.si_domain_filter = "All"
    st.session_state.si_minister_filter = "All"
    st.session_state.si_eu_filter = False
    st.session_state.si_post_committee_filter = False
    st.session_state.si_title_search = ""


def _set_eu_scrutiny_scope() -> None:
    """The 'Show these SIs' button on the callout / tab. Clears everything
    else and sets EU-derived ON + the precise post-Dec-2025 date filter, so
    the list below matches the scrutiny-gap count exactly."""
    _clear_all_filters()
    st.session_state.si_eu_filter = True
    st.session_state.si_post_committee_filter = True


def _eu_scrutiny_stats(full_df: pd.DataFrame) -> dict:
    eu_mask = full_df["si_is_eu"].fillna(False).astype(bool) & (
        pd.to_datetime(full_df["si_signed_date"], errors="coerce") >= _COMMITTEE_FORMED
    )
    eu_df = full_df[eu_mask]
    return {
        "count": int(len(eu_df)),
        # logic_firewall: display_only — EU scrutiny callout's "top 5
        # departments" panel; aggregation on the post-filter eu_df only.
        "top_depts": eu_df["si_department_label"].dropna().value_counts().head(5).to_dict(),
        "eu_df": eu_df,
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

    st.html("""
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
            f'<div style="margin:0.18rem 0; font-size:0.9rem;"><strong>{c:,}</strong> &nbsp; {html.escape(d)}</div>'
        )

    st.html('<div style="margin:0.9rem 0;"></div>')

    bc1, bc2 = st.columns(2)
    with bc1:
        # on_click — the button lives inside the tab (after the year-pill
        # widget in script order), so this must mutate state pre-render.
        st.button(
            f"Show these {n} SIs in the list below →",
            key="si_eu_tab_show",
            type="primary",
            on_click=_set_eu_scrutiny_scope,
        )
    with bc2:
        st.markdown(f"[Read the article ↗]({_ARTICLE_URL})")


# Facet → clear handler. Each entry knows how to reset its own widget's
# session-state key. Driven by the ?clear=<key> URL handler at the top of
# the page entry — clicking a chip in the active-filter bar triggers the
# handler before widgets render, so the next rerun sees the cleared state.
def _clear_facet(key: str) -> None:
    if key == "year":
        st.session_state.si_year_filter = []
    elif key == "dept":
        st.session_state.si_dept_filter = "All"
    elif key == "op":
        st.session_state.si_op_filter = "All"
    elif key == "dom":
        st.session_state.si_domain_filter = "All"
    elif key == "min":
        st.session_state.si_minister_filter = "All"
    elif key == "eu":
        st.session_state.si_eu_filter = False
    elif key == "post":
        st.session_state.si_post_committee_filter = False
    elif key == "search":
        st.session_state.si_title_search = ""
    elif key == "all":
        _clear_all_filters()


def _active_filter_chips(full_df: pd.DataFrame) -> list[tuple[str, str]]:
    """(label, clear_key) pairs for every facet currently filtering. The
    clear_key feeds into _clear_facet via the ?clear=<key> URL handler.
    Year is collapsed under one 'Years (N)' chip when ≥3 are selected so
    the bar stays readable; clicking the chip clears the year filter
    entirely (rather than picking off years one-by-one)."""
    chips: list[tuple[str, str]] = []
    all_yrs = set(int(y) for y in full_df["si_year"].dropna().unique())
    yrs = st.session_state.get("si_year_filter") or []
    if yrs and set(yrs) != all_yrs:
        if len(yrs) <= 2:
            for y in sorted(yrs, reverse=True):
                chips.append((str(int(y)), "year"))
        else:
            chips.append((f"Years ({len(yrs)})", "year"))
    if (d := st.session_state.get("si_dept_filter")) and d != "All":
        chips.append((d, "dept"))
    if (op := st.session_state.get("si_op_filter")) and op != "All":
        chips.append((_pretty_token(op), "op"))
    if (dom := st.session_state.get("si_domain_filter")) and dom != "All":
        chips.append((_pretty_token(dom), "dom"))
    if (m := st.session_state.get("si_minister_filter")) and m != "All":
        chips.append((m, "min"))
    if st.session_state.get("si_eu_filter"):
        chips.append(("EU-derived", "eu"))
    if st.session_state.get("si_post_committee_filter"):
        chips.append(("Since Dec 2025", "post"))
    s = (st.session_state.get("si_title_search") or "").strip()
    if s:
        chips.append((f'"{s}"', "search"))
    return chips


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
    # Each chip is an <a href="?clear=key"> — clicking it removes that
    # facet's filter via the handler at the top of the page entry. The
    # trailing "Clear all" chip clears every facet at once.
    active = _active_filter_chips(full_df)
    if active:
        chip_html = "".join(
            f'<a class="si-active-chip" href="?clear={key}" target="_self" '
            f'aria-label="Remove filter: {html.escape(lbl, quote=True)}">'
            f'{html.escape(lbl)}<span class="si-active-chip-x" aria-hidden="true">×</span>'
            "</a>"
            for lbl, key in active
        )
        chip_html += (
            '<a class="si-active-chip si-active-chip-all" href="?clear=all" '
            'target="_self" aria-label="Clear all filters">Clear all</a>'
        )
        st.html(f'<div class="si-active-bar"><span class="si-active-label">Filtered by</span>{chip_html}</div>')

    # ── Row 3: Year — always visible, single line, multi-select ──────────
    yrs = sorted((int(y) for y in full_df["si_year"].dropna().unique()), reverse=True)
    # logic_firewall: display_only — year/department/operation/policy_domain/
    # minister value_counts in this block power chip-width labels only
    # ("All departments · 5,910"). They run on the full corpus once per page
    # render; the cost is negligible at this scale.
    yr_counts = full_df["si_year"].astype("Int64").value_counts().to_dict()
    # The current year is necessarily year-to-date; tag it so readers don't
    # compare its partial count against full-year neighbours.
    _current_year = datetime.date.today().year
    st.pills(
        "Year",
        yrs,
        default=yrs[:3] if len(yrs) >= 3 else yrs,
        selection_mode="multi",
        key="si_year_filter",
        format_func=lambda y: (
            f"{y} · {yr_counts.get(y, 0):,} YTD" if y == _current_year else f"{y} · {yr_counts.get(y, 0):,}"
        ),
    )

    # ── Row 4: tabbed primary facets — only one set of pills shows at a
    # time, so the page stays short. The tab label carries its selected
    # value once filtered (e.g. "Department: Justice").
    dept_sel = st.session_state.get("si_dept_filter")
    op_sel = st.session_state.get("si_op_filter")
    dom_sel = st.session_state.get("si_domain_filter")
    min_sel = st.session_state.get("si_minister_filter")
    tabs = st.tabs(
        [
            _tab_label("Department", dept_sel if dept_sel and dept_sel != "All" else None),
            _tab_label("What it does", _pretty_token(op_sel) if op_sel and op_sel != "All" else None),
            _tab_label("Policy area", _pretty_token(dom_sel) if dom_sel and dom_sel != "All" else None),
            _tab_label("Minister", min_sel if min_sel and min_sel != "All" else None),
            "⚠ EU scrutiny",
        ]
    )

    with tabs[0]:
        # logic_firewall: display_only — chip-width counts (see panel header).
        dept_counts = full_df["si_department_label"].dropna().value_counts().to_dict()
        dept_opts = ["All"] + sorted(dept_counts, key=dept_counts.get, reverse=True)
        st.pills(
            "Department",
            dept_opts,
            default="All",
            key="si_dept_filter",
            label_visibility="collapsed",
            format_func=lambda x: "All departments" if x == "All" else f"{x} · {dept_counts.get(x, 0):,}",
        )

    with tabs[1]:
        # logic_firewall: display_only — chip-width counts.
        op_counts = full_df["si_operation"].dropna().value_counts().to_dict()
        op_opts = ["All"] + sorted(op_counts, key=op_counts.get, reverse=True)
        st.pills(
            "Operation",
            op_opts,
            default="All",
            key="si_op_filter",
            label_visibility="collapsed",
            format_func=lambda x: "All operations" if x == "All" else f"{_pretty_token(x)} · {op_counts.get(x, 0):,}",
        )

    with tabs[2]:
        # logic_firewall: display_only — chip-width counts.
        dom_counts = full_df["si_policy_domain"].dropna().value_counts().to_dict()
        dom_opts = ["All"] + sorted(dom_counts, key=dom_counts.get, reverse=True)
        st.pills(
            "Policy area",
            dom_opts,
            default="All",
            key="si_domain_filter",
            label_visibility="collapsed",
            format_func=lambda x: (
                "All policy areas" if x == "All" else f"{_pretty_token(x)} · {dom_counts.get(x, 0):,}"
            ),
        )

    with tabs[3]:
        # logic_firewall: display_only — chip-width counts.
        min_counts = full_df["si_minister_name"].dropna().value_counts().to_dict()
        min_opts = ["All"] + sorted(min_counts, key=min_counts.get, reverse=True)
        st.selectbox(
            "Minister",
            min_opts,
            index=0,
            key="si_minister_filter",
            label_visibility="collapsed",
            format_func=lambda x: "All ministers" if x == "All" else f"{x} · {min_counts.get(x, 0):,}",
        )

    with tabs[4]:
        _render_eu_scrutiny_tab(full_df)


# ──────────────────────────────────────────────────────────────────────────────
# View 3 — SI index (cards + pagination)
# ──────────────────────────────────────────────────────────────────────────────
def _render_si_card(row: pd.Series) -> str:
    si_id = html.escape(_safe(row.get("si_id")) or "—")
    title = html.escape(_safe(row.get("si_title")) or "—")
    date_str = html.escape(_fmt_date(row.get("si_signed_date")))
    domain = _pretty_token(_safe(row.get("si_policy_domain")))
    op = _pretty_token(_safe(row.get("si_operation")))
    dept = _safe(row.get("si_department_label"))
    bill = _safe(row.get("bill_short_title"))

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

    # Whole card is the click target — Streamlit can't put a native button
    # inside an HTML block, so the previous "<card> + detached View detail
    # button" pattern wasted vertical space and broke the click affordance.
    # An <a href="?si=…"> wrapper lets Streamlit pick up the param change
    # on the next rerun via the listener at the top of the page entry.
    return (
        f'<a class="si-card-link" href="?si={html.escape(si_id, quote=True)}" target="_self">'
        '<div class="si-card">'
        '<div class="si-card-head">'
        f'<span class="si-card-ref">SI No. {si_id}</span>'
        f'<span class="si-card-date">{date_str}</span>'
        "</div>"
        f'<div class="si-card-title">{title}</div>'
        f'<div class="si-card-foot">{"".join(pills)}</div>'
        "</div>"
        "</a>"
    )


def _render_si_index(df: pd.DataFrame) -> None:
    if df.empty:
        empty_state("No SIs in scope", "Adjust the filters to widen the year, domain, department or operation.")
        return

    total = len(df)
    st.html(
        f'<div class="si-section-h">{total:,} statutory instrument'
        f"{'s' if total != 1 else ''} match the current filters</div>"
    )

    page_idx = paginate(total, key_prefix="si_idx", page_size=PAGE_SIZE)
    visible = df.iloc[page_idx * PAGE_SIZE : (page_idx + 1) * PAGE_SIZE]

    for _, row in visible.iterrows():
        st.html(_render_si_card(row))

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

    si_id = html.escape(_safe(row.get("si_id")) or "—")
    title = html.escape(_safe(row.get("si_title")) or "—")
    domain = _safe(row.get("si_policy_domain"))
    op = _safe(row.get("si_operation"))
    dept = _safe(row.get("si_department_label"))
    actor = _safe(row.get("si_responsible_actor"))
    min_name = _safe(row.get("si_minister_name"))
    min_code = _safe(row.get("si_minister_member_code"))
    signatory = _safe(row.get("si_signatory_name"))
    signed_date = _fmt_date(row.get("si_signed_date"))
    si_form = _safe(row.get("si_form"))
    eu_rel = _safe(row.get("si_eu_relationship"))
    parent = _safe(row.get("si_parent_legislation"))
    op_flags = _split_multi(_safe(row.get("si_operation_flags")))
    domains = _split_multi(_safe(row.get("si_policy_domains_all")))
    confidence = row.get("si_taxonomy_confidence")

    eisb = _eisb_url(row)
    bill_id = _safe(row.get("bill_id"))
    bill_title = _safe(row.get("bill_short_title"))

    st.html(f"""
    <div class="si-detail">
      <div class="si-detail-ref">Statutory Instrument No. {si_id}</div>
      <div class="si-detail-title">{title}</div>
    </div>
    """)

    # Department isn't detected on ~60% of pre-2014 SIs; render the cell
    # only when there's a real value rather than parking an em-dash and
    # wasting a quarter of the stat strip.
    stat_items = [
        stat_item(_fmt_date(row.get("si_signed_date")), "Issued"),
        stat_item(_pretty_token(op) or "—", "Operation"),
        stat_item(_pretty_token(domain) or "—", "Policy domain"),
    ]
    if dept:
        stat_items.append(stat_item(dept, "Department"))
    render_stat_strip(*stat_items)

    rows_html: list[str] = []

    def _row(label: str, val_html: str) -> None:
        rows_html.append(
            f'<div class="si-detail-row">'
            f'<div class="si-detail-label">{html.escape(label)}</div>'
            f'<div class="si-detail-val">{val_html}</div>'
            f"</div>"
        )

    _row("SI form", html.escape(_pretty_token(si_form)) or "—")
    _row(
        "Operation flags",
        " ".join(f'<span class="si-pill si-pill-op">{html.escape(_pretty_token(f))}</span>' for f in op_flags)
        if op_flags
        else "—",
    )
    _row(
        "Policy domains",
        " ".join(f'<span class="si-pill si-pill-domain">{html.escape(_pretty_token(d))}</span>' for d in domains)
        if domains
        else "—",
    )
    if eu_rel and eu_rel != "none_detected":
        _row(
            "EU relationship",
            " ".join(
                f'<span class="si-pill si-pill-eu">{html.escape(_pretty_token(e))}</span>' for e in _split_multi(eu_rel)
            ),
        )

    # ── Who signed — office-primary, person attributed ────────────────────────
    # The gazette prints the signing OFFICE (a fact). The PERSON is either
    # printed in a signature block (si_signatory_name — also a fact) or, absent
    # that, inferred from office + signing date against the ministerial-tenure
    # record (si_minister_name — a derivation, labelled as such). We never
    # present the inferred person as if the gazette named them.
    def _profile_link(name: str) -> str:
        # A clickable profile is shown only when the pipeline kept a consistent
        # sitting-member match (contradicting inferences are suppressed upstream
        # in si_entity_enrichment, so a present min_code is safe to link here).
        if min_code:
            return (
                f'<a class="dt-source-link" '
                f'href="{html.escape(member_profile_url(min_code, section="legislation"), quote=True)}" '
                f'target="_self">{html.escape(name)}</a>'
            )
        return html.escape(name)

    def _attrib(text: str) -> str:
        return f' <span style="color:#5b6b73;font-size:0.82rem;">{html.escape(text)}</span>'

    if actor:
        _row("Signing office", html.escape(actor))

    if signatory:
        # Printed in the notice — ground truth.
        _row("Signed by", _profile_link(signatory) + _attrib("— as printed in the notice"))
    elif min_name:
        # Not printed — derived from the signing office + date.
        held = f"— office held on {signed_date}" if signed_date and signed_date != "—" else "— office holder"
        _row(
            "Signed by",
            _profile_link(min_name) + _attrib(f"{held}, per ministerial-tenure record (not printed in the notice)"),
        )
    elif not actor:
        _row("Signing office", _attrib("Not recorded in this notice").strip())
    if parent.strip():
        # Pipe-separated Act names — rendered as plain text now that the
        # /legislation-poc target has been retired.
        pieces = [p.strip(" .,;") for p in parent.split("|") if p.strip(" .,;")]
        _row(
            "Parent legislation",
            " &nbsp;·&nbsp; ".join(html.escape(p) for p in pieces) if pieces else html.escape(parent),
        )
    if isinstance(confidence, (int, float)) and pd.notna(confidence):
        _row("Taxonomy confidence", f"{float(confidence):.2f}")

    eisb_html = (
        source_link_html(
            eisb,
            "View on irishstatutebook.ie",
            aria_label="Open this SI on the Electronic Irish Statute Book",
        )
        if eisb
        else ""
    )
    src_iris = _safe(row.get("iris_source_pdf"))
    src_links = []
    if eisb_html:
        src_links.append(eisb_html)
    if src_iris:
        src_links.append(
            f'<span style="color:#5b6b73;font-size:0.85rem;">Iris Oifigiúil source: {html.escape(src_iris)}</span>'
        )
    if src_links:
        _row("Official sources", " &nbsp; · &nbsp; ".join(src_links))

    st.html('<div class="si-detail">' + "".join(rows_html) + "</div>")

    # ── Cross-link panel — the enabling Act ───────────────────────────────────
    if bill_id:
        is_pre2014 = bill_id.startswith("act_")
        ref_label = "Act" if is_pre2014 else "Bill"
        kicker = (
            "↪ Made under (pre-2014 primary Act, curated)"
            if is_pre2014
            else "↪ Made under (matched Act in the Oireachtas index)"
        )
        local_link = (
            f'<a class="dt-source-link" href="/legislation?bill={html.escape(bill_id)}" '
            f'target="_self">View {ref_label} detail →</a>'
        )
        # Pre-2014 bill_ids are internal slugs ('act_1993_statistics') — not
        # useful to the reader. Only show the meta line when bill_id is the
        # canonical Oireachtas reference (post-2014, e.g. 'bill-123-of-2024').
        meta_html = "" if is_pre2014 else f'<div class="si-billlink-meta">{ref_label} {html.escape(bill_id)}</div>'
        st.html(f"""
        <div class="si-billlink">
          <div class="si-billlink-kicker">{kicker}</div>
          <div class="si-billlink-title">{html.escape(bill_title) or "—"}</div>
          {meta_html}
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

    # Filter-chip click handler — chips in the active-filter bar link to
    # ?clear=<key>; we drain the param, mutate session state pre-widget,
    # then rerun so widgets pick up the cleared values. Must happen here,
    # before _render_facets instantiates the widgets it references.
    url_clear = st.query_params.get("clear")
    if url_clear:
        _clear_facet(url_clear)
        del st.query_params["clear"]
        st.rerun()

    # ── Sidebar removed (sidebar→filter-bar migration) ──────────────────────────
    # The sidebar was header-only — all filters already live in the main panel
    # (see _render_facets), and page identity is carried by the top-nav tab +
    # the main hero (index) / back button + breadcrumb (detail). hide_sidebar()
    # drops the now-empty rail and the brand band's sidebar-clearing gutter.
    hide_sidebar()

    # ── Detail view ───────────────────────────────────────────────────────────
    if selected:
        match = si_df[si_df["si_id"] == selected]
        if match.empty:
            # The bare st.warning was off-register with the rest of the
            # page; an empty_state in civic voice tells the user what
            # actually happened (corpus floor) and where to go next.
            empty_state(
                f"SI {selected!r} isn't in the index",
                "The Dáil Tracker SI corpus covers 2016 onwards — older "
                "instruments aren't yet ingested. Old bookmark or typed URL? "
                "Try browsing the index, or search by title.",
            )
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

    # KPI strip above the fold — anchors the headline numbers BEFORE the
    # facet machinery. Session-state values reflect the prior rerun's
    # widget state, so computing `filtered` here (pre-_render_facets)
    # produces the same result as computing it after. On the very first
    # render of a session, `si_year_filter` doesn't exist yet — fall back
    # to the same default the year-pill widget will use (most recent 3
    # years) so KPIs and the cards below show the same scope. .get() with
    # no default distinguishes 'never set' (use default) from '[] = user
    # cleared the filter' (don't re-seed).
    if "si_year_filter" in st.session_state:
        _year_filter = st.session_state["si_year_filter"] or []
    else:
        _yrs = sorted((int(y) for y in si_df["si_year"].dropna().unique()), reverse=True)
        _year_filter = _yrs[:3] if len(_yrs) >= 3 else _yrs
    filtered = _apply_filters(
        si_df,
        years=_year_filter,
        domain=st.session_state.get("si_domain_filter"),
        op=st.session_state.get("si_op_filter"),
        department=st.session_state.get("si_dept_filter"),
        minister=st.session_state.get("si_minister_filter"),
        eu_only=st.session_state.get("si_eu_filter", False),
        post_committee=st.session_state.get("si_post_committee_filter", False),
        search=st.session_state.get("si_title_search"),
    )
    _render_kpi_strip(filtered)

    # Facets next — each chip is a click-to-filter control with the SI
    # count baked into the label. Changing a facet reruns the page and
    # the KPI strip above updates with the new scope.
    _render_facets(si_df)

    _render_si_index(filtered)


if __name__ == "__main__":
    statutory_instruments_page()
