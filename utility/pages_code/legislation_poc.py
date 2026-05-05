"""
Legislation (POC) — "How Acts of the Oireachtas are expanded by SIs".

The canonical /legislation page shows BILLS — legislation under consideration
before either house. By definition those bills are not yet Acts, so SIs can't
cite them. This POC takes the opposite view: once a bill becomes an Act, how
is it being amended, commenced, revoked, or otherwise expanded through
Statutory Instruments? The unit of analysis is the parent Act, not the bill.

Stage 1 — ranked list of Acts by number of SIs made under them.
Stage 2 — Act detail: operation breakdown, domain mix, full SI list.

POC: business logic in Streamlit. Sources `iris_si_taxonomy.csv` via the
loader cached in legislation_si_poc so both POC pages share a single
extract.
"""
from __future__ import annotations

import html
import sys
from pathlib import Path
from urllib.parse import quote, unquote

import altair as alt
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

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
from ui.entity_links import source_link_html
from ui.export_controls import export_button
from ui.source_pdfs import provenance_expander

# POC: reach across to the SI POC for its loader + helpers. Both pages
# share the @st.cache_data extract so it's only paid once per session.
from pages_code.legislation_si_poc import (
    _eisb_url,
    _fmt_date,
    _pretty_token,
    _safe,
    load_si,
)


# ── Page-local CSS ────────────────────────────────────────────────────────────
def _inject_poc_css() -> None:
    st.html(
        """
        <style>
        .lp-stat-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(170px,1fr));
            gap: 0.85rem; margin: 1.1rem 0 1.6rem; }
        .lp-stat { background:#ffffff; border:1px solid #e5e2db; border-radius:8px;
            padding:0.95rem 1.1rem; }
        .lp-stat-num { font-family: ui-serif, Georgia, serif; font-size:1.7rem; font-weight:700;
            line-height:1.1; color:#14232b; }
        .lp-stat-label { font-size:0.7rem; text-transform:uppercase; letter-spacing:0.07em;
            color:#5b6b73; margin-top:0.35rem; }
        .lp-stat-sub { font-size:0.75rem; color:#5b6b73; margin-top:0.15rem; }

        .lp-act-card { background:#ffffff; border:1px solid #e5e2db; border-radius:8px;
            padding:1rem 1.15rem; margin-bottom:0.55rem; }
        .lp-act-head { display:flex; align-items:baseline; gap:0.7rem; flex-wrap:wrap; }
        .lp-act-rank { font-family: ui-monospace, "SF Mono", Menlo, monospace;
            font-size:0.85rem; color:#5b6b73; min-width:1.7rem; }
        .lp-act-count { background:#14232b; color:#fbf6ed; border-radius:999px;
            padding:0.25rem 0.7rem; font-size:0.78rem; font-weight:600;
            font-family: ui-monospace, "SF Mono", Menlo, monospace; }
        .lp-act-yspan { font-size:0.76rem; color:#5b6b73; margin-left:auto; }
        .lp-act-title { font-family: ui-serif, Georgia, serif; font-size:1.08rem;
            line-height:1.4; color:#14232b; margin: 0.4rem 0 0.55rem; }
        .lp-act-foot { display:flex; gap:0.4rem; flex-wrap:wrap; align-items:center; }

        .lp-pill { background:#f5f1ea; border:1px solid #e5e2db; border-radius:999px;
            padding:0.16rem 0.6rem; font-size:0.7rem; color:#14232b; line-height:1.4;
            white-space:nowrap; }
        .lp-pill-domain { background:#ecf2f6; border-color:#cfdde6; }
        .lp-pill-op     { background:#f6f0e6; border-color:#e6d9c2; }
        .lp-pill-eu     { background:#fff7e6; border-color:#f0d99b; color:#7a5a00; }

        .lp-detail-h { background:#ffffff; border:1px solid #e5e2db; border-radius:10px;
            padding:1.4rem 1.5rem; margin-top:0.4rem; }
        .lp-detail-kicker { font-size:0.72rem; text-transform:uppercase;
            letter-spacing:0.06em; color:#5b6b73; }
        .lp-detail-title { font-family: ui-serif, Georgia, serif; font-size:1.4rem;
            line-height:1.3; color:#14232b; margin: 0.45rem 0 0.4rem; }
        .lp-detail-meta { font-size:0.85rem; color:#5b6b73; }

        .lp-section-h { font-family: ui-serif, Georgia, serif; font-size:1.05rem;
            margin: 1.4rem 0 0.55rem; color:#14232b; }

        .lp-si-row { background:#ffffff; border:1px solid #e5e2db; border-radius:8px;
            padding:0.7rem 1rem; margin-bottom:0.45rem; }
        .lp-si-head { display:flex; align-items:baseline; gap:0.55rem; flex-wrap:wrap; }
        .lp-si-ref { font-family: ui-monospace, "SF Mono", Menlo, monospace;
            font-size:0.75rem; color:#5b6b73; }
        .lp-si-date { margin-left:auto; font-size:0.74rem; color:#5b6b73; }
        .lp-si-title { font-family: ui-serif, Georgia, serif; font-size:0.95rem;
            line-height:1.4; color:#14232b; margin: 0.3rem 0 0.45rem; }
        .lp-si-foot { display:flex; gap:0.45rem; flex-wrap:wrap; font-size:0.78rem; }

        .lp-poc-note { font-size:0.78rem; color:#5b6b73; font-style:italic;
            margin: 0.4rem 0 1rem; }
        </style>
        """
    )


# ── Act index — explode parent_legislation, aggregate per Act ─────────────────

@st.cache_data(show_spinner="Building Act index…")
def _exploded_acts() -> pd.DataFrame:
    """One row per (SI, parent_act) pair — splits the pipe-joined
    `si_parent_legislation` column. Cached at module level."""
    si = load_si()
    df = si[si["si_parent_legislation"].notna()].copy()
    if df.empty:
        return df
    df["_acts"] = df["si_parent_legislation"].astype(str).str.split("|")
    df = df.explode("_acts")
    df["act_name"] = df["_acts"].astype(str).str.strip(" .,;").str.replace(r"\s+", " ", regex=True)
    df = df[df["act_name"].str.len() > 3]
    # Year suffix from "<…> Act 2014" — kept for sorting/filtering.
    df["act_year"] = pd.to_numeric(
        df["act_name"].str.extract(r"\b(\d{4})\b", expand=False),
        errors="coerce",
    ).astype("Int64")
    return df.drop(columns=["_acts"]).reset_index(drop=True)


@st.cache_data(show_spinner=False)
def _act_summary() -> pd.DataFrame:
    """One row per parent Act with rolled-up stats."""
    exp = _exploded_acts()
    if exp.empty:
        return exp
    g = exp.groupby("act_name", dropna=False)

    def _mode_or_none(s: pd.Series):
        s = s.dropna()
        return s.mode().iloc[0] if not s.empty else None

    out = pd.DataFrame({
        "act_name": g.size().index,
        "si_count": g.size().values,
        "si_year_min": g["si_year"].min().values,
        "si_year_max": g["si_year"].max().values,
        "domain_top": g["si_policy_domain_primary"].agg(_mode_or_none).values,
        "op_top": g["si_operation_primary"].agg(_mode_or_none).values,
        "act_year": g["act_year"].first().values,
    }).sort_values("si_count", ascending=False).reset_index(drop=True)
    return out


# ── Stage 1 — ranked Acts list ────────────────────────────────────────────────

def _render_kpi_strip(acts: pd.DataFrame, exp: pd.DataFrame) -> None:
    if acts.empty:
        return
    total_acts = len(acts)
    total_sis  = int(acts["si_count"].sum())
    avg = total_sis / total_acts if total_acts else 0
    top_domain = (acts.dropna(subset=["domain_top"])
                      .groupby("domain_top")["si_count"].sum()
                      .sort_values(ascending=False).head(1))
    td = top_domain.index[0] if not top_domain.empty else "—"
    tdc = int(top_domain.iloc[0]) if not top_domain.empty else 0

    most_active = acts.iloc[0]
    most_name = str(most_active["act_name"])
    most_count = int(most_active["si_count"])

    st.html(f"""
    <div class="lp-stat-grid">
      <div class="lp-stat">
        <div class="lp-stat-num">{total_acts:,}</div>
        <div class="lp-stat-label">Acts referenced</div>
        <div class="lp-stat-sub">across {total_sis:,} SIs</div>
      </div>
      <div class="lp-stat">
        <div class="lp-stat-num">{avg:.1f}</div>
        <div class="lp-stat-label">Avg SIs per Act</div>
        <div class="lp-stat-sub">in current filter</div>
      </div>
      <div class="lp-stat">
        <div class="lp-stat-num">{html.escape(_pretty_token(td))}</div>
        <div class="lp-stat-label">Top expanded domain</div>
        <div class="lp-stat-sub">{tdc:,} SIs</div>
      </div>
      <div class="lp-stat">
        <div class="lp-stat-num">{most_count}</div>
        <div class="lp-stat-label">Most active Act</div>
        <div class="lp-stat-sub">{html.escape(most_name[:40])}</div>
      </div>
    </div>
    """)


def _render_top_chart(acts: pd.DataFrame, n: int = 12) -> None:
    """Horizontal bar of the top-N Acts by SI count — gives a corpus-shape
    overview before the user dives into the ranked card list."""
    if acts.empty:
        return
    top = acts.head(n).copy()
    top["act_short"] = top["act_name"].str.replace(r"\s+\d{4}$", "", regex=True).str.slice(0, 50)

    chart = (
        alt.Chart(top)
        .mark_bar(color="#3b6e8f", cornerRadiusTopRight=3, cornerRadiusBottomRight=3)
        .encode(
            y=alt.Y("act_short:N", sort="-x", title=None,
                    axis=alt.Axis(labelFontSize=11, ticks=False, labelLimit=320)),
            x=alt.X("si_count:Q", title=None,
                    axis=alt.Axis(labelFontSize=10, ticks=False)),
            tooltip=[alt.Tooltip("act_name:N", title="Act"),
                     alt.Tooltip("si_count:Q", title="SIs"),
                     alt.Tooltip("act_year:Q", title="Act year"),
                     alt.Tooltip("domain_top:N", title="Top domain")],
        )
        .properties(height=42 * len(top) + 20)
        .configure_view(stroke=None)
        .configure_axis(grid=False, domain=False)
    )
    st.html('<div class="lp-section-h">Top Acts by SI activity</div>')
    st.altair_chart(chart, use_container_width=True)


def _render_act_card(rank: int, row: pd.Series) -> str:
    name   = html.escape(_safe(row["act_name"]) or "—")
    count  = int(row["si_count"])
    yspan  = (
        f'{int(row["si_year_min"])}–{int(row["si_year_max"])}'
        if pd.notna(row["si_year_min"]) and pd.notna(row["si_year_max"]) else "—"
    )
    domain = _pretty_token(_safe(row.get("domain_top")))
    op     = _pretty_token(_safe(row.get("op_top")))

    pills: list[str] = []
    if domain:
        pills.append(f'<span class="lp-pill lp-pill-domain">{html.escape(domain)}</span>')
    if op:
        pills.append(f'<span class="lp-pill lp-pill-op">Top op: {html.escape(op)}</span>')

    return (
        '<div class="lp-act-card">'
        '<div class="lp-act-head">'
        f'<span class="lp-act-rank">#{rank}</span>'
        f'<span class="lp-act-count">{count} SIs</span>'
        f'<span class="lp-act-yspan">SI activity {html.escape(yspan)}</span>'
        '</div>'
        f'<div class="lp-act-title">{name}</div>'
        f'<div class="lp-act-foot">{"".join(pills)}</div>'
        '</div>'
    )


def _render_acts_index(acts: pd.DataFrame, exp: pd.DataFrame) -> None:
    if acts.empty:
        empty_state(
            "No Acts to show",
            "No SIs in the current filter cite a parent Act.",
        )
        return

    _render_kpi_strip(acts, exp)
    _render_top_chart(acts.head(12))

    total = len(acts)
    st.html(
        f'<div class="lp-section-h">{total:,} parent Act'
        f'{"s" if total != 1 else ""} referenced — ranked by SI activity</div>'
    )

    PAGE = 12
    page_idx = paginate(total, key_prefix="lpoc_acts", page_size=PAGE)
    visible = acts.iloc[page_idx * PAGE : (page_idx + 1) * PAGE]

    for rank, (_, row) in enumerate(visible.iterrows(), start=page_idx * PAGE + 1):
        st.html(_render_act_card(rank, row))
        if st.button("View Act detail →", key=f"lpoc_open_{rank}", type="tertiary"):
            st.session_state["lpoc_selected_act"] = row["act_name"]
            st.query_params["act"] = row["act_name"]
            st.rerun()

    pagination_controls(
        total,
        key_prefix="lpoc_acts",
        page_sizes=(PAGE,),
        default_page_size=PAGE,
        label="Acts",
    )

    export_cols = ["act_name", "si_count", "si_year_min", "si_year_max",
                   "domain_top", "op_top", "act_year"]
    export_button(acts[export_cols], "Export Act ranking as CSV",
                  "acts_by_si_activity.csv", "lpoc_acts_csv")


# ── Stage 2 — Act detail ──────────────────────────────────────────────────────

def _render_op_year_chart(rows: pd.DataFrame) -> None:
    if rows.empty:
        return
    df = rows.dropna(subset=["si_operation_primary"]).copy()
    if df.empty:
        return
    df["op_pretty"] = df["si_operation_primary"].map(_pretty_token)
    counts = (df.groupby(["si_year", "op_pretty"]).size().reset_index(name="n"))
    chart = (
        alt.Chart(counts)
        .mark_bar(cornerRadiusTopLeft=2, cornerRadiusTopRight=2)
        .encode(
            x=alt.X("si_year:O", title=None,
                    axis=alt.Axis(labelFontSize=11, ticks=False)),
            y=alt.Y("n:Q", title=None,
                    axis=alt.Axis(labelFontSize=10, ticks=False)),
            color=alt.Color("op_pretty:N",
                            scale=alt.Scale(scheme="tableau10"),
                            legend=alt.Legend(title="Operation", orient="bottom")),
            tooltip=[alt.Tooltip("si_year:O", title="Year"),
                     alt.Tooltip("op_pretty:N", title="Operation"),
                     alt.Tooltip("n:Q", title="SIs")],
        )
        .properties(height=240)
        .configure_view(stroke=None)
        .configure_axis(grid=False, domain=False)
    )
    st.altair_chart(chart, use_container_width=True)


def _render_si_under_act(row: pd.Series) -> str:
    si_id    = html.escape(_safe(row.get("si_id")) or "—")
    title    = html.escape(_safe(row.get("title")) or "—")
    issue_dt = html.escape(_fmt_date(row.get("issue_date")))
    domain   = _pretty_token(_safe(row.get("si_policy_domain_primary")))
    op       = _pretty_token(_safe(row.get("si_operation_primary")))
    eu       = _safe(row.get("si_eu_relationship"))

    pills: list[str] = []
    if domain:
        pills.append(f'<span class="lp-pill lp-pill-domain">{html.escape(domain)}</span>')
    if op:
        pills.append(f'<span class="lp-pill lp-pill-op">{html.escape(op)}</span>')
    if eu.startswith("eu_"):
        pills.append(f'<span class="lp-pill lp-pill-eu">{html.escape(_pretty_token(eu))}</span>')

    eisb_link = source_link_html(
        _eisb_url(row),
        "View on irishstatutebook.ie",
        aria_label=f"Open SI {si_id} on the Electronic Irish Statute Book",
    ) if _eisb_url(row) else ""

    si_poc_link = (
        f'<a class="dt-source-link" href="/si-poc?si={html.escape(_safe(row.get("si_id")))}" '
        f'target="_self">Open SI detail</a>'
    )

    return (
        '<div class="lp-si-row">'
        '<div class="lp-si-head">'
        f'<span class="lp-si-ref">SI No. {si_id}</span>'
        f'<span class="lp-si-date">{issue_dt}</span>'
        '</div>'
        f'<div class="lp-si-title">{title}</div>'
        f'<div class="lp-si-foot">{"".join(pills)} '
        f'&nbsp; {si_poc_link} &nbsp; · &nbsp; {eisb_link}</div>'
        '</div>'
    )


def _render_act_detail(act_name: str) -> None:
    if back_button("← Back to Acts ranking", key="lpoc_act"):
        st.session_state.pop("lpoc_selected_act", None)
        st.query_params.clear()
        st.rerun()

    exp = _exploded_acts()
    rows = exp[exp["act_name"] == act_name].copy()

    if rows.empty:
        st.warning(f"Act '{act_name}' not found in the current SI extract.")
        return

    n_total = len(rows)
    yspan = (
        f"{int(rows['si_year'].min())}–{int(rows['si_year'].max())}"
        if not rows.empty else "—"
    )
    n_amend     = int((rows["si_operation_primary"] == "amendment").sum())
    n_commence  = int((rows["si_operation_primary"] == "commencement").sum())
    n_revoke    = int((rows["si_operation_primary"] == "revocation").sum())
    n_other     = n_total - (n_amend + n_commence + n_revoke)
    domains_top = (rows.dropna(subset=["si_policy_domain_primary"])
                       .groupby("si_policy_domain_primary").size()
                       .sort_values(ascending=False).head(3))

    st.html(f"""
    <div class="lp-detail-h">
      <div class="lp-detail-kicker">↪ Parent Act of the Oireachtas</div>
      <div class="lp-detail-title">{html.escape(act_name)}</div>
      <div class="lp-detail-meta">
        {n_total} SIs made under this Act &nbsp;·&nbsp; SI activity {yspan}
      </div>
    </div>
    """)

    render_stat_strip(
        stat_item(str(n_amend),    "Amendments"),
        stat_item(str(n_commence), "Commencements"),
        stat_item(str(n_revoke),   "Revocations"),
        stat_item(str(n_other),    "Other"),
    )

    if not domains_top.empty:
        chips = " ".join(
            f'<span class="lp-pill lp-pill-domain">{html.escape(_pretty_token(d))} · {n_d}</span>'
            for d, n_d in domains_top.items()
        )
        st.html(
            '<div class="lp-section-h">Top policy domains touched</div>'
            f'<div style="margin-bottom:0.5rem;">{chips}</div>'
        )

    st.html('<div class="lp-section-h">Operation by year</div>')
    _render_op_year_chart(rows)

    st.html(f'<div class="lp-section-h">SIs made under this Act ({n_total})</div>')
    rows = rows.sort_values(["si_year", "si_number"], ascending=[False, False])

    PAGE = 10
    page_idx = paginate(n_total, key_prefix="lpoc_actsi", page_size=PAGE)
    visible = rows.iloc[page_idx * PAGE : (page_idx + 1) * PAGE]
    cards = [_render_si_under_act(r) for _, r in visible.iterrows()]
    st.html("\n".join(cards))

    pagination_controls(
        n_total,
        key_prefix="lpoc_actsi",
        page_sizes=(PAGE,),
        default_page_size=PAGE,
        label="SIs",
    )

    provenance_expander(
        sections=[
            f"**Act name (verbatim):** {act_name}",
            "**Source:** Iris Oifigiúil ETL — `si_parent_legislation` extracted from the body of each SI.",
            "**POC integration:** in-page Streamlit; not yet pipeline-blessed.",
        ]
    )


# ── Entry point ────────────────────────────────────────────────────────────────

def legislation_poc_page() -> None:
    inject_css()
    _inject_poc_css()

    # URL-driven entry: ?act=<name> jumps straight to the Act detail.
    url_act = st.query_params.get("act")
    if url_act:
        st.session_state["lpoc_selected_act"] = unquote(url_act) if "%" in url_act else url_act

    selected_act: str | None = st.session_state.get("lpoc_selected_act")

    with st.sidebar:
        sidebar_page_header("Legislation (POC)")
        if selected_act:
            st.html('<div class="page-subtitle">Act detail</div>')
        else:
            st.html('<div class="page-subtitle">POC · Acts of the Oireachtas → SIs</div>')
            st.divider()

            acts_full = _act_summary()
            if not acts_full.empty:
                # Domain filter
                domains = ["All"] + sorted(
                    [d for d in acts_full["domain_top"].dropna().unique()]
                )
                domain_sel = st.selectbox(
                    "Top domain",
                    domains,
                    format_func=_pretty_token,
                    key="lpoc_domain_filter",
                )

                # Min SI count slider
                max_count = int(acts_full["si_count"].max()) if not acts_full.empty else 1
                min_sis = st.slider(
                    "Minimum SIs per Act",
                    1, max(2, max_count), 1,
                    key="lpoc_min_si_count",
                )

                # Act-year window
                act_years = pd.to_numeric(acts_full["act_year"], errors="coerce").dropna()
                if not act_years.empty:
                    yr_lo = int(act_years.min())
                    yr_hi = int(act_years.max())
                    yr_sel = st.slider(
                        "Act year",
                        yr_lo, yr_hi, (yr_lo, yr_hi),
                        key="lpoc_act_year_filter",
                    )
                else:
                    yr_sel = None

                search = st.text_input(
                    "Search Act name",
                    placeholder="e.g. Companies, Planning, Health…",
                    key="lpoc_search",
                ).strip()

    if selected_act:
        _render_act_detail(selected_act)
        return

    # ── Stage 1 entry — apply filters then render the ranked list ─────────────
    hero_banner(
        kicker="Iris Oifigiúil · POC integration",
        title="Acts of the Oireachtas, expanded by SIs",
        dek=(
            "The canonical /legislation page tracks bills under consideration. "
            "This POC takes the inverse view — once a bill becomes an Act, how "
            "is it being amended, commenced, revoked, or otherwise expanded "
            "through Statutory Instruments? Acts here are ranked by the number "
            "of SIs made under them in the Iris Oifigiúil corpus."
        ),
    )
    st.html(
        '<div class="lp-poc-note">POC notice — Acts are extracted from the '
        '<code>si_parent_legislation</code> field of each SI; counts and rankings '
        'are derived in-page and not yet pipeline-blessed.</div>'
    )

    acts = _act_summary()
    exp  = _exploded_acts()

    # Filters
    domain_sel = st.session_state.get("lpoc_domain_filter")
    if domain_sel and domain_sel != "All":
        acts = acts[acts["domain_top"] == domain_sel]

    min_sis = st.session_state.get("lpoc_min_si_count", 1)
    if min_sis and min_sis > 1:
        acts = acts[acts["si_count"] >= min_sis]

    yr_sel = st.session_state.get("lpoc_act_year_filter")
    if yr_sel and isinstance(yr_sel, tuple):
        lo, hi = yr_sel
        acts = acts[
            acts["act_year"].fillna(-1).between(lo, hi)
            | acts["act_year"].isna()  # keep Acts where year couldn't be parsed
        ]

    search = (st.session_state.get("lpoc_search") or "").strip()
    if search:
        acts = acts[acts["act_name"].str.contains(search, case=False, na=False)]

    _render_acts_index(acts.reset_index(drop=True), exp)
