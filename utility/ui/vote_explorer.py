"""Vote evidence panel rendering for Dáil Tracker."""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from ui.components import stat_strip, outcome_badge, evidence_heading, todo_callout, empty_state
from ui.table_config import member_detail_column_config, td_history_column_config
from ui.export_controls import export_button
from ui.source_links import render_source_links

_VOTE_COLOURS: dict[str, str] = {
    "Voted Yes": "#2d7a52",
    "Voted No":  "#bf4a1e",
    "Abstained": "#8c8c80",
}


def _vote_icon(vote_type) -> str:
    vt = str(vote_type or "")
    if vt == "Voted Yes":
        return '<span class="dt-vt-yes">✓ Yes</span>'
    if vt == "Voted No":
        return '<span class="dt-vt-no">✗ No</span>'
    if vt == "Abstained":
        return '<span class="dt-vt-abs">— Abstained</span>'
    return '<span class="dt-vt-abs">—</span>'


def _outcome_chip(outcome) -> str:
    o = str(outcome or "").strip()
    lo = o.lower()
    if "carried" in lo:
        return f'<span class="dt-vt-outcome-carried">{o}</span>'
    if "lost" in lo:
        return f'<span class="dt-vt-outcome-lost">{o}</span>'
    if o:
        return f'<span class="dt-vt-outcome-other">{o}</span>'
    return ""


def _render_td_history_html(df: pd.DataFrame) -> str:
    has_url = "oireachtas_url" in df.columns
    rows_html = ""
    for _, row in df.iterrows():
        date_str = _fmt_date(row.get("vote_date"))
        title = str(row.get("debate_title") or "—")
        vt_html = _vote_icon(row.get("vote_type"))
        outcome_html = _outcome_chip(row.get("vote_outcome"))
        url = str(row.get("oireachtas_url") or "") if has_url else ""
        link_cell = (
            f'<a href="{url}" target="_blank" rel="noopener" class="dt-vt-link">↗ source</a>'
            if url.startswith("http") else ""
        )
        rows_html += (
            f"<tr>"
            f'<td class="dt-vt-date">{date_str}</td>'
            f"<td>{title}</td>"
            f"<td>{vt_html}</td>"
            f"<td>{outcome_html}</td>"
            f"<td>{link_cell}</td>"
            f"</tr>"
        )
    header = (
        "<tr>"
        "<th>Date</th><th>Division</th><th>Vote</th><th>Outcome</th><th></th>"
        "</tr>"
    )
    return f'<table class="dt-vt-table">{header}{rows_html}</table>'


def _render_member_list_html(df: pd.DataFrame) -> str:
    rows_html = ""
    for _, row in df.iterrows():
        name = str(row.get("member_name") or "—")
        party = str(row.get("party_name") or "")
        const = str(row.get("constituency") or "")
        vt_html = _vote_icon(row.get("vote_type"))
        rows_html += (
            f"<tr>"
            f"<td>{name}</td>"
            f'<td class="dt-vt-meta">{party}</td>'
            f'<td class="dt-vt-meta">{const}</td>'
            f"<td>{vt_html}</td>"
            f"</tr>"
        )
    header = "<tr><th>Member</th><th>Party</th><th>Constituency</th><th>Vote</th></tr>"
    return f'<table class="dt-vt-table">{header}{rows_html}</table>'


def _fmt_date(val) -> str:
    if val is None:
        return "—"
    if hasattr(val, "strftime"):
        return val.strftime("%d %b %Y")
    s = str(val)[:10]
    return s if s and s != "None" else "—"


def _party_chart(df: pd.DataFrame) -> go.Figure | None:
    """Build horizontal stacked bar from already-aggregated party_vote_breakdown rows."""
    if df.empty or "party_name" not in df.columns or "vote_type" not in df.columns:
        return None

    totals: dict[str, int] = {}
    for _, row in df.iterrows():
        pname = str(row.get("party_name") or "")
        if pname:
            totals[pname] = totals.get(pname, 0) + int(row.get("member_count") or 0)

    if not totals:
        return None

    parties = sorted(totals.keys(), key=lambda p: totals[p])
    order = ["Voted Yes", "Voted No", "Abstained"]
    fig = go.Figure()

    for vt in order:
        vt_rows = df[df["vote_type"] == vt]
        counts: dict[str, int] = {}
        for _, row in vt_rows.iterrows():
            pname = str(row.get("party_name") or "")
            if pname:
                counts[pname] = int(row.get("member_count") or 0)
        x_vals = [counts.get(p, 0) for p in parties]
        fig.add_trace(go.Bar(
            name=vt,
            y=parties,
            x=x_vals,
            orientation="h",
            marker_color=_VOTE_COLOURS.get(vt, "#adb5bd"),
            hovertemplate=f"<b>%{{y}}</b> · {vt}: %{{x}}<extra></extra>",
        ))

    fig.update_layout(
        barmode="stack",
        height=max(160, len(parties) * 30),
        margin=dict(l=0, r=20, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Epilogue, sans-serif", size=12),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False),
    )
    return fig


def _year_chart(df: pd.DataFrame) -> None:
    """Stacked bar of votes by year from td_vote_year_summary rows."""
    order = ["yes_count", "no_count", "abstained_count"]
    labels = {"yes_count": "Yes ✓", "no_count": "No ✗", "abstained_count": "Abstained"}

    years = sorted(df["year"].dropna().unique())
    fig = go.Figure()

    for col in order:
        if col not in df.columns:
            continue
        y_vals = []
        for yr in years:
            yr_rows = df[df["year"] == yr]
            y_vals.append(int(yr_rows[col].iloc[0]) if not yr_rows.empty else 0)
        fig.add_trace(go.Bar(
            name=labels[col],
            x=[int(y) for y in years],
            y=y_vals,
            marker_color=_VOTE_COLOURS.get(labels[col], "#adb5bd"),
        ))

    fig.update_layout(
        barmode="stack",
        height=200,
        margin=dict(l=0, r=0, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Epilogue, sans-serif", size=12),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        xaxis=dict(showgrid=False, tickmode="linear", dtick=1),
        yaxis=dict(showgrid=True, gridcolor="rgba(0,0,0,0.08)"),
    )
    st.plotly_chart(fig, use_container_width=True)


def render_division_panel(
    vote_row: pd.Series,
    members_df: pd.DataFrame,
    sources_df: pd.DataFrame,
    breakdown_df: pd.DataFrame,
) -> None:
    vote_id  = str(vote_row.get("vote_id") or "")
    outcome  = str(vote_row.get("vote_outcome") or "—")
    date_str = _fmt_date(vote_row.get("vote_date"))
    title    = str(vote_row.get("debate_title") or "")
    yes_n    = int(vote_row.get("yes_count") or 0)
    no_n     = int(vote_row.get("no_count") or 0)
    abs_n    = int(vote_row.get("abstained_count") or 0)
    margin   = int(vote_row.get("margin") or abs(yes_n - no_n))

    safe_key = ""
    for ch in vote_id:
        if ch.isalnum():
            safe_key += ch
    if not safe_key:
        safe_key = "div"

    with st.container(border=True):
        st.markdown(
            f'<div style="display:flex;align-items:center;gap:0.75rem;margin-bottom:0.3rem">'
            f'{outcome_badge(outcome)}'
            f'<span style="color:var(--text-meta);font-size:0.82rem">{date_str}</span>'
            f'</div>'
            f'<p style="font-size:0.95rem;font-weight:600;line-height:1.45;margin:0 0 0.5rem">{title}</p>',
            unsafe_allow_html=True,
        )

        stat_strip([
            (str(yes_n),  "Yes ✓",     "oklch(38% 0.130 145)"),
            (str(no_n),   "No ✗",      "oklch(45% 0.180 30)"),
            (str(abs_n),  "Abstained", "var(--text-meta)"),
            (str(margin), "Margin",    "var(--text-primary)"),
        ])

        evidence_heading("Party breakdown")
        if breakdown_df.empty:
            todo_callout("party_vote_breakdown — per-party vote counts per division")
        else:
            fig = _party_chart(breakdown_df)
            if fig is not None:
                st.plotly_chart(fig, use_container_width=True)
            else:
                todo_callout("party_vote_breakdown — party_name or vote_type column missing")

        evidence_heading("Member votes")
        if members_df.empty:
            empty_state(
                "No member detail available",
                "v_vote_member_detail did not return rows for this division.",
            )
        else:
            _req = frozenset({"member_name", "party_name", "vote_type"})
            missing = sorted(c for c in _req if c not in members_df.columns)
            if missing:
                mc_str = missing[0]
                for c in missing[1:]:
                    mc_str += ", " + c
                todo_callout(f"v_vote_member_detail missing columns: {mc_str}")
            else:
                clean_df = members_df.dropna(subset=["member_name"])
                pos = st.radio(
                    "Position",
                    ["All", "Voted Yes", "Voted No", "Abstained"],
                    horizontal=True,
                    key=f"pos_{safe_key}",
                    label_visibility="collapsed",
                )
                display = clean_df if pos == "All" else clean_df[clean_df["vote_type"] == pos]
                st.markdown(_render_member_list_html(display), unsafe_allow_html=True)
                show_cols = [c for c in ["member_name", "party_name", "constituency", "vote_type"] if c in display.columns]
                export_button(
                    display[show_cols],
                    label="Export member votes CSV",
                    filename=f"division_{safe_key}_votes.csv",
                    key=f"exp_mem_{safe_key}",
                )

        evidence_heading("Official sources")
        render_source_links(sources_df)


def vt_division_card_html(row) -> str:
    """HTML for one division card in the Mode A index.

    Uses existing _fmt_date and _outcome_chip helpers defined in this module.
    CSS classes: .vt-card family in shared_css.py.
    """
    date_str = _fmt_date(row.get("vote_date"))
    title    = str(row.get("debate_title") or "—")
    outcome  = str(row.get("vote_outcome") or "").strip()
    yes_n    = int(row.get("yes_count") or 0)
    no_n     = int(row.get("no_count") or 0)
    abs_n    = int(row.get("abstained_count") or 0)
    margin   = row.get("margin")

    margin_str = ""
    if margin is not None:
        try:
            m = int(margin)
            margin_str = f"+{m}" if m >= 0 else str(m)
        except (TypeError, ValueError):
            pass

    lo = outcome.lower()
    if "carried" in lo:
        outcome_html = f'<span class="vt-outcome-carried">Carried ✓</span>'
    elif "lost" in lo:
        outcome_html = f'<span class="vt-outcome-lost">Lost ✗</span>'
    elif outcome:
        outcome_html = f'<span class="vt-count-abs">{outcome}</span>'
    else:
        outcome_html = ""

    margin_html = f'<span class="vt-margin-pill">{margin_str}</span>' if margin_str else ""

    return (
        f'<div class="vt-card">'
        f'<div class="vt-card-header">'
        f'<span class="vt-card-date">{date_str}</span>'
        f'{outcome_html}'
        f'</div>'
        f'<div class="vt-card-title">{title}</div>'
        f'<div class="vt-card-footer">'
        f'<span class="vt-count-yes">✓ {yes_n}</span>'
        f'<span class="vt-count-no">✗ {no_n}</span>'
        f'<span class="vt-count-abs">— {abs_n}</span>'
        f'{margin_html}'
        f'</div>'
        f'</div>'
    )


def render_td_panel(
    td_row: pd.Series,
    history_df: pd.DataFrame,
    year_df: pd.DataFrame,
) -> None:
    member_id = str(td_row.get("member_id") or "")
    name      = str(td_row.get("member_name") or "—")
    party     = str(td_row.get("party_name") or "—")
    const     = str(td_row.get("constituency") or "—")
    yes_n      = int(td_row.get("yes_count") or 0)
    no_n       = int(td_row.get("no_count") or 0)
    abs_n      = int(td_row.get("abstained_count") or 0)
    div_count  = int(td_row.get("division_count") or 0)
    yes_rate   = td_row.get("yes_rate_pct")
    rate_str   = f"{float(yes_rate):.1f}%" if yes_rate is not None else "—"

    safe_mid = ""
    for ch in member_id:
        if ch.isalnum():
            safe_mid += ch
    if not safe_mid:
        safe_mid = "td"

    with st.container(border=True):
        st.markdown(
            f'<p class="td-name">{name}</p>'
            f'<p class="td-meta">{party} · {const}</p>',
            unsafe_allow_html=True,
        )

        stat_strip([
            (str(yes_n),    "Yes ✓",      "oklch(38% 0.130 145)"),
            (str(no_n),     "No ✗",       "oklch(45% 0.180 30)"),
            (str(abs_n),    "Abstained",  "var(--text-meta)"),
            (rate_str,      "Yes rate",   "oklch(38% 0.130 145)"),
            (str(div_count), "Divisions", "var(--text-primary)"),
        ])

        evidence_heading("Votes by year")
        if year_df.empty:
            todo_callout(
                "td_vote_year_summary — per-TD per-year yes/no/abstained counts (view not yet built)"
            )
        else:
            _yr_req = frozenset({"year", "yes_count", "no_count"})
            missing_y = sorted(c for c in _yr_req if c not in year_df.columns)
            if missing_y:
                mc_str = missing_y[0]
                for c in missing_y[1:]:
                    mc_str += ", " + c
                todo_callout(f"td_vote_year_summary missing columns: {mc_str}")
            else:
                _year_chart(year_df)

        evidence_heading("Vote history")
        if history_df.empty:
            empty_state(
                "No vote history found",
                "No records in v_vote_member_detail for this member and date range.",
            )
        else:
            st.markdown(_render_td_history_html(history_df), unsafe_allow_html=True)
            hist_cols = [c for c in ["vote_date", "debate_title", "vote_type", "vote_outcome"] if c in history_df.columns]
            export_button(
                history_df[hist_cols],
                label="Export vote history CSV",
                filename=f"td_{safe_mid}_votes.csv",
                key=f"exp_td_{safe_mid}",
            )
