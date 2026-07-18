"""Vote evidence panel rendering for Dáil Tracker."""

from __future__ import annotations

import sys
from html import escape as _h
from pathlib import Path
from typing import TYPE_CHECKING

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import plotly.graph_objects as go
import streamlit as st
from ui.components import empty_state, evidence_heading, outcome_badge, pagination_controls, stat_strip, todo_callout
from ui.entity_links import entity_cta_html, member_link_html, member_profile_url, source_link_html
from ui.export_controls import export_button
from ui.source_links import render_source_links

from dail_tracker_core.queries import votes as _vq

if TYPE_CHECKING:  # pandas is referenced only in type annotations (PEP 563 stringised)
    import pandas as pd

_VOTE_COLOURS: dict[str, str] = {
    "Voted Yes": "#2d7a52",
    "Voted No": "#bf4a1e",
    "Abstained": "#8c8c80",
}

_TD_HISTORY_LIMIT = 5000


class _NullCtx:
    """No-op context manager — lets render_td_panel skip the bordered
    container wrapper when embedded inside another container."""

    def __enter__(self):
        return None

    def __exit__(self, exc_type, exc, tb):
        return False


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
        return f'<span class="dt-vt-outcome-carried">{_h(o)}</span>'
    if "lost" in lo:
        return f'<span class="dt-vt-outcome-lost">{_h(o)}</span>'
    if o:
        return f'<span class="dt-vt-outcome-other">{_h(o)}</span>'
    return ""


def _render_td_history_html(df: pd.DataFrame) -> str:
    has_url = "oireachtas_url" in df.columns
    rows_html = ""
    for _, row in df.iterrows():
        date_str = _fmt_date(row.get("vote_date"))
        title = _h(str(row.get("debate_title") or "—"))
        vt_html = _vote_icon(row.get("vote_type"))
        outcome_html = _outcome_chip(row.get("vote_outcome"))
        url = str(row.get("oireachtas_url") or "") if has_url else ""
        link_cell = source_link_html(url, "Oireachtas", aria_label="Open this division on oireachtas.ie")
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
        '<th scope="col">Date</th><th scope="col">Division</th>'
        '<th scope="col">Vote</th><th scope="col">Outcome</th>'
        '<th scope="col"><span class="sr-only">Source link</span></th>'
        "</tr>"
    )
    return (
        '<table class="dt-vt-table" role="table" '
        'aria-label="Voting history with date, division, vote cast, and outcome">'
        '<caption class="sr-only">Voting history table</caption>'
        f"<thead>{header}</thead><tbody>{rows_html}</tbody></table>"
    )


def _render_member_list_html(df: pd.DataFrame) -> str:
    has_id = "member_id" in df.columns
    rows_html = ""
    for _, row in df.iterrows():
        raw_name = str(row.get("member_name") or "—")
        mid = str(row.get("member_id") or "") if has_id else ""
        # member_link_html escapes both name and href; falls back to plain text when mid is empty.
        name_cell = member_link_html(mid, raw_name) if mid else _h(raw_name)
        party = _h(str(row.get("party_name") or ""))
        const = _h(str(row.get("constituency") or ""))
        vt_html = _vote_icon(row.get("vote_type"))
        rows_html += (
            f"<tr>"
            f"<td>{name_cell}</td>"
            f'<td class="dt-vt-meta">{party}</td>'
            f'<td class="dt-vt-meta">{const}</td>'
            f"<td>{vt_html}</td>"
            f"</tr>"
        )
    header = (
        '<tr><th scope="col">Member</th><th scope="col">Party</th>'
        '<th scope="col">Constituency</th><th scope="col">Vote</th></tr>'
    )
    return (
        '<table class="dt-vt-table" role="table" '
        'aria-label="Members and how each voted in this division">'
        '<caption class="sr-only">Division member list</caption>'
        f"<thead>{header}</thead><tbody>{rows_html}</tbody></table>"
    )


def _fmt_date(val) -> str:
    if val is None:
        return "—"
    if hasattr(val, "strftime"):
        return val.strftime("%d %b %Y")
    s = str(val)[:10]
    return s if s and s != "None" else "—"


def _party_chart(df: pd.DataFrame) -> go.Figure | None:
    """Build horizontal stacked bar from already-aggregated v_party_vote_breakdown rows."""
    if df.empty or "party_name" not in df.columns or "vote_type" not in df.columns:
        return None

    parties = sorted(df.loc[df["party_name"].notna(), "party_name"].unique().tolist())
    if not parties:
        return None
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
        fig.add_trace(
            go.Bar(
                name=vt,
                y=parties,
                x=x_vals,
                orientation="h",
                marker_color=_VOTE_COLOURS.get(vt, "#adb5bd"),
                hovertemplate=f"<b>%{{y}}</b> · {vt}: %{{x}}<extra></extra>",
            )
        )

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
    """Votes by year as house-style rows: a CSS yes/no/abstained split bar per
    year plus the counts. Replaced the embedded Plotly stacked chart 2026-06-11
    (chart iframes clashed with the page's ink-on-paper style). The split bar
    is a per-row presentation ratio, same as the attendance year bars."""
    rows_html: list[str] = []
    for _, r in df.sort_values("year", ascending=False).iterrows():
        year = int(r["year"])
        yes_n = int(r.get("yes_count", 0) or 0)
        no_n = int(r.get("no_count", 0) or 0)
        abst_n = int(r.get("abstained_count", 0) or 0)
        total = yes_n + no_n + abst_n
        if total == 0:
            continue
        yes_pct = yes_n / total * 100
        no_pct = no_n / total * 100
        abst_pct = max(0.0, 100.0 - yes_pct - no_pct)
        counts = f"<strong>{yes_n}</strong> Yes · <strong>{no_n}</strong> No"
        if abst_n:
            counts += f" · <strong>{abst_n}</strong> Abstained"
        rows_html.append(
            f'<div class="att-year-row vote-year-row">'
            f'<span class="att-year-yr">{year}</span>'
            f'<div class="att-year-bar-track vote-year-track" role="img" '
            f'aria-label="{year}: {yes_n} yes, {no_n} no, {abst_n} abstained">'
            f'<div class="vote-year-seg vote-year-seg-yes" style="width:{yes_pct:.1f}%"></div>'
            f'<div class="vote-year-seg vote-year-seg-no" style="width:{no_pct:.1f}%"></div>'
            f'<div class="vote-year-seg vote-year-seg-abst" style="width:{abst_pct:.1f}%"></div>'
            f"</div>"
            f'<span class="att-year-days vote-year-counts">{counts}</span>'
            f'<span class="att-year-pct">{total}</span>'
            f"</div>"
        )
    st.html(
        '<div class="att-year-list">'
        '<div class="vote-year-legend">'
        '<span class="vote-year-key vote-year-key-yes"></span>Yes&nbsp;&nbsp;'
        '<span class="vote-year-key vote-year-key-no"></span>No&nbsp;&nbsp;'
        '<span class="vote-year-key vote-year-key-abst"></span>Abstained'
        "</div>" + "".join(rows_html) + "</div>"
    )


def render_division_panel(
    vote_row: pd.Series,
    members_df: pd.DataFrame,
    sources_df: pd.DataFrame,
    breakdown_df: pd.DataFrame,
) -> None:
    vote_id = str(vote_row.get("vote_id") or "")
    outcome = str(vote_row.get("vote_outcome") or "—")
    date_str = _fmt_date(vote_row.get("vote_date"))
    title = str(vote_row.get("debate_title") or "")
    yes_n = int(vote_row.get("yes_count") or 0)
    no_n = int(vote_row.get("no_count") or 0)
    abs_n = int(vote_row.get("abstained_count") or 0)
    _margin = vote_row.get("margin")
    margin_str = str(int(_margin)) if _margin is not None else "—"

    safe_key = ""
    for ch in vote_id:
        if ch.isalnum():
            safe_key += ch
    if not safe_key:
        safe_key = "div"

    oireachtas_url = str(vote_row.get("oireachtas_url") or "")
    link_html = source_link_html(
        oireachtas_url, "View on oireachtas.ie", aria_label="Open this division on oireachtas.ie"
    )

    with st.container(border=True):
        st.html(
            f'<div class="vt-division-header">'
            f"{outcome_badge(outcome)}"
            f'<span class="dt-vt-date">{_h(date_str)}</span>'
            f"{link_html}"
            f"</div>"
            f'<p class="vt-division-title">{_h(title)}</p>'
        )

        stat_strip(
            [
                (str(yes_n), "Yes", "oklch(38% 0.130 145)"),
                (str(no_n), "No", "oklch(45% 0.180 30)"),
                (str(abs_n), "Abstained", "var(--text-meta)"),
                (margin_str, "Margin", "var(--text-primary)"),
            ]
        )

        evidence_heading("Party breakdown")
        if breakdown_df.empty:
            todo_callout("v_party_vote_breakdown — per-party vote counts per division")
        else:
            fig = _party_chart(breakdown_df)
            if fig is not None:
                st.plotly_chart(fig, width="stretch")
            else:
                todo_callout("v_party_vote_breakdown — party_name or vote_type column missing")

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
                pos = st.segmented_control(
                    "Position",
                    ["All", "Voted Yes", "Voted No", "Abstained"],
                    key=f"pos_{safe_key}",
                    label_visibility="collapsed",
                )
                pos = pos or "All"
                display = clean_df if pos == "All" else clean_df[clean_df["vote_type"] == pos]
                st.html(_render_member_list_html(display))
                show_cols = [
                    c for c in ["member_name", "party_name", "constituency", "vote_type"] if c in display.columns
                ]
                export_button(
                    display[show_cols],
                    label="Export member votes CSV",
                    filename=f"division_{safe_key}_votes.csv",
                    key=f"exp_mem_{safe_key}",
                )

        evidence_heading("Official sources")
        render_source_links(sources_df)


def _split_title_and_stage(raw_title: str) -> tuple[str, str]:
    """Split a debate title into ``(bill_title, stage_label)``.

    Audit fix (2026-05-26, P1-1): cards for the same bill at different
    legislative stages (Second Stage, Committee, Report, Final) currently
    look like duplicates with the same title text and slightly different
    vote counts. The stage is embedded in the title after the first colon
    — e.g. ``Arbitration (Amendment) Bill 2025: Report and Final Stages``.
    Pulling it out lets the card title stay clean and the stage become a
    visible pill.

    Also strips the trailing ``[Private Members]`` jargon suffix (P2-8) —
    callers can render it as a separate small chip if desired (not done
    here yet).

    Returns ``(title, stage)``. ``stage`` is "" when no colon is found.
    """
    if ":" not in raw_title:
        return raw_title.strip(), ""
    head, _, tail = raw_title.partition(":")
    stage = tail.strip()
    # Drop the orphan "[Private Members]" suffix from the stage label —
    # it's classifier metadata, not a stage name.
    if stage.endswith("[Private Members]"):
        stage = stage[: -len("[Private Members]")].strip()
    # "Motion (Resumed)" → "Motion (Resumed)" is fine. "Second Stage (Resumed)"
    # is fine. Just trim trailing punctuation.
    stage = stage.rstrip(":.;,")
    return head.strip(), stage


def vt_division_card_html(row) -> str:
    """HTML for one division card in the Mode A index.

    Uses existing _fmt_date and _outcome_chip helpers defined in this module.
    CSS classes: .vt-card family in shared_css.py.
    """
    date_str = _fmt_date(row.get("vote_date"))
    raw_title = str(row.get("debate_title") or "—")
    bill_title, stage_label = _split_title_and_stage(raw_title)
    # P2-8: surface "[Private Members]" upstream tag as a small pill rather
    # than leaving it inline as jargon in the title. _split_title_and_stage
    # already strips it from the stage; here we also strip it from
    # bill_title (titles without a colon kept the tag in the head).
    is_private = False
    _pm_tag = "[Private Members]"
    if bill_title.rstrip().endswith(_pm_tag):
        bill_title = bill_title[: bill_title.rfind(_pm_tag)].rstrip()
        is_private = True
    elif raw_title.rstrip().endswith(_pm_tag):
        # Tag was in the stage half; helper already removed it from stage.
        is_private = True
    title = _h(bill_title)
    # The chip truncates long stages at 18ch (CSS ellipsis); carry the full label in the
    # tooltip so "Committee an…" / "Second Stage (Re…" are recoverable on hover.
    stage_html = (
        f'<span class="vt-card-stage" title="Legislative stage: {_h(stage_label)}">{_h(stage_label)}</span>'
        if stage_label
        else ""
    )
    private_html = (
        '<span class="vt-card-private" title="Private Members’ motion or bill '
        '— tabled by a TD/Senator who is not a government minister">Private Members</span>'
        if is_private
        else ""
    )
    outcome = str(row.get("vote_outcome") or "").strip()
    yes_n = int(row.get("yes_count") or 0)
    no_n = int(row.get("no_count") or 0)
    abs_n = int(row.get("abstained_count") or 0)
    margin = row.get("margin")
    url = str(row.get("oireachtas_url") or "")

    # P2-1: replace the opaque "Δ +N" pill with plain English ("won by N",
    # "lost by N"). Falls back to "margin N" when the outcome is neither
    # carried nor lost (rare edge case).
    margin_int: int | None
    try:
        margin_int = int(margin) if margin is not None else None
    except (TypeError, ValueError):
        margin_int = None

    lo = outcome.lower()
    if "carried" in lo:
        outcome_html = '<span class="vt-outcome-carried">Carried ✓</span>'
    elif "lost" in lo:
        outcome_html = '<span class="vt-outcome-lost">Lost ✗</span>'
    elif outcome:
        outcome_html = f'<span class="vt-count-abs">{_h(outcome)}</span>'
    else:
        outcome_html = ""

    if margin_int is not None:
        if "carried" in lo:
            margin_label = f"won by {abs(margin_int)}"
        elif "lost" in lo:
            margin_label = f"lost by {abs(margin_int)}"
        else:
            margin_label = f"margin {margin_int:+d}"
        margin_html = f'<span class="vt-margin-pill" title="Yes votes minus No votes">{margin_label}</span>'
    else:
        margin_html = ""

    # P1-4 + P2-2: Oireachtas link demoted from header (accent-coloured,
    # one per card) to footer (quiet grey, right-aligned). The internal
    # `→` navigation (added by clickable_card_link wrapping the card) is
    # now the visually-primary affordance; the external link is still
    # available but doesn't compete on mobile.
    source_html = source_link_html(url, "Oireachtas", aria_label="Open this division on oireachtas.ie")

    return (
        f'<div class="vt-card">'
        f'<div class="vt-card-header">'
        f'<span class="vt-card-date">{date_str}</span>'
        f"{outcome_html}"
        f"{stage_html}"
        f"{private_html}"
        f"</div>"
        f'<div class="vt-card-title">{title}</div>'
        f'<div class="vt-card-footer">'
        f'<span class="vt-count-yes">✓ {yes_n}</span>'
        f'<span class="vt-count-no">✗ {no_n}</span>'
        f'<span class="vt-count-abs">— {abs_n}</span>'
        f"{margin_html}"
        f"{source_html}"
        f"</div>"
        f"</div>"
    )


def member_vote_card_html(
    *,
    vote_date,
    debate_title: str,
    vote_type: str,
    vote_outcome: str,
    oireachtas_url: str = "",
) -> str:
    """Reusable card for **a single member's vote on one division**.

    Used by Member Overview's "Voting record by issue" and any other place we
    show how a TD voted on a single division. Uses green ✓ / red ✗ explicitly
    (matches the styling on the Votes page).

    All inputs are escaped — pass plain values, not HTML.
    """
    date_str = _fmt_date(vote_date)
    title = _h(str(debate_title or "—"))

    vt = str(vote_type or "")
    if vt == "Voted Yes":
        vote_chip_html = '<span class="vt-rec-vote vt-rec-vote-yes">✓ Voted Yes</span>'
        accent_cls = "vt-rec-card-yes"
    elif vt == "Voted No":
        vote_chip_html = '<span class="vt-rec-vote vt-rec-vote-no">✗ Voted No</span>'
        accent_cls = "vt-rec-card-no"
    elif vt == "Abstained":
        vote_chip_html = '<span class="vt-rec-vote vt-rec-vote-abs">— Abstained</span>'
        accent_cls = "vt-rec-card-abs"
    else:
        vote_chip_html = f'<span class="vt-rec-vote vt-rec-vote-abs">{_h(vt) or "—"}</span>'
        accent_cls = "vt-rec-card-abs"

    outcome_html = _outcome_chip(vote_outcome)

    source_html = source_link_html(oireachtas_url, "Oireachtas", aria_label="Open this division on oireachtas.ie")

    return (
        f'<div class="vt-rec-card {accent_cls}">'
        f'<div class="vt-rec-header">'
        f'<span class="vt-card-date">{date_str}</span>'
        f"{vote_chip_html}"
        f"{outcome_html}"
        f"{source_html}"
        f"</div>"
        f'<div class="vt-card-title">{title}</div>'
        f"</div>"
    )


def render_td_panel(
    td_row: pd.Series,
    history_df: pd.DataFrame,
    year_df: pd.DataFrame,
    *,
    show_header: bool = True,
    key_suffix: str = "",
) -> None:
    """Per-TD voting profile body.

    When ``show_header=False`` (embedded in member-overview Votes expander):
    - skip the ``st.container(border=True)`` wrapper (would nest inside the
      expander chrome)
    - skip the TD name + party · constituency line (hero shows it)
    - skip the "View full accountability profile" CTA (we're already there)

    ``key_suffix`` namespaces the pager / export widget keys so the embedded
    copy doesn't collide with the stand-alone /rankings-votes page state.
    """
    member_id = str(td_row.get("member_id") or "")
    name = str(td_row.get("member_name") or "—")
    party = str(td_row.get("party_name") or "—")
    const = str(td_row.get("constituency") or "—")
    yes_n = int(td_row.get("yes_count") or 0)
    no_n = int(td_row.get("no_count") or 0)
    abs_n = int(td_row.get("abstained_count") or 0)
    div_count = int(td_row.get("division_count") or 0)
    yes_rate = td_row.get("yes_rate_pct")
    rate_str = f"{float(yes_rate):.1f}%" if yes_rate is not None else "—"

    safe_mid = ""
    for ch in member_id:
        if ch.isalnum():
            safe_mid += ch
    if not safe_mid:
        safe_mid = "td"
    safe_mid = f"{safe_mid}{key_suffix}"

    # `dummy_ctx` lets the body run without a bordered container when embedded.
    body_ctx = st.container(border=True) if show_header else _NullCtx()
    with body_ctx:
        if show_header:
            st.html(f'<p class="td-name">{_h(name)}</p><p class="td-meta">{_h(party)} · {_h(const)}</p>')

        stat_strip(
            [
                (str(yes_n), "Yes ✓", "oklch(38% 0.130 145)"),
                (str(no_n), "No ✗", "oklch(45% 0.180 30)"),
                (str(abs_n), "Abstained", "var(--text-meta)"),
                (rate_str, "Yes rate", "oklch(38% 0.130 145)"),
                (str(div_count), "Divisions", "var(--text-primary)"),
            ]
        )

        if show_header and member_id:
            st.html(
                entity_cta_html(
                    member_profile_url(member_id),
                    "View full accountability profile →",
                )
            )

        # When the per-year summary view has no rows yet, skip the whole
        # sub-section — a heading + "Coming soon" box was pure noise on a
        # profile this long (same demotion as interests P2-1).
        if year_df.empty:
            st.caption("Votes by year: per-year breakdown will appear here when the pipeline view lands.")
        else:
            evidence_heading("Votes by year")
            _yr_req = frozenset({"year", "yes_count", "no_count"})
            missing_y = sorted(c for c in _yr_req if c not in year_df.columns)
            if missing_y:
                mc_str = missing_y[0]
                for c in missing_y[1:]:
                    mc_str += ", " + c
                todo_callout(f"v_td_vote_year_summary missing columns: {mc_str}")
            else:
                _year_chart(year_df)

        evidence_heading("Vote history")
        if history_df.empty:
            empty_state(
                "No vote history found",
                "No records in v_vote_member_detail for this member and date range.",
            )
        else:
            total = len(history_df)
            pager_key = f"td_hist_{safe_mid}"

            # Resolve current page slice from session state (set by the helper
            # below; we read first so the table reflects the active page).
            page_size = int(st.session_state.get(f"{pager_key}_size", 25))
            cur_page = int(st.session_state.get(f"{pager_key}_page", 1))
            total_pages = max(1, (total + page_size - 1) // page_size)
            if cur_page > total_pages:
                cur_page = 1
                st.session_state[f"{pager_key}_page"] = 1
            start = (cur_page - 1) * page_size
            page_df = history_df.iloc[start : start + page_size]

            st.html(_render_td_history_html(page_df))

            # Reusable pager — chips, "Showing X–Y of Z votes" caption, page-size selector.
            pagination_controls(total, key_prefix=pager_key, label="votes")

            hist_cols = [
                c for c in ["vote_date", "debate_title", "vote_type", "vote_outcome"] if c in history_df.columns
            ]
            export_button(
                history_df[hist_cols],
                label="Export vote history CSV",
                filename=f"td_{safe_mid}_votes.csv",
                key=f"exp_td_{safe_mid}",
            )


# ── Cross-page wrapper ────────────────────────────────────────────────────────


def render_member_votes(
    conn,
    member_id: str,
    *,
    show_header: bool = True,
    date_from: str | None = None,
    date_to: str | None = None,
    key_suffix: str = "",
) -> None:
    """One-call per-TD voting body: fetches v_td_vote_summary + history +
    year_summary against the supplied DuckDB connection, then renders via
    :func:`render_td_panel`.

    Used by both /rankings-votes (Mode B, stand-alone) and member-overview's
    Votes expander. The two pages register the same vote views — see
    ``data_access/votes_data.py`` and ``data_access/member_overview_data.py``.
    """
    if conn is None:
        empty_state("Vote data unavailable", "No DuckDB connection.")
        return

    # Retrieval SQL lives in the read layer (dail_tracker_core.queries.votes) — the
    # firewall keeps the UI free of raw conn.execute. A DuckDB failure surfaces as an
    # unavailable QueryResult, whose .data is an empty frame, matching the old
    # try/except-to-empty behaviour exactly.
    summary_res = _vq.member_vote_summary(conn, member_id)
    if not summary_res.ok:
        empty_state(
            "TD not found",
            "v_td_vote_summary returned no record for this member id.",
        )
        return

    td_df = summary_res.data
    if td_df.empty:
        empty_state(
            "No vote data for this member",
            "v_td_vote_summary returned no rows — this TD may have no recorded divisions.",
        )
        return

    history_df = _vq.member_vote_history(
        conn, member_id, date_from=date_from, date_to=date_to, limit=_TD_HISTORY_LIMIT
    ).data
    year_df = _vq.member_year_summary(conn, member_id).data

    render_td_panel(
        td_df.iloc[0],
        history_df,
        year_df,
        show_header=show_header,
        key_suffix=key_suffix,
    )
