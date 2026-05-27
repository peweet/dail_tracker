"""Committee Register — committee-first two-stage flow.

Primary view: register of every committee in the selected chamber.
Stage 2a: committee record (chair, composition, roster).
Stage 2b: TD profile (memberships, offices, tenure timeline).

Data flows through the four registered analytical views
(v_committee_assignments, v_committee_office_holders,
v_committee_member_detail, v_committee_party_seats), produced from
pipeline_sandbox/committees_long_format_etl.py. The wide→long reshape
that used to live in this page was the actual hot path; moving it to
the sandbox + views collapses each render to a flat retrieval.
"""

from __future__ import annotations

import os
import sys
from html import escape as _h
from pathlib import Path
from urllib.parse import quote

import altair as alt
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared_css import inject_css
from ui.avatars import avatar_credit_html, avatar_data_url, initials as _initials
from ui.components import (
    back_button,
    clean_meta,
    clickable_card_link,
    committee_identity_strip,
    committee_row_html,
    empty_state,
    evidence_heading,
    find_a_td_search,
    member_moved_callout,
    member_profile_header,
    page_error_boundary,
    paginate,
    pagination_controls,
    party_colour,
    render_stat_strip,
    sidebar_page_header,
    sidebar_provenance,
    sidebar_subtitle,
    stat_item,
    todo_callout,
)
from data_access.committees_data import (
    fetch_committee_assignments,
    fetch_committee_summary,
    fetch_office_holders,
    fetch_party_seats,
)
from data_access.identity_resolver import resolve_member_code
from ui.entity_links import member_profile_url
from ui.export_controls import export_button
from ui.table_config import committee_membership_column_config, committee_roster_column_config

from config import COMMITTEE_TYPES


# ── stage keys ────────────────────────────────────────────────────────
_STAGE_REGISTER = "register"
_STAGE_COMMITTEE = "committee"
_STAGE_TD = "td"


# ── transitional banner ───────────────────────────────────────────────


def _transition_notice() -> None:
    """Citizen-facing notice that this page is in transition.

    Round-3 audit P1-A fix: previously dumped a paragraph naming SILVER_MEMBERS_CSV,
    six v_committee_* view names, three derived-column names, and a yaml section
    reference — all developer-facing internals leaked to end users.

    Pipeline-side details retained as a code comment for maintainers:

      Pipeline gaps this page papers over while v_committee_* views are built:
        - reads SILVER_MEMBERS_CSV, unpivots committee_*/office_* columns in-page
        - is_chair derived from string match on role title
        - committee_status normalised in-page to {Active, Ended}
        - currently_in_government_office derived from office presence
      Tracked in committees.yaml § transition_state. Remove this notice +
      switch to direct view queries when v_committee_assignments,
      v_committee_member_detail, v_committee_sources, v_committee_party_seats
      land.

    Show the full dev detail in the rendered notice when
    DT_SHOW_TODO_DETAIL=1 in the environment.
    """
    detail_html = ""
    if os.getenv("DT_SHOW_TODO_DETAIL") == "1":
        detail_html = (
            '<div style="margin-top:0.4rem;font-family:monospace;font-size:0.72rem;'
            'color:var(--text-meta);">'
            "Reads SILVER_MEMBERS_CSV; unpivots committee_*/office_* columns; "
            "is_chair via role-title string match; awaiting v_committee_assignments / "
            "_member_detail / _sources / _party_seats."
            "</div>"
        )
    st.html(
        '<div class="dt-callout">'
        "<strong>Data refresh underway.</strong><br>"
        '<span style="color:var(--text-meta);font-size:0.85rem;line-height:1.55">'
        "Committee details on this page come from a transitional source while "
        "the full pipeline is being built. Composition is correct; some "
        "metadata (chair role, status) may be coarse."
        f"</span>{detail_html}</div>"
    )


# ── stage 1: register ─────────────────────────────────────────────────


def _stage_register(
    df_long: pd.DataFrame,
    offices: pd.DataFrame,
    chamber: str,
    member_label: str,
) -> None:
    # ── Editorial hero (no dek paragraph in primary view) ─────────────
    st.markdown(
        '<p class="dt-kicker">Dáil Tracker · Committee Register</p>'
        '<h1 style="margin:0.05rem 0 0.4rem;font-size:1.7rem;font-weight:800;letter-spacing:-0.01em">'
        "Who sits on which committee"
        "</h1>",
        unsafe_allow_html=True,
    )

    chosen_chamber = (
        st.pills(
            "Chamber",
            options=["Dáil", "Seanad"],
            default=chamber,
            key="comm_chamber_pills",
            label_visibility="collapsed",
        )
        or chamber
    )
    if chosen_chamber != chamber:
        st.session_state["comm_chamber"] = chosen_chamber
        st.session_state["comm_td"] = None
        st.session_state["comm_committee"] = None
        st.session_state.pop("reg_page", None)  # reset paginator to page 1
        st.rerun()

    _transition_notice()

    # ── Command bar ───────────────────────────────────────────────────
    cmd_l, cmd_r = st.columns([3, 2], gap="large")
    with cmd_l:
        st.markdown(
            '<p class="sidebar-label" style="margin-bottom:0.2rem">Filter committees</p>', unsafe_allow_html=True
        )
        f_search, f_type, f_status = st.columns([3, 2, 2])
        with f_search:
            # P1-2 audit fix: placeholder previously suggested live filter
            # ("e.g. Finance, Health…"); Streamlit's st.text_input applies
            # only on Enter / blur. Make the Enter-to-apply requirement
            # explicit so users don't conclude the filter is broken.
            search = st.text_input(
                "Committee name",
                placeholder="e.g. Finance, Health (press Enter)",
                label_visibility="collapsed",
                key="reg_search",
            )
        with f_type:
            type_options = ["All types"] + sorted({t for t in df_long["type"].dropna().unique() if t})
            type_filter = st.selectbox(
                "Committee type",
                type_options,
                key="reg_type",
                label_visibility="collapsed",
            )
        with f_status:
            status_filter = (
                st.segmented_control(
                    "Status",
                    ["All", "Active", "Ended"],
                    default="All",
                    key="reg_status",
                    label_visibility="collapsed",
                )
                or "All"
            )
    with cmd_r:
        st.markdown(
            '<p class="sidebar-label" style="margin-bottom:0.2rem">Or look up a member</p>', unsafe_allow_html=True
        )
        all_names = sorted(df_long["name"].dropna().unique().tolist())
        # Round-3 audit P1-E: the underlying typeahead only fires its
        # callback when the user PICKS from the suggestions dropdown
        # (typing + Enter doesn't return a value). Make that expectation
        # explicit so keyboard users aren't left wondering why Enter
        # does nothing.
        st.caption(
            "Type a name then **pick from the suggestions** to open that "
            "member's committee profile."
        )
        chosen_td = find_a_td_search(
            all_names,
            key_prefix="reg",
            placeholder=f"Type a {member_label[:-1]} name…",
        )
        # Phase 8: per-TD committee profile lives on /member-overview. The
        # typeahead renders an inline confirmation link (not a full-page
        # redirect — the register stays visible). Round-3 audit fix: resolve
        # the real unique_member_code; fall back to a clear "not found"
        # state if the name isn't in v_member_registry.
        if chosen_td:
            code = resolve_member_code(chosen_td)
            if code:
                target = member_profile_url(code, section="committees")
                link_html = (
                    f'<a class="dt-member-link" href="{_h(target)}" target="_self">'
                    f"Open this member's committee profile →</a>"
                )
            else:
                link_html = (
                    '<span style="color:var(--text-meta);font-style:italic;">'
                    "Not in member registry — try the "
                    '<a class="dt-member-link" href="/member-overview">All TDs browse</a>.</span>'
                )
            st.html(
                f'<div class="dt-callout" style="margin:0.5rem 0 0.75rem;">'
                f"<strong>{_h(chosen_td)}</strong> &nbsp;·&nbsp; {link_html}"
                f"</div>"
            )

    # ── Apply filters ─────────────────────────────────────────────────
    # status / type are committee-level (1:1 with summary rows), so applying
    # those filters post-rollup gives the same set as pre-rollup filtering.
    filtered = df_long.copy()
    if status_filter != "All":
        filtered = filtered[filtered["status"] == status_filter]
    if type_filter != "All types":
        filtered = filtered[filtered["type"] == type_filter]

    summary = fetch_committee_summary(chamber)
    if status_filter != "All":
        summary = summary[summary["status"] == status_filter]
    if type_filter != "All types":
        summary = summary[summary["type"] == type_filter]
    if search.strip():
        summary = summary[summary["committee"].str.contains(search.strip(), case=False, na=False)]
    summary = summary.reset_index(drop=True)

    # ── Register count strip ──────────────────────────────────────────
    if not summary.empty:
        member_count = int(filtered["name"].nunique())
        active_memberships = int((filtered["status"] == "Active").sum())
        chair_total = int(filtered["is_chair"].sum())
        render_stat_strip(
            stat_item(len(summary), "Committees"),
            stat_item(member_count, member_label),
            stat_item(active_memberships, "Current memberships"),
            stat_item(chair_total, "Chairs held"),
        )

    # ── Empty state ───────────────────────────────────────────────────
    if summary.empty:
        empty_state(
            "No committees match these filters",
            "Try clearing the name search, or switch Status to All.",
        )
        return

    # ── Committee row cards (NOT a dataframe) ─────────────────────────
    evidence_heading("Committees")
    REG_PAGE_SIZE = 25
    page_idx = paginate(len(summary), key_prefix="reg", page_size=REG_PAGE_SIZE)
    page_summary = summary.iloc[page_idx * REG_PAGE_SIZE : (page_idx + 1) * REG_PAGE_SIZE]
    cards_html: list[str] = []
    for i, row in page_summary.iterrows():
        committee_name = str(row["committee"])
        card_html = committee_row_html(
            name=committee_name,
            rank=int(i) + 1,
            chair=row["chair_name"] or None,
            chair_party=row["chair_party"] or None,
            members=int(row["members"]),
            type_=COMMITTEE_TYPES.get(str(row["type"]), str(row["type"])),
            status=str(row["status"]),
            party_seats=row["party_seats"],
            oireachtas_url=row["url"] if isinstance(row["url"], str) else None,
        )
        cards_html.append(
            clickable_card_link(
                href=f"?committee={quote(committee_name)}",
                inner_html=card_html,
                aria_label=f"Open {committee_name}",
                show_arrow=False,
            )
        )
    st.html("\n".join(cards_html))

    # ── Pagination controls (below the cards) ─────────────────────────
    pagination_controls(
        len(summary),
        key_prefix="reg",
        page_sizes=(REG_PAGE_SIZE,),
        default_page_size=REG_PAGE_SIZE,
        label="committees",
    )

    # ── CSV export of current displayed register ──────────────────────
    export_df = summary.drop(columns=["party_seats"], errors="ignore")
    export_button(export_df, "Export this register (CSV)", f"committee_register_{chamber.lower()}.csv", "reg_export")

    # ── Provenance ────────────────────────────────────────────────────
    _provenance(chamber)


# ── stage 2a: committee record ────────────────────────────────────────


def _stage_committee(
    df_long: pd.DataFrame,
    chamber: str,
    member_label: str,
) -> None:
    selected = st.session_state.get("comm_committee")
    members = df_long[df_long["committee"] == selected].copy() if selected else pd.DataFrame()
    if not selected or members.empty:
        empty_state("No committee selected", "Return to the register and pick a committee.")
        if back_button("← Back to register", key="cmt_back_empty"):
            st.session_state["comm_stage"] = _STAGE_REGISTER
            st.query_params.pop("committee", None)
            st.rerun()
        return

    # ── Back button (main content area, top of view) ──────────────────
    if back_button("← Back to register", key="cmt_back"):
        st.session_state["comm_stage"] = _STAGE_REGISTER
        st.session_state["comm_committee"] = None
        st.query_params.pop("committee", None)
        st.rerun()

    # ── Identity strip ────────────────────────────────────────────────
    chair_row = members[members["is_chair"]].head(1)
    chair_name = str(chair_row["name"].iloc[0]) if not chair_row.empty else None
    chair_party = str(chair_row["party"].iloc[0]) if not chair_row.empty else None
    url = members["committee_url"].dropna().iloc[0] if members["committee_url"].notna().any() else None

    committee_identity_strip(
        selected,
        type_=COMMITTEE_TYPES.get(str(members["type"].iloc[0]), str(members["type"].iloc[0])),
        status=str(members["status"].iloc[0]),
        chair=chair_name,
        chair_party=chair_party,
        member_count=int(len(members)),
        oireachtas_url=url,
        source_document_url=None,  # TODO_PIPELINE_VIEW_REQUIRED below
    )
    # P1-1 audit fix: the bare TODO token produced "Coming soon. More data
    # coming soon." after the round-3 P1-A helper rewrite — vacuous and
    # took prime above-the-fold space. Give the helper an em-dash split
    # point so it extracts a real citizen sentence.
    todo_callout(
        "source_document_url column on v_committee_sources — "
        "Source documents will link here in a future release: the official "
        "terms of reference and meeting transcripts for every committee."
    )

    # ── Composition + Roster (two evidence sections) ──────────────────
    comp_col, roster_col = st.columns([1, 2], gap="large")

    with comp_col:
        evidence_heading("Composition")
        # Round-3 audit P1-D guard: when members is empty (data join
        # returned no rows even though the identity strip claimed N members),
        # an unguarded value_counts → Altair chart renders as a blank panel.
        # Empty_state gives users a real explanation.
        if members.empty:
            empty_state(
                "Composition not available",
                "Member-level data for this committee hasn't loaded yet.",
            )
        else:
            # Per-committee party seats sourced from v_committee_party_seats —
            # the rollup is view-side, the page only reads it.
            seats = fetch_party_seats(chamber, selected)
            if seats.empty:
                # Defensive: chamber+committee filter returned nothing
                # (e.g. cache mismatch). Fall back gracefully.
                seats = pd.DataFrame(columns=["party", "seats"])
            else:
                seats = seats[["party", "seats"]]
            domain = seats["party"].tolist()
            rng = [party_colour(p) for p in domain]
            chart = (
                alt.Chart(seats)
                .mark_bar(cornerRadiusTopRight=3, cornerRadiusBottomRight=3)
                .encode(
                    x=alt.X("seats:Q", title="Seats", axis=alt.Axis(tickMinStep=1)),
                    # P1-4 audit fix: lift labelLimit so "Independent Ireland"
                    # doesn't get clipped to "Independent Irel..." on the
                    # narrow composition column. Default is 180 — bumping to
                    # 280 fits the longest party label comfortably without
                    # eating chart width.
                    y=alt.Y(
                        "party:N",
                        sort="-x",
                        title=None,
                        axis=alt.Axis(labelLimit=280),
                    ),
                    color=alt.Color("party:N", scale=alt.Scale(domain=domain, range=rng), legend=None),
                    tooltip=[
                        alt.Tooltip("party:N", title="Party"),
                        alt.Tooltip("seats:Q", title="Seats"),
                    ],
                )
                .properties(height=max(140, len(seats) * 26))
            )
            st.altair_chart(chart, width="stretch")

    with roster_col:
        evidence_heading("Roster")
        # Same P1-D guard: st.dataframe on an empty df renders as a silent
        # grey rectangle. Skip the dataframe AND the (useless) export
        # button when there are no rows to show.
        if members.empty:
            empty_state(
                "Roster not available",
                "No member rows on file for this committee in the current data.",
            )
        else:
            view = (
                members[["name", "party", "constituency", "role", "is_chair", "start", "end"]]
                .sort_values(["is_chair", "name"], ascending=[False, True])
                .reset_index(drop=True)
            )
            st.dataframe(
                view,
                hide_index=True,
                width="stretch",
                column_config=committee_roster_column_config(member_label[:-1]),
            )
            export_button(
                view,
                "Export roster (CSV)",
                f"{selected[:60].replace(' ', '_')}_roster.csv",
                "cmt_roster_export",
            )

    _provenance(chamber)


# ── stage 2b: TD profile ──────────────────────────────────────────────


def render_member_committees(
    td_name: str,
    df_long: pd.DataFrame,
    offices: pd.DataFrame,
    chamber: str = "Dáil",
    *,
    show_member_header: bool = True,
    member_label: str = "TDs",
    status_filter_key: str = "td_status",
    export_key_suffix: str = "",
) -> None:
    """Per-TD committee profile body.

    Public so :mod:`pages_code.member_overview` can embed it inside the
    Committees expander. When ``show_member_header=False``: skip the back
    button, ``member_profile_header`` (hero shows identity), provenance
    footer, and convert the two ``st.dataframe`` views (Government offices,
    Committee memberships) into card lists — required by
    feedback_member_overview_no_dataframes.

    ``df_long`` and ``offices`` come from :func:`_load` and are shared with
    the stand-alone register page; loading is cached so passing them through
    rather than re-loading avoids duplicate work on cold start.
    """
    person = df_long[df_long["name"] == td_name].copy() if td_name else pd.DataFrame()
    if not td_name or person.empty:
        empty_state(
            f"No {member_label[:-1].lower()} record",
            "No committee memberships on file for this member in the silver scaffold.",
        )
        return

    # ── Back button (stand-alone page only) ───────────────────────────
    if show_member_header and back_button("← Back to register", key="td_back"):
        st.session_state["comm_stage"] = _STAGE_REGISTER
        st.session_state["comm_td"] = None
        st.rerun()

    # ── Identity strip (stand-alone only — hero handles it when embedded)
    if show_member_header:
        party = str(person["party"].iloc[0])
        constituency = str(person["constituency"].iloc[0]) if pd.notna(person["constituency"].iloc[0]) else ""
        member_profile_header(
            td_name,
            clean_meta(party, constituency),
            avatar_url=avatar_data_url(td_name),
            avatar_initials=_initials(td_name),
            avatar_credit_html=avatar_credit_html(td_name),
        )

    # ── Summary chips ─────────────────────────────────────────────────
    total = int(person["committee"].nunique())
    active = int((person["status"] == "Active").sum())
    ended = int((person["status"] == "Ended").sum())
    chairs = int(person["is_chair"].sum())
    td_offices = offices[offices["name"] == td_name] if not offices.empty else pd.DataFrame()
    render_stat_strip(
        stat_item(total, "Committees"),
        stat_item(active, "Active"),
        stat_item(ended, "Ended"),
        stat_item(chairs, "Chairs held"),
        stat_item(int(len(td_offices)), "Govt offices"),
    )

    # ── Government offices (when present) ─────────────────────────────
    if not td_offices.empty:
        evidence_heading("Government offices")
        if show_member_header:
            st.dataframe(
                td_offices[["office", "start", "end"]].reset_index(drop=True),
                hide_index=True,
                width="stretch",
                column_config={
                    "office": st.column_config.TextColumn("Office", width="large"),
                    "start": st.column_config.DateColumn("Start", format="YYYY-MM-DD"),
                    "end": st.column_config.DateColumn("End", format="YYYY-MM-DD"),
                },
            )
        else:
            # Embedded: card list (member_overview is dataframe-free).
            cards: list[str] = []
            for _, row in td_offices.iterrows():
                office = str(row.get("office") or "—")
                start_raw = row.get("start")
                end_raw = row.get("end")
                try:
                    start_disp = pd.to_datetime(start_raw).strftime("%b %Y") if pd.notna(start_raw) else "—"
                except Exception:
                    start_disp = str(start_raw or "—")
                try:
                    end_disp = pd.to_datetime(end_raw).strftime("%b %Y") if pd.notna(end_raw) else "present"
                except Exception:
                    end_disp = str(end_raw or "present")
                cards.append(
                    f'<div class="comm-office-card">'
                    f'<div class="comm-office-card-title">{_h(office)}</div>'
                    f'<div class="comm-office-card-dates">{_h(start_disp)} → {_h(end_disp)}</div>'
                    f"</div>"
                )
            st.html(f'<div class="comm-office-list">{"".join(cards)}</div>')

    # ── Tenure timeline (Altair tick / span chart) ────────────────────
    evidence_heading("Tenure timeline")
    timeline = person.copy()
    today_ts = pd.Timestamp.today().normalize()
    timeline["end_filled"] = timeline["end"].fillna(today_ts)
    timeline = timeline[timeline["start"].notna()]
    if timeline.empty:
        st.caption("No dated memberships available.")
    else:
        domain = timeline["committee"].drop_duplicates().tolist()
        timeline_chart = (
            alt.Chart(timeline)
            .mark_bar(height=12, cornerRadius=2)
            .encode(
                x=alt.X("start:T", title=None),
                x2="end_filled:T",
                y=alt.Y("committee:N", sort=domain, title=None, axis=alt.Axis(labelLimit=320)),
                color=alt.Color(
                    "status:N",
                    scale=alt.Scale(
                        domain=["Active", "Ended", "Unknown"],
                        range=["#1e88e5", "#9e9e9e", "#bdbdbd"],
                    ),
                    legend=alt.Legend(orient="top", title=None),
                ),
                tooltip=[
                    alt.Tooltip("committee:N", title="Committee"),
                    alt.Tooltip("role:N", title="Role"),
                    alt.Tooltip("start:T", title="Start", format="%d %b %Y"),
                    alt.Tooltip("end:T", title="End", format="%d %b %Y"),
                ],
            )
            .properties(height=max(120, len(domain) * 26))
        )
        st.altair_chart(timeline_chart, width="stretch")

    # ── Memberships table + export ────────────────────────────────────
    evidence_heading("Committee memberships")
    status_tab = (
        st.segmented_control(
            "Show",
            ["All", "Active", "Ended"],
            default="All",
            key=status_filter_key,
        )
        or "All"
    )
    view = person if status_tab == "All" else person[person["status"] == status_tab]
    view = (
        view[["committee", "committee_url", "type", "role", "is_chair", "status", "start", "end"]]
        .sort_values(["status", "start"], ascending=[True, False], na_position="last")
        .reset_index(drop=True)
    )
    if view.empty:
        empty_state("No memberships in this filter", "Switch the Show filter to All to see every committee.")
    else:
        if show_member_header:
            st.dataframe(
                view,
                hide_index=True,
                width="stretch",
                column_config=committee_membership_column_config(),
            )
        else:
            # Embedded: card list per membership.
            cards: list[str] = []
            for _, row in view.iterrows():
                committee = str(row.get("committee") or "—")
                committee_url = str(row.get("committee_url") or "").strip()
                ctype = str(row.get("type") or "")
                role = str(row.get("role") or "Member")
                is_chair = bool(row.get("is_chair"))
                status = str(row.get("status") or "Unknown")
                start_raw = row.get("start")
                end_raw = row.get("end")
                try:
                    start_disp = pd.to_datetime(start_raw).strftime("%b %Y") if pd.notna(start_raw) else "—"
                except Exception:
                    start_disp = str(start_raw or "—")
                try:
                    end_disp = pd.to_datetime(end_raw).strftime("%b %Y") if pd.notna(end_raw) else "present"
                except Exception:
                    end_disp = str(end_raw or "present")
                title_html = (
                    f'<a class="dt-source-link" href="{_h(committee_url)}" target="_blank" rel="noopener">'
                    f"{_h(committee)}</a>"
                    if committee_url.startswith("http")
                    else _h(committee)
                )
                status_class = {
                    "Active": "comm-status-active",
                    "Ended": "comm-status-ended",
                }.get(status, "comm-status-unknown")
                chair_pill = (
                    '<span class="comm-chair-pill">Chair</span>' if is_chair else ""
                )
                cards.append(
                    f'<div class="comm-member-card">'
                    f'<div class="comm-member-card-header">'
                    f'<span class="comm-member-card-title">{title_html}</span>'
                    f'<span class="comm-status {status_class}">{_h(status)}</span>'
                    f"{chair_pill}"
                    f"</div>"
                    f'<div class="comm-member-card-meta">'
                    f"{_h(role)}"
                    f' &nbsp;·&nbsp; {_h(ctype)}'
                    f' &nbsp;·&nbsp; <span class="comm-member-card-dates">{_h(start_disp)} → {_h(end_disp)}</span>'
                    f"</div>"
                    f"</div>"
                )
            st.html(f'<div class="comm-member-list">{"".join(cards)}</div>')
        export_button(
            view,
            "Export profile (CSV)",
            f"{td_name.replace(' ', '_')}_committees.csv",
            f"td_export{export_key_suffix}",
        )

    if show_member_header:
        _provenance(chamber)


# Back-compat shim — committees_page() used to dispatch to _stage_td.
# After Phase 8, the dispatcher redirects cross-page; this wrapper stays so
# imports inside committees.py and external test harnesses don't break.
def _stage_td(
    df_long: pd.DataFrame,
    offices: pd.DataFrame,
    chamber: str,
    member_label: str,
) -> None:
    render_member_committees(
        st.session_state.get("comm_td", ""),
        df_long,
        offices,
        chamber,
        show_member_header=True,
        member_label=member_label,
    )


# ── provenance ────────────────────────────────────────────────────────


def _provenance(chamber: str) -> None:
    with st.expander("About & data provenance", expanded=False):
        st.markdown(
            f"""
            **Source.** Member registers published by the Houses of the Oireachtas
            (`{chamber}` chamber).

            **Backing during transition.** This page reads the silver members CSV and
            unpivots `committee_*` and `office_*` columns in the page itself. This is
            a temporary scaffold authorised in `committees.yaml § transition_state`
            (2026-05-03) until the analytical views land.

            **Caveats.**
            - "Active" is derived from the source `Live` status; "Ended" from `Dissolved`.
            - "Chair" is detected by string-matching `cathaoirleach` in the role title — to be
              replaced by an `is_chair` boolean from `v_committee_assignments`.
            - The "Currently in government office" filter (used by the older "fewest"
              ranking, now removed) will be replaced by `currently_in_government_office`
              on `v_committee_member_detail`.

            **Pending pipeline work.**
            - `TODO_PIPELINE_VIEW_REQUIRED: v_committee_assignments`
            - `TODO_PIPELINE_VIEW_REQUIRED: v_committee_member_detail`
            - `TODO_PIPELINE_VIEW_REQUIRED: v_committee_sources` (incl. per-year source PDF URL)
            - `TODO_PIPELINE_VIEW_REQUIRED: v_committee_party_seats`
            """
        )


# ── page entry ────────────────────────────────────────────────────────


@page_error_boundary
def committees_page() -> None:
    inject_css()

    if "comm_chamber" not in st.session_state:
        st.session_state["comm_chamber"] = "Dáil"
    if "comm_stage" not in st.session_state:
        st.session_state["comm_stage"] = _STAGE_REGISTER
    st.session_state.setdefault("comm_committee", None)
    st.session_state.setdefault("comm_td", None)

    # Seed session state from URL query param so cards (full-page links) drill
    # straight into the committee record stage on first load.
    qp_committee = st.query_params.get("committee")
    if qp_committee and st.session_state.get("comm_committee") != qp_committee:
        st.session_state["comm_committee"] = qp_committee
        st.session_state["comm_stage"] = _STAGE_COMMITTEE

    # Phase 8: legacy ?member=<name> URLs redirect to the canonical
    # /member-overview Committees expander. Shared helper resolves the
    # real unique_member_code, scrubs state, and calls st.stop() so the
    # register doesn't render below the callout (round-3 audit P0-3).
    qp_member = st.query_params.get("member")
    if qp_member:
        # Reset stale stage state BEFORE the helper stops the page.
        st.session_state.pop("comm_td", None)
        if st.session_state.get("comm_stage") == _STAGE_TD:
            st.session_state["comm_stage"] = _STAGE_REGISTER
        member_moved_callout(
            qp_member,
            section="committees",
            section_label="Per-TD committee membership",
            legacy_param="member",
        )

    chamber = st.session_state["comm_chamber"]
    member_label = "Senators" if chamber == "Seanad" else "TDs"

    df_long = fetch_committee_assignments(chamber)
    offices = fetch_office_holders(chamber)

    with st.sidebar:
        sidebar_page_header("Committee<br>Register")
        sidebar_subtitle("Standing & Joint Committees · membership and chairs")
        if df_long.empty:
            # Sidebar audit fix (2026-05-26, P1-4): `st.error` rendered a
            # red Streamlit box inside the otherwise calm sidebar voice.
            # `empty_state` matches the design-system register.
            empty_state(
                "No committee data",
                f"No records found for {chamber}. Run the pipeline to populate the register.",
            )
        else:
            sidebar_provenance(
                f"{df_long['committee'].nunique()} committees · "
                f"{df_long['name'].nunique()} {member_label} · "
                f"{int((df_long['status'] == 'Active').sum())} active memberships"
            )

    if df_long.empty:
        return

    # Phase 8: _STAGE_TD is dead. Any session state still pointing at it
    # (from a prior browser session) collapses back to the register view —
    # the Find-a-TD typeahead in the command bar handles cross-page jumps
    # to /member-overview now.
    stage = st.session_state["comm_stage"]
    if stage == _STAGE_TD:
        stage = _STAGE_REGISTER
        st.session_state["comm_stage"] = _STAGE_REGISTER
    if stage == _STAGE_COMMITTEE:
        _stage_committee(df_long, chamber, member_label)
    else:
        _stage_register(df_long, offices, chamber, member_label)
