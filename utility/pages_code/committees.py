"""Committee Register — committee-first two-stage flow.

Primary view: register of every committee in the selected chamber.
Stage 2a: committee record (chair, composition, roster).
Stage 2b: TD profile (memberships, offices, tenure timeline).

TRANSITIONAL: this page reads SILVER_MEMBERS_CSV and unpivots committee_*/office_*
columns in-page while the v_committee_* analytical views are being built.
The CSV scaffold and any in-page modelling derived from it is technical debt
authorised in committees.yaml § transition_state on 2026-05-03 and MUST be
removed once the views land. Every section that uses the scaffold renders a
todo_callout (see _transition_notice).
"""
from __future__ import annotations

import re
import sys
import unicodedata
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared_css import inject_css
from ui.components import (
    back_button,
    clean_meta,
    committee_identity_strip,
    committee_row_html,
    empty_state,
    evidence_heading,
    find_a_td_search,
    member_profile_header,
    pagination_controls,
    party_colour,
    render_stat_strip,
    sidebar_page_header,
    stat_item,
    todo_callout,
)
from ui.export_controls import export_button
from ui.table_config import committee_membership_column_config, committee_roster_column_config

from config import COMMITTEE_TYPES, SILVER_MEMBERS_CSV


# ── stage keys ────────────────────────────────────────────────────────
_STAGE_REGISTER = "register"
_STAGE_COMMITTEE = "committee"
_STAGE_TD = "td"


# ── helpers ───────────────────────────────────────────────────────────


def _coalesce(*values):
    for v in values:
        if not pd.isna(v):
            return v
    return None


def _committee_slug(name: str) -> str | None:
    if pd.isna(name):
        return None
    s = str(name)
    suffix = ""
    chamber_patterns = [
        (("Dáil Committee on ", "Dail Committee on "), "-dail"),
        (("Seanad Committee on ",), "-seanad"),
    ]
    matched_chamber = False
    for prefixes, suf in chamber_patterns:
        for prefix in prefixes:
            if s.startswith(prefix):
                s = s[len(prefix):]
                suffix = suf
                matched_chamber = True
                break
        if matched_chamber:
            break
    if not matched_chamber:
        for prefix in ("Joint Committee on ", "Select Committee on ", "Committee on "):
            if s.startswith(prefix):
                s = s[len(prefix):]
                break
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "-", s.strip())
    return (s + suffix) if s else None


def _committee_url(name, dail_number) -> str | None:
    if pd.isna(name) or pd.isna(dail_number):
        return None
    slug = _committee_slug(name)
    return f"https://www.oireachtas.ie/en/committees/{int(dail_number)}/{slug}/" if slug else None


# ── data loading (TRANSITIONAL CSV SCAFFOLD) ──────────────────────────


@st.cache_data(show_spinner=False)
def _load(chamber: str):
    """Read silver members CSV and unpivot committee_*/office_* columns.

    TRANSITIONAL: replace with parameter-bound SELECTs on
        v_committee_assignments, v_committee_member_detail, v_committee_sources,
        v_committee_party_seats
    once registered. See committees.yaml § transition_state.
    """
    df = pd.read_csv(SILVER_MEMBERS_CSV[chamber], na_values=["Null"])

    prefixes = sorted(
        {m.group(1) for c in df.columns if (m := re.match(r"(committee_\d+)_", c))},
        key=lambda p: int(p.split("_")[1]),
    )
    office_prefixes = sorted(
        {m.group(1) for c in df.columns if (m := re.match(r"(office_\d+)_", c))},
        key=lambda p: int(p.split("_")[1]),
    )

    office_records: list[dict] = []
    for _, row in df.iterrows():
        name = _coalesce(row.get("full_name")) or "Unknown"
        party = _coalesce(row.get("party")) or "Unknown"
        for op in office_prefixes:
            office_name = row.get(f"{op}_name")
            if pd.isna(office_name):
                continue
            office_records.append({
                "name":  name,
                "party": party,
                "office": str(office_name).strip(),
                "start": _coalesce(row.get(f"{op}_start_date")),
                "end":   _coalesce(row.get(f"{op}_end_date")),
            })
    offices = pd.DataFrame(office_records)

    status_map = {"Live": "Active", "Dissolved": "Ended"}
    records: list[dict] = []
    for _, row in df.iterrows():
        base = {
            "name":         _coalesce(row.get("full_name")) or "Unknown",
            "party":        _coalesce(row.get("party")) or "Unknown",
            "constituency": _coalesce(row.get("constituency_name")),
            "dail_number":  _coalesce(row.get("dail_number")),
        }
        for prefix in prefixes:
            c_name = row.get(f"{prefix}_name_en")
            if pd.isna(c_name):
                continue
            role = _coalesce(row.get(f"{prefix}_role_title")) or "Member"
            records.append({
                **base,
                "committee":     str(c_name).strip(),
                "committee_url": _committee_url(c_name, base["dail_number"]),
                "type":          _coalesce(row.get(f"{prefix}_type")) or "Unknown",
                "status":        status_map.get(row.get(f"{prefix}_main_status"), "Unknown"),
                "role":          role,
                "is_chair":      "cathaoirleach" in str(role).lower(),
                "start":         _coalesce(
                    row.get(f"{prefix}_role_start_date"),
                    row.get(f"{prefix}_member_start_date"),
                ),
                "end":           _coalesce(
                    row.get(f"{prefix}_role_end_date"),
                    row.get(f"{prefix}_member_end_date"),
                ),
            })

    df_long = pd.DataFrame(records)
    if not df_long.empty:
        df_long["start"] = pd.to_datetime(df_long["start"], errors="coerce", utc=True).dt.tz_localize(None)
        df_long["end"] = pd.to_datetime(df_long["end"], errors="coerce", utc=True).dt.tz_localize(None)

    return df_long, offices


@st.cache_data(show_spinner=False)
def _committee_summary(df_long: pd.DataFrame) -> pd.DataFrame:
    """Per-committee rollup. TRANSITIONAL — replace with v_committee_member_detail."""
    if df_long.empty:
        return pd.DataFrame(
            columns=["committee", "members", "parties", "chairs", "status", "type", "url",
                     "chair_name", "chair_party", "party_seats"]
        )

    # Single-column groupbys for count only — permitted by transition_state.
    summary = (
        df_long.groupby("committee")
        .agg(
            members=("name", "count"),
            parties=("party", "nunique"),
            chairs=("is_chair", "sum"),
            status=("status", "first"),
            type=("type", "first"),
            url=("committee_url", "first"),
        )
        .reset_index()
    )

    chair_lookup: dict[str, tuple[str, str]] = {}
    for cname, sub in df_long[df_long["is_chair"]].groupby("committee"):
        first = sub.iloc[0]
        chair_lookup[cname] = (str(first["name"]), str(first.get("party") or ""))

    party_seats_lookup: dict[str, list[tuple[str, int]]] = {}
    for cname, sub in df_long.groupby("committee"):
        counts = sub["party"].value_counts()
        party_seats_lookup[cname] = [(str(p), int(c)) for p, c in counts.items()]

    summary["chair_name"]  = summary["committee"].map(lambda c: chair_lookup.get(c, ("", ""))[0])
    summary["chair_party"] = summary["committee"].map(lambda c: chair_lookup.get(c, ("", ""))[1])
    summary["party_seats"] = summary["committee"].map(party_seats_lookup)

    return summary.sort_values(["status", "members"], ascending=[True, False]).reset_index(drop=True)


# ── transitional banner ───────────────────────────────────────────────


def _transition_notice() -> None:
    """Single visible callout naming every pipeline gap this page papers over."""
    st.markdown(
        '<div class="dt-callout">'
        '<strong>Transitional data backing.</strong><br>'
        '<span style="color:var(--text-meta);font-size:0.85rem;line-height:1.55">'
        'This page currently reads <code>SILVER_MEMBERS_CSV</code> and unpivots '
        'committee/office columns in-page while the analytical views are being built. '
        'Required views: '
        '<code>v_committee_assignments</code>, '
        '<code>v_committee_member_detail</code>, '
        '<code>v_committee_sources</code>, '
        '<code>v_committee_party_seats</code>. '
        'Required derived columns: <code>is_chair</code> (boolean — replaces in-page string match), '
        '<code>committee_status</code> normalised to {Active, Ended}, '
        '<code>currently_in_government_office</code> boolean. '
        'See <code>committees.yaml § transition_state</code>.'
        '</span></div>',
        unsafe_allow_html=True,
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
        'Who sits on which committee'
        '</h1>',
        unsafe_allow_html=True,
    )

    chosen_chamber = st.pills(
        "Chamber",
        options=["Dáil", "Seanad"],
        default=chamber,
        key="comm_chamber_pills",
        label_visibility="collapsed",
    ) or chamber
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
        st.markdown('<p class="sidebar-label" style="margin-bottom:0.2rem">Filter committees</p>',
                    unsafe_allow_html=True)
        f_search, f_status, f_type = st.columns([3, 2, 2])
        with f_search:
            search = st.text_input(
                "Committee name",
                placeholder="e.g. Finance, Health…",
                label_visibility="collapsed",
                key="reg_search",
            )
        with f_status:
            status_filter = st.segmented_control(
                "Status",
                ["All", "Active", "Ended"],
                default="All",
                key="reg_status",
                label_visibility="collapsed",
            ) or "All"
        with f_type:
            type_options = ["All types"] + sorted(
                {t for t in df_long["type"].dropna().unique() if t}
            )
            type_filter = st.selectbox(
                "Committee type",
                type_options,
                key="reg_type",
                label_visibility="collapsed",
            )
    with cmd_r:
        st.markdown('<p class="sidebar-label" style="margin-bottom:0.2rem">Or look up a member</p>',
                    unsafe_allow_html=True)
        all_names = sorted(df_long["name"].dropna().unique().tolist())
        chosen_td = find_a_td_search(
            all_names,
            key_prefix="reg",
            placeholder=f"Type a {member_label[:-1]} name…",
        )
        if chosen_td:
            st.session_state["comm_td"] = chosen_td
            st.session_state["comm_stage"] = _STAGE_TD
            st.rerun()

    # ── Apply filters ─────────────────────────────────────────────────
    filtered = df_long.copy()
    if status_filter != "All":
        filtered = filtered[filtered["status"] == status_filter]
    if type_filter != "All types":
        filtered = filtered[filtered["type"] == type_filter]

    summary = _committee_summary(filtered)
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
            stat_item(active_memberships, "Active memberships"),
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
    page_size, page_idx = pagination_controls(
        len(summary),
        key_prefix="reg",
        page_sizes=(5,),
        default_page_size=5,
        label="committees",
    )
    page_summary = summary.iloc[page_idx * page_size : (page_idx + 1) * page_size]
    for i, row in page_summary.iterrows():
        card_html = committee_row_html(
            name=str(row["committee"]),
            rank=int(i) + 1,
            chair=row["chair_name"] or None,
            chair_party=row["chair_party"] or None,
            members=int(row["members"]),
            type_=COMMITTEE_TYPES.get(str(row["type"]), str(row["type"])),
            status=str(row["status"]),
            party_seats=row["party_seats"],
            oireachtas_url=row["url"] if isinstance(row["url"], str) else None,
        )
        card_col, btn_col = st.columns([14, 1])
        with card_col:
            st.markdown(card_html, unsafe_allow_html=True)
        btn_col.markdown('<div class="dt-nav-anchor"></div>', unsafe_allow_html=True)
        if btn_col.button("→", key=f"reg_open_{i}", help=f"Open {row['committee']}"):
            st.session_state["comm_committee"] = str(row["committee"])
            st.session_state["comm_stage"] = _STAGE_COMMITTEE
            st.rerun()

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
            st.rerun()
        return

    # ── Back button (main content area, top of view) ──────────────────
    if back_button("← Back to register", key="cmt_back"):
        st.session_state["comm_stage"] = _STAGE_REGISTER
        st.session_state["comm_committee"] = None
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
    todo_callout("source_document_url column on v_committee_sources")

    # ── Composition + Roster (two evidence sections) ──────────────────
    comp_col, roster_col = st.columns([1, 2], gap="large")

    with comp_col:
        evidence_heading("Composition")
        # value_counts on a single column — presentation-layer aggregate
        seats = members["party"].value_counts().reset_index()
        seats.columns = ["party", "seats"]
        domain = seats["party"].tolist()
        rng = [party_colour(p) for p in domain]
        chart = (
            alt.Chart(seats)
            .mark_bar(cornerRadiusTopRight=3, cornerRadiusBottomRight=3)
            .encode(
                x=alt.X("seats:Q", title="Seats", axis=alt.Axis(tickMinStep=1)),
                y=alt.Y("party:N", sort="-x", title=None),
                color=alt.Color("party:N", scale=alt.Scale(domain=domain, range=rng), legend=None),
                tooltip=[
                    alt.Tooltip("party:N", title="Party"),
                    alt.Tooltip("seats:Q", title="Seats"),
                ],
            )
            .properties(height=max(140, len(seats) * 26))
        )
        st.altair_chart(chart, use_container_width=True)

    with roster_col:
        evidence_heading("Roster")
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


def _stage_td(
    df_long: pd.DataFrame,
    offices: pd.DataFrame,
    chamber: str,
    member_label: str,
) -> None:
    td = st.session_state.get("comm_td")
    person = df_long[df_long["name"] == td].copy() if td else pd.DataFrame()
    if not td or person.empty:
        empty_state(f"No {member_label[:-1].lower()} selected",
                    "Return to the register and use the Find a TD search.")
        if back_button("← Back to register", key="td_back_empty"):
            st.session_state["comm_stage"] = _STAGE_REGISTER
            st.rerun()
        return

    # ── Back button (main content area) ───────────────────────────────
    if back_button("← Back to register", key="td_back"):
        st.session_state["comm_stage"] = _STAGE_REGISTER
        st.session_state["comm_td"] = None
        st.rerun()

    # ── Identity ──────────────────────────────────────────────────────
    party = str(person["party"].iloc[0])
    constituency = str(person["constituency"].iloc[0]) if pd.notna(person["constituency"].iloc[0]) else ""
    member_profile_header(td, clean_meta(party, constituency))

    # ── Summary chips ─────────────────────────────────────────────────
    total = int(person["committee"].nunique())
    active = int((person["status"] == "Active").sum())
    ended = int((person["status"] == "Ended").sum())
    chairs = int(person["is_chair"].sum())
    td_offices = offices[offices["name"] == td] if not offices.empty else pd.DataFrame()
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
        st.dataframe(
            td_offices[["office", "start", "end"]].reset_index(drop=True),
            hide_index=True,
            width="stretch",
            column_config={
                "office": st.column_config.TextColumn("Office", width="large"),
                "start":  st.column_config.DateColumn("Start", format="YYYY-MM-DD"),
                "end":    st.column_config.DateColumn("End",   format="YYYY-MM-DD"),
            },
        )

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
                y=alt.Y("committee:N", sort=domain, title=None,
                        axis=alt.Axis(labelLimit=320)),
                color=alt.Color("status:N",
                                scale=alt.Scale(
                                    domain=["Active", "Ended", "Unknown"],
                                    range=["#1e88e5", "#9e9e9e", "#bdbdbd"],
                                ),
                                legend=alt.Legend(orient="top", title=None)),
                tooltip=[
                    alt.Tooltip("committee:N", title="Committee"),
                    alt.Tooltip("role:N",      title="Role"),
                    alt.Tooltip("start:T",     title="Start", format="%d %b %Y"),
                    alt.Tooltip("end:T",       title="End",   format="%d %b %Y"),
                ],
            )
            .properties(height=max(120, len(domain) * 26))
        )
        st.altair_chart(timeline_chart, use_container_width=True)

    # ── Memberships table + export ────────────────────────────────────
    evidence_heading("Committee memberships")
    status_tab = st.segmented_control("Show", ["All", "Active", "Ended"],
                                      default="All", key="td_status") or "All"
    view = person if status_tab == "All" else person[person["status"] == status_tab]
    view = (
        view[["committee", "committee_url", "type", "role", "is_chair", "status", "start", "end"]]
        .sort_values(["status", "start"], ascending=[True, False], na_position="last")
        .reset_index(drop=True)
    )
    if view.empty:
        empty_state("No memberships in this filter",
                    "Switch the Show filter to All to see every committee.")
    else:
        st.dataframe(
            view,
            hide_index=True,
            width="stretch",
            column_config=committee_membership_column_config(),
        )
        export_button(view, "Export profile (CSV)", f"{td.replace(' ', '_')}_committees.csv", "td_export")

    _provenance(chamber)


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


def committees_page() -> None:
    inject_css()

    if "comm_chamber" not in st.session_state:
        st.session_state["comm_chamber"] = "Dáil"
    if "comm_stage" not in st.session_state:
        st.session_state["comm_stage"] = _STAGE_REGISTER
    st.session_state.setdefault("comm_committee", None)
    st.session_state.setdefault("comm_td", None)

    chamber = st.session_state["comm_chamber"]
    member_label = "Senators" if chamber == "Seanad" else "TDs"

    df_long, offices = _load(chamber)

    with st.sidebar:
        sidebar_page_header("Committee<br>Register")
        if df_long.empty:
            st.error(f"No committee data found for {chamber}.")
        else:
            st.markdown(
                f'<p class="page-subtitle">{df_long["committee"].nunique()} committees · '
                f'{df_long["name"].nunique()} {member_label} · '
                f'{int((df_long["status"] == "Active").sum())} active memberships</p>',
                unsafe_allow_html=True,
            )

    if df_long.empty:
        return

    stage = st.session_state["comm_stage"]
    if stage == _STAGE_COMMITTEE:
        _stage_committee(df_long, chamber, member_label)
    elif stage == _STAGE_TD:
        _stage_td(df_long, offices, chamber, member_label)
    else:
        _stage_register(df_long, offices, chamber, member_label)
