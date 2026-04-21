import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared_css import inject_css

_ROOT = Path(__file__).parent.parent.parent
_CSV = {
    "Dáil": _ROOT / "data" / "silver" / "flattened_members.csv",
    "Seanad": _ROOT / "data" / "silver" / "flattened_seanad_members.csv",
}

COMMITTEE_TYPES = {
    "Policy": "Policy",
    "Oversight": "Oversight",
    "Statutory": "Statutory",
    "Shadow Department": "Shadow Department",
    "Parliamentary Regulation and Reform": "Parl. Regulation & Reform",
    "The Committee System and Parliamentary Administration": "Parl. Administration",
    "Parliamentary Business and Committee Membership": "Parl. Business",
}


# ── helpers ──────────────────────────────────────────────────────────

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
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "-", s.strip())
    return (s + suffix) if s else None


def _committee_url(name, dail_number) -> str | None:
    if pd.isna(name) or pd.isna(dail_number):
        return None
    slug = _committee_slug(name)
    return f"https://www.oireachtas.ie/en/committees/{int(dail_number)}/{slug}/" if slug else None


def _link_col():
    return st.column_config.LinkColumn("Link", display_text="Open ↗", width="small")


def _export(df: pd.DataFrame, filename: str, key: str, label: str = "Export CSV") -> None:
    st.download_button(label, df.to_csv(index=False).encode("utf-8"), filename, "text/csv", key=key)


# ── data loading ─────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def _load(chamber: str):
    df = pd.read_csv(_CSV[chamber], na_values=["Null"])

    prefixes = sorted(
        {m.group(1) for c in df.columns if (m := re.match(r"(committee_\d+)_", c))},
        key=lambda p: int(p.split("_")[1]),
    )

    # Office holders (ministerial roles)
    office_prefixes = sorted(
        {m.group(1) for c in df.columns if (m := re.match(r"(office_\d+)_", c))},
        key=lambda p: int(p.split("_")[1]),
    )
    office_records = []
    for _, row in df.iterrows():
        name = _coalesce(row.get("full_name")) or "Unknown"
        party = _coalesce(row.get("party")) or "Unknown"
        for op in office_prefixes:
            office_name = row.get(f"{op}_name")
            if pd.isna(office_name):
                continue
            office_records.append({
                "name": name,
                "party": party,
                "office": str(office_name).strip(),
                "start": _coalesce(row.get(f"{op}_start_date")),
                "end": _coalesce(row.get(f"{op}_end_date")),
            })
    offices = pd.DataFrame(office_records)

    # Committee memberships
    status_map = {"Live": "Active", "Dissolved": "Ended"}
    records = []
    for _, row in df.iterrows():
        base = {
            "name": _coalesce(row.get("full_name")) or "Unknown",
            "party": _coalesce(row.get("party")) or "Unknown",
            "constituency": _coalesce(row.get("constituency_name")),
            "dail_number": _coalesce(row.get("dail_number")),
        }
        for prefix in prefixes:
            c_name = row.get(f"{prefix}_name_en")
            if pd.isna(c_name):
                continue
            role = _coalesce(row.get(f"{prefix}_role_title")) or "Member"
            records.append({
                **base,
                "committee": str(c_name).strip(),
                "committee_url": _committee_url(c_name, base["dail_number"]),
                "type": _coalesce(row.get(f"{prefix}_type")) or "Unknown",
                "status": status_map.get(row.get(f"{prefix}_main_status"), "Unknown"),
                "role": role,
                "is_chair": "cathaoirleach" in str(role).lower(),
                "start": _coalesce(
                    row.get(f"{prefix}_role_start_date"),
                    row.get(f"{prefix}_member_start_date"),
                ),
                "end": _coalesce(
                    row.get(f"{prefix}_role_end_date"),
                    row.get(f"{prefix}_member_end_date"),
                ),
            })

    df_long = pd.DataFrame(records)

    if not df_long.empty:
        df_long["start"] = pd.to_datetime(df_long["start"], errors="coerce", utc=True).dt.tz_localize(None)
        df_long["end"] = pd.to_datetime(df_long["end"], errors="coerce", utc=True).dt.tz_localize(None)
        today = pd.Timestamp.today().normalize()
        df_long["end_filled"] = df_long["end"].fillna(today)
        df_long["duration_days"] = (df_long["end_filled"] - df_long["start"]).dt.days.clip(lower=0)

    # Per-TD activity summary
    all_members = (
        df[["full_name", "party", "constituency_name"]]
        .rename(columns={"full_name": "name", "constituency_name": "constituency"})
        .drop_duplicates(subset=["name"])
    )
    all_members["party"] = all_members["party"].fillna("Unknown")

    if df_long.empty:
        stats = pd.DataFrame(columns=["name", "committees", "active", "chairs"])
    else:
        stats = (
            df_long.groupby("name")
            .agg(
                committees=("committee", "nunique"),
                active=("status", lambda s: int((s == "Active").sum())),
                chairs=("is_chair", "sum"),
            )
            .reset_index()
        )

    activity = (
        all_members.merge(stats, on="name", how="left")
        .fillna({"committees": 0, "active": 0, "chairs": 0})
        .astype({"committees": int, "active": int, "chairs": int})
    )

    return df_long, activity, offices


# ── page sections ─────────────────────────────────────────────────────

def _stat(num, label):
    return (
        f'<div>'
        f'<div class="stat-num">{num}</div>'
        f'<div class="stat-lbl">{label}</div>'
        f'</div>'
    )


def _overview(df: pd.DataFrame, activity: pd.DataFrame, offices: pd.DataFrame, member_label: str = "TDs") -> None:
    active_committees = df[df["status"] == "Active"]["committee"].nunique()
    total_tds = activity["name"].nunique()
    chairs = int(df["is_chair"].sum())
    minister_count = offices["name"].nunique() if not offices.empty else 0

    st.markdown('<h1 class="page-title">Oireachtas Committees</h1>', unsafe_allow_html=True)
    st.markdown(
        """
        <div style="font-size:1.1rem;line-height:1.6;margin-bottom:0.7em">
        <strong>Who does the real work?</strong> Committees are where much of the Oireachtas' scrutiny, investigation, and lawmaking happens. This dashboard shows every committee, its members, and their roles across the Dáil and Seanad.
        </div>
        """,
        unsafe_allow_html=True,
    )
    with st.expander("What is this data? (Click for details)", expanded=False):
        st.markdown(
            """
            **About Oireachtas Committees**  
            Committees are small groups of TDs and Senators who examine legislation, hold hearings, and scrutinise government work. Membership and roles are published by the Oireachtas and updated after each Dáil/Seanad election.
            
            **Data sources:**
            - Official Oireachtas committee membership records (flattened_members.csv, flattened_seanad_members.csv)
            - Aggregated and cleaned in the 'silver' layer for analysis
            
            **Caveats:**
            - Some committee assignments may be missing or out of date
            - Role titles and committee names are as published by the Oireachtas
            - "Active" means currently serving; "Chairs" includes all chairing roles
            """
        )
    st.markdown(
        f'<div class="stat-strip">'
        + _stat(total_tds, member_label)
        + _stat(df["committee"].nunique(), "Committees")
        + _stat(active_committees, "Active committees")
        + _stat(int((df["status"] == "Active").sum()), "Active memberships")
        + _stat(chairs, "Chairs held")
        + _stat(minister_count, "Office holders")
        + "</div>",
        unsafe_allow_html=True,
    )

    # ── Most & least active ───────────────────────────────────────
    col_l, col_r = st.columns(2)

    with col_l:
        st.markdown('<p class="section-heading">Most committee memberships</p>', unsafe_allow_html=True)
        top = activity.sort_values(["committees", "chairs"], ascending=False).head(15).reset_index(drop=True)
        max_c = int(top["committees"].max()) or 1
        st.dataframe(
            top,
            hide_index=True,
            use_container_width=True,
            column_config={
                "name": st.column_config.TextColumn(member_label[:-1]),
                "party": st.column_config.TextColumn("Party"),
                "constituency": st.column_config.TextColumn("Constituency"),
                "committees": st.column_config.ProgressColumn("Committees", format="%d", min_value=0, max_value=max_c),
                "active": st.column_config.NumberColumn("Active"),
                "chairs": st.column_config.NumberColumn("Chairs"),
            },
        )
        _export(top, "most_active_committees.csv", "ov_top_export")

    with col_r:
        st.markdown('<p class="section-heading">Fewest committee memberships</p>', unsafe_allow_html=True)
        st.caption("Zero means no committee seat at all. Ministers currently serving are excluded.")

        # Identify currently serving ministers (no end date, or end date in the future)
        today = pd.Timestamp.today().normalize()
        if not offices.empty:
            current_ministers = set(
                offices[(offices["end"].isna()) | (pd.to_datetime(offices["end"], errors="coerce") >= today)]["name"]
            )
        else:
            current_ministers = set()

        # Exclude current ministers from the 'fewest' table
        non_minister_activity = activity[~activity["name"].isin(current_ministers)]
        bottom = non_minister_activity.sort_values(["committees", "active", "chairs"]).head(15).reset_index(drop=True)
        max_c2 = int(activity["committees"].max()) or 1
        st.dataframe(
            bottom,
            hide_index=True,
            use_container_width=True,
            column_config={
                "name": st.column_config.TextColumn(member_label[:-1]),
                "party": st.column_config.TextColumn("Party"),
                "constituency": st.column_config.TextColumn("Constituency"),
                "committees": st.column_config.ProgressColumn("Committees", format="%d", min_value=0, max_value=max_c2),
                "active": st.column_config.NumberColumn("Active"),
                "chairs": st.column_config.NumberColumn("Chairs"),
            },
        )
        _export(bottom, "least_active_committees.csv", "ov_bottom_export")

    # ── Party breakdown ───────────────────────────────────────────
    st.markdown('<p class="section-heading">Committee seats by party</p>', unsafe_allow_html=True)
    active_df = df[df["status"] == "Active"]
    party_seats = (
        active_df.groupby("party")
        .agg(seats=("name", "count"), tds=("name", "nunique"), chairs=("is_chair", "sum"))
        .sort_values("seats", ascending=False)
        .reset_index()
    )
    max_seats = int(party_seats["seats"].max()) or 1
    st.dataframe(
        party_seats,
        hide_index=True,
        use_container_width=True,
        column_config={
            "party": st.column_config.TextColumn("Party"),
            "seats": st.column_config.ProgressColumn("Active seats", format="%d", min_value=0, max_value=max_seats),
            "tds": st.column_config.NumberColumn("TDs"),
            "chairs": st.column_config.NumberColumn("Chairs held"),
        },
    )


    # ── Office holders ────────────────────────────────────────────
    if not offices.empty:
        st.markdown('<p class="section-heading">Government office holders</p>', unsafe_allow_html=True)
        st.caption("TDs who hold or have held ministerial or state office. Cross-reference with their committee memberships in the TD Profile view.")
        office_summary = (
            offices.groupby("name")
            .agg(party=("party", "first"), offices_held=("office", "nunique"))
            .sort_values("offices_held", ascending=False)
            .reset_index()
        )
        # Mark which office holders also chair committees
        chair_names = set(df[df["is_chair"]]["name"].unique())
        office_summary["chairs_committee"] = office_summary["name"].isin(chair_names)
        st.dataframe(
            office_summary,
            hide_index=True,
            use_container_width=True,
            column_config={
                "name": st.column_config.TextColumn("TD"),
                "party": st.column_config.TextColumn("Party"),
                "offices_held": st.column_config.NumberColumn("Offices held"),
                "chairs_committee": st.column_config.CheckboxColumn("Also chairs committee"),
            },
        )
        _export(offices, "office_holders.csv", "ov_offices_export")


def _browse_committees(df: pd.DataFrame, member_label: str = "TDs") -> None:
    """Browse all committees; click one to see its full member list."""
    col_l, col_r = st.columns([1, 2])

    with col_l:
        st.markdown('<p class="section-heading">Filter</p>', unsafe_allow_html=True)

        status_filter = st.radio("Status", ["All", "Active", "Ended"], horizontal=True, key="br_status")
        type_options = ["All types"] + sorted(df["type"].dropna().unique())
        type_filter = st.selectbox("Committee type", type_options, key="br_type")

        search = st.text_input("Search committee name", placeholder="e.g. Finance…", label_visibility="collapsed", key="br_search")

    filtered = df.copy()
    if status_filter != "All":
        filtered = filtered[filtered["status"] == status_filter]
    if type_filter != "All types":
        filtered = filtered[filtered["type"] == type_filter]

    # Committee-level summary
    summary = (
        filtered.groupby("committee")
        .agg(
            members=("name", "count"),
            parties=("party", "nunique"),
            chairs=("is_chair", "sum"),
            status=("status", "first"),
            type=("type", "first"),
            url=("committee_url", "first"),
        )
        .reset_index()
        .sort_values("members", ascending=False)
        .reset_index(drop=True)
    )

    if search.strip():
        summary = summary[summary["committee"].str.contains(search.strip(), case=False, na=False)]

    with col_r:
        st.markdown(
            f'<p class="section-heading">{len(summary)} committee{"s" if len(summary) != 1 else ""}</p>',
            unsafe_allow_html=True,
        )

    if summary.empty:
        st.info("No committees match these filters.")
        return

    max_m = int(summary["members"].max()) or 1
    st.dataframe(
        summary,
        hide_index=True,
        use_container_width=True,
        column_config={
            "committee": st.column_config.TextColumn("Committee", width="large"),
            "members": st.column_config.ProgressColumn("Members", format="%d", min_value=0, max_value=max_m),
            "parties": st.column_config.NumberColumn("Parties"),
            "chairs": st.column_config.NumberColumn("Chairs"),
            "status": st.column_config.TextColumn("Status", width="small"),
            "type": st.column_config.TextColumn("Type"),
            "url": _link_col(),
        },
    )
    _export(summary, "committees_filtered.csv", "br_summary_export")

    st.markdown("---")
    st.markdown('<p class="section-heading">Drill into a committee</p>', unsafe_allow_html=True)

    chosen = st.selectbox(
        "Select committee",
        summary["committee"].tolist(),
        label_visibility="collapsed",
        key="br_chosen",
    )
    if chosen:
        members = filtered[filtered["committee"] == chosen].sort_values("is_chair", ascending=False)
        url = members["committee_url"].dropna().iloc[0] if members["committee_url"].notna().any() else None
        c_type = members["type"].iloc[0]
        c_status = members["status"].iloc[0]

        signals = f'<span class="signal signal-neutral">{c_type}</span>'
        signals += f'<span class="signal {"signal-accent" if c_status == "Active" else "signal-dark"}">{c_status}</span>'
        if url:
            signals += f'&nbsp;<a href="{url}" target="_blank" style="font-family:\'Epilogue\',sans-serif;font-size:0.8rem;color:var(--accent);">Open on Oireachtas.ie ↗</a>'

        st.markdown(
            f'<div style="margin:0.75rem 0 1rem 0;">'
            f'<div class="td-name">{chosen}</div>'
            f'<div style="margin-top:0.4rem;">{signals}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

        party_split = members["party"].value_counts().reset_index()
        party_split.columns = ["Party", "Seats"]
        c1, c2 = st.columns([2, 1])
        with c1:
            view = members[["name", "party", "constituency", "role", "is_chair", "start", "end"]].reset_index(drop=True)
            st.dataframe(
                view,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "name": st.column_config.TextColumn(member_label[:-1]),
                    "party": st.column_config.TextColumn("Party"),
                    "constituency": st.column_config.TextColumn("Constituency"),
                    "role": st.column_config.TextColumn("Role"),
                    "is_chair": st.column_config.CheckboxColumn("Chair"),
                    "start": st.column_config.DateColumn("Start", format="YYYY-MM-DD"),
                    "end": st.column_config.DateColumn("End", format="YYYY-MM-DD"),
                },
            )
            _export(view, f"{chosen[:40].replace(' ','_')}_members.csv", "br_members_export")
        with c2:
            st.markdown('<p class="section-heading">Party composition</p>', unsafe_allow_html=True)
            max_s = int(party_split["Seats"].max()) or 1
            st.dataframe(
                party_split,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Seats": st.column_config.ProgressColumn("Seats", format="%d", min_value=0, max_value=max_s),
                },
            )


def _td_profile(df: pd.DataFrame, offices: pd.DataFrame, member_label: str = "TDs") -> None:
    all_names = sorted(df["name"].unique())

    search = st.text_input(f"Search {member_label[:-1]}", placeholder="Type a name…", label_visibility="collapsed", key="pr_search")
    query = search.strip().lower()
    filtered_names = [n for n in all_names if query in n.lower()] if query else all_names

    current = st.session_state.get("pr_selected")
    default_idx = filtered_names.index(current) if current in filtered_names else None

    chosen = st.selectbox(
        f"Select {member_label[:-1]}",
        filtered_names,
        index=default_idx,
        placeholder="Select a TD…",
        label_visibility="collapsed",
        key="pr_selectbox",
    )
    if chosen and chosen != current:
        st.session_state["pr_selected"] = chosen

    td = st.session_state.get("pr_selected")
    if not td:
        return

    person = df[df["name"] == td].copy()
    if person.empty:
        st.warning(f"No committee data for {td}.")
        return

    party = person["party"].iloc[0]
    constituency = person["constituency"].iloc[0]
    meta = " · ".join(x for x in [party, str(constituency)] if x and str(x) != "nan")

    total = person["committee"].nunique()
    active = int((person["status"] == "Active").sum())
    ended = int((person["status"] == "Ended").sum())
    chairs = int(person["is_chair"].sum())

    # Office roles
    td_offices = offices[offices["name"] == td] if not offices.empty else pd.DataFrame()

    st.markdown(
        f'<hr class="section-rule">'
        f'<div style="padding:0 0 1rem 0;">'
        f'<div class="td-name">{td}</div>'
        f'<div class="td-meta">{meta}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        f'<div class="stat-strip">'
        + _stat(total, "Committees")
        + _stat(active, "Active")
        + _stat(ended, "Ended")
        + _stat(chairs, "Chairs held")
        + _stat(len(td_offices), "Govt offices")
        + "</div>",
        unsafe_allow_html=True,
    )

    if not td_offices.empty:
        st.markdown('<p class="section-heading">Government offices</p>', unsafe_allow_html=True)
        st.dataframe(
            td_offices[["office", "start", "end"]],
            hide_index=True,
            use_container_width=True,
            column_config={
                "office": st.column_config.TextColumn("Office", width="large"),
                "start": st.column_config.DateColumn("Start", format="YYYY-MM-DD"),
                "end": st.column_config.DateColumn("End", format="YYYY-MM-DD"),
            },
        )

    st.markdown('<p class="section-heading">Committee memberships</p>', unsafe_allow_html=True)

    status_tab = st.radio("Show", ["All", "Active", "Ended"], horizontal=True, key="pr_status")
    view = person if status_tab == "All" else person[person["status"] == status_tab]
    view = view.sort_values(["status", "start"], ascending=[True, False], na_position="last")

    max_d = int(view["duration_days"].max()) if len(view) and "duration_days" in view.columns else 1
    st.dataframe(
        view[["committee", "committee_url", "type", "role", "is_chair", "status", "start", "end", "duration_days"]],
        hide_index=True,
        use_container_width=True,
        column_config={
            "committee": st.column_config.TextColumn("Committee", width="large"),
            "committee_url": _link_col(),
            "type": st.column_config.TextColumn("Type"),
            "role": st.column_config.TextColumn("Role"),
            "is_chair": st.column_config.CheckboxColumn("Chair"),
            "status": st.column_config.TextColumn("Status", width="small"),
            "start": st.column_config.DateColumn("Start", format="YYYY-MM-DD"),
            "end": st.column_config.DateColumn("End", format="YYYY-MM-DD"),
            "duration_days": st.column_config.ProgressColumn(
                "Days served", format="%d", min_value=0, max_value=max(max_d, 1)
            ),
        },
    )
    _export(view, f"{td.replace(' ','_')}_committees.csv", "pr_export")


# ── page entry ────────────────────────────────────────────────────────

def committees_page() -> None:
    inject_css()

    with st.sidebar:
        st.markdown('<div class="page-kicker">Dáil Tracker</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-title">Committee<br>Register</div>', unsafe_allow_html=True)

        st.markdown('<p class="sidebar-label">Chamber</p>', unsafe_allow_html=True)
        chamber = st.radio("Chamber", ["Dáil", "Seanad"], horizontal=True, key="comm_chamber", label_visibility="collapsed")
        if st.session_state.get("_comm_last_chamber") != chamber:
            st.session_state["pr_selected"] = None
            st.session_state["_comm_last_chamber"] = chamber

        df, activity, offices = _load(chamber)
        member_label = "Senators" if chamber == "Seanad" else "TDs"

        if df.empty:
            st.error(f"No committee data found for {chamber}.")
            return

        st.markdown(
            f'<div class="page-subtitle">{df["committee"].nunique()} committees · '
            f'{activity["name"].nunique()} {member_label} · '
            f'{int((df["status"] == "Active").sum())} active memberships</div>',
            unsafe_allow_html=True,
        )

        st.markdown('<p class="sidebar-label">View</p>', unsafe_allow_html=True)
        view = st.radio(
            "View",
            ["Overview", "Browse committees", f"{member_label[:-1]} Profile"],
            label_visibility="collapsed",
            key="comm_view",
        )

    st.markdown('<hr class="section-rule">', unsafe_allow_html=True)

    if view == "Overview":
        _overview(df, activity, offices, member_label)
    elif view == "Browse committees":
        _browse_committees(df, member_label)
    else:
        _td_profile(df, offices, member_label)
