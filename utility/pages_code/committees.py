import re
import unicodedata
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st


CSV_PATH = Path(r"C:\Users\pglyn\PycharmProjects\dail_extractor\data\silver\flattened_members.csv")


# =====================================================================
# HELPERS
# =====================================================================
def _coalesce(*values):
    """Return first non-null value. Handles NaN/None/NaT."""
    for v in values:
        if not pd.isna(v):
            return v
    return None


# def _committee_slug(name: str) -> str | None:
#     """Best-effort slug for oireachtas.ie committee URLs."""
#     if pd.isna(name):
#         return None
#     s = str(name)
#     suffix = ""

#     # Chamber-specific committees: prefix moves to the end as a suffix.
#     # e.g. "Dáil Committee on Privileges and Oversight" → "privileges-and-oversight-dail"
#     chamber_patterns = [
#         (("Dáil Committee on ", "Dail Committee on "), "-dail"),
#         (("Seanad Committee on ",), "-seanad"),
#     ]
#     matched_chamber = False
#     for prefixes, suf in chamber_patterns:
#         for prefix in prefixes:
#             if s.startswith(prefix):
#                 s = s[len(prefix):]
#                 suffix = suf
#                 matched_chamber = True
#                 break
#         if matched_chamber:
#             break

#     # Joint/Select committees: just strip the prefix (no suffix).
#     if not matched_chamber:
#         for prefix in ("Joint Committee on ", "Select Committee on "):
#             if s.startswith(prefix):
#                 s = s[len(prefix):]
#                 break

#     s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
#     s = s.lower()
#     s = re.sub(r"[^\w\s-]", "", s)
#     s = re.sub(r"\s+", "-", s.strip())

#     if not s:
#         return None
#     return s + suffix
def _committee_slug(name: str) -> str | None:
    """Best-effort slug for oireachtas.ie committee URLs."""
    if pd.isna(name):
        return None
    s = str(name)
    suffix = ""

    # Chamber-specific committees: prefix moves to the end as a suffix.
    # e.g. "Dáil Committee on Privileges and Oversight" → "privileges-and-oversight-dail"
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

    # All other committee-type prefixes just get stripped.
    # Order matters: longer/more-specific prefixes first, so "Joint Committee on"
    # isn't partially-matched by "Committee on".
    if not matched_chamber:
        for prefix in (
            "Joint Committee on ",
            "Select Committee on ",
            "Committee on ",
        ):
            if s.startswith(prefix):
                s = s[len(prefix):]
                break

    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "-", s.strip())

    if not s:
        return None
    return s + suffix
def _committee_url(name, dail_number) -> str | None:
    """Construct the oireachtas.ie committee page URL, or None if we can't."""
    if pd.isna(name) or pd.isna(dail_number):
        return None
    slug = _committee_slug(name)
    if not slug:
        return None
    return f"https://www.oireachtas.ie/en/committees/{int(dail_number)}/{slug}/"


# =====================================================================
# DATA LOAD + TRANSFORM
# =====================================================================
@st.cache_data(show_spinner="Loading members…")
def _load_members(path: Path):
    """
    Pivot the wide flattened CSV into one row per (member, committee) membership.

    Returns:
        df_long:  long-format memberships, one row per committee slot.
        activity: one row per member with counts (includes members who have
                  no committees — they get zeros).
    """
    df = pd.read_csv(path, na_values=["Null"])

    committee_prefixes = sorted(
        {m.group(1) for c in df.columns if (m := re.match(r"(committee_\d+)_", c))},
        key=lambda p: int(p.split("_")[1]),
    )

    status_map = {"Live": "Active", "Dissolved": "Ended"}
    records = []

    for _, row in df.iterrows():
        base = {
            "name": _coalesce(row.get("full_name")) or "Unknown",
            "party": _coalesce(row.get("party")) or "Unknown",
            "constituency": _coalesce(row.get("constituency_name")),
            "dail_number": _coalesce(row.get("dail_number")),
        }

        for prefix in committee_prefixes:
            name = row.get(f"{prefix}_name_en")
            if pd.isna(name):
                continue

            status = status_map.get(row.get(f"{prefix}_main_status"), "Unknown")
            role = _coalesce(
                row.get(f"{prefix}_role_title"),
                row.get(f"{prefix}_role"),
            ) or "Member"

            start = _coalesce(
                row.get(f"{prefix}_role_start_date"),
                row.get(f"{prefix}_member_start_date"),
            )
            end = _coalesce(
                row.get(f"{prefix}_role_end_date"),
                row.get(f"{prefix}_member_end_date"),
            )

            records.append({
                **base,
                "committee": name,
                "committee_url": _committee_url(name, base["dail_number"]),
                "role": role,
                "status": status,
                "start": start,
                "end": end,
            })

    df_long = pd.DataFrame(records)

    # Build one-row-per-member activity table (includes zero-committee members)
    all_members = (
        df[["full_name", "party", "constituency_name"]]
        .rename(columns={"full_name": "name", "constituency_name": "constituency"})
        .drop_duplicates(subset=["name"])
    )
    all_members["party"] = all_members["party"].fillna("Unknown")

    if df_long.empty:
        stats = pd.DataFrame(columns=["name", "committees", "active", "leadership"])
    else:
        df_long["start"] = pd.to_datetime(df_long["start"], errors="coerce", utc=True).dt.tz_localize(None)
        df_long["end"] = pd.to_datetime(df_long["end"], errors="coerce", utc=True).dt.tz_localize(None)
        today = pd.Timestamp.today().normalize()
        df_long["end_filled"] = df_long["end"].fillna(today)
        df_long["duration_days"] = (df_long["end_filled"] - df_long["start"]).dt.days.clip(lower=0)
        df_long["is_leadership"] = df_long["role"].ne("Member")

        stats = (
            df_long.groupby("name")
            .agg(
                committees=("committee", "nunique"),
                active=("status", lambda s: int((s == "Active").sum())),
                leadership=("is_leadership", "sum"),
            )
            .reset_index()
        )

    activity = (
        all_members.merge(stats, on="name", how="left")
        .fillna({"committees": 0, "active": 0, "leadership": 0})
        .astype({"committees": "int", "active": "int", "leadership": "int"})
    )

    return df_long, activity


# =====================================================================
# RENDERING HELPERS
# =====================================================================
def _download_button(df: pd.DataFrame, filename: str, key: str, label: str = "⬇️ Export current view as CSV"):
    st.download_button(
        label=label,
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=filename,
        mime="text/csv",
        key=key,
    )


def _party_dominance(df: pd.DataFrame, min_members: int = 3, threshold: float = 0.6) -> pd.DataFrame:
    columns = ["Committee", "Committee URL", "Dominant party", "Share", "Members", "Total"]
    active = df[df["status"] == "Active"]
    if active.empty:
        return pd.DataFrame(columns=columns)

    rows = []
    for committee, grp in active.groupby("committee"):
        total = len(grp)
        if total < min_members:
            continue
        top_party = grp["party"].value_counts()
        dominant = top_party.index[0]
        count = int(top_party.iloc[0])
        share = count / total
        if share >= threshold:
            url = grp["committee_url"].dropna().iloc[0] if grp["committee_url"].notna().any() else None
            rows.append({
                "Committee": committee,
                "Committee URL": url,
                "Dominant party": dominant,
                "Share": share,
                "Members": count,
                "Total": total,
            })

    if not rows:
        return pd.DataFrame(columns=columns)

    return (
        pd.DataFrame(rows)
        .sort_values(["Share", "Total"], ascending=[False, False])
        .reset_index(drop=True)
    )


def _link_column():
    """Standard LinkColumn for the 'View on oireachtas.ie' affordance."""
    return st.column_config.LinkColumn(
        "Info",
        help="Open the committee page on oireachtas.ie",
        display_text="Open ↗",
    )


# =====================================================================
# SUB-SECTIONS
# =====================================================================
def _overview_section(df: pd.DataFrame, activity: pd.DataFrame):
    ongoing_committees = df.loc[df["status"] == "Active", "committee"].nunique()

    st.caption(
        f"{len(df):,} committee memberships · {activity['name'].nunique()} politicians · "
        f"{df['committee'].nunique()} committees ({ongoing_committees} ongoing)"
    )

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Politicians", activity["name"].nunique())
    c2.metric("Committees", df["committee"].nunique())
    c3.metric("Ongoing committees", ongoing_committees)
    c4.metric("Active memberships", int((df["status"] == "Active").sum()))
    c5.metric("Leadership roles", int(df["is_leadership"].sum()))

    st.divider()

    # ── TD search ────────────────────────────────────────────────
    st.subheader("🔎 Find a TD")

    top_td_names = (
        activity.sort_values(["leadership", "committees"], ascending=False)
        .head(6)["name"].tolist()
    )

    if "committees_td_search" not in st.session_state:
        st.session_state["committees_td_search"] = ""

    st.caption("Suggestions:")
    suggestion_cols = st.columns(len(top_td_names))
    for i, nm in enumerate(top_td_names):
        if suggestion_cols[i].button(nm, use_container_width=True, key=f"committees_suggest_{i}"):
            st.session_state["committees_td_search"] = nm

    query = st.text_input(
        "Search by name",
        placeholder="Type part of a TD's name…",
        key="committees_td_search",
    )

    if query:
        matches_mask = df["name"].str.contains(query, case=False, na=False)
        matches = df[matches_mask]
        matched_activity = activity[activity["name"].str.contains(query, case=False, na=False)]

        if not matched_activity.empty:
            st.caption(
                f"{len(matched_activity)} TD{'s' if len(matched_activity) != 1 else ''} matched "
                f"· {len(matches)} committee membership{'s' if len(matches) != 1 else ''}"
            )

            summary = matched_activity.sort_values("committees", ascending=False)
            st.dataframe(
                summary,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "name": st.column_config.TextColumn("Politician"),
                    "party": st.column_config.TextColumn("Party"),
                    "constituency": st.column_config.TextColumn("Constituency"),
                    "committees": st.column_config.NumberColumn("Committees"),
                    "active": st.column_config.NumberColumn("Active"),
                    "leadership": st.column_config.NumberColumn("Leadership"),
                },
            )

            # Per-TD committee lists with URLs
            if not matches.empty:
                st.markdown("**Committees for matched TDs:**")
                for name in summary["name"]:
                    person_rows = matches[matches["name"] == name]
                    if person_rows.empty:
                        continue
                    with st.expander(f"{name} · {len(person_rows)} memberships", expanded=len(summary) == 1):
                        st.dataframe(
                            person_rows[["committee", "committee_url", "role", "status", "start", "end"]]
                            .sort_values("start", ascending=False, na_position="last"),
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "committee": st.column_config.TextColumn("Committee", width="large"),
                                "committee_url": _link_column(),
                                "role": st.column_config.TextColumn("Role"),
                                "status": st.column_config.TextColumn("Status", width="small"),
                                "start": st.column_config.DateColumn("Start", format="YYYY-MM-DD"),
                                "end": st.column_config.DateColumn("End", format="YYYY-MM-DD"),
                            },
                        )

            _download_button(
                matches,
                filename=f"td_search_{query}.csv",
                key="committees_td_search_export",
                label=f"⬇️ Export {len(matches)} matching memberships",
            )
        else:
            st.warning(f"No TDs found matching '{query}'.")

    st.divider()

    # ── Top committees / party split ─────────────────────────────
    left, right = st.columns(2)

    with left:
        st.subheader("Top committees")
        top = (
            df.groupby("committee")
            .agg(members=("name", "count"), committee_url=("committee_url", "first"))
            .sort_values("members", ascending=False)
            .head(15)
            .reset_index()
            .rename(columns={"committee": "Committee", "members": "Members", "committee_url": "Info"})
        )
        st.dataframe(
            top,
            hide_index=True,
            use_container_width=True,
            column_config={
                "Committee": st.column_config.TextColumn("Committee", width="large"),
                "Info": _link_column(),
                "Members": st.column_config.ProgressColumn(
                    "Members",
                    format="%d",
                    min_value=0,
                    max_value=int(top["Members"].max()) if len(top) else 1,
                ),
            },
        )

    with right:
        st.subheader("Politicians per party")
        party_counts = (
            activity.groupby("party")["name"].nunique()
            .sort_values(ascending=False)
            .rename_axis("Party").reset_index(name="Politicians")
        )
        st.dataframe(
            party_counts,
            hide_index=True,
            use_container_width=True,
            column_config={
                "Politicians": st.column_config.ProgressColumn(
                    "Politicians",
                    format="%d",
                    min_value=0,
                    max_value=int(party_counts["Politicians"].max()) if len(party_counts) else 1,
                ),
            },
        )

    # ── Party-dominated committees ───────────────────────────────
    st.subheader("⚖️ Committees dominated by one party")
    c1, c2 = st.columns(2)
    with c1:
        min_members = st.slider(
            "Minimum active members on committee",
            min_value=2, max_value=15, value=3,
            key="committees_dom_min",
        )
    with c2:
        threshold_pct = st.slider(
            "Dominance threshold (%)",
            min_value=40, max_value=100, value=60, step=5,
            key="committees_dom_threshold",
        )

    dominated = _party_dominance(df, min_members=min_members, threshold=threshold_pct / 100)
    if dominated.empty:
        st.info("No committees meet the current thresholds.")
    else:
        st.dataframe(
            dominated,
            hide_index=True,
            use_container_width=True,
            column_config={
                "Committee": st.column_config.TextColumn("Committee", width="large"),
                "Committee URL": _link_column(),
                "Dominant party": st.column_config.TextColumn("Dominant party"),
                "Share": st.column_config.ProgressColumn(
                    "Share", format="%.0f%%", min_value=0, max_value=1,
                ),
                "Members": st.column_config.NumberColumn("Party members"),
                "Total": st.column_config.NumberColumn("Total members"),
            },
        )
        _download_button(
            dominated,
            filename="party_dominated_committees.csv",
            key="committees_dom_export",
        )

    # ── Activity rankings ────────────────────────────────────────
    left, right = st.columns(2)

    with left:
        st.subheader("Most active politicians (by committee)")
        most_active = (
            activity.sort_values(["committees", "leadership"], ascending=False)
            .head(25)
            .reset_index(drop=True)
        )
        st.dataframe(
            most_active,
            hide_index=True,
            use_container_width=True,
            column_config={
                "name": st.column_config.TextColumn("Politician"),
                "party": st.column_config.TextColumn("Party"),
                "constituency": st.column_config.TextColumn("Constituency"),
                "committees": st.column_config.ProgressColumn(
                    "Committees",
                    format="%d",
                    min_value=0,
                    max_value=int(activity["committees"].max()) if len(activity) else 1,
                ),
                "active": st.column_config.NumberColumn("Active"),
                "leadership": st.column_config.NumberColumn("Leadership"),
            },
        )
        _download_button(
            most_active,
            filename="most_active_by_committee.csv",
            key="committees_most_active_export",
        )

    with right:
        st.subheader("Least active politicians (by committee)")
        st.caption("Zero means they sit on no committees at all.")
        least_active = (
            activity.sort_values(["committees", "leadership", "active"], ascending=[True, True, True])
            .head(25)
            .reset_index(drop=True)
        )
        st.dataframe(
            least_active,
            hide_index=True,
            use_container_width=True,
            column_config={
                "name": st.column_config.TextColumn("Politician"),
                "party": st.column_config.TextColumn("Party"),
                "constituency": st.column_config.TextColumn("Constituency"),
                "committees": st.column_config.ProgressColumn(
                    "Committees",
                    format="%d",
                    min_value=0,
                    max_value=int(activity["committees"].max()) if len(activity) else 1,
                ),
                "active": st.column_config.NumberColumn("Active"),
                "leadership": st.column_config.NumberColumn("Leadership"),
            },
        )
        _download_button(
            least_active,
            filename="least_active_by_committee.csv",
            key="committees_least_active_export",
        )


def _explorer_section(df: pd.DataFrame):
    with st.container(border=True):
        c1, c2 = st.columns([1, 1])
        with c1:
            status = st.segmented_control(
                "Status",
                options=["All", "Active", "Ended"],
                default="All",
                selection_mode="single",
                key="committees_status",
            )
            search = st.text_input(
                "Politician name",
                placeholder="Type to filter…",
                key="committees_search",
            )
        with c2:
            parties = st.multiselect(
                "Parties",
                sorted(df["party"].dropna().unique()),
                key="committees_parties",
            )
            committees = st.multiselect(
                "Committees",
                sorted(df["committee"].dropna().unique()),
                key="committees_committees",
            )

        if df["start"].notna().any():
            min_d = df["start"].min().date()
            max_d = df["start"].max().date()
            date_range = st.slider(
                "Membership start range",
                min_value=min_d,
                max_value=max_d,
                value=(min_d, max_d),
                format="YYYY-MM-DD",
                key="committees_daterange",
            )
        else:
            date_range = None

    filtered = df
    if status and status != "All":
        filtered = filtered[filtered["status"] == status]
    if parties:
        filtered = filtered[filtered["party"].isin(parties)]
    if committees:
        filtered = filtered[filtered["committee"].isin(committees)]
    if search:
        filtered = filtered[filtered["name"].str.contains(search, case=False, na=False)]
    if date_range:
        lo, hi = date_range
        filtered = filtered[filtered["start"].dt.date.between(lo, hi)]

    st.caption(f"{len(filtered):,} memberships match")

    view_cols = ["name", "party", "committee", "committee_url", "role", "status", "start", "end", "duration_days"]
    view = filtered[view_cols].sort_values("start", ascending=False, na_position="last")

    max_dur = int(filtered["duration_days"].max()) if len(filtered) else 1
    st.dataframe(
        view,
        use_container_width=True,
        hide_index=True,
        column_config={
            "name": st.column_config.TextColumn("Politician"),
            "party": st.column_config.TextColumn("Party"),
            "committee": st.column_config.TextColumn("Committee", width="large"),
            "committee_url": _link_column(),
            "role": st.column_config.TextColumn("Role"),
            "status": st.column_config.TextColumn("Status", width="small"),
            "start": st.column_config.DateColumn("Start", format="YYYY-MM-DD"),
            "end": st.column_config.DateColumn("End", format="YYYY-MM-DD"),
            "duration_days": st.column_config.ProgressColumn(
                "Days served",
                format="%d",
                min_value=0,
                max_value=max(max_dur, 1),
            ),
        },
    )

    _download_button(
        view,
        filename="committee_memberships_filtered.csv",
        key="committees_explorer_export",
    )


def _profile_section(df: pd.DataFrame):
    parties = sorted(df["party"].dropna().unique())
    chosen = st.pills(
        "Filter by party",
        ["All", *parties],
        default="All",
        selection_mode="single",
        key="committees_profile_party",
    )
    pool = df if chosen in (None, "All") else df[df["party"] == chosen]

    names = sorted(pool["name"].unique())
    if not names:
        st.info("No politicians match that party filter.")
        return

    selected = st.selectbox("Politician", names, key="committees_profile_name")
    person = df[df["name"] == selected].copy()

    party = person["party"].iloc[0]
    constituency = person["constituency"].iloc[0]
    constituency_str = constituency if pd.notna(constituency) else "Unknown constituency"

    st.subheader(selected)
    st.caption(f"{party} · {constituency_str}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Committees", person["committee"].nunique())
    c2.metric("Active", int((person["status"] == "Active").sum()))
    c3.metric("Ended", int((person["status"] == "Ended").sum()))
    c4.metric("Leadership roles", int(person["is_leadership"].sum()))

    st.divider()

    st.subheader("📅 Timeline")
    gantt = (
        alt.Chart(person)
        .mark_bar(cornerRadius=3)
        .encode(
            x=alt.X("start:T", title="Start"),
            x2="end_filled:T",
            y=alt.Y(
                "committee:N",
                sort=alt.SortField(field="start", order="ascending"),
                title=None,
            ),
            color=alt.Color(
                "status:N",
                scale=alt.Scale(
                    domain=["Active", "Ended", "Unknown"],
                    range=["#2ca02c", "#888888", "#cccccc"],
                ),
            ),
            tooltip=[
                alt.Tooltip("committee:N"),
                alt.Tooltip("role:N"),
                alt.Tooltip("status:N"),
                alt.Tooltip("start:T", format="%Y-%m-%d"),
                alt.Tooltip("end:T", format="%Y-%m-%d"),
                alt.Tooltip("duration_days:Q", title="Days"),
            ],
        )
        .properties(height=max(200, 35 * len(person)))
    )
    st.altair_chart(gantt, use_container_width=True)

    st.subheader("Memberships")
    person_view = person[["committee", "committee_url", "role", "status", "start", "end", "duration_days"]] \
        .sort_values("start", ascending=False, na_position="last")

    max_d = int(person["duration_days"].max()) if len(person) else 1
    st.dataframe(
        person_view,
        use_container_width=True,
        hide_index=True,
        column_config={
            "committee": st.column_config.TextColumn("Committee", width="large"),
            "committee_url": _link_column(),
            "role": st.column_config.TextColumn("Role"),
            "status": st.column_config.TextColumn("Status", width="small"),
            "start": st.column_config.DateColumn("Start", format="YYYY-MM-DD"),
            "end": st.column_config.DateColumn("End", format="YYYY-MM-DD"),
            "duration_days": st.column_config.ProgressColumn(
                "Days",
                format="%d",
                min_value=0,
                max_value=max(max_d, 1),
            ),
        },
    )

    safe_name = selected.replace(" ", "_").replace("/", "_")
    _download_button(
        person_view,
        filename=f"{safe_name}_committees.csv",
        key="committees_profile_export",
    )


# =====================================================================
# PAGE ENTRY POINT
# =====================================================================
def committees_page():
    st.title("🏛️ Committees")

    df, activity = _load_members(CSV_PATH)
    if df.empty:
        st.error(f"No committee data found in {CSV_PATH}")
        return

    section = st.segmented_control(
        "View",
        options=["Overview", "Explorer", "Profile"],
        default="Overview",
        selection_mode="single",
        key="committees_section",
    )

    st.divider()

    if section == "Overview":
        _overview_section(df, activity)
    elif section == "Explorer":
        _explorer_section(df)
    elif section == "Profile":
        _profile_section(df)


#CHATGPT
# import re
# import unicodedata
# from pathlib import Path

# import altair as alt
# import pandas as pd
# import streamlit as st


# CSV_PATH = Path(r"C:\Users\pglyn\PycharmProjects\dail_extractor\data\silver\flattened_members.csv")


# # =====================================================================
# # HELPERS
# # =====================================================================
# def _coalesce(*values):
#     for v in values:
#         if not pd.isna(v):
#             return v
#     return None


# def _committee_slug(name: str) -> str | None:
#     if pd.isna(name):
#         return None
#     s = str(name)
#     suffix = ""

#     chamber_patterns = [
#         (("Dáil Committee on ", "Dail Committee on "), "-dail"),
#         (("Seanad Committee on ",), "-seanad"),
#     ]
#     matched_chamber = False
#     for prefixes, suf in chamber_patterns:
#         for prefix in prefixes:
#             if s.startswith(prefix):
#                 s = s[len(prefix):]
#                 suffix = suf
#                 matched_chamber = True
#                 break
#         if matched_chamber:
#             break

#     if not matched_chamber:
#         for prefix in (
#             "Joint Committee on ",
#             "Select Committee on ",
#             "Committee on ",
#         ):
#             if s.startswith(prefix):
#                 s = s[len(prefix):]
#                 break

#     s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
#     s = s.lower()
#     s = re.sub(r"[^\w\s-]", "", s)
#     s = re.sub(r"\s+", "-", s.strip())

#     if not s:
#         return None
#     return s + suffix


# def _committee_url(name, dail_number) -> str | None:
#     if pd.isna(name) or pd.isna(dail_number):
#         return None
#     slug = _committee_slug(name)
#     if not slug:
#         return None
#     return f"https://www.oireachtas.ie/en/committees/{int(dail_number)}/{slug}/"


# # =====================================================================
# # DATA LOAD + TRANSFORM
# # =====================================================================
# @st.cache_data(show_spinner="Loading members…")
# def _load_members(path: Path):
#     df = pd.read_csv(path, na_values=["Null"])

#     committee_prefixes = sorted(
#         {m.group(1) for c in df.columns if (m := re.match(r"(committee_\d+)_", c))},
#         key=lambda p: int(p.split("_")[1]),
#     )

#     status_map = {"Live": "Active", "Dissolved": "Ended"}
#     records = []

#     for _, row in df.iterrows():
#         base = {
#             "name": _coalesce(row.get("full_name")) or "Unknown",
#             "party": _coalesce(row.get("party")) or "Unknown",
#             "constituency": _coalesce(row.get("constituency_name")),
#             "dail_number": _coalesce(row.get("dail_number")),
#         }

#         for prefix in committee_prefixes:
#             name = row.get(f"{prefix}_name_en")
#             if pd.isna(name):
#                 continue

#             status = status_map.get(row.get(f"{prefix}_main_status"), "Unknown")
#             role = _coalesce(
#                 row.get(f"{prefix}_role_title"),
#                 row.get(f"{prefix}_role"),
#             ) or "Member"

#             start = _coalesce(
#                 row.get(f"{prefix}_role_start_date"),
#                 row.get(f"{prefix}_member_start_date"),
#             )
#             end = _coalesce(
#                 row.get(f"{prefix}_role_end_date"),
#                 row.get(f"{prefix}_member_end_date"),
#             )

#             records.append({
#                 **base,
#                 "committee": name,
#                 "committee_url": _committee_url(name, base["dail_number"]),
#                 "role": role,
#                 "status": status,
#                 "type": row.get(f"{prefix}_type") or "Unknown",  # ✅ ADDED
#                 "start": start,
#                 "end": end,
#             })

#     df_long = pd.DataFrame(records)

#     all_members = (
#         df[["full_name", "party", "constituency_name"]]
#         .rename(columns={"full_name": "name", "constituency_name": "constituency"})
#         .drop_duplicates(subset=["name"])
#     )
#     all_members["party"] = all_members["party"].fillna("Unknown")

#     if df_long.empty:
#         stats = pd.DataFrame(columns=["name", "committees", "active", "leadership"])
#     else:
#         df_long["start"] = pd.to_datetime(df_long["start"], errors="coerce", utc=True).dt.tz_localize(None)
#         df_long["end"] = pd.to_datetime(df_long["end"], errors="coerce", utc=True).dt.tz_localize(None)
#         today = pd.Timestamp.today().normalize()
#         df_long["end_filled"] = df_long["end"].fillna(today)
#         df_long["duration_days"] = (df_long["end_filled"] - df_long["start"]).dt.days.clip(lower=0)
#         df_long["is_leadership"] = df_long["role"].ne("Member")

#         stats = (
#             df_long.groupby("name")
#             .agg(
#                 committees=("committee", "nunique"),
#                 active=("status", lambda s: int((s == "Active").sum())),
#                 leadership=("is_leadership", "sum"),
#             )
#             .reset_index()
#         )

#     activity = (
#         all_members.merge(stats, on="name", how="left")
#         .fillna({"committees": 0, "active": 0, "leadership": 0})
#         .astype({"committees": "int", "active": "int", "leadership": "int"})
#     )

#     return df_long, activity


# # =====================================================================
# # RENDERING HELPERS
# # =====================================================================
# def _download_button(df: pd.DataFrame, filename: str, key: str, label: str = "⬇️ Export current view as CSV"):
#     st.download_button(
#         label=label,
#         data=df.to_csv(index=False).encode("utf-8"),
#         file_name=filename,
#         mime="text/csv",
#         key=key,
#     )


# def _link_column():
#     return st.column_config.LinkColumn(
#         "Info",
#         display_text="Open ↗",
#     )


# # =====================================================================
# # OVERVIEW
# # =====================================================================
# def _overview_section(df: pd.DataFrame, activity: pd.DataFrame):
#     ongoing_committees = df.loc[df["status"] == "Active", "committee"].nunique()

#     st.caption(
#         f"{len(df):,} memberships · {activity['name'].nunique()} politicians · "
#         f"{df['committee'].nunique()} committees ({ongoing_committees} ongoing)"
#     )

#     c1, c2, c3, c4, c5, c6 = st.columns(6)  # ✅ expanded
#     c1.metric("Politicians", activity["name"].nunique())
#     c2.metric("Committees", df["committee"].nunique())
#     c3.metric("Ongoing", ongoing_committees)
#     c4.metric("Active memberships", int((df["status"] == "Active").sum()))
#     c5.metric("Leadership roles", int(df["is_leadership"].sum()))
#     c6.metric("Committee types", df["type"].nunique())  # ✅ added

#     st.divider()

#     st.subheader("🔎 Find a TD")

#     query = st.text_input("Search by name")

#     if query:
#         matches = df[df["name"].str.contains(query, case=False, na=False)]
#         st.dataframe(matches[[
#             "name", "party", "committee", "type", "committee_url", "role", "status"
#         ]], use_container_width=True)

#         _download_button(matches, f"{query}.csv", "search_export")


# # =====================================================================
# # EXPLORER
# # =====================================================================
# def _explorer_section(df: pd.DataFrame):
#     c1, c2, c3 = st.columns(3)

#     with c1:
#         status = st.selectbox("Status", ["All", "Active", "Ended"])
#     with c2:
#         parties = st.multiselect("Party", df["party"].unique())
#     with c3:
#         types = st.multiselect("Committee type", sorted(df["type"].unique()))  # ✅ added

#     filtered = df.copy()

#     if status != "All":
#         filtered = filtered[filtered["status"] == status]
#     if parties:
#         filtered = filtered[filtered["party"].isin(parties)]
#     if types:
#         filtered = filtered[filtered["type"].isin(types)]  # ✅ added

#     st.dataframe(filtered[[
#         "name", "party", "committee", "type", "committee_url", "role", "status"
#     ]], use_container_width=True)

#     _download_button(filtered, "explorer.csv", "explorer_export")


# # =====================================================================
# # ENTRY
# # =====================================================================
# def committees_page():
#     st.title("🏛️ Committees")

#     df, activity = _load_members(CSV_PATH)

#     section = st.radio("View", ["Overview", "Explorer"])

#     if section == "Overview":
#         _overview_section(df, activity)
#     else:
#         _explorer_section(df)