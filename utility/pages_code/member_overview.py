"""
Member Overview — single-politician public accountability record.

Stage 1: Browse all TDs (identity columns; mart pending)
Stage 2: Full accountability profile — Attendance, Votes, Payments, Lobbying, Legislation

Entry points: row click, sidebar selectbox, ?member=join_key URL param

TODO_PIPELINE_VIEW_REQUIRED: v_member_overview_browse
  Pending columns: attendance_rate, payment_total_eur, declared_interests_count,
  lobbying_interactions_count, revolving_door_flag, government_status

TODO_PIPELINE_VIEW_REQUIRED: sponsor_join_key on v_legislation_index
TODO_PIPELINE_VIEW_REQUIRED: unique_member_code on v_lobbying_revolving_door
TODO_PIPELINE_VIEW_REQUIRED: per-member lobbying view with unique_member_code filter
"""
from __future__ import annotations

import logging
import re
import unicodedata
from html import escape as _h
from pathlib import Path
import sys

import pandas as pd
import streamlit as st

_UTIL = Path(__file__).resolve().parent.parent
if str(_UTIL) not in sys.path:
    sys.path.insert(0, str(_UTIL))

from shared_css import inject_css
from ui.components import clean_meta, empty_state, member_card_html, sidebar_page_header
from data_access.member_overview_data import get_member_overview_conn

_log = logging.getLogger(__name__)
_STAGE_KEY = "mo_join_key"

_POLICY_AREAS: list[tuple[str, str]] = [
    ("Housing",        "housing"),
    ("Health",         "health"),
    ("Education",      "education"),
    ("Defence",        "defence"),
    ("Europe",         "europe"),
    ("Crime",          "crime"),
    ("Environment",    "environment"),
    ("Social Welfare", "social welfare"),
    ("Finance",        "finance"),
    ("Agriculture",    "agriculture"),
    ("Transport",      "transport"),
    ("Immigration",    "immigration"),
]
_AREA_LABELS:      list[str]      = [lbl for lbl, _ in _POLICY_AREAS]
_AREA_LABEL_TO_KW: dict[str, str] = {lbl: kw for lbl, kw in _POLICY_AREAS}


# ── Name normalisation (mirrors normalise_join_key.py pipeline logic) ──────────

def _norm_name(name: str) -> str:
    """Sorted-character key identical to the pipeline's normalise_df_td_name()."""
    name = name.lower()
    name = unicodedata.normalize("NFD", name)
    name = re.sub(r"[̀-ͯ]", "", name)
    name = re.sub(r"[\x27‘’ʼʹ`´＇]", "", name)
    name = re.sub(r"[^a-z\s]", "", name)
    name = re.sub(r"^\s*(dr|prof|rev|fr|sr|mr|mrs|ms|miss|br)\s+", "", name)
    name = re.sub(r"\s+", "", name)
    return "".join(sorted(name))


# ── Data retrieval ─────────────────────────────────────────────────────────────

def _q(conn, sql: str, params: list | None = None) -> pd.DataFrame:
    if conn is None:
        return pd.DataFrame()
    try:
        return conn.execute(sql, params or []).df()
    except Exception as exc:
        _log.warning("member_overview | %s | %s", sql[:80], exc)
        return pd.DataFrame()


@st.cache_data(ttl=300)
def _member_list(_conn) -> pd.DataFrame:
    return _q(
        _conn,
        "SELECT DISTINCT member_name, unique_member_code, party_name, constituency"
        " FROM v_attendance_member_year_summary ORDER BY member_name LIMIT 2000",
    )


@st.cache_data(ttl=300)
def _join_key_by_name(_conn, name: str) -> str | None:
    df = _q(
        _conn,
        "SELECT DISTINCT unique_member_code FROM v_attendance_member_year_summary"
        " WHERE member_name = ? LIMIT 1",
        [name],
    )
    return str(df.iloc[0]["unique_member_code"]) if not df.empty else None


@st.cache_data(ttl=300)
def _identity(_conn, join_key: str) -> dict:
    df = _q(
        _conn,
        "SELECT member_name, party_name, constituency, is_minister, year"
        " FROM v_attendance_member_year_summary"
        " WHERE unique_member_code = ? ORDER BY year DESC LIMIT 1",
        [join_key],
    )
    return df.iloc[0].to_dict() if not df.empty else {}


@st.cache_data(ttl=300)
def _att_all_years(_conn, join_key: str) -> pd.DataFrame:
    return _q(
        _conn,
        "SELECT year, attended_count, is_minister"
        " FROM v_attendance_member_year_summary"
        " WHERE unique_member_code = ? ORDER BY year DESC LIMIT 20",
        [join_key],
    )


@st.cache_data(ttl=300)
def _votes_summary(_conn, join_key: str) -> pd.DataFrame:
    return _q(
        _conn,
        "SELECT yes_count, no_count, abstained_count, division_count, yes_rate_pct"
        " FROM td_vote_summary WHERE member_id = ? LIMIT 1",
        [join_key],
    )


@st.cache_data(ttl=300)
def _votes_by_topic(_conn, join_key: str, keyword: str) -> pd.DataFrame:
    pattern = f"%{keyword}%"
    return _q(
        _conn,
        "SELECT vote_date, debate_title, vote_type, vote_outcome, oireachtas_url"
        " FROM v_vote_member_detail"
        " WHERE member_id = ? AND LOWER(debate_title) LIKE LOWER(?)"
        " ORDER BY vote_date DESC LIMIT 200",
        [join_key, pattern],
    )


@st.cache_data(ttl=300)
def _pay_overview(_conn, join_key: str) -> pd.DataFrame:
    return _q(
        _conn,
        "SELECT payment_year, total_paid, taa_band_label, payment_count"
        " FROM v_payments_yearly_evolution"
        " WHERE unique_member_code = ? ORDER BY payment_year DESC LIMIT 20",
        [join_key],
    )


@st.cache_data(ttl=300)
def _pay_grand_total(_conn, join_key: str) -> float:
    # SUM permitted as presentation-layer scalar — contract §headline_metrics_row note
    df = _q(
        _conn,
        "SELECT SUM(amount_num) AS total FROM v_payments_member_detail"
        " WHERE unique_member_code = ?",
        [join_key],
    )
    if df.empty or pd.isna(df.iloc[0]["total"]):
        return 0.0
    return float(df.iloc[0]["total"])


@st.cache_data(ttl=300)
def _lobbying_rd(_conn, member_name: str) -> pd.DataFrame:
    # Approximate name match — TODO_PIPELINE_VIEW_REQUIRED: unique_member_code on this view
    return _q(
        _conn,
        "SELECT individual_name, former_position, return_count, distinct_firms"
        " FROM v_lobbying_revolving_door WHERE individual_name = ? LIMIT 5",
        [member_name],
    )


@st.cache_data(ttl=300)
def _legislation(_conn, member_name: str) -> pd.DataFrame:
    # ILIKE on last name — TODO_PIPELINE_VIEW_REQUIRED: sponsor_join_key on v_legislation_index
    last = member_name.strip().split()[-1]
    return _q(
        _conn,
        "SELECT bill_title, bill_status, bill_year, oireachtas_url"
        " FROM v_legislation_index"
        " WHERE LOWER(sponsor) LIKE LOWER(?)"
        " ORDER BY introduced_date DESC NULLS LAST LIMIT 50",
        [f"%{last}%"],
    )


# ── Domain tab renderers ───────────────────────────────────────────────────────

def _tab_attendance(conn, join_key: str) -> None:
    df = _att_all_years(conn, join_key)
    if df.empty:
        empty_state(
            "No attendance records",
            "No rows in v_attendance_member_year_summary for this member.",
        )
        return

    # Bold headline: most recent year
    row0      = df.iloc[0]
    latest_yr = int(row0["year"])
    attended  = int(row0["attended_count"])
    is_min    = str(row0.get("is_minister", "false")).lower() == "true"

    st.html(
        f'<p style="font-family:\'Epilogue\',sans-serif;font-size:1.05rem;'
        f'font-weight:400;color:var(--text-secondary);margin:0 0 1rem;">'
        f'Recorded <span style="font-family:\'Zilla Slab\',Georgia,serif;'
        f'font-size:1.75rem;font-weight:700;color:var(--text-primary);">'
        f'{attended}</span> sitting days in {latest_yr}.'
        + (" Ministerial duties are not captured in plenary attendance records." if is_min else "")
        + "</p>"
    )

    if is_min:
        st.info(
            "This member held ministerial office. Plenary PDFs do not record ministerial "
            "attendance — figures show Dáil chamber presence only.",
            icon=":material/info:",
        )

    display = df.copy()
    display["year"] = display["year"].astype(str)
    display["is_minister"] = display["is_minister"].apply(
        lambda v: "Yes" if str(v).lower() == "true" else ""
    )
    st.dataframe(
        display.rename(columns={
            "year":          "Year",
            "attended_count":"Days Recorded",
            "is_minister":   "Minister",
        }),
        use_container_width=True,
        hide_index=True,
    )
    st.caption(
        "Plenary sittings only — does not include committee hearings, ministerial duties, "
        "illness, or other legitimate absences."
    )
    st.download_button(
        label="Export attendance (CSV)",
        data=display.to_csv(index=False).encode("utf-8"),
        file_name=f"attendance_{join_key}.csv",
        mime="text/csv",
        key="mo_att_export",
        width="stretch",
    )


def _tab_votes(conn, join_key: str) -> None:
    summary = _votes_summary(conn, join_key)

    if not summary.empty:
        r         = summary.iloc[0]
        yes       = int(r.get("yes_count",       0) or 0)
        no        = int(r.get("no_count",        0) or 0)
        ab        = int(r.get("abstained_count", 0) or 0)
        div       = int(r.get("division_count",  0) or 0)
        rate_pct  = float(r.get("yes_rate_pct",  0) or 0)
        cast      = yes + no + ab
        voted_pct = round(100.0 * cast / div, 1) if div else 0.0

        st.html(
            f'<p style="font-family:\'Epilogue\',sans-serif;font-size:1.05rem;'
            f'color:var(--text-secondary);margin:0 0 0.5rem;">'
            f'Voted in <strong>{voted_pct}%</strong> of divisions recorded. '
            f'<strong>{rate_pct}%</strong> of votes cast were Aye.</p>'
        )
        c1, c2, c3 = st.columns(3)
        c1.metric("Aye",       yes)
        c2.metric("Níl",       no)
        c3.metric("Abstained", ab)
    else:
        empty_state(
            "No aggregate vote data",
            "No row in td_vote_summary for this member.",
        )

    st.divider()

    st.html('<p class="section-heading">Voting record by policy area</p>')

    selected_area = st.pills(
        "Policy area",
        options=_AREA_LABELS,
        default="Housing",
        key="mo_vote_area",
        label_visibility="collapsed",
    )

    keyword  = _AREA_LABEL_TO_KW.get(selected_area or "Housing", "housing")
    topic_df = _votes_by_topic(conn, join_key, keyword)

    if topic_df.empty:
        empty_state(
            f"No votes on {selected_area}",
            "No divisions with this topic found in the debate title.",
        )
    else:
        st.caption(
            f"{len(topic_df)} division{'s' if len(topic_df) != 1 else ''} on {selected_area}"
        )
        for _, row in topic_df.iterrows():
            date    = str(row.get("vote_date",    ""))[:10]
            title   = str(row.get("debate_title", "—"))
            vtype   = str(row.get("vote_type",    "—"))
            outcome = str(row.get("vote_outcome", "—"))
            url     = str(row.get("oireachtas_url", "") or "")

            vl = vtype.lower()
            if "yes" in vl:
                vote_colour, vote_bg = "#1d4ed8", "#dbeafe"
            elif "no" in vl:
                vote_colour, vote_bg = "#92400e", "#fef3c7"
            else:
                vote_colour, vote_bg = "#6b7280", "#f3f4f6"

            url_html = (
                f' <a href="{_h(url)}" target="_blank" '
                f'style="font-size:0.78rem;color:var(--accent);text-decoration:none;">'
                f'Oireachtas →</a>'
                if url and url not in ("nan", "None", "")
                else ""
            )
            st.html(
                f'<div style="padding:0.55rem 0.9rem;margin-bottom:0.25rem;'
                f'border:1px solid var(--border);border-left:3px solid {vote_colour};'
                f'border-radius:8px;background:#ffffff;">'
                f'<div style="display:flex;align-items:center;gap:0.55rem;margin-bottom:0.2rem;">'
                f'<span style="font-size:0.73rem;color:var(--text-meta);">{_h(date)}</span>'
                f'<span style="padding:0.1rem 0.5rem;background:{vote_bg};color:{vote_colour};'
                f'border-radius:4px;font-size:0.73rem;font-weight:700;">{_h(vtype)}</span>'
                f'<span style="font-size:0.73rem;color:var(--text-meta);">{_h(outcome)}</span>'
                f'{url_html}'
                f'</div>'
                f'<div style="font-family:\'Zilla Slab\',Georgia,serif;font-size:0.94rem;'
                f'font-weight:600;color:var(--text-primary);line-height:1.35;">'
                f'{_h(title)}</div>'
                f'</div>'
            )


def _tab_payments(conn, join_key: str) -> None:
    grand_total = _pay_grand_total(conn, join_key)
    df          = _pay_overview(conn, join_key)

    if df.empty:
        empty_state(
            "No payment records",
            "No rows in v_payments_yearly_evolution for this member's join_key.",
        )
        return

    st.html(
        f'<p style="font-family:\'Epilogue\',sans-serif;font-size:1.05rem;'
        f'color:var(--text-secondary);margin:0 0 1rem;">'
        f'<span style="font-family:\'Zilla Slab\',Georgia,serif;'
        f'font-size:1.75rem;font-weight:700;color:var(--text-primary);">'
        f'€{grand_total:,.0f}</span> received in parliamentary allowances across all recorded years.</p>'
    )

    display = df.copy()
    display["payment_year"] = display["payment_year"].astype(str)
    if "total_paid" in display.columns:
        display["total_paid"] = display["total_paid"].round(2)

    st.dataframe(
        display.rename(columns={
            "payment_year":   "Year",
            "total_paid":     "Total (€)",
            "taa_band_label": "TAA Band",
            "payment_count":  "Payments",
        }),
        column_config={
            "Total (€)": st.column_config.NumberColumn(format="€{:,.0f}"),
        },
        use_container_width=True,
        hide_index=True,
    )
    st.caption(
        "Parliamentary Standard Allowance (PSA) payments from the Houses of the Oireachtas. "
        "TAA band reflects distance from Leinster House."
    )
    st.download_button(
        label="Export payments (CSV)",
        data=display.to_csv(index=False).encode("utf-8"),
        file_name=f"payments_{join_key}.csv",
        mime="text/csv",
        key="mo_pay_export",
        width="stretch",
    )


def _tab_lobbying(conn, join_key: str, member_name: str) -> None:
    rd_df = _lobbying_rd(conn, member_name)

    if not rd_df.empty:
        row   = rd_df.iloc[0]
        rc    = int(row.get("return_count",   0) or 0)
        firms = int(row.get("distinct_firms", 0) or 0)
        pos   = str(row.get("former_position", "")).strip()
        pos_line = f"Former position: <strong>{_h(pos)}</strong>. " if pos else ""

        st.badge("Revolving door", icon=":material/warning:", color="orange")
        st.html(
            f'<div class="lob-revolving-callout">'
            f'<div class="lob-revolving-heading">Revolving door flag</div>'
            f'<p style="margin:0;font-size:0.88rem;color:var(--text-secondary);">'
            f'{pos_line}'
            f'Appears on <strong>{rc}</strong> lobbying return{"s" if rc != 1 else ""} '
            f'across <strong>{firms}</strong> distinct firm{"s" if firms != 1 else ""}. '
            f'Matched by display name — approximate.</p>'
            f'</div>'
        )
    else:
        st.html(
            '<div class="dt-callout">No revolving door flag found for this member '
            '(name-based lookup — approximate).</div>'
        )

    st.html(
        '<div class="leg-todo-callout" style="margin-top:1rem;">'
        '<span class="leg-todo-label">TODO PIPELINE VIEW REQUIRED</span>'
        ' Per-member lobbying contact table requires a view with'
        ' <code>unique_member_code</code> filter.'
        '</div>'
    )


def _tab_legislation(conn, member_name: str) -> None:
    st.html(
        '<div class="leg-todo-callout">'
        '<span class="leg-todo-label">TODO PIPELINE VIEW REQUIRED</span>'
        ' <code>sponsor_join_key</code> missing from <code>v_legislation_index</code>. '
        'Bills below matched by last name — may include false positives.</div>'
    )

    df = _legislation(conn, member_name)
    if df.empty:
        empty_state(
            "No bills found",
            f"No bills matching '{member_name.split()[-1]}' as sponsor in v_legislation_index.",
        )
        return

    n = len(df)
    st.html(
        f'<p style="font-family:\'Epilogue\',sans-serif;font-size:1.0rem;'
        f'color:var(--text-secondary);margin:0 0 0.85rem;">'
        f'<strong>{n}</strong> bill{"s" if n != 1 else ""} matched by name.</p>'
    )

    for _, row in df.iterrows():
        title  = str(row.get("bill_title",  "—"))
        status = str(row.get("bill_status", "—"))
        year   = str(row.get("bill_year",   "—"))
        url    = str(row.get("oireachtas_url", "") or "")

        sl = status.lower()
        status_css = (
            "leg-status-enacted" if ("enact" in sl or "sign" in sl)
            else "leg-status-lapsed" if sl in ("lapsed", "withdrawn", "defeated")
            else "leg-status-active"
        )
        url_html = (
            f'<a class="leg-bill-oireachtas-link" href="{_h(url)}" target="_blank">'
            f'Oireachtas.ie →</a>'
            if url and url not in ("nan", "None", "")
            else ""
        )
        st.html(
            f'<div class="leg-bill-card" style="margin-bottom:0.3rem;">'
            f'<div class="leg-bill-card-header">'
            f'<span class="leg-bill-card-date">{_h(year)}</span>'
            f'<span class="signal {status_css}">{_h(status)}</span>'
            f'</div>'
            f'<div class="leg-bill-card-title">{_h(title)}</div>'
            f'<div style="margin-top:0.2rem;">{url_html}</div>'
            f'</div>'
        )

    st.download_button(
        label="Export legislation (CSV)",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=f"legislation_{member_name.replace(' ', '_')}.csv",
        mime="text/csv",
        disabled=df.empty,
        key="mo_leg_export",
        width="stretch",
    )


def _party_pill_options(df: pd.DataFrame) -> list[str]:
    if df.empty or "party_name" not in df.columns:
        return []
    parties = df["party_name"].dropna().astype(str).unique().tolist()
    parties = [p for p in parties if p and p.lower() != "nan"]
    counts  = df["party_name"].value_counts()
    return sorted(parties, key=lambda p: (-int(counts.get(p, 0)), p))


def _render_browse(conn) -> None:
    df = _member_list(conn)

    st.html(
        '<div class="dt-hero">'
        '<p class="dt-kicker">MEMBER OVERVIEW</p>'
        '<h1 style="margin:0.1rem 0 0.25rem;font-size:1.85rem;font-weight:700;'
        'font-family:\'Zilla Slab\',Georgia,serif;">Browse all TDs</h1>'
        '<p class="dt-dek">Pick a TD to open their accountability profile — '
        'attendance, votes by policy area, payments, lobbying, and legislation.</p>'
        '</div>'
    )

    if df.empty:
        empty_state("No member data", "Run the pipeline to generate attendance parquet files.")
        return

    df = df.drop_duplicates(subset=["unique_member_code"], keep="first").reset_index(drop=True)

    search = st.text_input(
        "Search TDs",
        placeholder="Search by name, party or constituency…",
        key="mo_browse_search",
        label_visibility="collapsed",
    )

    party_options = _party_pill_options(df)
    selected_party = st.pills(
        "Party",
        options=["All parties"] + party_options,
        default="All parties",
        key="mo_browse_party",
        label_visibility="collapsed",
    )

    sq = (search or "").strip().lower()
    filtered = df.copy()
    if selected_party and selected_party != "All parties":
        filtered = filtered[filtered["party_name"] == selected_party]
    if sq:
        mask = (
            filtered["member_name"].astype(str).str.lower().str.contains(sq, na=False)
            | filtered["party_name"].astype(str).str.lower().str.contains(sq, na=False)
            | filtered["constituency"].astype(str).str.lower().str.contains(sq, na=False)
        )
        filtered = filtered[mask]

    filtered = filtered.sort_values("member_name", kind="stable").reset_index(drop=True)

    total   = len(df)
    showing = len(filtered)
    st.caption(f"{showing} of {total} TDs")

    if filtered.empty:
        empty_state(
            "No TDs match your filters",
            "Try clearing the search box or choosing a different party.",
        )
        return

    PAGE_SIZE = 60
    show_all  = bool(st.session_state.get("mo_browse_show_all", False))
    visible   = filtered if show_all else filtered.head(PAGE_SIZE)

    left_col, right_col = st.columns(2, gap="small")
    columns = [left_col, right_col]

    for i, (_, row) in enumerate(visible.iterrows()):
        target = columns[i % 2]
        with target:
            card_col, btn_col = st.columns([14, 1])
            name    = str(row.get("member_name", ""))
            party   = str(row.get("party_name", "") or "")
            constit = str(row.get("constituency", "") or "")
            meta    = clean_meta(party, constit)
            card_col.html(member_card_html(name=name, meta=meta))
            btn_col.html('<div class="dt-nav-anchor"></div>')
            if btn_col.button("→", key=f"mo_browse_{i}", help=f"View {name}"):
                st.session_state[_STAGE_KEY] = str(row["unique_member_code"])
                st.query_params["member"]    = str(row["unique_member_code"])
                st.rerun()

    if not show_all and showing > PAGE_SIZE:
        if st.button(f"Show all {showing} TDs", key="mo_browse_show_more", width="stretch"):
            st.session_state["mo_browse_show_all"] = True
            st.rerun()


# ── Stage 2 — Profile ──────────────────────────────────────────────────────────

def _render_stage2(conn, join_key: str) -> None:

    if st.button("← All TDs", key="mo_back_all", help="Return to the full TD list"):
        st.session_state.pop(_STAGE_KEY, None)
        st.session_state.pop("mo_browse_show_all", None)
        st.query_params.clear()
        st.rerun()

    identity = _identity(conn, join_key)
    if not identity:
        st.error("Member not found — join_key has no match in v_attendance_member_year_summary.")
        return

    member_name  = str(identity.get("member_name",  ""))
    party        = str(identity.get("party_name",   ""))
    constituency = str(identity.get("constituency", ""))
    is_minister  = str(identity.get("is_minister", "false")).lower() == "true"
    meta         = clean_meta(party, constituency)

    role_html = (
        '<span style="display:inline-flex;align-items:center;padding:0.15rem 0.55rem;'
        'background:#dbeafe;border:1px solid #93c5fd;color:#1e40af;border-radius:2px;'
        'font-size:0.78rem;font-weight:600;font-family:\'Epilogue\',sans-serif;">Minister</span>'
        if is_minister else
        '<span style="display:inline-flex;align-items:center;padding:0.15rem 0.55rem;'
        'background:#fef3c7;border:1px solid #fcd34d;color:#92400e;border-radius:2px;'
        'font-size:0.78rem;font-weight:600;font-family:\'Epilogue\',sans-serif;">TD</span>'
    )

    rd_df   = _lobbying_rd(conn, member_name)
    rd_html = (
        '<span style="display:inline-flex;align-items:center;gap:0.25rem;'
        'padding:0.15rem 0.55rem;background:#fffbeb;border:1px solid #fcd34d;'
        'color:#92400e;border-radius:2px;font-size:0.78rem;font-weight:600;'
        'font-family:\'Epilogue\',sans-serif;margin-left:0.35rem;">'
        '⚠ Revolving door</span>'
        if not rd_df.empty else ""
    )

    st.html(
        f'<div class="dt-hero">'
        f'<p class="dt-kicker">TD ACCOUNTABILITY RECORD</p>'
        f'<h1 class="td-name" style="margin:0.15rem 0 0.2rem;">{_h(member_name)}</h1>'
        f'<p class="td-meta" style="margin:0 0 0.55rem;">{_h(meta)}</p>'
        f'<div style="display:flex;flex-wrap:wrap;gap:0.3rem;">{role_html}{rd_html}</div>'
        f'</div>'
    )

    # Quick stats
    att_df    = _att_all_years(conn, join_key)
    att_stat  = f"{int(att_df.iloc[0]['attended_count'])} days ({int(att_df.iloc[0]['year'])})" if not att_df.empty else "—"
    pay_total = _pay_grand_total(conn, join_key)
    pay_stat  = f"€{pay_total:,.0f}" if pay_total else "—"
    vote_df   = _votes_summary(conn, join_key)
    vote_stat = str(int(vote_df.iloc[0]["division_count"])) if not vote_df.empty else "—"

    c1, c2, c3 = st.columns(3)
    c1.metric("Attendance (latest)", att_stat)
    c2.metric("Payments (total)",    pay_stat)
    c3.metric("Divisions Voted",     vote_stat)

    st.divider()

    tabs = st.tabs([
        ":material/calendar_today: Attendance",
        ":material/how_to_vote: Votes",
        ":material/payments: Payments",
        ":material/groups: Lobbying",
        ":material/gavel: Legislation",
    ])
    with tabs[0]: _tab_attendance(conn, join_key)
    with tabs[1]: _tab_votes(conn, join_key)
    with tabs[2]: _tab_payments(conn, join_key)
    with tabs[3]: _tab_lobbying(conn, join_key, member_name)
    with tabs[4]: _tab_legislation(conn, member_name)


# ── Main entry point ───────────────────────────────────────────────────────────

def member_overview_page() -> None:
    inject_css()
    conn = get_member_overview_conn()

    url_jk = st.query_params.get("member")
    if url_jk:
        st.session_state[_STAGE_KEY] = url_jk

    join_key = st.session_state.get(_STAGE_KEY)

    with st.sidebar:
        sidebar_page_header("Member<br>Overview", "OIREACHTAS EXPLORER")

        df_all       = _member_list(conn)
        member_names = df_all["member_name"].tolist() if not df_all.empty else []

        st.html('<p class="sidebar-label">Find a member</p>')
        search = st.text_input(
            "Search", placeholder="Type a name…",
            key="mo_sidebar_search", label_visibility="collapsed",
        )
        sq             = search.strip().lower()
        filtered_names = [n for n in member_names if sq in n.lower()] if sq else member_names
        chosen         = st.selectbox(
            "Select member", ["— select —"] + filtered_names,
            key="mo_sidebar_select", label_visibility="collapsed",
        )
        if chosen and chosen != "— select —":
            jk = _join_key_by_name(conn, chosen)
            if jk and jk != join_key:
                st.session_state[_STAGE_KEY] = jk
                st.query_params["member"]    = jk
                st.rerun()

    if join_key:
        _render_stage2(conn, join_key)
    else:
        _render_browse(conn)
