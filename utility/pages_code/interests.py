"""
Register of Members' Interests — Dáil Tracker.

Data source: silver CSV via in-memory DuckDB (registered view simulation).
All retrieval follows SELECT / WHERE / ORDER BY / LIMIT only — no aggregation in Streamlit.

TODO_PIPELINE_VIEW_REQUIRED: v_member_interests_index
    Member-level summary per year: member_name, party_name, constituency,
    declaration_count, categories_count, landlord_flag, property_flag.
    Needed to replace the flat declaration browse table with a clean member index.

TODO_PIPELINE_VIEW_REQUIRED: v_member_interests_detail
    Replace _load_interests() silver CSV simulation with a registered view.

TODO_PIPELINE_VIEW_REQUIRED: v_member_interests_yearly_summary
    Year + interest_category + declarations_count.
    Needed for the year-responsive category breakdown chart.

TODO_PIPELINE_VIEW_REQUIRED: v_member_interests_sources
    Per-declaration source PDF links: source_pdf_url, source_page_number, oireachtas_url.

TODO_PIPELINE_VIEW_REQUIRED: directorship_flag — derive from interest_category in pipeline
TODO_PIPELINE_VIEW_REQUIRED: shareholding_flag — derive from interest_category in pipeline
TODO_PIPELINE_VIEW_REQUIRED: source_pdf_url — add PDF URL to silver output
TODO_PIPELINE_VIEW_REQUIRED: member_id — stable Oireachtas API member URI
TODO_PIPELINE_VIEW_REQUIRED: mart_version, code_version, latest_fetch_timestamp_utc
    on v_member_interests_detail
"""
from __future__ import annotations

import datetime
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

from shared_css import inject_css
from ui.components import (
    empty_state,
    evidence_heading,
    interest_declaration_item,
    todo_callout,
)
from ui.export_controls import export_button
from ui.source_pdfs import interests_pdf_url, render_pdf_source_links

# ── Paths ──────────────────────────────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parents[2]
_CSV: dict[str, Path] = {
    "Dáil":   _ROOT / "data" / "silver" / "dail_member_interests_combined.csv",
    "Seanad": _ROOT / "data" / "silver" / "seanad_member_interests_combined.csv",
}
_RANKING_PARQUET = _ROOT / "data" / "gold" / "parquet" / "interests_member_ranking.parquet"

_REQUIRED_COLS: set[str] = {
    "member_name", "party_name", "constituency", "declaration_year",
    "interest_category", "interest_text", "landlord_flag", "property_flag",
}

_NOTABLE_DAIL: list[str] = [
    "Michael Healy-Rae",
    "Michael Lowry",
    "Robert Troy",
    "Mary Lou McDonald",
    "Micheál Martin",
    "Simon Harris",
]
_NOTABLE_SEANAD: list[str] = []

_CATEGORY_ORDER: list[str] = [
    "Occupations",
    "Directorships",
    "Remunerated Position",
    "Shares",
    "Land (including property)",
    "Contracts",
    "Gifts",
    "Travel Facilities",
    "Property supplied or lent or a Service supplied",
]

_CATEGORY_LABELS: dict[str, str] = {
    "Occupations":                                        "Occupations & Employment",
    "Directorships":                                      "Directorships & Company Roles",
    "Remunerated Position":                               "Remunerated Positions",
    "Shares":                                             "Shareholdings",
    "Land (including property)":                          "Land & Property",
    "Contracts":                                          "Contracts",
    "Gifts":                                              "Gifts Received",
    "Travel Facilities":                                  "Travel Facilities",
    "Property supplied or lent or a Service supplied":    "Property or Services Supplied",
}


# ── Data access — retrieval only (SELECT / WHERE / ORDER BY / LIMIT) ──────────

@st.cache_data(ttl=3600, show_spinner="Loading interests data…")
def _load_interests(house: str) -> pd.DataFrame:
    """
    Load silver CSV and normalise to v_member_interests contract column shape.
    TODO_PIPELINE_VIEW_REQUIRED: Replace with SELECT FROM v_member_interests_detail.
    """
    csv_path = _CSV.get(house)
    if csv_path is None or not csv_path.exists():
        return pd.DataFrame()
    df = pd.read_csv(csv_path, low_memory=False)
    df.columns = df.columns.str.strip()
    if "interest_category" in df.columns:
        df = df[df["interest_category"] != "15"].copy()
    for csv_col, contract_col in [
        ("is_landlord",     "landlord_flag"),
        ("is_property_owner", "property_flag"),
    ]:
        if csv_col in df.columns:
            df[contract_col] = df[csv_col].astype(str).str.lower() == "true"
    df = df.rename(columns={
        "full_name":                    "member_name",
        "party":                        "party_name",
        "constituency_name":            "constituency",
        "year_declared":                "declaration_year",
        "interest_description_cleaned": "interest_text",
    })
    for col, default in [
        ("directorship_flag",  False),   # TODO_PIPELINE_VIEW_REQUIRED
        ("shareholding_flag",  False),   # TODO_PIPELINE_VIEW_REQUIRED
        ("source_pdf_url",     None),    # TODO_PIPELINE_VIEW_REQUIRED
        ("source_page_number", None),    # TODO_PIPELINE_VIEW_REQUIRED
    ]:
        if col not in df.columns:
            df[col] = default
    if "declaration_year" in df.columns:
        df["declaration_year"] = pd.to_numeric(df["declaration_year"], errors="coerce")
    return df


@st.cache_data(ttl=300)
def _fetch_filter_options(house: str) -> dict[str, list]:
    """Retrieval SQL: SELECT DISTINCT <col> FROM v_member_interests ORDER BY <col>."""
    base = _load_interests(house)
    if base.empty:
        return {"years": [], "categories": [], "members": []}
    con = duckdb.connect(":memory:")
    con.register("v_member_interests", base)
    years = (
        con.execute(
            "SELECT DISTINCT TRY_CAST(declaration_year AS INTEGER) AS y "
            "FROM v_member_interests WHERE declaration_year IS NOT NULL ORDER BY y DESC"
        )
        .fetchdf()["y"]
        .dropna()
        .astype(int)
        .tolist()
    )
    categories = (
        con.execute(
            "SELECT DISTINCT interest_category FROM v_member_interests "
            "WHERE interest_category IS NOT NULL ORDER BY interest_category"
        )
        .fetchdf()["interest_category"]
        .tolist()
    )
    members = (
        con.execute(
            "SELECT DISTINCT member_name FROM v_member_interests "
            "WHERE member_name IS NOT NULL ORDER BY member_name"
        )
        .fetchdf()["member_name"]
        .tolist()
    )
    return {"years": years, "categories": categories, "members": members}


@st.cache_data(ttl=300)
def _fetch_interests(
    house: str,
    name_q: str,
    text_q: str,
    years: tuple[int, ...],
    categories: tuple[str, ...],
    landlord_only: bool,
    property_only: bool,
) -> pd.DataFrame:
    """
    Retrieval SQL: SELECT <cols> FROM v_member_interests
    WHERE <approved filters> ORDER BY declaration_year DESC, member_name LIMIT 1000.
    """
    base = _load_interests(house)
    if base.empty:
        return pd.DataFrame()
    con = duckdb.connect(":memory:")
    con.register("v_member_interests", base)
    where: list[str] = []
    params: list = []
    if name_q:
        where.append("member_name ILIKE ?")
        params.append(f"%{name_q}%")
    if text_q:
        where.append("interest_text ILIKE ?")
        params.append(f"%{text_q}%")
    if years:
        ph = ", ".join("?" for _ in years)
        where.append(f"TRY_CAST(declaration_year AS INTEGER) IN ({ph})")
        params.extend(int(y) for y in years)
    if categories:
        ph = ", ".join("?" for _ in categories)
        where.append(f"interest_category IN ({ph})")
        params.extend(categories)
    if landlord_only:
        where.append("landlord_flag = ?")
        params.append(True)
    if property_only:
        where.append("property_flag = ?")
        params.append(True)
    where_clause = "WHERE " + " AND ".join(where) if where else ""
    return con.execute(
        f"""
        SELECT member_name, party_name, constituency,
               TRY_CAST(declaration_year AS INTEGER) AS declaration_year,
               interest_category, interest_text, landlord_flag, property_flag
        FROM v_member_interests
        {where_clause}
        ORDER BY declaration_year DESC, member_name
        LIMIT 1000
        """,
        params,
    ).fetchdf()


@st.cache_data(ttl=300)
def _fetch_td_data(house: str, td_name: str) -> pd.DataFrame:
    """
    Retrieval SQL: SELECT <cols> FROM v_member_interests
    WHERE member_name = ? ORDER BY declaration_year DESC, interest_category.
    """
    base = _load_interests(house)
    if base.empty:
        return pd.DataFrame()
    con = duckdb.connect(":memory:")
    con.register("v_member_interests", base)
    return con.execute(
        """
        SELECT member_name, party_name, constituency,
               TRY_CAST(declaration_year AS INTEGER) AS declaration_year,
               interest_category, interest_text, landlord_flag, property_flag
        FROM v_member_interests
        WHERE member_name = ?
        ORDER BY declaration_year DESC, interest_category
        """,
        [td_name],
    ).fetchdf()


# ── Gold ranking (pre-aggregated pipeline output) ─────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def _load_ranking(house: str, year: int) -> pd.DataFrame:
    """
    Read pre-aggregated member ranking from gold parquet.
    One row per member for the given house + year — no aggregation in Streamlit.
    """
    if not _RANKING_PARQUET.exists():
        return pd.DataFrame()
    df = pd.read_parquet(_RANKING_PARQUET)
    return df[(df["house"] == house) & (df["declaration_year"] == year)].copy()


def _int_member_card_html(row) -> str:
    rank    = int(row["rank"])
    name    = str(row["member_name"])
    party   = str(row.get("party_name") or "")
    constit = str(row.get("constituency") or "")
    total   = int(row.get("total_declarations") or 0)
    d_count = int(row.get("directorship_count") or 0)
    p_count = int(row.get("property_count") or 0)
    s_count = int(row.get("share_count") or 0)
    landlord = bool(row.get("is_landlord", False))
    is_prop  = bool(row.get("is_property_owner", False))

    meta = " · ".join(p for p in [party, constit] if p and p.lower() not in ("nan", ""))
    rank_cls = "int-rank-num int-rank-num-top" if rank <= 3 else "int-rank-num"

    pills = f'<span class="int-stat-pill">{total} declarations</span>'
    if d_count:
        pills += f'<span class="int-stat-pill">🏢 {d_count} compan{"ies" if d_count != 1 else "y"}</span>'
    if p_count:
        pills += f'<span class="int-stat-pill">🏠 {p_count} propert{"ies" if p_count != 1 else "y"}</span>'
    if s_count:
        pills += f'<span class="int-stat-pill">📈 {s_count} share{"s" if s_count != 1 else ""}</span>'
    if landlord:
        pills += '<span class="int-stat-pill int-stat-pill-accent">🔑 Landlord</span>'
    elif is_prop:
        pills += '<span class="int-stat-pill">🏗️ Property owner</span>'

    return (
        f'<div class="int-member-card">'
        f'<div style="display:flex;align-items:flex-start;gap:0.75rem">'
        f'<span class="{rank_cls}" style="font-size:1.1rem;min-width:2rem;padding-top:0.1rem;text-align:right">#{rank}</span>'
        f'<div style="flex:1;min-width:0">'
        f'<p style="margin:0 0 0.1rem;font-family:\'Zilla Slab\',Georgia,serif;'
        f'font-size:1rem;font-weight:700;color:var(--text-primary)">{name}</p>'
        f'<p style="margin:0 0 0.3rem;font-size:0.8rem;color:var(--text-meta)">{meta}</p>'
        f'<div style="display:flex;flex-wrap:wrap;gap:0.3rem">{pills}</div>'
        f'</div></div></div>'
    )


def _render_leaderboard(ranking_df: pd.DataFrame) -> str | None:
    """Render ranked member cards (all members, paginated). Returns member_name if clicked."""
    if ranking_df.empty:
        empty_state(
            "No members found",
            "Adjust the name filter or choose a different year.",
        )
        return None

    total    = len(ranking_df)
    show_all = st.session_state.get("int_show_all", False)
    visible  = ranking_df if show_all else ranking_df.head(25)

    for i, (_, row) in enumerate(visible.iterrows()):
        card_col, btn_col = st.columns([14, 1])
        with card_col:
            st.markdown(_int_member_card_html(row), unsafe_allow_html=True)
        btn_col.markdown('<div class="dt-nav-anchor"></div>', unsafe_allow_html=True)
        if btn_col.button("→", key=f"int_mem_{i}", help=f"View {row['member_name']}'s declarations"):
            return str(row["member_name"])

    if not show_all and total > 25 and st.button(f"Show all {total:,} members", key="int_show_all_btn"):
        st.session_state["int_show_all"] = True
        st.rerun()

    return None


# ── Pure helpers ───────────────────────────────────────────────────────────────

def _real_descriptions(rows: pd.DataFrame) -> list[str]:
    """Return non-empty, non-boilerplate interest_text entries, deduplicated."""
    if rows.empty or "interest_text" not in rows.columns:
        return []
    seen: dict[str, None] = {}
    for d in rows["interest_text"].tolist():
        s = str(d).strip()
        if s and s.lower() not in ("no interests declared", "", "nan"):
            seen[s] = None
    return list(seen)


# ── Profile view ───────────────────────────────────────────────────────────────

def _render_profile(house: str, td_name: str) -> None:
    td_df = _fetch_td_data(house, td_name)
    if td_df.empty:
        empty_state(
            "No records found",
            f"No interest declarations found for {td_name}. Try a different name.",
        )
        return

    info    = td_df.iloc[0]
    party   = str(info.get("party_name",   "") or "")
    constit = str(info.get("constituency", "") or "")
    meta    = " · ".join(p for p in [party, constit] if p and p.lower() not in ("nan", ""))

    td_years = sorted(td_df["declaration_year"].dropna().astype(int).unique(), reverse=True)

    # ── Identity strip ─────────────────────────────────────────────────────────
    is_landlord = bool(td_df["landlord_flag"].any())
    is_property = bool(td_df["property_flag"].any())

    badges_html = ""
    if is_landlord:
        badges_html += (
            '<span class="dt-badge" style="border-color:#dc2626;color:#dc2626;">'
            "Landlord declared</span> "
        )
    if is_property and not is_landlord:
        badges_html += '<span class="dt-badge">Property interest</span> '

    st.markdown(
        f'<p class="td-name">{td_name}</p>'
        f'<p class="td-meta">{meta}</p>'
        + (f'<p style="margin:0.3rem 0 0.6rem;">{badges_html}</p>' if badges_html else ""),
        unsafe_allow_html=True,
    )

    # ── Year pills (profile-scoped key) ───────────────────────────────────────
    year_opts = [str(y) for y in td_years]
    selected_year_str: str | None = st.pills(
        "Year",
        options=year_opts,
        default=year_opts[0],
        key="int_profile_year",
        label_visibility="collapsed",
    )
    selected_year = int(selected_year_str) if selected_year_str else int(year_opts[0])

    year_df  = td_df[td_df["declaration_year"] == selected_year].copy()
    prior_year = selected_year - 1
    prior_df   = td_df[td_df["declaration_year"] == prior_year].copy()
    has_prior  = not prior_df.empty

    # ── Editorial callout ──────────────────────────────────────────────────────
    name_short = td_name.split()[-1]
    if year_df.empty:
        glance = f"No declarations recorded for {selected_year}."
    else:
        descs_all  = _real_descriptions(year_df)
        n_entries  = len(descs_all)
        n_cats     = len(year_df["interest_category"].dropna().unique())
        parts: list[str] = [
            f"In {selected_year}, {name_short} filed "
            f"<strong>{n_entries}</strong> declaration{'s' if n_entries != 1 else ''} "
            f"across <strong>{n_cats}</strong> "
            f"categor{'ies' if n_cats != 1 else 'y'}."
        ]
        if has_prior:
            prior_all   = set(_real_descriptions(prior_df))
            current_all = set(descs_all)
            n_new     = len(current_all - prior_all)
            n_removed = len(prior_all - current_all)
            if n_new:
                parts.append(f"<strong>{n_new} new</strong> since {prior_year}.")
            if n_removed:
                parts.append(f"<strong>{n_removed} removed</strong> since {prior_year}.")
        glance = " ".join(parts)

    st.markdown(
        f'<div class="dt-callout" style="margin:0.5rem 0 0.9rem;">'
        f'<p style="margin:0;font-size:0.95rem;line-height:1.65;">{glance}</p>'
        f"</div>",
        unsafe_allow_html=True,
    )

    # ── Diff toggle — prominent, above category sections ──────────────────────
    show_diff = False
    if has_prior:
        show_diff = st.toggle(
            f"Show changes since {prior_year}",
            value=True,
            key=f"int_diff_{td_name}_{selected_year}",
        )
    else:
        st.caption(f"No {prior_year} declarations on record — year-on-year comparison unavailable.")

    st.divider()

    pdf_url = interests_pdf_url(house, selected_year)
    if pdf_url:
        st.markdown(
            f'<div class="dt-provenance-box" style="margin-bottom:0.75rem">'
            f'<span style="font-size:0.68rem;font-weight:700;letter-spacing:0.08em;'
            f'text-transform:uppercase;color:var(--text-meta)">Source document</span><br>'
            f'<a class="leg-source-link" href="{pdf_url}" target="_blank" rel="noopener">'
            f'↗ Register of Members\' Interests · {house} · {selected_year} (Oireachtas.ie PDF)</a>'
            f'</div>',
            unsafe_allow_html=True,
        )

    evidence_heading(f"Declarations · {selected_year}")

    # ── Category sections — non-empty only ────────────────────────────────────
    cats_with_data: list[str] = []
    cats_empty: list[str] = []
    for cat in _CATEGORY_ORDER:
        cat_rows = year_df[year_df["interest_category"] == cat] if not year_df.empty else pd.DataFrame()
        if _real_descriptions(cat_rows):
            cats_with_data.append(cat)
        else:
            cats_empty.append(cat)

    # Any categories present in data but not in the standard order
    if not year_df.empty:
        for cat in year_df["interest_category"].dropna().unique():
            if cat not in _CATEGORY_ORDER and _real_descriptions(year_df[year_df["interest_category"] == cat]):
                cats_with_data.append(cat)

    if not cats_with_data:
        empty_state(
            f"Nothing declared for {selected_year}",
            "No interest declarations recorded for this member in this year.",
        )
    else:
        for cat in cats_with_data:
            cat_rows       = year_df[year_df["interest_category"] == cat]
            descs          = _real_descriptions(cat_rows)
            prior_cat_rows = prior_df[prior_df["interest_category"] == cat] if has_prior else pd.DataFrame()
            prior_cat_set  = set(_real_descriptions(prior_cat_rows))
            current_cat_set = set(descs)
            label = _CATEGORY_LABELS.get(cat, cat)

            st.markdown(
                f'<p class="int-category-section">{label}&nbsp;&nbsp;·&nbsp;&nbsp;{len(descs)}</p>',
                unsafe_allow_html=True,
            )

            if show_diff and has_prior:
                for d in descs:
                    interest_declaration_item(d, "new" if d not in prior_cat_set else "unchanged")
                for d in sorted(prior_cat_set - current_cat_set):
                    interest_declaration_item(d, "removed")
            else:
                for d in descs:
                    interest_declaration_item(d, "unchanged")

        # Categories that existed in prior year but have nothing in current year
        if show_diff and has_prior:
            for cat in _CATEGORY_ORDER:
                if cat in cats_with_data:
                    continue
                prior_cat_rows = prior_df[prior_df["interest_category"] == cat]
                prior_descs    = _real_descriptions(prior_cat_rows)
                if not prior_descs:
                    continue
                label = _CATEGORY_LABELS.get(cat, cat)
                st.markdown(
                    f'<p class="int-category-section">{label}&nbsp;&nbsp;·&nbsp;&nbsp;'
                    f'0 <span style="font-weight:400;text-transform:none;font-size:0.75rem;">'
                    f'(all removed)</span></p>',
                    unsafe_allow_html=True,
                )
                for d in sorted(prior_descs):
                    interest_declaration_item(d, "removed")

    # ── Empty categories — single collapsed summary ────────────────────────────
    if cats_empty:
        empty_labels = [_CATEGORY_LABELS.get(c, c) for c in cats_empty]
        with st.expander(
            f"Nothing declared · {len(cats_empty)} categories", expanded=False
        ):
            st.markdown(
                '<p class="int-empty-cats">' + " · ".join(empty_labels) + "</p>",
                unsafe_allow_html=True,
            )

    st.divider()

    # ── Source links (pipeline gap) ────────────────────────────────────────────
    todo_callout(
        "v_member_interests_sources — per-declaration source_pdf_url and oireachtas_url. "
        "Official declaration PDFs will link here once the pipeline exposes source URLs."
    )

    # ── Export ─────────────────────────────────────────────────────────────────
    today = datetime.date.today().isoformat()
    export_button(
        year_df,
        label=f"Export {td_name} · {selected_year} · {len(year_df)} rows",
        filename=f"dail_tracker_interests_{td_name.replace(' ', '_')}_{selected_year}_{today}.csv",
        key="int_td_export",
    )


# ── Provenance footer ──────────────────────────────────────────────────────────

def _render_provenance(house: str) -> None:
    csv_label = f"data/silver/{house.lower()}_member_interests_combined.csv"
    with st.expander("About & data provenance", expanded=False):
        st.markdown(
            "Declarations are extracted from published Oireachtas PDF documents. "
            "Flags (landlord, property) are pipeline navigation aids, not legal conclusions. "
            "Office holders (Ministers, Ceann Comhairle) may be exempt from filing — "
            "records can be incomplete. "
            "A high declaration count reflects transparency, not wrongdoing."
        )
        st.caption(f"Source: {csv_label}")
        st.caption(
            "TODO_PIPELINE_VIEW_REQUIRED: mart_version · code_version · "
            "latest_fetch_timestamp_utc on v_member_interests_detail"
        )


# ── Page entry point ───────────────────────────────────────────────────────────

def interests_page() -> None:
    if "selected_td" not in st.session_state:
        st.session_state["selected_td"] = None

    inject_css()

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown('<p class="page-kicker">Dáil Tracker</p>', unsafe_allow_html=True)
        st.markdown(
            '<p class="page-title">Register of<br>Members&rsquo; Interests</p>',
            unsafe_allow_html=True,
        )

        house: str = st.radio(
            "Chamber", ["Dáil", "Seanad"], horizontal=True, key="interests_house"
        )

        # Clear year pill and member selection on chamber switch
        if st.session_state.get("_interests_last_house") != house:
            for k in ("int_profile_year", "selected_td", "int_member_sel", "int_member_q"):
                st.session_state.pop(k, None)
            st.session_state["_interests_last_house"] = house

        opts = _fetch_filter_options(house)

        member_search: str = st.text_input(
            "", placeholder="Type a name…", key="int_member_q", label_visibility="collapsed"
        )
        sq = member_search.strip().lower()
        filtered_names = (
            [n for n in opts["members"] if sq in n.lower()] if sq else opts["members"]
        )
        chosen: str = st.selectbox(
            "Browse all members",
            options=["— browse all —"] + filtered_names,
            key="int_member_sel",
            label_visibility="collapsed",
        )
        if chosen and chosen != "— browse all —" and st.session_state.get("selected_td") != chosen:
            st.session_state["selected_td"] = chosen
            st.rerun()

        st.divider()
        st.markdown('<p class="sidebar-label">Notable members</p>', unsafe_allow_html=True)
        notable = _NOTABLE_DAIL if house == "Dáil" else _NOTABLE_SEANAD
        if notable:
            chip_cols = st.columns(2)
            for i, name in enumerate(notable):
                if chip_cols[i % 2].button(
                    name, key=f"chip_int_{name}", use_container_width=True
                ):
                    st.session_state["selected_td"] = name
                    st.rerun()

    # ── Guard ─────────────────────────────────────────────────────────────────
    csv_path = _CSV.get(house)
    if csv_path is None or not csv_path.exists():
        empty_state(
            "Register data not available",
            f"Source data for {house} not found. Run the pipeline to populate data/silver/.",
        )
        return

    base_df = _load_interests(house)
    if base_df.empty:
        empty_state(
            "No interests data available",
            "The source CSV loaded but contains no rows. Re-run the pipeline (member_interests.py).",
        )
        return

    missing = sorted(_REQUIRED_COLS - set(base_df.columns))
    if missing:
        empty_state(
            "View shape changed",
            f"Required columns missing: {', '.join(missing)}. Align pipeline output with contract.",
        )
        return

    # ── Page header ───────────────────────────────────────────────────────────
    st.markdown(
        '<p class="dt-kicker">Dáil Tracker &middot; Register of Interests</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<h1 style="margin:0.1rem 0 0.6rem;font-size:1.65rem;font-weight:700;">'
        "What has your TD declared?</h1>",
        unsafe_allow_html=True,
    )

    # ── Profile mode ──────────────────────────────────────────────────────────
    selected_td = st.session_state.get("selected_td")

    if selected_td:
        if st.button("← Back to register", key="int_back"):
            st.session_state["selected_td"] = None
            st.session_state.pop("int_member_sel", None)
            st.rerun()
        st.divider()
        _render_profile(house, selected_td)
        st.divider()
        _render_provenance(house)
        return

    # ── Browse mode ───────────────────────────────────────────────────────────

    # Year pills — main content, newest first
    year_opts = [str(y) for y in opts["years"]]
    if not year_opts:
        empty_state("No year data found", "v_member_interests_detail returned no years.")
        _render_provenance(house)
        return

    selected_year_str: str | None = st.pills(
        "Year",
        options=year_opts,
        default=year_opts[0],
        key="int_year",
        label_visibility="collapsed",
    )
    selected_year = int(selected_year_str) if selected_year_str else int(year_opts[0])

    # Inline command bar: name filter | flags
    cb1, cb2 = st.columns([5, 2])
    with cb1:
        name_q: str = st.text_input(
            "Filter by name",
            placeholder="Filter by name…",
            key="int_name_q",
            label_visibility="collapsed",
        )
    with cb2:
        landlord_only: bool = st.toggle("Landlords only", key="int_landlord")

    # ── Leaderboard — one card per member, ranked by declarations ─────────────
    ranking_df = _load_ranking(house, selected_year)

    if ranking_df.empty:
        if not _RANKING_PARQUET.exists():
            todo_callout(
                "interests_member_ranking.parquet not found. "
                "Run: python generate_interests_ranking.py"
            )
        else:
            empty_state(
                f"No data for {selected_year}",
                "No ranking data found for the selected year and chamber.",
            )
        _render_provenance(house)
        return

    # Apply filters (Python on already-aggregated rows — no GROUP BY)
    if name_q.strip():
        q = name_q.strip().lower()
        ranking_df = ranking_df[ranking_df["member_name"].str.lower().str.contains(q, na=False)]
    if landlord_only:
        ranking_df = ranking_df[ranking_df["is_landlord"]]
    ranking_df = ranking_df.sort_values("rank").reset_index(drop=True)

    n_members        = len(ranking_df)
    n_landlords      = int(ranking_df["is_landlord"].sum())
    n_prop_owners    = int(ranking_df["is_property_owner"].sum()) if "is_property_owner" in ranking_df.columns else 0
    n_properties     = int(ranking_df["property_count"].sum())
    n_companies      = int(ranking_df["directorship_count"].sum())

    pdf_url = interests_pdf_url(house, selected_year)
    if pdf_url:
        st.markdown(
            f'<div class="dt-provenance-box" style="margin-bottom:0.75rem">'
            f'<span style="font-size:0.68rem;font-weight:700;letter-spacing:0.08em;'
            f'text-transform:uppercase;color:var(--text-meta)">Source document</span><br>'
            f'<a class="leg-source-link" href="{pdf_url}" target="_blank" rel="noopener">'
            f'↗ Register of Members\' Interests · {house} · {selected_year} (Oireachtas.ie PDF)</a>'
            f'</div>',
            unsafe_allow_html=True,
        )

    evidence_heading(f"Interest Register · {selected_year}")
    st.caption(
        f"{n_members} members · {n_landlords} landlords declared · "
        f"{n_prop_owners} property owners · "
        f"{n_properties} properties · {n_companies} company directorships"
        + (" · filtered" if name_q.strip() or landlord_only else "")
    )
    st.markdown(
        '<div class="dt-callout" style="margin:0.5rem 0 0.9rem;">'
        "<strong>How to read this list</strong><br>"
        '<span style="color:var(--text-meta);font-size:0.88rem;line-height:1.6;">'
        "Ranked by total declarations filed in the register. "
        "Some Ministers and office-holders are legally exempt from certain disclosure requirements "
        "under the Ethics in Public Office Acts — gaps in the register do not mean no interests exist. "
        "A long list of declarations reflects a member's transparency, not wrongdoing."
        "</span></div>",
        unsafe_allow_html=True,
    )

    clicked_name = _render_leaderboard(ranking_df)
    if clicked_name:
        st.session_state["selected_td"] = clicked_name
        st.rerun()

    # Export uses the flat declaration table for the selected year (current displayed view)
    result_df = _fetch_interests(
        house=house,
        name_q=name_q.strip(),
        text_q="",
        years=(selected_year,),
        categories=(),
        landlord_only=landlord_only,
        property_only=False,
    )
    if not result_df.empty:
        today = datetime.date.today().isoformat()
        show_cols = [
            c for c in [
                "member_name", "declaration_year", "party_name", "constituency",
                "interest_category", "interest_text", "landlord_flag", "property_flag",
            ]
            if c in result_df.columns
        ]
        export_button(
            result_df[show_cols],
            label=f"Export all declarations · {selected_year} · {len(result_df):,} rows",
            filename=f"dail_tracker_interests_{house.lower()}_{selected_year}_{today}.csv",
            key="int_browse_export",
        )

    # Category breakdown — pipeline gap (collapsed)
    with st.expander("Declarations by category", expanded=False):
        todo_callout(
            "v_member_interests_yearly_summary — year-responsive category breakdown "
            "(interest_category, declarations_count, year). When available this will render "
            "an Altair horizontal bar chart for the selected year."
        )

    _render_provenance(house)


if __name__ == "__main__":
    st.set_page_config(page_title="Interests · Dáil Tracker", layout="wide")
    interests_page()
