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
from html import escape as _h
import duckdb
import pandas as pd
import streamlit as st

from shared_css import inject_css
from ui.components import (
    back_button,
    clean_meta,
    empty_state,
    evidence_heading,
    hero_banner,
    interest_declaration_item,
    member_card_html,
    member_profile_header,
    render_notable_chips,
    sidebar_member_filter,
    sidebar_page_header,
    todo_callout,
    year_selector,
)
from ui.export_controls import export_button
from ui.source_pdfs import interests_pdf_url, provenance_expander

from config import (
    INTEREST_CATEGORY_LABELS,
    INTEREST_CATEGORY_ORDER,
    NOTABLE_SENATORS,
    NOTABLE_TDS,
    SILVER_INTERESTS_CSV,
    SILVER_INTERESTS_PARQUET,
)

_REQUIRED_COLS: set[str] = {
    "member_name", "party_name", "constituency", "declaration_year",
    "interest_category", "interest_text", "landlord_flag", "property_flag",
}


# ── Data access — retrieval only (SELECT / WHERE / ORDER BY / LIMIT) ──────────

@st.cache_data(ttl=3600, show_spinner="Loading interests data…")
def _load_interests(house: str) -> pd.DataFrame:
    """
    Load silver parquet and normalise to v_member_interests contract column shape.
    Falls back to silver CSV if parquet not yet built.
    TODO_PIPELINE_VIEW_REQUIRED: Replace with SELECT FROM v_member_interests_detail.
    """
    parquet_path = SILVER_INTERESTS_PARQUET.get(house)
    if parquet_path is not None and parquet_path.exists():
        df = pd.read_parquet(parquet_path)
    else:
        csv_path = SILVER_INTERESTS_CSV.get(house)
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
        return {"years": [], "members": []}
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
    members = (
        con.execute(
            "SELECT DISTINCT member_name FROM v_member_interests "
            "WHERE member_name IS NOT NULL ORDER BY member_name"
        )
        .fetchdf()["member_name"]
        .tolist()
    )
    return {"years": years, "members": members}


@st.cache_data(ttl=300)
def _fetch_interests(
    house: str,
    name_q: str,
    years: tuple[int, ...],
    landlord_only: bool,
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
    if years:
        ph = ", ".join("?" for _ in years)
        where.append(f"TRY_CAST(declaration_year AS INTEGER) IN ({ph})")
        params.extend(int(y) for y in years)
    if landlord_only:
        where.append("landlord_flag = ?")
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
    TODO_PIPELINE_VIEW_REQUIRED: v_member_interests_index
    Replace with: SELECT ... FROM v_member_interests_index WHERE house=? AND declaration_year=?
    read_parquet is forbidden in Streamlit (streamlit_may_read_parquet: false).
    """
    return pd.DataFrame()


@st.cache_data(ttl=300)
def _fetch_member_index_fallback(house: str, year: int) -> pd.DataFrame:
    """
    Fallback until v_member_interests_index is registered.
    One row per member: declaration count, landlord/property flags, rank.
    """
    base = _load_interests(house)
    if base.empty:
        return pd.DataFrame()
    con = duckdb.connect(":memory:")
    con.register("v_member_interests", base)
    return con.execute(
        """
        WITH agg AS (
            SELECT
                member_name,
                MAX(party_name)    AS party_name,
                MAX(constituency)  AS constituency,
                COUNT(*)           AS total_declarations,
                0                  AS directorship_count,
                COUNT(DISTINCT CASE
                    WHEN interest_category = 'Land (including property)'
                     AND interest_text IS NOT NULL
                     AND TRIM(interest_text) <> ''
                     AND LOWER(TRIM(interest_text)) <> 'no interests declared'
                    THEN interest_text END)            AS property_count,
                COUNT(DISTINCT CASE
                    WHEN interest_category = 'Shares'
                     AND interest_text IS NOT NULL
                     AND TRIM(interest_text) <> ''
                     AND LOWER(TRIM(interest_text)) <> 'no interests declared'
                    THEN interest_text END)            AS share_count,
                BOOL_OR(landlord_flag)    AS is_landlord,
                BOOL_OR(property_flag)    AS is_property_owner
            FROM v_member_interests
            WHERE TRY_CAST(declaration_year AS INTEGER) = ?
              AND member_name IS NOT NULL
            GROUP BY member_name
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY total_declarations DESC, member_name) AS rank,
            *
        FROM agg
        ORDER BY rank
        """,
        [year],
    ).fetchdf()


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

    meta  = clean_meta(party, constit)
    pills = f'<span class="int-stat-pill int-pill-decl">{total} declarations</span>'
    if landlord:
        pills += '<span class="int-stat-pill int-stat-pill-accent">🔑 Landlord</span>'
    elif is_prop:
        pills += '<span class="int-stat-pill">🏗️ Property owner</span>'
    if p_count:
        pills += (
            f'<span class="int-stat-pill int-pill-prop">'
            f'🏠 {p_count} propert{"ies" if p_count != 1 else "y"}</span>'
        )
    if s_count:
        pills += (
            f'<span class="int-stat-pill int-pill-shares">'
            f'📈 Shareholder · {s_count}</span>'
        )
    if d_count:
        pills += (
            f'<span class="int-stat-pill int-pill-company">'
            f'🏢 {d_count} compan{"ies" if d_count != 1 else "y"}</span>'
        )

    return member_card_html(name=name, meta=meta, rank=rank, pills_html=pills)


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
            st.html(_int_member_card_html(row))
        btn_col.html('<div class="dt-nav-anchor"></div>')
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
    meta    = clean_meta(party, constit)

    td_years = sorted(td_df["declaration_year"].dropna().astype(int).unique(), reverse=True)

    # ── Identity strip ─────────────────────────────────────────────────────────
    is_landlord = bool(td_df["landlord_flag"].any())
    is_property = bool(td_df["property_flag"].any())

    badges_html = ""
    if is_landlord:
        badges_html += '<span class="dt-badge dt-badge-landlord">Landlord declared</span> '
    if is_property and not is_landlord:
        badges_html += '<span class="dt-badge">Property interest</span> '

    member_profile_header(td_name, meta, badges_html)

    # ── Year pills (profile-scoped key) ───────────────────────────────────────
    year_opts = [str(y) for y in td_years]
    selected_year = year_selector(year_opts, key="int_profile_year", skip_current=False)

    year_df  = td_df[td_df["declaration_year"] == selected_year].copy()
    prior_year = selected_year - 1
    prior_df   = td_df[td_df["declaration_year"] == prior_year].copy()
    has_prior  = not prior_df.empty

    # ── Editorial callout ──────────────────────────────────────────────────────
    name_short = _h(td_name.split()[-1])
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

    st.html(
        f'<div class="dt-callout" style="margin:0.5rem 0 0.9rem;">'
        f'<p style="margin:0;font-size:0.95rem;line-height:1.65;">{glance}</p>'
        f"</div>"
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
        st.html(
            f'<div class="dt-provenance-box" style="margin-bottom:0.75rem">'
            f'<span style="font-size:0.68rem;font-weight:700;letter-spacing:0.08em;'
            f'text-transform:uppercase;color:var(--text-meta)">Source document</span><br>'
            f'<a class="leg-source-link" href="{_h(pdf_url)}" target="_blank" rel="noopener">'
            f'↗ Register of Members\' Interests · {_h(house)} · {selected_year} (Oireachtas.ie PDF)</a>'
            f'</div>'
        )

    evidence_heading(f"Declarations · {selected_year}")

    # ── Category sections — non-empty only ────────────────────────────────────
    # Pre-compute descriptions per category once to avoid repeated calls inside the loop.
    all_cats = list(INTEREST_CATEGORY_ORDER)
    if not year_df.empty:
        for cat in year_df["interest_category"].dropna().unique():
            if cat not in INTEREST_CATEGORY_ORDER:
                all_cats.append(cat)

    year_descs_by_cat:  dict[str, list[str]] = {
        cat: _real_descriptions(year_df[year_df["interest_category"] == cat] if not year_df.empty else pd.DataFrame())
        for cat in all_cats
    }
    prior_descs_by_cat: dict[str, list[str]] = {
        cat: (_real_descriptions(prior_df[prior_df["interest_category"] == cat]) if has_prior else [])
        for cat in all_cats
    }

    cats_with_data = [cat for cat in all_cats if year_descs_by_cat[cat]]
    cats_empty     = [cat for cat in INTEREST_CATEGORY_ORDER if not year_descs_by_cat.get(cat)]

    if not cats_with_data:
        empty_state(
            f"Nothing declared for {selected_year}",
            "No interest declarations recorded for this member in this year.",
        )
    else:
        for cat in cats_with_data:
            descs           = year_descs_by_cat[cat]
            prior_cat_set   = set(prior_descs_by_cat[cat])
            current_cat_set = set(descs)
            label = INTEREST_CATEGORY_LABELS.get(cat, cat)

            st.html(
                f'<p class="int-category-section">{_h(label)}&nbsp;&nbsp;·&nbsp;&nbsp;{len(descs)}</p>'
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
            for cat in INTEREST_CATEGORY_ORDER:
                if cat in cats_with_data:
                    continue
                prior_descs = prior_descs_by_cat.get(cat, [])
                if not prior_descs:
                    continue
                label = INTEREST_CATEGORY_LABELS.get(cat, cat)
                st.html(
                    f'<p class="int-category-section">{_h(label)}&nbsp;&nbsp;·&nbsp;&nbsp;'
                    f'0 <span style="font-weight:400;text-transform:none;font-size:0.75rem;">'
                    f'(all removed)</span></p>'
                )
                for d in sorted(prior_descs):
                    interest_declaration_item(d, "removed")

    # ── Empty categories — single collapsed summary ────────────────────────────
    if cats_empty:
        empty_labels = [INTEREST_CATEGORY_LABELS.get(c, c) for c in cats_empty]
        with st.expander(
            f"Nothing declared · {len(cats_empty)} categories", expanded=False
        ):
            st.html('<p class="int-empty-cats">' + " · ".join(_h(lbl) for lbl in empty_labels) + "</p>")

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

def _render_provenance() -> None:
    provenance_expander(
        sections=[
            "Declarations are extracted from published Oireachtas PDF documents. "
            "Flags (landlord, property) are pipeline navigation aids, not legal conclusions. "
            "Office holders (Ministers, Ceann Comhairle) may be exempt from filing — "
            "records can be incomplete. "
            "A high declaration count reflects transparency, not wrongdoing."
        ],
        source_caption="Data: Oireachtas Register of Members' Interests (data.oireachtas.ie)",
    )


# ── Page entry point ───────────────────────────────────────────────────────────

def interests_page() -> None:
    inject_css()

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        sidebar_page_header("Register of<br>Members&rsquo; Interests")

        house: str = st.segmented_control(
            "Chamber", ["Dáil", "Seanad"], default="Dáil", key="interests_house",
        ) or "Dáil"

        # Clear year pill and member selection on chamber switch
        if st.session_state.get("_interests_last_house") != house:
            for k in ("int_profile_year", "selected_td", "int_member_sel", "int_member_q"):
                st.session_state.pop(k, None)
            st.session_state["_interests_last_house"] = house

        opts = _fetch_filter_options(house)

        chosen = sidebar_member_filter(
            "Browse all members",
            opts["members"],
            key_search="int_member_q",
            key_select="int_member_sel",
            placeholder="Type a name…",
        )
        if chosen and st.session_state.get("selected_td") != chosen:
            st.session_state["selected_td"] = chosen
            st.rerun()

        st.divider()
        notable = NOTABLE_TDS if house == "Dáil" else NOTABLE_SENATORS
        if notable and render_notable_chips(notable, opts["members"], "chip_int", "selected_td"):
            st.rerun()

    # ── Guard ─────────────────────────────────────────────────────────────────
    parquet_path = SILVER_INTERESTS_PARQUET.get(house)
    csv_path     = SILVER_INTERESTS_CSV.get(house)
    if not ((parquet_path and parquet_path.exists()) or (csv_path and csv_path.exists())):
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

    selected_td = st.session_state.get("selected_td")

    # ── Page header ───────────────────────────────────────────────────────────
    if not selected_td:
        hero_banner(
            kicker="REGISTER OF MEMBERS' INTERESTS",
            title="What has your TD declared?",
        )

    if selected_td:
        if back_button("← Back to register", key="int"):
            st.session_state["selected_td"] = None
            st.session_state.pop("int_member_sel", None)
            st.rerun()
        st.divider()
        _render_profile(house, selected_td)
        st.divider()
        _render_provenance()
        return

    # ── Browse mode ───────────────────────────────────────────────────────────

    # Year pills — main content, newest first
    year_opts = [str(y) for y in opts["years"]]
    if not year_opts:
        empty_state("No year data found", "v_member_interests_detail returned no years.")
        _render_provenance()
        return

    selected_year = year_selector(year_opts, key="int_year")

    # ── Leaderboard — one card per member, ranked by declarations ─────────────
    # TODO_PIPELINE_VIEW_REQUIRED: v_member_interests_index
    # Command bar (name filter + landlord toggle) reinstated once the view is registered.
    ranking_df = _load_ranking(house, selected_year)

    if ranking_df.empty:
        todo_callout(
            "v_member_interests_index not yet registered. "
            "Register this view to enable the ranked member leaderboard."
        )

        members_df = _fetch_member_index_fallback(house, selected_year)
        if members_df.empty:
            empty_state(
                f"No declarations for {selected_year}",
                "No interest declarations on record for this year and chamber.",
            )
            _render_provenance()
            return

        evidence_heading(f"Members · {selected_year} · {len(members_df)}")
        for i, (_, row) in enumerate(members_df.iterrows()):
            card_col, btn_col = st.columns([14, 1])
            card_col.html(_int_member_card_html(row))
            btn_col.html('<div class="dt-nav-anchor"></div>')
            if btn_col.button("→", key=f"int_fb_{i}", help=f"View {row['member_name']}'s declarations"):
                st.session_state["selected_td"] = str(row["member_name"])
                st.rerun()

        _render_provenance()
        return


