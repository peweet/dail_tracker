"""
Register of Members' Interests — Dáil Tracker.

Temporary data source: reads directly from silver CSV files.
Once the pipeline delivers data/gold/dail.duckdb, replace the three _simulate_*
functions below with direct SELECT queries against the pipeline-owned views.

TODO_PIPELINE_VIEW_REQUIRED: Create the following views in data/gold/dail.duckdb.
Target SQL (place in sql_queries/create_member_interests_views.sql):

    CREATE OR REPLACE VIEW v_member_interests AS
    SELECT
        row_number() OVER ()                                    AS interest_record_id,
        NULL::VARCHAR                                           AS member_id,
        full_name                                               AS member_name,
        COALESCE(party, '')                                     AS party_name,
        COALESCE(constituency_name, '')                         AS constituency,
        'Dáil'                                                  AS house,
        NULL::VARCHAR                                           AS dail_term,
        TRY_CAST(year_declared AS INTEGER)                      AS declaration_year,
        interest_category,
        COALESCE(interest_description_cleaned, '')              AS interest_text,
        CASE WHEN lower(TRY_CAST(is_landlord AS VARCHAR)) = 'true'
             THEN TRUE ELSE FALSE END                           AS landlord_flag,
        CASE WHEN lower(TRY_CAST(is_property_owner AS VARCHAR)) = 'true'
             THEN TRUE ELSE FALSE END                           AS property_flag,
        FALSE                                                   AS directorship_flag,
        FALSE                                                   AS shareholding_flag,
        NULL::VARCHAR                                           AS source_document_name,
        NULL::VARCHAR                                           AS source_pdf_url,
        NULL::INTEGER                                           AS source_page_number,
        current_timestamp                                       AS latest_fetch_timestamp_utc
    FROM read_csv_auto('data/silver/dail_member_interests_combined.csv')
    WHERE interest_category IS DISTINCT FROM '15';

    CREATE OR REPLACE VIEW v_member_interests_summary AS
    SELECT
        'pipeline' AS latest_run_id,
        COUNT(DISTINCT full_name) AS members_with_interests_count,
        COUNT(*) AS declarations_count,
        MAX(TRY_CAST(year_declared AS INTEGER)) AS latest_declaration_year,
        1 AS source_documents_count,
        current_timestamp AS latest_fetch_timestamp_utc,
        'data/silver/dail_member_interests_combined.csv' AS source_summary,
        NULL::VARCHAR AS mart_version,
        NULL::VARCHAR AS code_version
    FROM read_csv_auto('data/silver/dail_member_interests_combined.csv')
    WHERE interest_category IS DISTINCT FROM '15';

    CREATE OR REPLACE VIEW v_member_interests_category_summary AS
    SELECT interest_category, COUNT(*) AS declarations_count
    FROM read_csv_auto('data/silver/dail_member_interests_combined.csv')
    WHERE interest_category IS DISTINCT FROM '15' AND interest_category IS NOT NULL
    GROUP BY interest_category
    ORDER BY declarations_count DESC;

TODO_PIPELINE_VIEW_REQUIRED: directorship_flag — derive from interest_category in member_interests.py
TODO_PIPELINE_VIEW_REQUIRED: shareholding_flag — derive from interest_category in member_interests.py
TODO_PIPELINE_VIEW_REQUIRED: source_pdf_url — add PDF URL to silver output
TODO_PIPELINE_VIEW_REQUIRED: source_page_number — add page number to silver output
TODO_PIPELINE_VIEW_REQUIRED: member_id — stable Oireachtas API member URI in silver output
TODO_PIPELINE_VIEW_REQUIRED: property_count — pipeline text analysis of Land/property entries
"""

from __future__ import annotations

import datetime
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

# ── Paths ──────────────────────────────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parents[2]
_CSS  = _ROOT / "utility" / "styles" / "base.css"
_CSV  = {
    "Dáil":   _ROOT / "data" / "silver" / "dail_member_interests_combined.csv",
    "Seanad": _ROOT / "data" / "silver" / "seanad_member_interests_combined.csv",
}

_REQUIRED_COLS: set[str] = {
    "member_name", "party_name", "constituency", "declaration_year",
    "interest_category", "interest_text", "landlord_flag", "property_flag",
}

_DISPLAY_COLS: list[str] = [
    "member_name", "party_name", "constituency", "declaration_year",
    "interest_category", "interest_text",
    "landlord_flag", "property_flag", "directorship_flag", "shareholding_flag",
    "source_pdf_url",
]

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
    "Occupations":                                          "Occupations & Employment",
    "Directorships":                                        "Directorships & Company Roles",
    "Remunerated Position":                                 "Remunerated Positions",
    "Shares":                                               "Shareholdings",
    "Land (including property)":                            "Land & Property",
    "Contracts":                                            "Contracts",
    "Gifts":                                                "Gifts Received",
    "Travel Facilities":                                    "Travel Facilities",
    "Property supplied or lent or a Service supplied":      "Property or Services Supplied",
}


# ── Data access ────────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner="Loading interests data…")
def _load_interests(house: str) -> pd.DataFrame:
    """
    Load silver CSV and normalise to v_member_interests contract column shape.
    TODO_PIPELINE_VIEW_REQUIRED: Replace with persistent DuckDB view query.
    """
    csv_path = _CSV.get(house)
    if csv_path is None or not csv_path.exists():
        return pd.DataFrame()
    df = pd.read_csv(csv_path, low_memory=False)
    df.columns = df.columns.str.strip()
    if "interest_category" in df.columns:
        # TODO_PIPELINE_VIEW_REQUIRED: exclusion of category "15" belongs in silver/view
        df = df[df["interest_category"] != "15"].copy()
    for csv_col, contract_col in [("is_landlord", "landlord_flag"), ("is_property_owner", "property_flag")]:
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
        ("directorship_flag", False),   # TODO_PIPELINE_VIEW_REQUIRED
        ("shareholding_flag", False),   # TODO_PIPELINE_VIEW_REQUIRED
        ("source_pdf_url",   None),     # TODO_PIPELINE_VIEW_REQUIRED
        ("source_page_number", None),   # TODO_PIPELINE_VIEW_REQUIRED
    ]:
        if col not in df.columns:
            df[col] = default
    if "declaration_year" in df.columns:
        df["declaration_year"] = pd.to_numeric(df["declaration_year"], errors="coerce")
    return df


@st.cache_data(ttl=3600)
def _simulate_summary(house: str) -> pd.Series:
    """
    Simulate v_member_interests_summary.
    TODO_PIPELINE_VIEW_REQUIRED: Replace with:
        SELECT ... FROM v_member_interests_summary LIMIT 1
    """
    base = _load_interests(house)
    if base.empty:
        return pd.Series(dtype=object)
    con = duckdb.connect(":memory:")
    con.register("t", base)
    source = f"data/silver/{house.lower()}_member_interests_combined.csv"
    row = con.execute(f"""
        SELECT
            'csv_local'                                AS latest_run_id,
            COUNT(DISTINCT member_name)                AS members_with_interests_count,
            COUNT(*)                                   AS declarations_count,
            MAX(TRY_CAST(declaration_year AS INTEGER)) AS latest_declaration_year,
            NULL                                       AS latest_fetch_timestamp_utc,
            '{source}'                                 AS source_summary,
            NULL                                       AS mart_version,
            NULL                                       AS code_version
        FROM t
    """).fetchdf()
    return row.iloc[0]


@st.cache_data(ttl=3600)
def _simulate_category_chart(house: str) -> pd.DataFrame:
    """
    Simulate v_member_interests_category_summary.
    TODO_PIPELINE_VIEW_REQUIRED: Replace with:
        SELECT interest_category, declarations_count FROM v_member_interests_category_summary
    """
    base = _load_interests(house)
    if base.empty:
        return pd.DataFrame(columns=["interest_category", "declarations_count"])
    con = duckdb.connect(":memory:")
    con.register("t", base)
    return con.execute("""
        SELECT interest_category, COUNT(*) AS declarations_count
        FROM t
        WHERE interest_category IS NOT NULL
        GROUP BY interest_category
        ORDER BY declarations_count DESC
    """).fetchdf()


@st.cache_data(ttl=300)
def _fetch_filter_options(house: str) -> dict[str, list]:
    """Retrieval SQL: SELECT DISTINCT <col> FROM v_member_interests ORDER BY <col>"""
    base = _load_interests(house)
    if base.empty:
        return {"years": [], "categories": [], "parties": [], "constituencies": [], "members": []}
    con = duckdb.connect(":memory:")
    con.register("v_member_interests", base)
    years = con.execute(
        "SELECT DISTINCT TRY_CAST(declaration_year AS INTEGER) AS y "
        "FROM v_member_interests WHERE declaration_year IS NOT NULL ORDER BY y DESC"
    ).fetchdf()["y"].dropna().astype(int).tolist()
    categories = con.execute(
        "SELECT DISTINCT interest_category FROM v_member_interests "
        "WHERE interest_category IS NOT NULL ORDER BY interest_category"
    ).fetchdf()["interest_category"].tolist()
    parties = con.execute(
        "SELECT DISTINCT party_name FROM v_member_interests "
        "WHERE party_name IS NOT NULL AND party_name != '' ORDER BY party_name"
    ).fetchdf()["party_name"].tolist()
    constituencies = con.execute(
        "SELECT DISTINCT constituency FROM v_member_interests "
        "WHERE constituency IS NOT NULL AND constituency != '' ORDER BY constituency"
    ).fetchdf()["constituency"].tolist()
    members = con.execute(
        "SELECT DISTINCT member_name FROM v_member_interests "
        "WHERE member_name IS NOT NULL ORDER BY member_name"
    ).fetchdf()["member_name"].tolist()
    return {
        "years": years, "categories": categories,
        "parties": parties, "constituencies": constituencies, "members": members,
    }


@st.cache_data(ttl=300)
def _fetch_interests(
    house: str,
    member_name_q: str,
    interest_text_q: str,
    years: tuple[int, ...],
    categories: tuple[str, ...],
    parties: tuple[str, ...],
    constituencies: tuple[str, ...],
    landlord_only: bool,
    property_only: bool,
) -> pd.DataFrame:
    """
    Approved retrieval SQL:
        SELECT <cols> FROM v_member_interests
        WHERE <approved filters>
        ORDER BY declaration_year DESC, member_name
        LIMIT 1000
    """
    base = _load_interests(house)
    if base.empty:
        return pd.DataFrame()
    con = duckdb.connect(":memory:")
    con.register("v_member_interests", base)
    where: list[str] = []
    params: list = []
    if member_name_q:
        where.append("member_name ILIKE ?")
        params.append(f"%{member_name_q}%")
    if interest_text_q:
        where.append("interest_text ILIKE ?")
        params.append(f"%{interest_text_q}%")
    if years:
        ph = ", ".join("?" for _ in years)
        where.append(f"TRY_CAST(declaration_year AS INTEGER) IN ({ph})")
        params.extend(int(y) for y in years)
    if categories:
        ph = ", ".join("?" for _ in categories)
        where.append(f"interest_category IN ({ph})")
        params.extend(categories)
    if parties:
        ph = ", ".join("?" for _ in parties)
        where.append(f"party_name IN ({ph})")
        params.extend(parties)
    if constituencies:
        ph = ", ".join("?" for _ in constituencies)
        where.append(f"constituency IN ({ph})")
        params.extend(constituencies)
    if landlord_only:
        where.append("landlord_flag = ?")
        params.append(True)
    if property_only:
        where.append("property_flag = ?")
        params.append(True)
    where_clause = "WHERE " + " AND ".join(where) if where else ""
    sql = f"""
        SELECT member_name, party_name, constituency,
               TRY_CAST(declaration_year AS INTEGER) AS declaration_year,
               interest_category, interest_text,
               landlord_flag, property_flag, directorship_flag, shareholding_flag,
               source_pdf_url, source_page_number
        FROM v_member_interests
        {where_clause}
        ORDER BY declaration_year DESC, member_name
        LIMIT 1000
    """
    return con.execute(sql, params).fetchdf()


@st.cache_data(ttl=300)
def _fetch_td_data(house: str, td_name: str) -> pd.DataFrame:
    """
    All interest records for one member across all years.
    Retrieval SQL: SELECT ... FROM v_member_interests WHERE member_name = ?
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
               interest_category, interest_text,
               landlord_flag, property_flag, directorship_flag, shareholding_flag
        FROM v_member_interests
        WHERE member_name = ?
        ORDER BY declaration_year DESC, interest_category
        """,
        [td_name],
    ).fetchdf()


@st.cache_data(ttl=300)
def _fetch_year_export(house: str, year: int) -> pd.DataFrame:
    """
    All declarations for a given year — for bulk CSV export.
    Retrieval SQL: SELECT ... FROM v_member_interests WHERE declaration_year = ?
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
        WHERE TRY_CAST(declaration_year AS INTEGER) = ?
        ORDER BY member_name, interest_category
        """,
        [year],
    ).fetchdf()


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


def _count_property_interests(year_df: pd.DataFrame) -> int:
    """
    Heuristic count of likely property entries within the Land/property category.
    TODO_PIPELINE_VIEW_REQUIRED: replace with pipeline-derived property_count column
    in silver output (member_interests.py should parse addresses and count distinct
    property entries, writing the result as a silver column).
    """
    if year_df.empty:
        return 0
    land = year_df[year_df["interest_category"].fillna("").str.contains("Land|[Pp]ropert", na=False)]
    valid = land[
        (land["interest_text"].fillna("").str.len() > 10) &
        (~land["interest_text"].fillna("").str.lower().str.contains(
            "no interests|nothing declared|no interest", na=False
        ))
    ]
    return len(valid)


# ── CSS ────────────────────────────────────────────────────────────────────────

def _load_css() -> None:
    if _CSS.exists():
        st.markdown(f"<style>{_CSS.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)


# ── Render helpers ─────────────────────────────────────────────────────────────

def _validate_columns(df: pd.DataFrame) -> list[str]:
    return sorted(_REQUIRED_COLS - set(df.columns))


def _render_hero(house: str, summary: pd.Series) -> None:
    declarations = int(summary.get("declarations_count", 0)) if not summary.empty else 0
    members      = int(summary.get("members_with_interests_count", 0)) if not summary.empty else 0
    latest_year  = (
        int(summary["latest_declaration_year"])
        if not summary.empty and pd.notna(summary.get("latest_declaration_year"))
        else "—"
    )
    st.markdown(
        f"""
        <div class="dt-hero">
          <div class="dt-kicker">Dáil Tracker &middot; {house}</div>
          <h1 style="margin:0.2rem 0 0.4rem 0;">Register of Members' Interests</h1>
        </div>
        """,
        unsafe_allow_html=True,
    )
    with st.expander("About this register", expanded=False):
        st.markdown(
            f"Every member of the {house} must publicly declare financial interests annually "
            "under the Ethics in Public Office Acts 1995 and 2001. Declarations cover property, "
            "company directorships, shareholdings, outside income, gifts, and travel facilities. "
            "**Office holders (Ministers, Ceann Comhairle) are exempt from filing.** "
            "A high declaration count reflects transparency, not wrongdoing."
        )
    c1, c2, c3 = st.columns(3)
    c1.metric("Declarations on record", f"{declarations:,}")
    c2.metric("Members with interests",  f"{members:,}")
    c3.metric("Latest year",             str(latest_year))


def _render_category_chart(chart_df: pd.DataFrame) -> None:
    if chart_df.empty:
        return
    st.markdown(
        '<p class="dt-kicker" style="margin:1.25rem 0 0.3rem 0;">Declarations by category</p>',
        unsafe_allow_html=True,
    )
    st.bar_chart(
        chart_df.set_index("interest_category")["declarations_count"],
        use_container_width=True,
        height=195,
    )


def _render_table(result_df: pd.DataFrame) -> None:
    n = len(result_df)
    st.caption(f"{n:,} declaration{'s' if n != 1 else ''} shown  ·  max 1,000 per query")
    display = result_df[[c for c in _DISPLAY_COLS if c in result_df.columns]]
    st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "member_name":      st.column_config.TextColumn("Member",       width="medium"),
            "party_name":       st.column_config.TextColumn("Party",        width="small"),
            "constituency":     st.column_config.TextColumn("Constituency", width="medium"),
            "declaration_year": st.column_config.NumberColumn("Year",       format="%.0f", width="small"),
            "interest_category":st.column_config.TextColumn("Category",     width="medium"),
            "interest_text":    st.column_config.TextColumn("Declaration",  width="large"),
            "landlord_flag":    st.column_config.CheckboxColumn("Landlord", width="small"),
            "property_flag":    st.column_config.CheckboxColumn("Property", width="small"),
            "directorship_flag":st.column_config.CheckboxColumn("Director", width="small"),
            "shareholding_flag":st.column_config.CheckboxColumn("Shares",   width="small"),
            "source_pdf_url":   st.column_config.LinkColumn(
                "Source PDF", width="medium",
                help="Not yet available — TODO_PIPELINE_VIEW_REQUIRED",
            ),
        },
    )


def _render_td_profile(td_df: pd.DataFrame, td_name: str, year: int, show_diff: bool) -> None:
    year_df  = td_df[td_df["declaration_year"] == year].copy()
    prior_df = td_df[td_df["declaration_year"] == year - 1].copy()

    info    = td_df.iloc[0]
    party   = str(info.get("party_name",  "") or "")
    constit = str(info.get("constituency","") or "")
    meta    = " · ".join(p for p in [party, constit] if p and p.lower() != "nan")

    is_landlord = bool(year_df["landlord_flag"].any()) if not year_df.empty else False
    is_property = bool(year_df["property_flag"].any()) if not year_df.empty else False

    badges = ""
    if is_landlord:
        badges += (
            '<span class="dt-badge" '
            'style="border-color:var(--dt-accent);color:var(--dt-accent);">'
            "Landlord declared</span>&nbsp;"
        )
    if is_property and not is_landlord:
        badges += '<span class="dt-badge">Property interest</span>&nbsp;'

    st.markdown(
        f"""
        <div class="dt-hero" style="border-left-color:var(--dt-accent);">
          <div class="dt-kicker">Member profile</div>
          <h2 style="margin:0.15rem 0 0.2rem 0;">{td_name}</h2>
          {"" if not meta else f'<p style="margin:0 0 0.45rem 0;color:var(--dt-text-muted);">{meta}</p>'}
          {badges}
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Build category list ordered by declaration count (desc) for this year
    all_cats_in_year = (
        year_df["interest_category"].dropna().unique().tolist() if not year_df.empty else []
    )
    cat_counts: list[tuple[str, int]] = []
    for cat in all_cats_in_year:
        cat_rows = year_df[year_df["interest_category"] == cat]
        cat_counts.append((cat, len(_real_descriptions(cat_rows))))
    cat_counts.sort(key=lambda x: -x[1])

    ordered_cats = [cat for cat, _ in cat_counts]
    # Append standard categories not yet seen (shown collapsed as empty)
    ordered_cats += [c for c in _CATEGORY_ORDER if c not in ordered_cats]

    total_entries   = sum(c for _, c in cat_counts if c > 0)
    cats_with_data  = [cat for cat, cnt in cat_counts if cnt > 0]
    prop_count      = _count_property_interests(year_df) if not year_df.empty else 0

    # ── At-a-glance callout ───────────────────────────────────────────────────
    if year_df.empty:
        glance = f"No declarations recorded for {year}."
    else:
        parts = [f"<strong>{total_entries}</strong> declaration{'s' if total_entries != 1 else ''}"]
        if cats_with_data:
            parts.append(
                f"across <strong>{len(cats_with_data)}</strong> "
                f"categor{'ies' if len(cats_with_data) != 1 else 'y'}"
            )
        if prop_count:
            parts.append(
                f"~<strong>{prop_count}</strong> "
                f"property interest{'s' if prop_count != 1 else ''}"
            )
        if is_landlord:
            parts.append("<strong>Landlord declared</strong>")
        glance = " · ".join(parts)

    st.markdown(
        f"""
        <div class="dt-callout" style="margin:0.65rem 0 1rem 0;">
          <div class="dt-kicker" style="margin-bottom:0.3rem;">At a glance &middot; {year}</div>
          <p style="margin:0;">{glance}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Category expanders (most → least declarations) ────────────────────────
    max_count = cat_counts[0][1] if cat_counts and cat_counts[0][1] > 0 else 1

    for cat in ordered_cats:
        cat_rows  = year_df[year_df["interest_category"] == cat] if not year_df.empty else pd.DataFrame()
        descs     = _real_descriptions(cat_rows)
        prior_cat = prior_df[prior_df["interest_category"] == cat] if not prior_df.empty else pd.DataFrame()
        prior_set = set(_real_descriptions(prior_cat))
        label     = _CATEGORY_LABELS.get(cat, cat)
        pct       = int(len(descs) / max_count * 100) if descs else 0

        with st.expander(f"{label} ({len(descs)})", expanded=bool(descs)):
            if descs and pct > 0:
                st.markdown(
                    f'<div style="height:3px;background:var(--dt-primary);'
                    f'width:{pct}%;border-radius:2px;margin-bottom:0.5rem;"></div>',
                    unsafe_allow_html=True,
                )
            if not descs:
                st.markdown(
                    '<p style="color:var(--dt-text-muted);font-style:italic;'
                    'font-size:0.875rem;margin:0.2rem 0;">Nothing declared in this category.</p>',
                    unsafe_allow_html=True,
                )
                continue

            if show_diff and prior_set:
                current_set = set(descs)
                for d in descs:
                    if d not in prior_set:
                        st.markdown(
                            f'<div style="background:#f0fdf4;border-left:3px solid #16a34a;'
                            f'padding:0.35rem 0.65rem;margin:0.2rem 0;border-radius:0 4px 4px 0;'
                            f'font-size:0.9rem;line-height:1.5;">'
                            f'<span style="font-size:0.63rem;font-weight:700;letter-spacing:0.07em;'
                            f'text-transform:uppercase;color:#16a34a;margin-right:0.45rem;">New</span>'
                            f'{d}</div>',
                            unsafe_allow_html=True,
                        )
                    else:
                        st.markdown(
                            f'<div style="padding:0.35rem 0;border-bottom:1px solid var(--dt-border);'
                            f'font-size:0.9rem;line-height:1.5;">{d}</div>',
                            unsafe_allow_html=True,
                        )
                for d in sorted(prior_set - current_set):
                    st.markdown(
                        f'<div style="background:#fef2f2;border-left:3px solid #dc2626;'
                        f'padding:0.35rem 0.65rem;margin:0.2rem 0;border-radius:0 4px 4px 0;'
                        f'font-size:0.9rem;line-height:1.5;opacity:0.82;">'
                        f'<span style="font-size:0.63rem;font-weight:700;letter-spacing:0.07em;'
                        f'text-transform:uppercase;color:#dc2626;margin-right:0.45rem;">Removed</span>'
                        f'<s>{d}</s></div>',
                        unsafe_allow_html=True,
                    )
            else:
                for d in descs:
                    st.markdown(
                        f'<div style="padding:0.35rem 0;border-bottom:1px solid var(--dt-border);'
                        f'font-size:0.9rem;line-height:1.55;">{d}</div>',
                        unsafe_allow_html=True,
                    )


def _render_provenance(house: str, summary: pd.Series) -> None:
    csv_label = f"data/silver/{house.lower()}_member_interests_combined.csv"
    run_id    = summary.get("latest_run_id", "csv_local") if not summary.empty else "csv_local"
    with st.expander("Data provenance & caveats"):
        st.markdown(
            f"""
            <div class="dt-provenance-box">
              <div class="dt-kicker">Source</div>
              <p style="margin:0.25rem 0 0.75rem 0;">{csv_label}</p>
              <div class="dt-kicker">Run ID</div>
              <p style="margin:0.25rem 0 0.75rem 0;">{run_id}</p>
              <div class="dt-kicker">Caveats</div>
              <p style="margin:0.25rem 0;">
                Declarations are extracted from published Oireachtas PDF documents.
                Flags (landlord, property) are pipeline navigation aids, not legal conclusions.
                Office holders may be exempt from filing — records can be incomplete.
                A high declaration count reflects transparency, not wrongdoing.
              </p>
            </div>
            """,
            unsafe_allow_html=True,
        )


# ── Page entry point ───────────────────────────────────────────────────────────

def interests_page() -> None:  # noqa: C901
    _load_css()

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown('<div class="dt-kicker">Dáil Tracker</div>', unsafe_allow_html=True)

        # NOTE: contract has no explicit house filter because v_member_interests
        # should contain both houses. While using two separate CSVs this radio is
        # a data-source selector. When pipeline view exists, replace with WHERE house IN (?).
        house: str = st.radio("Chamber", ["Dáil", "Seanad"], horizontal=True, key="interests_house")

        # Clear all filter state on house switch
        if st.session_state.get("_interests_last_house") != house:
            for k in ("int_year_radio", "int_constits", "int_parties", "int_cats",
                      "selected_td", "int_member_sel", "int_member_q"):
                st.session_state.pop(k, None)
            st.session_state["_interests_last_house"] = house

        selected_td: str | None = st.session_state.get("selected_td")
        opts = _fetch_filter_options(house)

        st.divider()

        # ── Back button (profile mode only) ──────────────────────────────────
        if selected_td:
            if st.button("← All members", use_container_width=True, key="btn_back"):
                st.session_state["selected_td"] = None
                st.session_state.pop("int_member_sel", None)
                st.rerun()
            st.markdown(
                f'<p style="font-size:0.82rem;color:var(--dt-text-muted);margin:0.35rem 0 0 0;">'
                f'Viewing: <strong>{selected_td}</strong></p>',
                unsafe_allow_html=True,
            )

        # ── Find a member (always visible) ────────────────────────────────────
        st.markdown('<div class="dt-kicker" style="margin-top:0.5rem;">Find a member</div>',
                    unsafe_allow_html=True)

        notable = _NOTABLE_DAIL if house == "Dáil" else _NOTABLE_SEANAD
        if notable:
            chip_cols = st.columns(2)
            for i, name in enumerate(notable):
                if chip_cols[i % 2].button(
                    name.split()[-1], key=f"qs_{house}_{i}",
                    help=name, use_container_width=True,
                ):
                    st.session_state["selected_td"] = name
                    st.session_state.pop("int_member_sel", None)
                    st.rerun()

        member_search: str = st.text_input(
            "Search", placeholder="Type a name…", key="int_member_q",
            label_visibility="collapsed",
        )
        sq = member_search.strip().lower()
        filtered_names = [n for n in opts["members"] if sq in n.lower()] if sq else opts["members"]

        chosen: str = st.selectbox(
            "Select member",
            options=[""] + filtered_names,
            index=0,
            format_func=lambda x: "— select a member —" if x == "" else x,
            key="int_member_sel",
            label_visibility="collapsed",
        )
        if chosen and chosen != selected_td:
            st.session_state["selected_td"] = chosen
            st.rerun()

        # ── Browse-mode filters (hidden in profile mode) ──────────────────────
        if not selected_td:
            st.divider()
            st.markdown('<div class="dt-kicker">Year</div>', unsafe_allow_html=True)
            year_labels = ["All"] + [str(y) for y in opts["years"]]
            year_radio: str = st.radio(
                "Declaration year", year_labels,
                index=1 if len(year_labels) > 1 else 0,
                horizontal=True, key="int_year_radio",
                label_visibility="collapsed",
            )

            st.divider()
            st.markdown('<div class="dt-kicker">Filters</div>', unsafe_allow_html=True)
            # Constituency placed first — reduces upward-dropdown occurrence in sidebar
            constit_sel: list[str] = st.multiselect(
                "Constituency", opts["constituencies"], default=[], key="int_constits",
            )
            party_sel: list[str] = st.multiselect(
                "Party", opts["parties"], default=[], key="int_parties",
            )
            cat_sel: list[str] = st.multiselect(
                "Category", opts["categories"], default=[], key="int_cats",
            )
            interest_q: str = st.text_input(
                "Search declaration text",
                placeholder="e.g. Kildare, director, rental…",
                key="int_interest_q",
            )

            st.divider()
            st.markdown('<div class="dt-kicker">Flags</div>', unsafe_allow_html=True)
            landlord_only: bool = st.toggle("Landlord only",  key="int_landlord")
            property_only: bool = st.toggle("Property only",  key="int_property")
        else:
            year_radio    = "All"
            constit_sel   = []
            party_sel     = []
            cat_sel       = []
            interest_q    = ""
            landlord_only = False
            property_only = False

    # ── Guard: data validation ────────────────────────────────────────────────
    csv_path = _CSV.get(house)
    if csv_path is None or not csv_path.exists():
        st.markdown(
            """<div class="dt-callout">
              <h3>Register of Members' Interests view is missing</h3>
              <p>The source data for this house was not found.</p>
              <p><em>Recovery: run the pipeline to populate data/silver/.</em></p>
            </div>""",
            unsafe_allow_html=True,
        )
        return

    base_df = _load_interests(house)
    if base_df.empty:
        st.markdown(
            """<div class="dt-callout">
              <h3>No interests data available</h3>
              <p>The source CSV loaded but contains no rows.</p>
              <p><em>Recovery: re-run the pipeline (step 8: member_interests.py).</em></p>
            </div>""",
            unsafe_allow_html=True,
        )
        return

    missing_cols = _validate_columns(base_df)
    if missing_cols:
        st.markdown(
            f"""<div class="dt-callout">
              <h3>View shape changed</h3>
              <p>Required columns missing: <strong>{', '.join(missing_cols)}</strong>.</p>
              <p><em>Recovery: align the pipeline output with the YAML contract.</em></p>
            </div>""",
            unsafe_allow_html=True,
        )
        return

    summary = _simulate_summary(house)

    # ── Profile mode ──────────────────────────────────────────────────────────
    selected_td = st.session_state.get("selected_td")
    if selected_td:
        td_df = _fetch_td_data(house, selected_td)
        if td_df.empty:
            st.warning(f"No records found for {selected_td}. Try a different name.")
            _render_provenance(house, summary)
            return

        td_years = sorted(td_df["declaration_year"].dropna().astype(int).unique(), reverse=True)

        # Controls row: year radio | diff toggle | export
        col_yr, col_diff, col_exp = st.columns([6, 3, 2])
        with col_yr:
            st.markdown('<div class="dt-kicker">Declaration year</div>', unsafe_allow_html=True)
            sel_year_str: str = st.radio(
                "Year", [str(y) for y in td_years],
                horizontal=True, index=0,
                key=f"td_year_{selected_td}",
                label_visibility="collapsed",
            )
            sel_year = int(sel_year_str)

        prior_year = sel_year - 1
        has_prior  = prior_year in td_years

        with col_diff:
            st.markdown('<div class="dt-kicker">Compare</div>', unsafe_allow_html=True)
            if has_prior:
                show_diff: bool = st.checkbox(
                    f"Changes from {prior_year}",
                    key=f"td_diff_{selected_td}_{sel_year}",
                )
            else:
                st.caption(f"No {prior_year} data.")
                show_diff = False

        with col_exp:
            st.markdown('<div class="dt-kicker">Export</div>', unsafe_allow_html=True)
            yr_rows  = td_df[td_df["declaration_year"] == sel_year]
            today    = datetime.date.today().isoformat()
            st.download_button(
                "Export CSV",
                data=yr_rows.to_csv(index=False),
                file_name=f"interests_{selected_td.replace(' ', '_')}_{sel_year}_{today}.csv",
                mime="text/csv",
                use_container_width=True,
            )

        st.divider()
        _render_td_profile(td_df, selected_td, sel_year, show_diff)
        _render_provenance(house, summary)
        return

    # ── Browse mode ───────────────────────────────────────────────────────────
    chart_df = _simulate_category_chart(house)
    _render_hero(house, summary)
    _render_category_chart(chart_df)
    st.divider()

    active_year: int | None = None
    if year_radio != "All":
        try:
            active_year = int(year_radio)
        except ValueError:
            active_year = None

    # Year-level export row
    if active_year:
        hcol, ecol = st.columns([6, 2])
        with hcol:
            st.markdown(
                f'<p class="dt-kicker" style="margin:0 0 0.15rem 0;">'
                f'Showing {active_year} declarations</p>',
                unsafe_allow_html=True,
            )
        with ecol:
            year_export_df = _fetch_year_export(house, active_year)
            today = datetime.date.today().isoformat()
            st.download_button(
                f"Export all {active_year}",
                data=year_export_df.to_csv(index=False),
                file_name=f"dail_tracker_interests_{house.lower()}_{active_year}_{today}.csv",
                mime="text/csv",
                use_container_width=True,
                key="btn_year_export",
            )

    result_df = _fetch_interests(
        house=house,
        member_name_q="",
        interest_text_q=interest_q.strip(),
        years=(active_year,) if active_year else (),
        categories=tuple(cat_sel),
        parties=tuple(party_sel),
        constituencies=tuple(constit_sel),
        landlord_only=landlord_only,
        property_only=property_only,
    )

    if result_df.empty:
        st.markdown(
            """<div class="dt-callout">
              <h3>No interests records match these filters</h3>
              <p>Try clearing one or more filters to broaden the search.</p>
            </div>""",
            unsafe_allow_html=True,
        )
    else:
        _render_table(result_df)
        today = datetime.date.today().isoformat()
        st.download_button(
            "Export filtered CSV",
            data=result_df.to_csv(index=False),
            file_name=f"dail_tracker_interests_{today}.csv",
            mime="text/csv",
        )

    _render_provenance(house, summary)


if __name__ == "__main__":
    st.set_page_config(page_title="Interests · Dáil Tracker", layout="wide")
    interests_page()
