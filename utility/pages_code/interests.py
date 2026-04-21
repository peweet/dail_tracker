from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import streamlit as st
from shared_css import inject_css

_ROOT = Path(__file__).parent.parent.parent

_CSV = {
    "Dáil": _ROOT / "data" / "silver" / "dail_member_interests_combined.csv",
    "Seanad": _ROOT / "data" / "silver" / "seanad_member_interests_combined.csv",
}

_NOTABLE_DAIL = [
    "Michael Healy-Rae",
    "Michael Lowry",
    "Robert Troy",
    "Mary Lou McDonald",
    "Micheál Martin",
    "Simon Harris",
]


def _notable_senators(df: pd.DataFrame, n: int = 6) -> list[str]:
    """Return senators with the most declared interests or property/landlord flags."""
    latest = df["year_declared"].max()
    latest_df = df[df["year_declared"] == latest]
    scored = (
        latest_df.groupby("full_name")
        .agg(
            interest_count=("interest_count", "max"),
            is_landlord=("is_landlord", "any"),
            is_property_owner=("is_property_owner", "any"),
        )
        .reset_index()
    )
    scored = scored[
        (scored["interest_count"] > 0)
        | scored["is_landlord"]
        | scored["is_property_owner"]
    ]
    scored = scored.sort_values("interest_count", ascending=False)
    return scored["full_name"].head(n).tolist()

CATEGORY_ORDER = [
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

CATEGORY_LABELS = {
    "Occupations": "Occupations & Employment",
    "Directorships": "Directorships & Company Roles",
    "Remunerated Position": "Remunerated Positions",
    "Shares": "Shareholdings",
    "Land (including property)": "Land & Property",
    "Contracts": "Contracts",
    "Gifts": "Gifts Received",
    "Travel Facilities": "Travel Facilities",
    "Property supplied or lent or a Service supplied": "Property or Services Supplied",
}


@st.cache_data
def _load(chamber: str) -> pd.DataFrame:
    df = pd.read_csv(_CSV[chamber], low_memory=False)
    df.columns = df.columns.str.strip()
    df = df[df["interest_category"] != "15"]
    df["interest_description_cleaned"] = df["interest_description_cleaned"].fillna("").str.strip()
    df["is_landlord"] = df["is_landlord"].astype(str).str.lower() == "true"
    df["is_property_owner"] = df["is_property_owner"].astype(str).str.lower() == "true"
    # Seanad CSV has no ministerial_office_filled column
    if "ministerial_office_filled" in df.columns:
        df["ministerial_office_filled"] = df["ministerial_office_filled"].astype(str).str.lower() == "true"
    else:
        df["ministerial_office_filled"] = False
    return df


def _real_descriptions(rows: pd.DataFrame) -> list[str]:
    descs = [
        d for d in rows["interest_description_cleaned"].tolist()
        if d and d.lower() not in ("no interests declared", "")
    ]
    return list(dict.fromkeys(descs))  # deduplicate, preserve order


def _css() -> None:
    inject_css()
    # Page-specific styles not in the shared module
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Zilla+Slab:wght@400;600;700&family=Epilogue:ital,wght@0,400;0,500;0,600;1,400&display=swap');

        :root {
            --bg:             oklch(97.5% 0.004 75);
            --surface:        oklch(94%   0.007 75);
            --surface-deep:   oklch(90%   0.010 75);
            --border:         oklch(85%   0.008 75);
            --border-strong:  oklch(72%   0.010 75);
            --text-primary:   oklch(18%   0.008 75);
            --text-secondary: oklch(44%   0.010 75);
            --text-meta:      oklch(58%   0.010 75);
            --accent:         oklch(51%   0.130 62);
            --accent-subtle:  oklch(95%   0.055 72);
            --accent-dim:     oklch(86%   0.040 72);
            --new-bg:         oklch(94%   0.045 145);
            --removed-bg:     oklch(94%   0.030  22);
        }

        html, body, .stApp,
        p, li, label, input, select, textarea,
        button, div.stMarkdown {
            font-family: 'Epilogue', -apple-system, sans-serif !important;
        }
        .stApp { color: var(--text-primary); }

        .stApp { background-color: var(--bg) !important; }

        .main .block-container {
            padding-top: 1.5rem;
            padding-bottom: 4rem;
            max-width: 1300px;
        }

        /* ── Sidebar ─────────────────────────────── */
        [data-testid="stSidebar"] {
            background-color: var(--surface) !important;
            border-right: 1px solid var(--border) !important;
        }
        [data-testid="stSidebar"] > div:first-child {
            padding-top: 1.75rem;
        }
        [data-testid="stSidebarContent"] {
            padding: 0 1rem 1rem 1rem;
        }

        /* ── Headings ────────────────────────────── */
        h1, h2, h3, h4 {
            font-family: 'Zilla Slab', Georgia, serif !important;
            letter-spacing: -0.015em;
        }

        /* ── Text inputs ─────────────────────────── */
        .stTextInput input {
            background: var(--bg) !important;
            border: 1px solid var(--border) !important;
            border-radius: 2px !important;
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.92rem !important;
            color: var(--text-primary) !important;
            padding: 0.45rem 0.75rem !important;
        }
        .stTextInput input:focus {
            border-color: var(--accent) !important;
            box-shadow: 0 0 0 2px var(--accent-dim) !important;
            outline: none !important;
        }

        /* ── Selectbox ───────────────────────────── */
        .stSelectbox > div > div {
            background: var(--bg) !important;
            border: 1px solid var(--border) !important;
            border-radius: 2px !important;
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.92rem !important;
        }

        /* ── Buttons ─────────────────────────────── */
        .stButton > button {
            background: var(--bg) !important;
            border: 1px solid var(--border) !important;
            border-radius: 2px !important;
            color: var(--text-primary) !important;
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.78rem !important;
            font-weight: 600 !important;
            padding: 0.28rem 0.55rem !important;
            text-align: left !important;
            transition: background 100ms ease, border-color 100ms ease !important;
            white-space: nowrap !important;
            overflow: hidden !important;
            text-overflow: ellipsis !important;
        }
        .stButton > button:hover {
            background: var(--accent-subtle) !important;
            border-color: var(--accent) !important;
        }

        /* ── Download button ─────────────────────── */
        .stDownloadButton > button {
            background: var(--text-primary) !important;
            color: var(--bg) !important;
            border: none !important;
            border-radius: 2px !important;
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.78rem !important;
            font-weight: 600 !important;
            letter-spacing: 0.05em !important;
            text-transform: uppercase !important;
            padding: 0.4rem 1rem !important;
        }
        .stDownloadButton > button:hover { opacity: 0.82 !important; }

        /* ── Radio (year pills) ──────────────────── */
        div[data-testid="stRadio"] > label {
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.75rem !important;
            font-weight: 600 !important;
            letter-spacing: 0.07em !important;
            text-transform: uppercase !important;
            color: var(--text-meta) !important;
            margin-bottom: 0.35rem !important;
        }
        div[data-testid="stRadio"] > div {
            flex-direction: row !important;
            flex-wrap: wrap !important;
            gap: 0.35rem !important;
        }
        div[data-testid="stRadio"] > div > label {
            background: var(--surface) !important;
            border: 1px solid var(--border) !important;
            border-radius: 2px !important;
            padding: 0.22rem 0.65rem !important;
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.85rem !important;
            font-weight: 600 !important;
            color: var(--text-secondary) !important;
            cursor: pointer !important;
            transition: all 90ms ease !important;
        }
        div[data-testid="stRadio"] > div > label:has(input:checked) {
            background: var(--accent) !important;
            color: var(--bg) !important;
            border-color: var(--accent) !important;
        }

        /* ── Expander ────────────────────────────── */
        .stExpander {
            border: 1px solid var(--border) !important;
            border-radius: 2px !important;
            background: var(--bg) !important;
            margin-bottom: 0.4rem !important;
        }
        /* Target only the label text — leave the Material icon span untouched */
        .stExpander summary p {
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.78rem !important;
            font-weight: 600 !important;
            letter-spacing: 0.06em !important;
            text-transform: uppercase !important;
            color: var(--text-secondary) !important;
            margin: 0 !important;
        }
        .stExpander summary {
            padding: 0.6rem 0.9rem !important;
            align-items: center !important;
        }
        .stExpander summary:hover { background: var(--surface) !important; }
        details[open] > summary { background: var(--surface) !important; }
        details[open] > summary p { color: var(--text-primary) !important; }

        /* ── Checkbox ────────────────────────────── */
        .stCheckbox > label {
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.85rem !important;
            font-weight: 500 !important;
            color: var(--text-secondary) !important;
        }

        /* ── Divider ─────────────────────────────── */
        hr {
            border: none !important;
            border-top: 1px solid var(--border) !important;
            margin: 1.25rem 0 !important;
        }

        /* ── Alerts ──────────────────────────────── */
        .stAlert {
            border-radius: 2px !important;
            font-family: 'Epilogue', sans-serif !important;
            font-size: 0.88rem !important;
        }

        /* ── Custom components ───────────────────── */
        .page-kicker {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.1em;
            text-transform: uppercase;
            color: var(--accent);
            margin-bottom: 0.3rem;
        }
        .page-title {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.55rem;
            font-weight: 700;
            color: var(--text-primary);
            line-height: 1.15;
            margin-bottom: 0.2rem;
        }
        .page-subtitle {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.8rem;
            color: var(--text-meta);
            line-height: 1.5;
            margin-bottom: 1.2rem;
        }
        .sidebar-label {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.7rem;
            font-weight: 700;
            letter-spacing: 0.09em;
            text-transform: uppercase;
            color: var(--text-meta);
            margin: 0.85rem 0 0.35rem 0;
        }

        .td-header-block {
            padding: 0 0 1.25rem 0;
            margin-bottom: 1.25rem;
        }
        .td-name {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 2.1rem;
            font-weight: 700;
            color: var(--text-primary);
            line-height: 1.1;
            margin: 0 0 0.35rem 0;
        }
        .td-meta {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.88rem;
            color: var(--text-meta);
            font-weight: 500;
            margin-bottom: 0.75rem;
        }
        .td-signals {
            display: flex;
            flex-wrap: wrap;
            gap: 0.4rem;
            margin-top: 0.6rem;
        }
        .signal {
            display: inline-block;
            padding: 0.18rem 0.55rem;
            font-family: 'Epilogue', sans-serif;
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            border-radius: 2px;
        }
        .signal-landlord {
            background: var(--accent-subtle);
            color: var(--accent);
            border: 1px solid var(--accent-dim);
        }
        .signal-property {
            background: var(--surface);
            color: var(--text-secondary);
            border: 1px solid var(--border);
        }
        .signal-minister {
            background: var(--surface-deep);
            color: var(--text-secondary);
            border: 1px solid var(--border-strong);
        }
        .signal-neutral {
            background: var(--surface);
            color: var(--text-meta);
            border: 1px solid var(--border);
        }

        .interest-entry {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.9rem;
            color: var(--text-primary);
            line-height: 1.55;
            padding: 0.45rem 0;
        }
        .interest-entry + .interest-entry {
            border-top: 1px solid var(--border);
        }
        .interest-none {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.85rem;
            color: var(--text-meta);
            font-style: italic;
            padding: 0.35rem 0;
        }
        .interest-new {
            background: var(--new-bg);
            border-radius: 2px;
            padding: 0.4rem 0.65rem;
            margin: 0.2rem 0;
        }
        .interest-removed {
            background: var(--removed-bg);
            border-radius: 2px;
            padding: 0.4rem 0.65rem;
            margin: 0.2rem 0;
            opacity: 0.7;
        }
        .interest-removed span { text-decoration: line-through; }
        .change-tag {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.65rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            padding: 0.1rem 0.35rem;
            border-radius: 2px;
            margin-right: 0.5rem;
            vertical-align: middle;
        }
        .tag-new { background: oklch(38% 0.11 145); color: white; }
        .tag-removed { background: oklch(38% 0.10 22); color: white; }

        .stat-strip {
            display: flex;
            gap: 2.5rem;
            padding: 1rem 0;
            border-top: 1px solid var(--border);
            border-bottom: 1px solid var(--border);
            margin: 1rem 0 1.75rem 0;
        }
        .stat-num {
            font-family: 'Zilla Slab', Georgia, serif;
            font-size: 1.75rem;
            font-weight: 700;
            color: var(--text-primary);
            line-height: 1;
        }
        .stat-lbl {
            font-family: 'Epilogue', sans-serif;
            font-size: 0.72rem;
            font-weight: 600;
            letter-spacing: 0.07em;
            text-transform: uppercase;
            color: var(--text-meta);
            margin-top: 0.2rem;
        }

        .landing-intro {
            max-width: 60ch;
            font-family: 'Epilogue', sans-serif;
            font-size: 0.95rem;
            color: var(--text-secondary);
            line-height: 1.65;
            margin-bottom: 0.5rem;
        }
        .section-rule {
            border: none;
            border-top: 2px solid var(--text-primary);
            margin: 0 0 1.5rem 0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_signals(row: pd.Series) -> str:
    parts = []
    if row.get("is_landlord"):
        parts.append('<span class="signal signal-landlord">Landlord</span>')
    if row.get("is_property_owner") and not row.get("is_landlord"):
        parts.append('<span class="signal signal-property">Property owner</span>')
    if row.get("ministerial_office_filled"):
        parts.append('<span class="signal signal-minister">Ministerial office</span>')
    yr = row.get("year_elected")
    if yr and str(yr) not in ("nan", ""):
        parts.append(f'<span class="signal signal-neutral">First elected {int(yr)}</span>')
    return '<div class="td-signals">' + "".join(parts) + "</div>" if parts else ""


def _render_category(
    cat_rows: pd.DataFrame,
    show_diff: bool,
    prior_rows: pd.DataFrame | None,
) -> None:
    entries = _real_descriptions(cat_rows)
    prior_entries = set(_real_descriptions(prior_rows)) if prior_rows is not None else set()

    if not entries:
        st.markdown('<p class="interest-none">Nothing declared in this category</p>', unsafe_allow_html=True)
        return

    if show_diff and prior_rows is not None:
        current_set = set(entries)
        removed = prior_entries - current_set
        for e in entries:
            is_new = e not in prior_entries
            if is_new:
                st.markdown(
                    f'<div class="interest-entry interest-new">'
                    f'<span class="change-tag tag-new">New</span>{e}</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(f'<div class="interest-entry">{e}</div>', unsafe_allow_html=True)
        for e in sorted(removed):
            st.markdown(
                f'<div class="interest-entry interest-removed">'
                f'<span class="change-tag tag-removed">Removed</span><span>{e}</span></div>',
                unsafe_allow_html=True,
            )
    else:
        for e in entries:
            st.markdown(f'<div class="interest-entry">{e}</div>', unsafe_allow_html=True)


def _render_landing(df: pd.DataFrame) -> None:
    landlord_count = df.groupby("full_name")["is_landlord"].any().sum()
    td_count = df["full_name"].nunique()
    minister_count = df.groupby("full_name")["ministerial_office_filled"].any().sum()
    years = sorted(df["year_declared"].unique())

    st.markdown('<hr class="section-rule">', unsafe_allow_html=True)

    with st.expander("What is this data? (Click for details)", expanded=False):
        st.markdown(
            """
            **About the Register of Members' Interests**

            Every member of the Dáil and Seanad is legally required to declare their financial
            interests each year under the Ethics in Public Office Acts 1995 and 2001. Declarations
            cover property, company directorships, shareholdings, outside income, gifts, travel,
            and other interests that could influence how they vote or act in their public role.

            **Data source:** Declarations are published annually by the Committee on Members'
            Interests and scraped from the official Oireachtas PDFs.

            **How to use this page:**
            - Use the sidebar to search by name or pick a notable TD
            - The landing view shows aggregate stats and a leaderboard of declared interests
            - Selecting a TD shows their full year-by-year record, with landlord and directorship flags highlighted

            **Caveats:**
            - Office holders (Ministers, Ministers of State, Ceann Comhairle) are exempt from
              filing, so their records may be incomplete
            - TDs are not required to declare spouse or dependent interests
            - A high number of declarations reflects transparency, not wrongdoing
            """
        )

    st.markdown(
        f'''
<p class="landing-intro">
Every TD in Dáil Éireann must publicly declare their financial interests each year—including property, company directorships, shareholdings, outside income, gifts, and more. This register covers declarations from {years[0]} to {years[-1]}. Select a TD in the sidebar to view their full record.
</p>
<div style="margin-top:1.2em;margin-bottom:0.5em;"><strong>Important context for interpreting this data:</strong></div>
<ul style="font-size:0.97em;line-height:1.7;max-width:60ch;">
  <li><strong>Office holders</strong> (Ministers, Ministers of State, and the Ceann Comhairle) are not required to declare their interests, so their records may be incomplete.</li>
  <li>The official definition of an office holder is set out in the Dáil’s standing orders. See the full rulebook <a href="https://data.oireachtas.ie/ie/oireachtas/committee/dail/34/committee_on_members_interests_of_dail_eireann/termsOfReference/2025/2025-12-18_guidelines-for-members-of-dail-eireann-who-are-not-office-holders-concerning-the-steps-to-be-taken-by-them-to-ensure-compliance-with-the-provisions-of-the-ethics-in-public-office-acts-1995-and-2001_en.pdf" target="_blank">here (PDF, pg. 5)</a>.</li>
  <li>TDs are not required to declare the interests of their spouse or dependents, so household financial interests may not be fully reflected.</li>
  <li>A higher number of declared interests does not imply wrongdoing—often, it reflects greater transparency.</li>
</ul>
        ''',
        unsafe_allow_html=True,
    )

    st.markdown(
        f'<div class="stat-strip">'
        f'<div><div class="stat-num">{td_count}</div><div class="stat-lbl">TDs on record</div></div>'
        f'<div><div class="stat-num">{landlord_count}</div><div class="stat-lbl">Declared landlords</div></div>'
        f'<div><div class="stat-num">{minister_count}</div><div class="stat-lbl">Have held ministerial office</div></div>'
        f'<div><div class="stat-num">{len(years)}</div><div class="stat-lbl">Years of declarations</div></div>'
        f"</div>",
        unsafe_allow_html=True,
    )

    # ── Leaderboard ───────────────────────────────────────────────
    latest_year = df["year_declared"].max()
    latest = df[df["year_declared"] == latest_year]

    leaderboard = (
        latest.groupby("full_name")
        .agg(
            interest_count=("interest_count", "max"),
            party=("party", "first"),
            is_landlord=("is_landlord", "any"),
        )
        .reset_index()
        .sort_values("interest_count", ascending=False)
        .head(10)
        .reset_index(drop=True)
    )

    st.markdown(
        f'<p style="font-family:\'Epilogue\',sans-serif;font-size:0.72rem;font-weight:700;'
        f'letter-spacing:0.09em;text-transform:uppercase;color:var(--text-meta);margin:1.5rem 0 0.5rem 0;">'
        f"Most declared interests · {latest_year}</p>",
        unsafe_allow_html=True,
    )

    header_cols = st.columns([0.4, 3, 2, 1.2, 1])
    for col, label in zip(
        header_cols,
        ["#", "TD", "Party", "Interests", ""],
    ):
        col.markdown(
            f'<p style="font-family:\'Epilogue\',sans-serif;font-size:0.68rem;font-weight:700;'
            f'letter-spacing:0.08em;text-transform:uppercase;color:var(--text-meta);margin:0 0 0.2rem 0;">'
            f"{label}</p>",
            unsafe_allow_html=True,
        )

    for i, row in leaderboard.iterrows():
        rank = i + 1
        cols = st.columns([0.4, 3, 2, 1.2, 1])
        cols[0].markdown(
            f'<p style="font-family:\'Zilla Slab\',serif;font-size:1rem;font-weight:700;'
            f'color:var(--text-meta);margin:0;padding-top:0.35rem;">{rank}</p>',
            unsafe_allow_html=True,
        )
        landlord_tag = (
            ' &nbsp;<span style="font-size:0.65rem;font-weight:700;letter-spacing:0.06em;'
            'text-transform:uppercase;color:var(--accent);background:var(--accent-subtle);'
            'border:1px solid var(--accent-dim);border-radius:2px;padding:0.1rem 0.35rem;">Landlord</span>'
            if row["is_landlord"]
            else ""
        )
        cols[1].markdown(
            f'<p style="font-family:\'Epilogue\',sans-serif;font-size:0.9rem;font-weight:600;'
            f'color:var(--text-primary);margin:0;padding-top:0.35rem;">{row["full_name"]}{landlord_tag}</p>',
            unsafe_allow_html=True,
        )
        cols[2].markdown(
            f'<p style="font-family:\'Epilogue\',sans-serif;font-size:0.85rem;'
            f'color:var(--text-meta);margin:0;padding-top:0.35rem;">{row["party"]}</p>',
            unsafe_allow_html=True,
        )
        cols[3].markdown(
            f'<p style="font-family:\'Zilla Slab\',serif;font-size:1rem;font-weight:700;'
            f'color:var(--text-primary);margin:0;padding-top:0.35rem;">{int(row["interest_count"])}</p>',
            unsafe_allow_html=True,
        )
        if cols[4].button("View", key=f"lb_{i}", use_container_width=True):
            st.session_state["selected_td"] = row["full_name"]
            st.rerun()


def _render_profile(df: pd.DataFrame, td_name: str) -> None:
    td_df = df[df["full_name"] == td_name].copy()
    if td_df.empty:
        st.warning(f"No data found for {td_name}.")
        return

    info = td_df.iloc[0]
    party = info.get("party", "")
    constituency = info.get("constituency_name", "")
    meta_parts = [x for x in [party, constituency] if x and str(x) != "nan"]
    meta_str = " · ".join(meta_parts)
    signals_html = _render_signals(info)

    st.markdown(
        f'<hr class="section-rule">'
        f'<div class="td-header-block">'
        f'<div class="td-name">{td_name}</div>'
        f'<div class="td-meta">{meta_str}</div>'
        f"{signals_html}"
        f"</div>",
        unsafe_allow_html=True,
    )

    years_available = sorted(td_df["year_declared"].unique(), reverse=True)
    year_labels = [str(y) for y in years_available]

    col_year, col_toggle, col_export = st.columns([3, 2, 2])

    with col_year:
        selected_year_str = st.radio(
            "Declaration year",
            year_labels,
            index=0,
            horizontal=True,
            key=f"year_{td_name}",
        )
        selected_year = int(selected_year_str)

    with col_toggle:
        prior_year = selected_year - 1
        has_prior = prior_year in years_available
        show_diff = False
        if has_prior:
            show_diff = st.checkbox(
                f"Show changes from {prior_year}",
                key=f"diff_{td_name}_{selected_year}",
            )

    year_df = td_df[td_df["year_declared"] == selected_year]
    prior_df = td_df[td_df["year_declared"] == prior_year] if has_prior else None

    with col_export:
        st.markdown("<br>", unsafe_allow_html=True)
        real_rows = year_df[
            ~year_df["interest_description_cleaned"].str.lower().isin(["no interests declared", ""])
        ]
        st.download_button(
            "Export CSV",
            real_rows.to_csv(index=False),
            file_name=f"{td_name.replace(' ', '_')}_interests_{selected_year}.csv",
            mime="text/csv",
        )

    st.markdown("---")

    cats_in_data = year_df["interest_category"].unique().tolist()
    ordered = [c for c in CATEGORY_ORDER if c in cats_in_data]
    ordered += [c for c in cats_in_data if c not in ordered]

    has_any_real = False
    for cat in ordered:
        cat_rows = year_df[year_df["interest_category"] == cat]
        entries = _real_descriptions(cat_rows)
        prior_cat = prior_df[prior_df["interest_category"] == cat] if prior_df is not None else None
        prior_entries = _real_descriptions(prior_cat) if prior_cat is not None else []

        label = CATEGORY_LABELS.get(cat, cat)
        count_str = f" ({len(entries)})" if entries else ""
        diff_str = ""
        if show_diff and prior_entries is not None:
            added = len(set(entries) - set(prior_entries))
            removed = len(set(prior_entries) - set(entries))
            if added or removed:
                parts = []
                if added:
                    parts.append(f"+{added}")
                if removed:
                    parts.append(f"−{removed}")
                diff_str = f" [{', '.join(parts)}]"

        # Expand categories that have real content; collapse empty ones
        default_open = bool(entries)
        if entries:
            has_any_real = True

        with st.expander(f"{label}{count_str}{diff_str}", expanded=default_open):
            _render_category(cat_rows, show_diff, prior_cat)

    if not has_any_real:
        st.info(f"{td_name} has no declared interests on record for {selected_year}.")


def interests_page() -> None:
    _css()

    with st.sidebar:
        st.markdown('<div class="page-kicker">Dáil Tracker</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-title">Interests<br>Register</div>', unsafe_allow_html=True)

        # ── Chamber toggle ────────────────────────────────────────
        chamber = st.radio(
            "Chamber",
            ["Dáil", "Seanad"],
            horizontal=True,
            key="interests_chamber",
            label_visibility="collapsed",
        )

        # Clear selected member when switching chamber
        if st.session_state.get("_last_chamber") != chamber:
            st.session_state["selected_td"] = None
            st.session_state["td_search"] = ""
            st.session_state["_last_chamber"] = chamber

        member_label = "TD" if chamber == "Dáil" else "Senator"
        df = _load(chamber)
        count = df["full_name"].nunique()
        years = sorted(df["year_declared"].unique())

        st.markdown(
            f'<div class="page-subtitle">Declared financial interests<br>'
            f'for all {count} {member_label}s · {years[0]}–{years[-1]}</div>',
            unsafe_allow_html=True,
        )

        st.markdown(f'<p class="sidebar-label">Notable {member_label}s</p>', unsafe_allow_html=True)
        notable = _NOTABLE_DAIL if chamber == "Dáil" else _notable_senators(df)
        btn_cols = st.columns(2)
        for i, name in enumerate(notable):
            short = name.split()[-1]
            if btn_cols[i % 2].button(short, key=f"qs_{chamber}_{i}", use_container_width=True, help=name):
                st.session_state["selected_td"] = name
                st.session_state["td_search"] = ""
                st.rerun()

        st.markdown("---")
        st.markdown(f'<p class="sidebar-label">Search all {member_label}s</p>', unsafe_allow_html=True)

        search = st.text_input(
            "Search",
            placeholder="Type a name…",
            label_visibility="collapsed",
            key="td_search",
        )

        all_members = sorted(df["full_name"].unique())
        query = search.strip().lower()
        filtered = [m for m in all_members if query in m.lower()] if query else all_members

        current = st.session_state.get("selected_td")
        default_idx = filtered.index(current) if current in filtered else None

        chosen = st.selectbox(
            f"Select {member_label}",
            filtered if filtered else [],
            index=default_idx,
            placeholder=f"Select a {member_label}…",
            label_visibility="collapsed",
            disabled=not filtered,
        )
        if chosen and chosen != current:
            st.session_state["selected_td"] = chosen
            st.rerun()

    selected = st.session_state.get("selected_td")

    if selected:
        _render_profile(df, selected)
    else:
        _render_landing(df)
