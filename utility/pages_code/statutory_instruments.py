"""
Statutory Instruments — standalone browser page.

Sources from the registered DuckDB view v_statutory_instruments
(sql_views/legislation_si_index.sql), which reads
data/gold/parquet/statutory_instruments.parquet — produced by
si_entity_enrichment.py. The SI is treated as a first-class entity: the
full ~5,900-SI corpus (2016+), NOT gated on a bill match. No raw parquet
read here; filtering/facets/KPIs happen in pandas off the single
registered frame.

Features:
  1. Editorial hero + KPI strip (totals, top domain, top department, EU share)
  2. Trends — domain × year heatmap, department activity, operation breakdown
  3. Filterable SI index — year / domain / department / operation / EU-only /
     title search, paginated cards
  4. SI detail panel — full taxonomy, irishstatutebook.ie link, Iris source
  5. Cross-link to legislation — the enabling Act, where a confident match
     exists (~30% of SIs).
"""

from __future__ import annotations

import datetime
import html
import re
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.legislation_data import (
    fetch_si_amendments_made,
    fetch_si_entity_index,
    fetch_si_entity_index_classified,
)
from shared_css import inject_css
from ui.components import (
    back_button,
    empty_state,
    fmt_civic_date as _fmt_date,
    hero_banner,
    hide_sidebar,
    paginate,
    pagination_controls,
    render_stat_strip,
    stat_item,
)
from ui.entity_links import member_profile_url, source_link_html

# ── Page config ────────────────────────────────────────────────────────────────
PAGE_SIZE = 10


# ──────────────────────────────────────────────────────────────────────────────
# Page-local CSS. Reuses dt-* tokens but lives here rather than shared_css.py
# so the canonical class set is not polluted by this page's si-* classes.
# ──────────────────────────────────────────────────────────────────────────────
def _inject_si_css() -> None:
    # Use st.markdown with unsafe_allow_html so styles inject into the
    # document head (shared_css.inject_css does the same). st.html()
    # renders inside a sandboxed iframe in recent Streamlit versions,
    # which silently scopes any <style> block to that iframe only and
    # leaves the rest of the page unstyled.
    st.markdown(
        """
        <style>
        .si-stat-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(170px,1fr));
            gap: 0.85rem; margin: 1.1rem 0 1.6rem; }
        .si-stat { background:#ffffff; border:1px solid #e5e2db; border-radius:8px; padding:0.95rem 1.1rem; }
        .si-stat-num { font-family: ui-serif, Georgia, serif; font-size:1.7rem; font-weight:700;
            line-height:1.1; color:#14232b; }
        .si-stat-label { font-size:0.7rem; text-transform:uppercase; letter-spacing:0.07em;
            color:#5b6b73; margin-top:0.35rem; }
        .si-stat-sub { font-size:0.75rem; color:#5b6b73; margin-top:0.15rem; }

        /* Whole card is wrapped in <a class="si-card-link"> — strip the
           default link colour/underline and give it a subtle hover lift so
           the affordance reads as clickable without screaming. */
        .si-card-link { display:block; text-decoration:none; color:inherit;
            margin-bottom:0.55rem; }
        .si-card-link:focus-visible { outline:2px solid #14232b; outline-offset:2px; }
        .si-card-link:hover .si-card { border-color:#c9cfd3;
            box-shadow: 0 1px 3px rgba(20,35,43,0.08); }
        .si-card-link:hover .si-card-title { color:#0a1418; }

        .si-card { background:#ffffff; border:1px solid #e5e2db; border-radius:8px;
            padding:1rem 1.15rem;
            transition: border-color 120ms ease-out, box-shadow 120ms ease-out; }
        .si-card-head { display:flex; align-items:baseline; gap:0.6rem; flex-wrap:wrap; }
        .si-card-ref { font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size:0.78rem;
            color:#5b6b73; }
        .si-card-date { margin-left:auto; font-size:0.76rem; color:#5b6b73; }
        .si-card-title { font-family: ui-serif, Georgia, serif; font-size:1.02rem; line-height:1.4;
            color:#14232b; margin: 0.35rem 0 0.55rem; }
        .si-card-foot { display:flex; gap:0.4rem; flex-wrap:wrap; align-items:center; }

        .si-pill { background:#f5f1ea; border:1px solid #e5e2db; border-radius:999px;
            padding:0.16rem 0.6rem; font-size:0.7rem; color:#14232b; line-height:1.4;
            white-space:nowrap; }
        .si-pill-domain { background:#ecf2f6; border-color:#cfdde6; }
        .si-pill-op     { background:#f6f0e6; border-color:#e6d9c2; }
        .si-pill-eu     { background:#fff7e6; border-color:#f0d99b; color:#7a5a00; }
        .si-pill-act    { background:#e8efe6; border-color:#bcd1b3; color:#2c4a23; }
        .si-pill-dept   { background:#ffffff; border-color:#dfd9cf; color:#5b6b73; }
        /* Legal-state pills on index cards — surface only the negative states
           (revoked / amended), so the list reads as a change-log at a glance. */
        .si-pill-revoked { background:#fbe3e3; border-color:#e3a3a3; color:#9b1c1c; font-weight:600; }
        .si-pill-amended { background:#fbeecb; border-color:#e6c87a; color:#7a5a00; font-weight:600; }
        .si-pill-partial { background:#fbe6cb; border-color:#e6b77a; color:#7a4a00; font-weight:600; }

        .si-detail { background:#ffffff; border:1px solid #e5e2db; border-radius:10px;
            padding:1.4rem 1.55rem; margin-top:0.5rem; }
        .si-detail-ref { font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size:0.82rem;
            color:#5b6b73; }
        .si-detail-title { font-family: ui-serif, Georgia, serif; font-size:1.4rem; line-height:1.3;
            color:#14232b; margin: 0.4rem 0 1rem; }
        .si-detail-row { display:flex; gap:0.85rem; padding:0.55rem 0;
            border-top:1px solid #f0ece5; align-items:flex-start; }
        .si-detail-row:first-of-type { border-top:none; padding-top:0; }
        .si-detail-label { width:170px; flex-shrink:0; font-size:0.72rem; text-transform:uppercase;
            letter-spacing:0.06em; color:#5b6b73; padding-top:0.18rem; }
        .si-detail-val { flex:1; font-size:0.93rem; color:#14232b; line-height:1.5; }
        .si-detail-val .si-pill { margin-right:0.25rem; }

        /* Legal-status block — sits directly under the SI title. Sourced from
           the eISB Legislation Directory (v_si_current_state). Discovery only:
           we surface the negative state eISB records, never a positive "in
           force" claim, and a missing record reads as "status not checked". */
        .si-legal { display:flex; align-items:flex-start; gap:0.7rem; flex-wrap:wrap;
            padding:0.75rem 0.95rem; border:1px solid #e5e2db; border-radius:8px;
            background:#fbfaf7; margin:0.1rem 0 1.05rem; }
        .si-legal-chip { display:inline-flex; align-items:center; border:1px solid;
            border-radius:999px; padding:0.2rem 0.7rem; font-size:0.72rem; font-weight:600;
            text-transform:uppercase; letter-spacing:0.04em; white-space:nowrap; line-height:1.5; }
        .si-legal-body { flex:1; min-width:210px; font-size:0.88rem; color:#14232b; line-height:1.5; }
        .si-legal-src { font-size:0.79rem; color:#5b6b73; margin-top:0.15rem; }
        .si-legal-caveat { font-size:0.76rem; color:#5b6b73; margin-top:0.4rem;
            padding-top:0.35rem; border-top:1px dashed #e5e2db; }
        .si-legal--revoked { background:#fdf2f2; border-color:#f1c6c6; }
        .si-legal--revoked .si-legal-chip { background:#fbe3e3; border-color:#e3a3a3; color:#9b1c1c; }
        .si-legal--partial { background:#fdf6ee; border-color:#eccfa8; }
        .si-legal--partial .si-legal-chip { background:#fbe6cb; border-color:#e6b77a; color:#7a4a00; }
        .si-legal--amended { background:#fdf8ee; border-color:#ecd9a8; }
        .si-legal--amended .si-legal-chip { background:#fbeecb; border-color:#e6c87a; color:#7a5a00; }
        .si-legal--made { background:#f3f7f2; border-color:#cfe0c8; }
        .si-legal--made .si-legal-chip { background:#e6efe3; border-color:#bcd1b3; color:#2c4a23; }
        .si-legal--other, .si-legal--unknown { background:#f6f5f2; border-color:#e0dcd3; }
        .si-legal--other .si-legal-chip, .si-legal--unknown .si-legal-chip {
            background:#efeee9; border-color:#d8d3c8; color:#5b6b73; }

        .si-billlink { background:#fbfcf9; border:1px solid #c9d6c0; border-radius:8px;
            padding:1.1rem 1.25rem; margin-top:1.1rem; }
        .si-billlink-kicker { font-size:0.7rem; text-transform:uppercase; letter-spacing:0.07em;
            color:#2c4a23; font-weight:600; }
        .si-billlink-title { font-family: ui-serif, Georgia, serif; font-size:1.1rem; margin:0.35rem 0 0.45rem;
            color:#14232b; line-height:1.35; }
        .si-billlink-meta { font-size:0.82rem; color:#5b6b73; margin-bottom:0.55rem; }

        /* Amendment graph — the forward direction ("what this instrument
           changes"). Reverse ("amended/revoked by") is the legal-status block.
           Derived from v_si_amendments (inverted from the eISB directory). */
        .si-amends { background:#ffffff; border:1px solid #e5e2db; border-radius:8px;
            padding:1.0rem 1.2rem; margin-top:1.1rem; }
        .si-amends-kicker { font-size:0.7rem; text-transform:uppercase; letter-spacing:0.07em;
            color:#14232b; font-weight:600; margin-bottom:0.6rem; }
        .si-amends-list { list-style:none; margin:0; padding:0; display:flex;
            flex-direction:column; gap:0.5rem; }
        .si-amends-item { display:flex; align-items:baseline; gap:0.55rem; flex-wrap:wrap;
            font-size:0.9rem; line-height:1.45; color:#14232b;
            padding-bottom:0.5rem; border-bottom:1px solid #f0ede7; }
        .si-amends-item:last-child { border-bottom:none; padding-bottom:0; }
        .si-amends-eff { flex-shrink:0; display:inline-flex; align-items:center; border:1px solid;
            border-radius:999px; padding:0.12rem 0.55rem; font-size:0.68rem; font-weight:600;
            text-transform:uppercase; letter-spacing:0.03em; white-space:nowrap; }
        .si-amends-eff--revokes { background:#fbe3e3; border-color:#e3a3a3; color:#9b1c1c; }
        .si-amends-eff--amends { background:#fbeecb; border-color:#e6c87a; color:#7a5a00; }
        .si-amends-prov { color:#5b6b73; font-size:0.8rem; }
        .si-amends-src { font-size:0.76rem; color:#5b6b73; margin-top:0.6rem;
            padding-top:0.45rem; border-top:1px dashed #e5e2db; }

        /* LRC subject classification — discovery/topic only, never legal status.
           Sits alongside the legal-status block; visually quieter (it's an
           index aid, not a consequential state). */
        .si-lrc { display:flex; align-items:flex-start; gap:0.7rem; flex-wrap:wrap;
            padding:0.7rem 0.95rem; border:1px solid #d9e2ea; border-radius:8px;
            background:#f7fafc; margin:0 0 1.05rem; }
        .si-lrc-chip { flex-shrink:0; display:inline-flex; align-items:center; border:1px solid #bcd0de;
            background:#e8f1f7; color:#1f4a63; border-radius:999px; padding:0.2rem 0.7rem;
            font-size:0.68rem; font-weight:600; text-transform:uppercase; letter-spacing:0.04em;
            white-space:nowrap; line-height:1.5; }
        .si-lrc-body { flex:1; min-width:210px; font-size:0.9rem; color:#14232b; line-height:1.45; }
        .si-lrc-leaf { color:#5b6b73; }
        .si-lrc-src { font-size:0.76rem; color:#5b6b73; margin-top:0.3rem; }

        .si-section-h { font-family: ui-serif, Georgia, serif; font-size:1.05rem; margin: 1.5rem 0 0.55rem;
            color:#14232b; }

        .si-trend-card-h { font-size:0.78rem; text-transform:uppercase; letter-spacing:0.07em;
            color:#5b6b73; margin-bottom:0.35rem; }

        /* Active-filter scope bar — read-only chips that show what's
           currently filtered. Modify filters via the facet tabs below. */
        .si-active-bar { display:flex; flex-wrap:wrap; gap:0.4rem; align-items:center;
            padding:0.5rem 0.75rem; background:#f5f1ea; border:1px solid #e5e2db;
            border-radius:6px; margin:0.4rem 0 0.6rem; }
        .si-active-label { font-size:0.7rem; text-transform:uppercase;
            letter-spacing:0.07em; color:#5b6b73; margin-right:0.3rem; }
        .si-active-chip { display:inline-flex; align-items:center; gap:0.3rem;
            background:#ffffff; border:1px solid #cfdde6; border-radius:999px;
            padding:0.18rem 0.45rem 0.18rem 0.7rem; font-size:0.78rem;
            color:#14232b; line-height:1.4; white-space:nowrap;
            text-decoration:none;
            transition: background 120ms ease-out, border-color 120ms ease-out; }
        .si-active-chip:hover { background:#fef2f2; border-color:#fca5a5;
            color:#7f1d1d; }
        .si-active-chip:focus-visible { outline:2px solid #14232b; outline-offset:1px; }
        .si-active-chip-x { font-size:0.95rem; line-height:1; color:#5b6b73;
            font-weight:400; }
        .si-active-chip:hover .si-active-chip-x { color:#7f1d1d; }
        .si-active-chip-all { background:transparent; border-color:#5b6b73;
            color:#5b6b73; padding-right:0.7rem; }
        .si-active-chip-all:hover { background:#14232b; border-color:#14232b;
            color:#ffffff; }

        /* Mobile hero tightening (≤640px). Audit P1-2: at 390×844 the
           hero+dek pushed all data below the fold. The kicker
           ("Iris Oifigiúil · Secondary legislation") is fully redundant
           on mobile — the page title and sidebar already establish
           context — so hide it. Clamp the dek to 2 lines so the search
           input and first KPI/card land above the fold. Page-scoped via
           _inject_si_css so other heroes are unaffected. */
        @media (max-width: 640px) {
            .dt-hero .dt-kicker { display: none; }
            .dt-hero .dt-dek {
                display: -webkit-box;
                -webkit-line-clamp: 2;
                -webkit-box-orient: vertical;
                overflow: hidden;
                font-size: 0.82rem;
                margin-top: 0.25rem;
            }
            .dt-hero { padding: 0.7rem 0.95rem 0.65rem !important; }
            .dt-hero h1 { font-size: 1.2rem !important; }
        }

        </style>
        """,
        unsafe_allow_html=True,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Data loading
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Loading Statutory Instruments…")
def load_si() -> pd.DataFrame:
    """The full SI entity table via the registered v_statutory_instruments
    view. Year floor, taxonomy-confidence, quarantine and category filters are
    already applied upstream by si_entity_enrichment.py.

    Prefers the LRC-classified view (adds lrc_primary_subject etc. for the
    subject chip + topic browse); falls back to the unclassified index if the
    LRC gold table is absent, so the page never goes dark on a missing enrichment.
    """
    df = fetch_si_entity_index_classified()
    if df.empty:
        df = fetch_si_entity_index()
    if df.empty:
        return df
    df["si_signed_date"] = pd.to_datetime(df["si_signed_date"], errors="coerce")
    # Defensive: drop any mojibake-bearing titles.
    df = df[~df["si_title"].astype(str).str.contains("�", na=False)]
    return df.reset_index(drop=True)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _eisb_url(row: pd.Series) -> str:
    """Prefer the extracted eisb_url; fall back to the canonical eli pattern."""
    eisb = row.get("eisb_url")
    if isinstance(eisb, str) and eisb.startswith("http"):
        return eisb
    yr, no = row.get("si_year"), row.get("si_number")
    if pd.notna(yr) and pd.notna(no):
        return f"https://www.irishstatutebook.ie/eli/{int(yr)}/si/{int(no)}/made/en/print"
    return ""


def _safe(v) -> str:
    """Coerce a possibly-NaN/None cell to a string. NaN is truthy, so
    `row.get(x) or ''` does not guard missing values — anything heading for
    html.escape or string ops goes through this."""
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    return str(v)


def _pretty_token(s: str) -> str:
    """snake_case / lowercase taxonomy values → sentence case; mixed-case
    human strings pass through. Pill rows read calmer with sentence case
    than Title Case. Two-axis decision tree:
      - underscores OR plain-lowercase → strip underscores + capitalise
      - already mixed-case → leave alone
    Special-case: tokens beginning with 'eu_'/'eu ' keep the EU prefix
    upper ('Eu instrument referenced' → 'EU instrument referenced')."""
    if not isinstance(s, str) or not s:
        return ""
    if "_" in s and s.lower() == s:
        out = s.replace("_", " ").capitalize()
    elif s == s.lower():
        out = s.capitalize()
    else:
        out = s
    # Preserve the EU acronym at word boundaries (handles 'Eu full effect',
    # 'Eu instrument referenced', and the underscore-converted 'Eu derived').
    if out.lower().startswith("eu "):
        out = "EU" + out[2:]
    return out


def _split_multi(s, sep="|"):
    if not isinstance(s, str):
        return []
    return [p.strip() for p in s.split(sep) if p.strip()]


# ── Legal status (eISB Legislation Directory, via v_si_current_state) ─────────
# Display-only formatting for the legal-state block. The state itself is a
# sourced fact from the directory; these helpers only render it. Per the
# no-inference rule we NEVER present a positive "in force" claim — a null state
# is "status not checked", and even in_force_as_made is phrased as the directory
# fact ("no amendment or revocation recorded"), not a legal assertion.

# state → (css modifier, chip label) for the detail-panel block.
_LEGAL_PRESENTATION = {
    "revoked": ("revoked", "Revoked"),
    "partially_revoked": ("partial", "Partially revoked"),
    "amended_and_partially_revoked": ("partial", "Amended & partly revoked"),
    "amended": ("amended", "Amended"),
    "other_affected": ("other", "Affected"),
    "in_force_as_made": ("made", "No changes recorded"),
}

# state → (card-pill css class, short label). Only the negative states get a
# card pill — in_force/other/unchecked stay bare so the list reads as a
# change-log and we never put a positive "in force" claim on a card.
_CARD_STATE_PILL = {
    "revoked": ("si-pill-revoked", "Revoked"),
    "partially_revoked": ("si-pill-partial", "Part. revoked"),
    "amended_and_partially_revoked": ("si-pill-partial", "Amended/revoked"),
    "amended": ("si-pill-amended", "Amended"),
}

# Legal-status facet — friendly labels + a stable display order. The "__unchecked__"
# sentinel maps to a NULL current_state (SI absent from the directory crawl).
_STATE_FACET_LABELS = {
    "revoked": "Revoked",
    "amended": "Amended",
    "partially_revoked": "Partially revoked",
    "amended_and_partially_revoked": "Amended & part. revoked",
    "other_affected": "Other change",
    "in_force_as_made": "No changes recorded",
    "__unchecked__": "Not checked",
}
_STATE_FACET_ORDER = [
    "revoked",
    "partially_revoked",
    "amended",
    "amended_and_partially_revoked",
    "other_affected",
    "in_force_as_made",
]


def _state_card_pill(state: str) -> str:
    """Pill HTML for an index card's legal state, or '' for states we don't pill."""
    cls_label = _CARD_STATE_PILL.get(state)
    if not cls_label:
        return ""
    cls, label = cls_label
    return f'<span class="si-pill {cls}">{label}</span>'


def _fmt_si_ref(ref) -> str:
    """'332/2025' → 'S.I. No. 332 of 2025' (display formatting only)."""
    s = _safe(ref).strip()
    num, _, yr = s.partition("/")
    return f"S.I. No. {num} of {yr}" if num.isdigit() and yr.isdigit() else s


def _si_ref_eli_url(ref) -> str:
    """Canonical eISB ELI link to an affecting SI's made text — the 'confirm'
    link — built from its 'number/year' citation (same pattern as _eisb_url)."""
    s = _safe(ref).strip()
    num, _, yr = s.partition("/")
    return f"https://www.irishstatutebook.ie/eli/{yr}/si/{num}/made/en/html" if num.isdigit() and yr.isdigit() else ""


def _affecting_list(val) -> list[str]:
    """The joined affecting_sis cell arrives as a list / numpy array / None /
    NaN depending on the LEFT-JOIN match — coerce to a clean list of refs."""
    if val is None:
        return []
    try:
        return [str(x) for x in list(val) if _safe(x)]
    except TypeError:  # scalar NaN for an unmatched (NULL) row
        return []


# ──────────────────────────────────────────────────────────────────────────────
# Filters
# ──────────────────────────────────────────────────────────────────────────────
# The Seanad Committee on EU Scrutiny & Transparency was formally established
# in December 2025. Statutory instruments signed on/after this date are the
# population the committee was meant to scrutinise — and per its chair (Irish
# Times, Feb 2026), zero have been received for prior review.
_COMMITTEE_FORMED = pd.Timestamp("2025-12-01")


def _apply_filters(
    df, years, domain, op, department, minister, eu_only, search, post_committee=False, state=None, subject=None
) -> pd.DataFrame:
    out = df
    if years:
        out = out[out["si_year"].isin(years)]
    if state and state != "All" and "current_state" in out.columns:
        out = out[out["current_state"].isna()] if state == "__unchecked__" else out[out["current_state"] == state]
    if subject and subject != "All" and "lrc_primary_subject" in out.columns:
        out = out[out["lrc_primary_subject"] == subject]
    if domain and domain != "All":
        out = out[out["si_policy_domain"] == domain]
    if op and op != "All":
        out = out[out["si_operation"] == op]
    if department and department != "All":
        out = out[out["si_department_label"] == department]
    if minister and minister != "All":
        out = out[out["si_minister_name"] == minister]
    if eu_only:
        out = out[out["si_is_eu"].fillna(False).astype(bool)]
    if post_committee:
        signed = pd.to_datetime(out["si_signed_date"], errors="coerce")
        out = out[signed >= _COMMITTEE_FORMED]
    if search:
        s = search.strip().lower()
        out = out[out["si_title"].astype(str).str.lower().str.contains(s, na=False)]
    return out


# ──────────────────────────────────────────────────────────────────────────────
# View 1 — KPI strip
# ──────────────────────────────────────────────────────────────────────────────
def _render_kpi_strip(df: pd.DataFrame) -> None:
    """Four cells: total + year span, most active department, EU-derived
    share, enabling-Act link rate. The previous version repeated 'Finance'
    in two adjacent cells (policy-domain top almost always == dept top);
    dropped the domain cell and added the enabling-Act stat — that's the
    bridge to /legislation and the cleanest editorial fact about SI
    provenance."""
    total = len(df)
    if total == 0:
        return
    # logic_firewall: display_only — KPI strip operates on the active filter
    # set (df is post-filter). View-side rollup would need the same filter
    # parameters passed to a registered view; render-time aggregation on a
    # ≤6k-row frame is the simpler call.
    top_dept = df["si_department_label"].dropna().value_counts().head(1)
    eu_count = int(df["si_is_eu"].fillna(False).astype(bool).sum())
    eu_share = (eu_count / total * 100) if total else 0
    bill_count = int(df["bill_id"].notna().sum()) if "bill_id" in df.columns else 0
    bill_share = (bill_count / total * 100) if total else 0
    yrs = sorted(int(y) for y in df["si_year"].dropna().unique())
    yr_span = f"{yrs[0]}–{yrs[-1]}" if len(yrs) >= 2 else (str(yrs[0]) if yrs else "—")

    tdep = top_dept.index[0] if not top_dept.empty else "—"
    tdepc = int(top_dept.iloc[0]) if not top_dept.empty else 0

    # Revoked-count cell — a factual count of eISB-recorded whole revocations
    # over the active frame. Only shown when legal-state has been checked for at
    # least some SIs in scope (else the cell would read a misleading "0").
    revoked_cell = ""
    if "current_state" in df.columns:
        checked = int(df["current_state"].notna().sum())
        if checked:
            revoked = int((df["current_state"] == "revoked").sum())
            revoked_cell = (
                '<div class="si-stat">'
                f'<div class="si-stat-num">{revoked:,}</div>'
                '<div class="si-stat-label">Revoked (per eISB)</div>'
                f'<div class="si-stat-sub">of {checked:,} checked</div>'
                "</div>"
            )

    st.html(f"""
    <div class="si-stat-grid">
      <div class="si-stat">
        <div class="si-stat-num">{total:,}</div>
        <div class="si-stat-label">Statutory Instruments</div>
        <div class="si-stat-sub">{html.escape(yr_span)}</div>
      </div>
      <div class="si-stat">
        <div class="si-stat-num">{html.escape(_pretty_token(tdep))}</div>
        <div class="si-stat-label">Most active department</div>
        <div class="si-stat-sub">{tdepc:,} SIs</div>
      </div>
      <div class="si-stat">
        <div class="si-stat-num">{eu_count:,}</div>
        <div class="si-stat-label">EU-derived</div>
        <div class="si-stat-sub">{eu_share:.0f}% of scope</div>
      </div>
      <div class="si-stat">
        <div class="si-stat-num">{bill_share:.0f}%</div>
        <div class="si-stat-label">Linked to enabling Act</div>
        <div class="si-stat-sub">{bill_count:,} of {total:,} SIs</div>
      </div>
      {revoked_cell}
    </div>
    """)


# ──────────────────────────────────────────────────────────────────────────────
# View 2 — Facet pills (replaces the old static heatmaps + bar charts)
#
# Every chip is the filter for its facet — clicking it sets the corresponding
# session_state key and the SI list below refreshes. Counts come from the
# full corpus so chip widths stay stable across interactions; the index's
# heading reports the post-filter total. ~50 ministers is too many for a
# pill row, so that one falls back to a native selectbox.
# ──────────────────────────────────────────────────────────────────────────────
def _clear_all_filters() -> None:
    """Reset every facet to its 'no filter' state. The chip bar's Clear All
    button calls this; widgets that own these keys pick the cleared values
    up on the next rerun."""
    st.session_state.si_year_filter = []
    st.session_state.si_dept_filter = "All"
    st.session_state.si_op_filter = "All"
    st.session_state.si_domain_filter = "All"
    st.session_state.si_subject_filter = "All"
    st.session_state.si_minister_filter = "All"
    st.session_state.si_state_filter = "All"
    st.session_state.si_eu_filter = False
    st.session_state.si_post_committee_filter = False
    st.session_state.si_title_search = ""


def _set_eu_scrutiny_scope() -> None:
    """The 'Show these SIs' button on the callout / tab. Clears everything
    else and sets EU-derived ON + the precise post-Dec-2025 date filter, so
    the list below matches the scrutiny-gap count exactly."""
    _clear_all_filters()
    st.session_state.si_eu_filter = True
    st.session_state.si_post_committee_filter = True


def _eu_scrutiny_stats(full_df: pd.DataFrame) -> dict:
    eu_mask = full_df["si_is_eu"].fillna(False).astype(bool) & (
        pd.to_datetime(full_df["si_signed_date"], errors="coerce") >= _COMMITTEE_FORMED
    )
    eu_df = full_df[eu_mask]
    return {
        "count": int(len(eu_df)),
        # logic_firewall: display_only — EU scrutiny callout's "top 5
        # departments" panel; aggregation on the post-filter eu_df only.
        "top_depts": eu_df["si_department_label"].dropna().value_counts().head(5).to_dict(),
        "eu_df": eu_df,
    }


_ARTICLE_URL = (
    "https://www.irishtimes.com/politics/2026/02/18/"
    "eu-directives-not-being-passed-to-special-committee-before-being-signed-into-law/"
)
# Source for the €1.54m CJEU fine (work-life balance directive), ruled 1 Aug 2025.
_FINE_URL = (
    "https://www.irishtimes.com/business/work/2025/08/01/"
    "ireland-fined-154m-by-ecj-for-delay-in-writing-work-life-balance-directive-into-irish-law/"
)


def _render_eu_scrutiny_tab(full_df: pd.DataFrame) -> None:
    """Expanded 'EU scrutiny' tab — same data as the callout plus more
    framing and a fuller department breakdown."""
    s = _eu_scrutiny_stats(full_df)
    n = s["count"]
    if n == 0:
        st.caption("No EU-derived SIs have been signed since the committee was established (1 December 2025).")
        return

    st.html(f"""
    <p style="font-size:0.95rem; line-height:1.55; margin: 0.6rem 0 0.7rem;">
      A new <strong>Seanad Committee on EU Scrutiny &amp; Transparency</strong>,
      chaired by Cathaoirleach Mark Daly, was established in December 2025 to
      examine the draft statutory instruments that transpose EU directives
      <em>before</em> they become Irish law. The Taoiseach committed in writing
      that all such instruments would be sent to the committee at least six
      months before their transposition deadlines.
    </p>
    <p style="font-size:0.95rem; line-height:1.55; margin: 0 0 0.8rem;">
      In February 2026, the chair reported that
      <strong>not one draft EU law had been received</strong>
      (<a href="{_ARTICLE_URL}" target="_blank" rel="noopener">Irish Times ↗</a>).
      The State has also paid a <strong>€1.54&nbsp;m</strong> fine for failing to
      transpose the EU work-life balance directive on time
      (<a href="{_FINE_URL}" target="_blank" rel="noopener">Irish Times ↗</a>).
    </p>
    """)

    render_stat_strip(
        stat_item(f"{n:,}", "EU SIs signed since 1 Dec 2025"),
        stat_item(f"{len(s['top_depts'])}+", "Departments transposing"),
    )

    st.markdown("**By department**")
    for d, c in s["top_depts"].items():
        st.html(
            f'<div style="margin:0.18rem 0; font-size:0.9rem;"><strong>{c:,}</strong> &nbsp; {html.escape(d)}</div>'
        )

    st.html('<div style="margin:0.9rem 0;"></div>')

    bc1, bc2 = st.columns(2)
    with bc1:
        # on_click — the button lives inside the tab (after the year-pill
        # widget in script order), so this must mutate state pre-render.
        st.button(
            f"Show these {n} SIs in the list below →",
            key="si_eu_tab_show",
            type="primary",
            on_click=_set_eu_scrutiny_scope,
        )
    with bc2:
        st.markdown(f"[Read the article ↗]({_ARTICLE_URL})")


# Facet → clear handler. Each entry knows how to reset its own widget's
# session-state key. Driven by the ?clear=<key> URL handler at the top of
# the page entry — clicking a chip in the active-filter bar triggers the
# handler before widgets render, so the next rerun sees the cleared state.
def _clear_facet(key: str) -> None:
    if key == "year":
        st.session_state.si_year_filter = []
    elif key == "dept":
        st.session_state.si_dept_filter = "All"
    elif key == "op":
        st.session_state.si_op_filter = "All"
    elif key == "dom":
        st.session_state.si_domain_filter = "All"
    elif key == "subject":
        st.session_state.si_subject_filter = "All"
    elif key == "min":
        st.session_state.si_minister_filter = "All"
    elif key == "state":
        st.session_state.si_state_filter = "All"
    elif key == "eu":
        st.session_state.si_eu_filter = False
    elif key == "post":
        st.session_state.si_post_committee_filter = False
    elif key == "search":
        st.session_state.si_title_search = ""
    elif key == "all":
        _clear_all_filters()


def _active_filter_chips(full_df: pd.DataFrame) -> list[tuple[str, str]]:
    """(label, clear_key) pairs for every facet currently filtering. The
    clear_key feeds into _clear_facet via the ?clear=<key> URL handler.
    Year is collapsed under one 'Years (N)' chip when ≥3 are selected so
    the bar stays readable; clicking the chip clears the year filter
    entirely (rather than picking off years one-by-one)."""
    chips: list[tuple[str, str]] = []
    all_yrs = set(int(y) for y in full_df["si_year"].dropna().unique())
    yrs = st.session_state.get("si_year_filter") or []
    if yrs and set(yrs) != all_yrs:
        if len(yrs) <= 2:
            for y in sorted(yrs, reverse=True):
                chips.append((str(int(y)), "year"))
        else:
            chips.append((f"Years ({len(yrs)})", "year"))
    if (d := st.session_state.get("si_dept_filter")) and d != "All":
        chips.append((d, "dept"))
    if (op := st.session_state.get("si_op_filter")) and op != "All":
        chips.append((_pretty_token(op), "op"))
    if (dom := st.session_state.get("si_domain_filter")) and dom != "All":
        chips.append((_pretty_token(dom), "dom"))
    if (subj := st.session_state.get("si_subject_filter")) and subj != "All":
        chips.append((subj, "subject"))
    if (m := st.session_state.get("si_minister_filter")) and m != "All":
        chips.append((m, "min"))
    if (stt := st.session_state.get("si_state_filter")) and stt != "All":
        chips.append((_STATE_FACET_LABELS.get(stt, stt), "state"))
    if st.session_state.get("si_eu_filter"):
        chips.append(("EU-derived", "eu"))
    if st.session_state.get("si_post_committee_filter"):
        chips.append(("Since Dec 2025", "post"))
    s = (st.session_state.get("si_title_search") or "").strip()
    if s:
        chips.append((f'"{s}"', "search"))
    return chips


def _tab_label(base: str, active_value: str | None) -> str:
    """A tab label that carries its currently-selected value (truncated when
    long). Plain `base` when the facet is unfiltered."""
    if not active_value:
        return base
    val = str(active_value)
    if len(val) > 22:
        val = val[:20] + "…"
    return f"{base}: {val}"


def _render_facets(full_df: pd.DataFrame) -> None:
    if full_df.empty:
        return

    # ── Row 1: search + EU toggle ─────────────────────────────────────────
    c1, c2 = st.columns([3, 1])
    with c1:
        st.text_input(
            "Search title",
            placeholder="Search SI titles — e.g. fisheries, vehicles, sanctions, COVID…",
            key="si_title_search",
            label_visibility="collapsed",
        )
    with c2:
        st.toggle("EU-derived only", key="si_eu_filter")

    # ── Row 2: active-filter scope bar (only when something is filtered) ──
    # Each chip is an <a href="?clear=key"> — clicking it removes that
    # facet's filter via the handler at the top of the page entry. The
    # trailing "Clear all" chip clears every facet at once.
    active = _active_filter_chips(full_df)
    if active:
        chip_html = "".join(
            f'<a class="si-active-chip" href="?clear={key}" target="_self" '
            f'aria-label="Remove filter: {html.escape(lbl, quote=True)}">'
            f'{html.escape(lbl)}<span class="si-active-chip-x" aria-hidden="true">×</span>'
            "</a>"
            for lbl, key in active
        )
        chip_html += (
            '<a class="si-active-chip si-active-chip-all" href="?clear=all" '
            'target="_self" aria-label="Clear all filters">Clear all</a>'
        )
        st.html(f'<div class="si-active-bar"><span class="si-active-label">Filtered by</span>{chip_html}</div>')

    # ── Row 3: Year — always visible, single line, multi-select ──────────
    yrs = sorted((int(y) for y in full_df["si_year"].dropna().unique()), reverse=True)
    # logic_firewall: display_only — year/department/operation/policy_domain/
    # minister value_counts in this block power chip-width labels only
    # ("All departments · 5,910"). They run on the full corpus once per page
    # render; the cost is negligible at this scale.
    yr_counts = full_df["si_year"].astype("Int64").value_counts().to_dict()
    # The current year is necessarily year-to-date; tag it so readers don't
    # compare its partial count against full-year neighbours.
    _current_year = datetime.date.today().year
    st.pills(
        "Year",
        yrs,
        default=yrs[:3] if len(yrs) >= 3 else yrs,
        selection_mode="multi",
        key="si_year_filter",
        format_func=lambda y: (
            f"{y} · {yr_counts.get(y, 0):,} YTD" if y == _current_year else f"{y} · {yr_counts.get(y, 0):,}"
        ),
    )

    # ── Row 4: tabbed primary facets — only one set of pills shows at a
    # time, so the page stays short. The tab label carries its selected
    # value once filtered (e.g. "Department: Justice").
    dept_sel = st.session_state.get("si_dept_filter")
    op_sel = st.session_state.get("si_op_filter")
    dom_sel = st.session_state.get("si_domain_filter")
    subj_sel = st.session_state.get("si_subject_filter")
    min_sel = st.session_state.get("si_minister_filter")
    state_sel = st.session_state.get("si_state_filter")
    tabs = st.tabs(
        [
            _tab_label("Department", dept_sel if dept_sel and dept_sel != "All" else None),
            _tab_label("What it does", _pretty_token(op_sel) if op_sel and op_sel != "All" else None),
            _tab_label("Policy area", _pretty_token(dom_sel) if dom_sel and dom_sel != "All" else None),
            _tab_label("Topic (LRC)", subj_sel if subj_sel and subj_sel != "All" else None),
            _tab_label("Minister", min_sel if min_sel and min_sel != "All" else None),
            _tab_label(
                "Legal status", _STATE_FACET_LABELS.get(state_sel) if state_sel and state_sel != "All" else None
            ),
            "EU scrutiny",
        ]
    )

    with tabs[0]:
        # logic_firewall: display_only — chip-width counts (see panel header).
        dept_counts = full_df["si_department_label"].dropna().value_counts().to_dict()
        dept_opts = ["All"] + sorted(dept_counts, key=dept_counts.get, reverse=True)
        st.pills(
            "Department",
            dept_opts,
            default="All",
            key="si_dept_filter",
            label_visibility="collapsed",
            format_func=lambda x: "All departments" if x == "All" else f"{x} · {dept_counts.get(x, 0):,}",
        )

    with tabs[1]:
        # logic_firewall: display_only — chip-width counts.
        op_counts = full_df["si_operation"].dropna().value_counts().to_dict()
        op_opts = ["All"] + sorted(op_counts, key=op_counts.get, reverse=True)
        st.pills(
            "Operation",
            op_opts,
            default="All",
            key="si_op_filter",
            label_visibility="collapsed",
            format_func=lambda x: "All operations" if x == "All" else f"{_pretty_token(x)} · {op_counts.get(x, 0):,}",
        )

    with tabs[2]:
        # logic_firewall: display_only — chip-width counts.
        dom_counts = full_df["si_policy_domain"].dropna().value_counts().to_dict()
        dom_opts = ["All"] + sorted(dom_counts, key=dom_counts.get, reverse=True)
        st.pills(
            "Policy area",
            dom_opts,
            default="All",
            key="si_domain_filter",
            label_visibility="collapsed",
            format_func=lambda x: (
                "All policy areas" if x == "All" else f"{_pretty_token(x)} · {dom_counts.get(x, 0):,}"
            ),
        )

    with tabs[3]:
        # logic_firewall: display_only — chip-width counts power the pill labels
        # only ("Transport · 237"); run once on the full corpus per render.
        if "lrc_primary_subject" not in full_df.columns:
            st.caption("LRC subject classification isn't available for this corpus yet.")
        else:
            subj_counts = full_df["lrc_primary_subject"].dropna().value_counts().to_dict()
            subj_opts = ["All"] + sorted(subj_counts, key=subj_counts.get, reverse=True)
            st.pills(
                "Topic (LRC)",
                subj_opts,
                default="All",
                key="si_subject_filter",
                label_visibility="collapsed",
                format_func=lambda x: "All topics" if x == "All" else f"{x} · {subj_counts.get(x, 0):,}",
            )
            st.caption(
                "Subject heading from the Law Reform Commission Classified List of "
                "in-force legislation — a discovery aid, not a legal-status claim. "
                "Unmatched SIs (often spent or revoked) are absent from these counts."
            )

    with tabs[4]:
        # logic_firewall: display_only — chip-width counts.
        min_counts = full_df["si_minister_name"].dropna().value_counts().to_dict()
        min_opts = ["All"] + sorted(min_counts, key=min_counts.get, reverse=True)
        st.selectbox(
            "Minister",
            min_opts,
            index=0,
            key="si_minister_filter",
            label_visibility="collapsed",
            format_func=lambda x: "All ministers" if x == "All" else f"{x} · {min_counts.get(x, 0):,}",
        )

    with tabs[5]:
        # logic_firewall: display_only — legal-state value_counts power the
        # chip-width labels only ("Revoked · 1,195"); run once on the full corpus.
        if "current_state" not in full_df.columns:
            st.caption("Legal-state data isn't available for this corpus yet.")
        else:
            sc = full_df["current_state"]
            state_counts = sc.dropna().value_counts().to_dict()
            n_unchecked = int(sc.isna().sum())
            state_opts = ["All"] + [s for s in _STATE_FACET_ORDER if state_counts.get(s)]
            if n_unchecked:
                state_opts.append("__unchecked__")

            def _state_label(x: str) -> str:
                if x == "All":
                    return "All statuses"
                if x == "__unchecked__":
                    return f"Not checked · {n_unchecked:,}"
                return f"{_STATE_FACET_LABELS.get(x, x)} · {state_counts.get(x, 0):,}"

            st.pills(
                "Legal status",
                state_opts,
                default="All",
                key="si_state_filter",
                label_visibility="collapsed",
                format_func=_state_label,
            )
            st.caption(
                "Whether the eISB Legislation Directory records a later amendment or "
                "revocation. “Not checked” = not yet matched to the directory — not a "
                "statement that the SI is in force."
            )

    with tabs[6]:
        _render_eu_scrutiny_tab(full_df)


# ──────────────────────────────────────────────────────────────────────────────
# View 3 — SI index (cards + pagination)
# ──────────────────────────────────────────────────────────────────────────────
def _render_si_card(row: pd.Series) -> str:
    si_id = html.escape(_safe(row.get("si_id")) or "—")
    title = html.escape(_safe(row.get("si_title")) or "—")
    date_str = html.escape(_fmt_date(row.get("si_signed_date")))
    domain = _pretty_token(_safe(row.get("si_policy_domain")))
    op = _pretty_token(_safe(row.get("si_operation")))
    dept = _safe(row.get("si_department_label"))
    bill = _safe(row.get("bill_short_title"))

    pills = []
    # Legal state first — a revoked/amended pill is the most consequential thing
    # on the card, so it leads the pill row (empty string for in-force/unchecked).
    state_pill = _state_card_pill(_safe(row.get("current_state")))
    if state_pill:
        pills.append(state_pill)
    if domain:
        pills.append(f'<span class="si-pill si-pill-domain">{html.escape(domain)}</span>')
    if op:
        pills.append(f'<span class="si-pill si-pill-op">{html.escape(op)}</span>')
    if bool(row.get("si_is_eu")):
        pills.append('<span class="si-pill si-pill-eu">EU-derived</span>')
    if bill:
        pills.append(f'<span class="si-pill si-pill-act">↪ Made under {html.escape(bill)}</span>')
    if dept:
        pills.append(f'<span class="si-pill si-pill-dept">{html.escape(dept)}</span>')

    # Whole card is the click target — Streamlit can't put a native button
    # inside an HTML block, so the previous "<card> + detached View detail
    # button" pattern wasted vertical space and broke the click affordance.
    # An <a href="?si=…"> wrapper lets Streamlit pick up the param change
    # on the next rerun via the listener at the top of the page entry.
    return (
        f'<a class="si-card-link" href="?si={html.escape(si_id, quote=True)}" target="_self">'
        '<div class="si-card">'
        '<div class="si-card-head">'
        f'<span class="si-card-ref">SI No. {si_id}</span>'
        f'<span class="si-card-date">{date_str}</span>'
        "</div>"
        f'<div class="si-card-title">{title}</div>'
        f'<div class="si-card-foot">{"".join(pills)}</div>'
        "</div>"
        "</a>"
    )


def _render_si_index(df: pd.DataFrame) -> None:
    if df.empty:
        empty_state("No SIs in scope", "Adjust the filters to widen the year, domain, department or operation.")
        return

    total = len(df)
    st.html(
        f'<div class="si-section-h">{total:,} statutory instrument'
        f"{'s' if total != 1 else ''} match the current filters</div>"
    )

    page_idx = paginate(total, key_prefix="si_idx", page_size=PAGE_SIZE)
    visible = df.iloc[page_idx * PAGE_SIZE : (page_idx + 1) * PAGE_SIZE]

    for _, row in visible.iterrows():
        st.html(_render_si_card(row))

    pagination_controls(
        total,
        key_prefix="si_idx",
        page_sizes=(PAGE_SIZE,),
        default_page_size=PAGE_SIZE,
        label="SIs",
    )


# ──────────────────────────────────────────────────────────────────────────────
# View 4 — SI detail (+ View 5 cross-link, inline)
# ──────────────────────────────────────────────────────────────────────────────
def _render_legal_status(row: pd.Series) -> None:
    """Legal-status block under the SI title, sourced from the eISB Legislation
    Directory (v_si_current_state, LEFT-joined into v_statutory_instruments).
    Reports the state eISB records — revoked / amended / etc. — with confirm
    links and a verify-before-reliance caveat. A missing directory row renders
    as 'status not checked', never as 'in force'."""
    state = _safe(row.get("current_state"))
    updated = _safe(row.get("directory_updated_to"))
    src_url = _safe(row.get("state_source_url"))
    affecting = _affecting_list(row.get("affecting_sis"))

    # No directory row → not checked. NEVER rendered as "in force".
    if not state:
        eisb = _eisb_url(row)
        verify = (
            source_link_html(
                eisb, "Check the eISB entry", aria_label="Open this SI on the Electronic Irish Statute Book"
            )
            if eisb
            else ""
        )
        st.html(
            '<div class="si-legal si-legal--unknown">'
            '<span class="si-legal-chip">Status not checked</span>'
            '<div class="si-legal-body">Not yet checked against the eISB Legislation Directory '
            "for later amendment or revocation. "
            f"{verify}</div></div>"
        )
        return

    css_mod, chip = _LEGAL_PRESENTATION.get(state, ("other", "Affected"))

    # Affecting SIs in plain English, each linked to its own made text (confirm).
    ref_parts = []
    for ref in affecting:
        url = _si_ref_eli_url(ref)
        label = html.escape(_fmt_si_ref(ref))
        ref_parts.append(source_link_html(url, label) if url else label)
    refs = ", ".join(ref_parts)
    by = f" by {refs}" if refs else ""

    if state == "revoked":
        body = f"Revoked{by}."
    elif state == "partially_revoked":
        body = f"Partially revoked — one or more provisions revoked{by}."
    elif state == "amended_and_partially_revoked":
        body = f"Amended and partially revoked{by}."
    elif state == "amended":
        body = f"Amended{by}."
    elif state == "in_force_as_made":
        body = "The eISB Legislation Directory records no amendment or revocation of this instrument."
    else:  # other_affected — show the directory's own wording, no interpretation
        raw = _safe(row.get("how_affected_raw")).split(" || ")[0].strip()
        body = (
            f"Affected — the directory records: “{html.escape(raw)}”." if raw else "Affected — see the directory entry."
        )

    upd_phrase = f", updated {html.escape(updated)}" if updated else ""
    src_link = (
        source_link_html(
            src_url,
            "View the directory entry",
            aria_label="Open the eISB Legislation Directory entry for this SI",
        )
        if src_url
        else ""
    )
    src_line = f'<div class="si-legal-src">Per the eISB Legislation Directory{upd_phrase}. {src_link}</div>'

    # Verify-before-reliance caveat on every state that is not a plain "no
    # changes recorded" — this is a discovery index, not a legal register.
    caveat = ""
    if state != "in_force_as_made":
        caveat = (
            '<div class="si-legal-caveat">Discovery / indexing only — verify the official eISB '
            "entry before any legal reliance. Whole vs partial revocation is derived from the "
            "directory’s wording.</div>"
        )

    st.html(
        f'<div class="si-legal si-legal--{css_mod}">'
        f'<span class="si-legal-chip">{html.escape(chip)}</span>'
        f'<div class="si-legal-body">{body}{src_line}{caveat}</div>'
        "</div>"
    )


# A provision marker ("Reg. 2", "Sch.", "pt. B", "art. 9") signals that the
# directory's note names a specific provision worth showing alongside the effect
# pill; a bare "Revoked" / "Revoked on <date>" does not.
_PROVISION_MARKER = re.compile(r"\b(reg|regs|art|arts|sch|para|paras|pt|pts|s|ss)\.", re.I)


def _render_lrc_classification(row: pd.Series) -> None:
    """LRC subject classification block — the topic this SI is filed under in the
    Law Reform Commission Classified List. DISCOVERY ONLY: it never asserts legal
    status (that is the legal-status block above). Renders nothing when the SI is
    unmatched, so an absent classification is silent rather than 'unclassified'."""
    subject = _safe(row.get("lrc_primary_subject"))
    if not subject:
        return
    leaf = _safe(row.get("lrc_primary_leaf"))
    updated = _safe(row.get("lrc_list_updated_to"))
    path = f"<strong>{html.escape(subject)}</strong>"
    if leaf and leaf != subject:
        path += f' <span class="si-lrc-leaf">› {html.escape(leaf)}</span>'
    upd = f" (updated {html.escape(updated)})" if updated else ""
    src = (
        '<div class="si-lrc-src">Classified by the Law Reform Commission '
        f"Classified List of in-force legislation{upd}. A source-linked research "
        "aid for discovery, not legal advice — see the legal status above for "
        "amendment/revocation.</div>"
    )
    st.html(
        '<div class="si-lrc"><span class="si-lrc-chip">LRC subject</span>'
        f'<div class="si-lrc-body">{path}{src}</div></div>'
    )


def _render_amendments_made(row: pd.Series) -> None:
    """The forward direction of the SI→SI amendment graph: the instruments THIS
    SI amends or revokes (e.g. a consolidating regulation that sweeps away its
    predecessors). The reverse direction ("amended / revoked BY …") is already
    shown by the legal-status block from affecting_sis, so this surfaces only the
    new, made-side relationships. Reads v_si_amendments via the data-access layer
    — the page computes no edges itself (logic firewall)."""
    try:
        si_year = int(row.get("si_year"))
        si_number = int(row.get("si_number"))
    except (TypeError, ValueError):
        return
    df = fetch_si_amendments_made(si_year, si_number)
    if df.empty:
        return

    items: list[str] = []
    for r in df.itertuples(index=False):
        effect = _safe(r.effect)
        eff_cls = "revokes" if "revok" in effect.lower() else "amends"
        cite = f"S.I. No. {int(r.affected_number)} of {int(r.affected_year)}"
        title = _safe(r.affected_title)
        if title:
            # affected SI is in our index → keep the reader in the tracker
            href = f"?si={int(r.affected_year)}-{int(r.affected_number)}"
            link = (
                f'<a class="dt-source-link" href="{html.escape(href, quote=True)}" target="_self">'
                f"{html.escape(cite)} — {html.escape(title)}</a>"
            )
        else:
            # affected SI predates our index (pre-2016) → link the eISB made text
            url = _safe(r.affected_eli_url)
            link = source_link_html(url, cite) if url else html.escape(cite)
        # provision_note adds value only when it names a specific provision
        # ("Reg. 2 amended", "Sch., pt. B amended"); a bare "Revoked" or a
        # "Revoked on <date>" just duplicates the effect pill, so gate on a
        # provision marker rather than on any digit.
        prov = _safe(r.provision_note)
        prov_html = (
            f' <span class="si-amends-prov">({html.escape(prov)})</span>'
            if prov and _PROVISION_MARKER.search(prov)
            else ""
        )
        items.append(
            f'<li class="si-amends-item">'
            f'<span class="si-amends-eff si-amends-eff--{eff_cls}">{html.escape(effect)}</span>'
            f"<span>{link}{prov_html}</span></li>"
        )

    n = len(items)
    kicker = f"This instrument changes {n} earlier instrument{'s' if n != 1 else ''}"
    src = (
        '<div class="si-amends-src">Amendment relationships derived from the eISB '
        "Legislation Directory (SI→SI only). Discovery / indexing — verify the official "
        "entry before any legal reliance.</div>"
    )
    st.html(
        f'<div class="si-amends"><div class="si-amends-kicker">↳ {html.escape(kicker)}</div>'
        f'<ul class="si-amends-list">{"".join(items)}</ul>{src}</div>'
    )


def _render_si_detail(row: pd.Series) -> None:
    if back_button("← Back to SI Index", key="si_detail"):
        st.session_state.pop("si_selected_id", None)
        st.query_params.clear()
        st.rerun()

    si_id = html.escape(_safe(row.get("si_id")) or "—")
    title = html.escape(_safe(row.get("si_title")) or "—")
    domain = _safe(row.get("si_policy_domain"))
    op = _safe(row.get("si_operation"))
    dept = _safe(row.get("si_department_label"))
    actor = _safe(row.get("si_responsible_actor"))
    min_name = _safe(row.get("si_minister_name"))
    min_code = _safe(row.get("si_minister_member_code"))
    signatory = _safe(row.get("si_signatory_name"))
    signed_date = _fmt_date(row.get("si_signed_date"))
    si_form = _safe(row.get("si_form"))
    eu_rel = _safe(row.get("si_eu_relationship"))
    parent = _safe(row.get("si_parent_legislation"))
    op_flags = _split_multi(_safe(row.get("si_operation_flags")))
    domains = _split_multi(_safe(row.get("si_policy_domains_all")))

    eisb = _eisb_url(row)
    bill_id = _safe(row.get("bill_id"))
    bill_title = _safe(row.get("bill_short_title"))

    st.html(f"""
    <div class="si-detail">
      <div class="si-detail-ref">Statutory Instrument No. {si_id}</div>
      <div class="si-detail-title">{title}</div>
    </div>
    """)

    # Legal status (eISB Legislation Directory) — directly under the title so a
    # revocation/amendment is the first thing the reader sees.
    _render_legal_status(row)

    # LRC subject classification (discovery/topic) — distinct from legal status.
    _render_lrc_classification(row)

    # Department isn't detected on ~60% of pre-2014 SIs; render the cell
    # only when there's a real value rather than parking an em-dash and
    # wasting a quarter of the stat strip.
    stat_items = [
        stat_item(_fmt_date(row.get("si_signed_date")), "Issued"),
        stat_item(_pretty_token(op) or "—", "Operation"),
        stat_item(_pretty_token(domain) or "—", "Policy domain"),
    ]
    if dept:
        stat_items.append(stat_item(dept, "Department"))
    render_stat_strip(*stat_items)

    rows_html: list[str] = []

    def _row(label: str, val_html: str) -> None:
        rows_html.append(
            f'<div class="si-detail-row">'
            f'<div class="si-detail-label">{html.escape(label)}</div>'
            f'<div class="si-detail-val">{val_html}</div>'
            f"</div>"
        )

    _row("SI form", html.escape(_pretty_token(si_form)) or "—")
    _row(
        "Operation flags",
        " ".join(f'<span class="si-pill si-pill-op">{html.escape(_pretty_token(f))}</span>' for f in op_flags)
        if op_flags
        else "—",
    )
    _row(
        "Policy domains",
        " ".join(f'<span class="si-pill si-pill-domain">{html.escape(_pretty_token(d))}</span>' for d in domains)
        if domains
        else "—",
    )
    if eu_rel and eu_rel != "none_detected":
        _row(
            "EU relationship",
            " ".join(
                f'<span class="si-pill si-pill-eu">{html.escape(_pretty_token(e))}</span>' for e in _split_multi(eu_rel)
            ),
        )

    # ── Who signed — office-primary, person attributed ────────────────────────
    # The gazette prints the signing OFFICE (a fact). The PERSON is either
    # printed in a signature block (si_signatory_name — also a fact) or, absent
    # that, inferred from office + signing date against the ministerial-tenure
    # record (si_minister_name — a derivation, labelled as such). We never
    # present the inferred person as if the gazette named them.
    def _profile_link(name: str) -> str:
        # A clickable profile is shown only when the pipeline kept a consistent
        # sitting-member match (contradicting inferences are suppressed upstream
        # in si_entity_enrichment, so a present min_code is safe to link here).
        if min_code:
            return (
                f'<a class="dt-source-link" '
                f'href="{html.escape(member_profile_url(min_code, section="legislation"), quote=True)}" '
                f'target="_self">{html.escape(name)}</a>'
            )
        return html.escape(name)

    def _attrib(text: str) -> str:
        return f' <span style="color:#5b6b73;font-size:0.82rem;">{html.escape(text)}</span>'

    if actor:
        _row("Signing office", html.escape(actor))

    if signatory:
        # Printed in the notice — ground truth.
        _row("Signed by", _profile_link(signatory) + _attrib("— as printed in the notice"))
    elif min_name:
        # Not printed — derived from the signing office + date.
        held = f"— office held on {signed_date}" if signed_date and signed_date != "—" else "— office holder"
        _row(
            "Signed by",
            _profile_link(min_name) + _attrib(f"{held}, per ministerial-tenure record (not printed in the notice)"),
        )
    elif not actor:
        _row("Signing office", _attrib("Not recorded in this notice").strip())
    if parent.strip():
        # Pipe-separated Act names — rendered as plain text now that the
        # /legislation-poc target has been retired.
        pieces = [p.strip(" .,;") for p in parent.split("|") if p.strip(" .,;")]
        _row(
            "Parent legislation",
            " &nbsp;·&nbsp; ".join(html.escape(p) for p in pieces) if pieces else html.escape(parent),
        )
    eisb_html = (
        source_link_html(
            eisb,
            "View on irishstatutebook.ie",
            aria_label="Open this SI on the Electronic Irish Statute Book",
        )
        if eisb
        else ""
    )
    src_iris = _safe(row.get("iris_source_pdf"))
    src_links = []
    if eisb_html:
        src_links.append(eisb_html)
    if src_iris:
        src_links.append(
            f'<span style="color:#5b6b73;font-size:0.85rem;">Iris Oifigiúil source: {html.escape(src_iris)}</span>'
        )
    if src_links:
        _row("Official sources", " &nbsp; · &nbsp; ".join(src_links))

    st.html('<div class="si-detail">' + "".join(rows_html) + "</div>")

    # ── Cross-link panel — the enabling Act ───────────────────────────────────
    if bill_id:
        is_pre2014 = bill_id.startswith("act_")
        ref_label = "Act" if is_pre2014 else "Bill"
        kicker = (
            "↪ Made under (pre-2014 primary Act, curated)"
            if is_pre2014
            else "↪ Made under (matched Act in the Oireachtas index)"
        )
        local_link = (
            f'<a class="dt-source-link" href="/legislation?bill={html.escape(bill_id)}" '
            f'target="_self">View {ref_label} detail →</a>'
        )
        # Pre-2014 bill_ids are internal slugs ('act_1993_statistics') — not
        # useful to the reader. Only show the meta line when bill_id is the
        # canonical Oireachtas reference (post-2014, e.g. 'bill-123-of-2024').
        meta_html = "" if is_pre2014 else f'<div class="si-billlink-meta">{ref_label} {html.escape(bill_id)}</div>'
        st.html(f"""
        <div class="si-billlink">
          <div class="si-billlink-kicker">{kicker}</div>
          <div class="si-billlink-title">{html.escape(bill_title) or "—"}</div>
          {meta_html}
          {local_link}
        </div>
        """)
    elif parent.strip():
        st.html(f"""
        <div class="si-billlink" style="background:#fbf6ed; border-color:#e9dab3;">
          <div class="si-billlink-kicker" style="color:#7a5a00;">Parent legislation (unmatched)</div>
          <div class="si-billlink-title">{html.escape(parent)}</div>
          <div class="si-billlink-meta">
            No confident match against the Oireachtas bills index or the curated
            pre-2014 Acts table. Many SIs are made under framework Acts that
            predate the bills database (2014).
          </div>
        </div>
        """)

    # Forward amendment relationships — what this instrument changes. Renders
    # nothing when this SI amends/revokes no indexed instrument.
    _render_amendments_made(row)


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────
def statutory_instruments_page() -> None:
    inject_css()
    _inject_si_css()

    si_df = load_si()

    url_si = st.query_params.get("si")
    if url_si:
        st.session_state["si_selected_id"] = url_si
    selected = st.session_state.get("si_selected_id")

    # Filter-chip click handler — chips in the active-filter bar link to
    # ?clear=<key>; we drain the param, mutate session state pre-widget,
    # then rerun so widgets pick up the cleared values. Must happen here,
    # before _render_facets instantiates the widgets it references.
    url_clear = st.query_params.get("clear")
    if url_clear:
        _clear_facet(url_clear)
        del st.query_params["clear"]
        st.rerun()

    # ── Sidebar removed (sidebar→filter-bar migration) ──────────────────────────
    # The sidebar was header-only — all filters already live in the main panel
    # (see _render_facets), and page identity is carried by the top-nav tab +
    # the main hero (index) / back button + breadcrumb (detail). hide_sidebar()
    # drops the now-empty rail and the brand band's sidebar-clearing gutter.
    hide_sidebar()

    # ── Detail view ───────────────────────────────────────────────────────────
    if selected:
        match = si_df[si_df["si_id"] == selected]
        if match.empty:
            # The bare st.warning was off-register with the rest of the
            # page; an empty_state in civic voice tells the user what
            # actually happened (corpus floor) and where to go next.
            empty_state(
                f"SI {selected!r} isn't in the index",
                "The Dáil Tracker SI corpus covers 2016 onwards — older "
                "instruments aren't yet ingested. Old bookmark or typed URL? "
                "Try browsing the index, or search by title.",
            )
            if back_button("← Back to SI Index", key="si_detail_nf"):
                st.session_state.pop("si_selected_id", None)
                st.query_params.clear()
                st.rerun()
            return
        _render_si_detail(match.iloc[0])
        return

    # ── Index view ────────────────────────────────────────────────────────────
    hero_banner(
        kicker="Iris Oifigiúil · Secondary legislation",
        title="Statutory Instruments",
        dek=(
            "The regulations, orders and commencement instruments that put Acts "
            "into force and fill in their detail — far more of Irish law happens "
            "here than in primary legislation. Browse the full corpus by policy "
            "domain, responsible department, operation type, or EU origin; every "
            "instrument links to its authoritative text on irishstatutebook.ie."
        ),
    )

    # KPI strip above the fold — anchors the headline numbers BEFORE the
    # facet machinery. Session-state values reflect the prior rerun's
    # widget state, so computing `filtered` here (pre-_render_facets)
    # produces the same result as computing it after. On the very first
    # render of a session, `si_year_filter` doesn't exist yet — fall back
    # to the same default the year-pill widget will use (most recent 3
    # years) so KPIs and the cards below show the same scope. .get() with
    # no default distinguishes 'never set' (use default) from '[] = user
    # cleared the filter' (don't re-seed).
    if "si_year_filter" in st.session_state:
        _year_filter = st.session_state["si_year_filter"] or []
    else:
        _yrs = sorted((int(y) for y in si_df["si_year"].dropna().unique()), reverse=True)
        _year_filter = _yrs[:3] if len(_yrs) >= 3 else _yrs
    filtered = _apply_filters(
        si_df,
        years=_year_filter,
        domain=st.session_state.get("si_domain_filter"),
        op=st.session_state.get("si_op_filter"),
        department=st.session_state.get("si_dept_filter"),
        minister=st.session_state.get("si_minister_filter"),
        eu_only=st.session_state.get("si_eu_filter", False),
        post_committee=st.session_state.get("si_post_committee_filter", False),
        search=st.session_state.get("si_title_search"),
        state=st.session_state.get("si_state_filter"),
        subject=st.session_state.get("si_subject_filter"),
    )
    _render_kpi_strip(filtered)

    # Facets next — each chip is a click-to-filter control with the SI
    # count baked into the label. Changing a facet reruns the page and
    # the KPI strip above updates with the new scope.
    _render_facets(si_df)

    _render_si_index(filtered)


if __name__ == "__main__":
    statutory_instruments_page()
