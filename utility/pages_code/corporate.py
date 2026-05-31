"""
Corporate — standalone browser page.

Sources from the registered DuckDB view v_corporate_notices
(sql_views/corporate_corporate_notices.sql), which reads
data/gold/parquet/corporate_notices.parquet — produced by
pipeline_sandbox/corporate_notices_enrichment.py.

Civic frame: corporate notices in Iris Oifigiúil. The page leads with the
recognition story — who's been calling in Irish loans (brand → parent fund
translation: Promontoria → Cerberus, Beltany → Goldman Sachs etc.) — followed
by a per-company search and a sectioned feed of the recent record.

Personal insolvency (individual bankruptcies) is excluded by policy at the
enrichment layer — see [[feedback_personal_insolvency_privacy]].

Sections (top → bottom):
  1. Editorial hero + glossary strip (SPV / ICAV / SCARP / MVL / CVL) +
     constitutional / privacy caveat
  2. Featured: receiver-appointer ranking (by parent fund) + year sparkline
     of the receiver wave. Honest 31%-coverage subhead.
  3. EXPERIMENTAL: regulated firms in repeat distress — CBI-authorised firms
     that appear in 2+ genuine-distress notices. Sourced from
     v_corporate_cbi_repeat_distress (sandbox), see
     sql_views/corporate_cbi_distress.sql header for provenance.
  4. Per-company search (entity_name primary, raw_text fallback flagged)
  5. Sectioned feed: tabbed by sub-type, month-grouped cards, paginated.
     Cards carry a CBI authorisation badge when notice_ref matches the
     sandbox cross-ref.
  6. Detail view on ?ref= (also carries the CBI badge when matched)
"""
from __future__ import annotations

import datetime
import html
import io
import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.corporate_data import (
    fetch_cbi_notice_matches,
    fetch_cbi_repeat_distress,
    fetch_corporate_notices,
)
from shared_css import inject_css
from ui.components import (
    back_button,
    empty_state,
    fmt_civic_date as _fmt_date,
    glossary_strip,
    hero_banner,
    paginate,
    pagination_controls,
    sidebar_page_header,
    sidebar_subtitle,
)

PAGE_SIZE = 12
FEATURED_TOP_N = 8

_SUBTYPE_LABEL = {
    "receivership": "Receivership",
    "examinership": "Examinership",
    "scarp_process_adviser": "SCARP (small-company rescue)",
    "members_voluntary_liquidation": "Members' Voluntary Liquidation",
    "creditors_voluntary_liquidation": "Creditors' Voluntary Liquidation",
    "voluntary_liquidation_unspecified": "Voluntary Liquidation",
    "liquidation_unspecified": "Liquidation",
    "court_winding_up": "Court Winding-up",
    "companies_act_notice": "Companies Act notice",
    "icav_voluntary_strike_off": "ICAV strike-off",
}

# Tabs in the feed. Order: receiverships first (the lead story), then rescue,
# then volume bulk, then ICAV.
_TYPE_GROUPS = [
    ("All", None),
    ("Receiverships", {"receivership"}),
    ("Examinership", {"examinership", "scarp_process_adviser"}),
    ("Liquidations", {"members_voluntary_liquidation", "creditors_voluntary_liquidation",
                      "voluntary_liquidation_unspecified", "liquidation_unspecified",
                      "court_winding_up"}),
    ("Companies Act notices", {"companies_act_notice"}),
    ("ICAV strike-offs", {"icav_voluntary_strike_off"}),
]

# Junk-pattern rejection for the entity_name display fallback. When entity_name
# matches one of these, the card shows display_title instead (or a graceful
# "Company name not extracted in this notice" if both are junky).
_JUNK_RE = (
    "NOTICE IS HEREBY|ABOVE NAMED|IN THE MATTER|COMPANIES ACT|ICAV ACT|"
    "COLLECTIVE ASSET|^Notice is hereby"
)


# ──────────────────────────────────────────────────────────────────────────────
# Page-local CSS (corp-* family). Same precedent as si-* / pa-*.
# ──────────────────────────────────────────────────────────────────────────────
def _inject_corp_css() -> None:
    st.markdown(
        """
        <style>
        .corp-context {
            font-size: 0.82rem; color: #5b6b73; line-height: 1.5;
            margin: 0.35rem 0 1rem; max-width: 62rem;
        }
        .corp-context strong { color: #14232b; font-weight: 600; }
        .corp-context .corp-privacy {
            display: block; margin-top: 0.4rem;
            color: #7a5a00; font-size: 0.78rem;
        }

        /* FEATURED panel — receiver-appointer recognition story */
        .corp-featured {
            background: #fbf8f1;
            border: 1px solid #e5dfd0;
            border-radius: 10px;
            padding: 1.1rem 1.3rem 1.25rem;
            margin: 0 0 1.6rem;
            display: grid;
            grid-template-columns: minmax(0, 1.6fr) minmax(0, 1fr);
            gap: 1.4rem;
        }
        @media (max-width: 760px) {
            .corp-featured { grid-template-columns: 1fr; gap: 1.1rem; padding: 0.95rem 1rem 1.1rem; }
        }
        .corp-featured-kicker {
            font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.08em;
            color: #7a5a00; font-weight: 600;
        }
        .corp-featured-h {
            font-family: ui-serif, Georgia, serif; font-size: 1.18rem;
            line-height: 1.35; margin: 0.25rem 0 0.55rem; color: #14232b;
        }
        .corp-featured-sub {
            font-size: 0.82rem; color: #5b6b73; margin-bottom: 0.85rem; line-height: 1.45;
        }
        .corp-rank-row {
            display: grid; grid-template-columns: 9rem 1fr 2.5rem;
            align-items: center; gap: 0.65rem;
            padding: 0.22rem 0; font-size: 0.86rem;
        }
        .corp-rank-name {
            color: #14232b; overflow: hidden; text-overflow: ellipsis;
            white-space: nowrap;
        }
        .corp-rank-name .corp-rank-type {
            display: block; font-size: 0.68rem; color: #7a5a00;
            text-transform: uppercase; letter-spacing: 0.05em;
            margin-top: -0.05rem;
        }
        .corp-rank-bar {
            background: #efe6cf; border-radius: 2px; height: 0.55rem;
            overflow: hidden; position: relative;
        }
        .corp-rank-bar > span {
            display: block; height: 100%; background: #6b3f00; border-radius: 2px;
        }
        .corp-rank-count {
            text-align: right; color: #5b6b73; font-variant-numeric: tabular-nums;
            font-size: 0.85rem;
        }

        /* Year sparkline (clickable bars; no JS) */
        .corp-spark-wrap { display: flex; flex-direction: column; gap: 0.35rem; }
        .corp-spark-label {
            font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.07em;
            color: #7a5a00; font-weight: 600;
        }
        .corp-spark-row {
            display: flex; align-items: flex-end; gap: 4px;
            height: 78px; padding: 0.25rem 0.1rem 0;
            border-bottom: 1px solid #d8cda9;
        }
        .corp-spark-bar {
            flex: 1 1 0; min-width: 10px; max-width: 26px;
            background: #c8b787; border-radius: 2px 2px 0 0;
            text-decoration: none;
            transition: background 100ms ease-out;
        }
        .corp-spark-bar:hover { background: #6b3f00; }
        .corp-spark-bar.is-spike { background: #6b3f00; }
        .corp-spark-bar.is-current { outline: 2px solid #14232b; outline-offset: 2px; }
        .corp-spark-years {
            display: flex; justify-content: space-between;
            font-size: 0.65rem; color: #7a5a00; letter-spacing: 0.04em;
            font-variant-numeric: tabular-nums;
        }
        .corp-spark-note {
            font-size: 0.75rem; color: #5b6b73; margin-top: 0.35rem;
        }

        /* Active-filter chip bar */
        .corp-active-bar {
            display: flex; flex-wrap: wrap; gap: 0.4rem; align-items: center;
            padding: 0.5rem 0.75rem; background: #f5f1ea;
            border: 1px solid #e5e2db; border-radius: 6px;
            margin: 0.4rem 0 0.85rem;
        }
        .corp-active-label {
            font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.07em;
            color: #5b6b73; margin-right: 0.3rem;
        }
        .corp-active-chip {
            display: inline-flex; align-items: center; gap: 0.3rem;
            background: #ffffff; border: 1px solid #cfdde6; border-radius: 999px;
            padding: 0.18rem 0.45rem 0.18rem 0.7rem; font-size: 0.78rem;
            color: #14232b; line-height: 1.4; white-space: nowrap;
            text-decoration: none;
            transition: background 120ms ease-out, border-color 120ms ease-out;
        }
        .corp-active-chip:hover { background: #fef2f2; border-color: #fca5a5; color: #7f1d1d; }
        .corp-active-chip:focus-visible { outline: 2px solid #14232b; outline-offset: 1px; }
        .corp-active-chip-x { font-size: 0.95rem; line-height: 1; color: #5b6b73; font-weight: 400; }
        .corp-active-chip:hover .corp-active-chip-x { color: #7f1d1d; }
        .corp-active-chip-all {
            background: transparent; border-color: #5b6b73; color: #5b6b73; padding-right: 0.7rem;
        }
        .corp-active-chip-all:hover { background: #14232b; border-color: #14232b; color: #ffffff; }

        /* Recent feed */
        .corp-month-h {
            font-family: ui-serif, Georgia, serif;
            font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.12em;
            color: #5b6b73; font-weight: 700;
            margin: 1.2rem 0 0.45rem;
            padding-bottom: 0.3rem; border-bottom: 1px solid #e5e2db;
        }
        .corp-month-h:first-of-type { margin-top: 0.4rem; }

        .corp-card-link {
            display: block; text-decoration: none; color: inherit;
            margin-bottom: 0.5rem;
        }
        .corp-card-link:focus-visible {
            outline: 2px solid #14232b; outline-offset: 2px; border-radius: 8px;
        }
        .corp-card-link:hover .corp-card { border-color: #c9cfd3;
            box-shadow: 0 1px 3px rgba(20,35,43,0.08); }
        .corp-card-link:hover .corp-card-who { color: #0a1418; }

        .corp-card {
            background: #ffffff; border: 1px solid #e5e2db; border-radius: 8px;
            padding: 0.85rem 1.05rem;
            display: grid; grid-template-columns: 5.5rem 1fr auto;
            gap: 1rem; align-items: baseline;
            transition: border-color 120ms ease-out, box-shadow 120ms ease-out;
        }
        @media (max-width: 640px) {
            .corp-card { grid-template-columns: 1fr; gap: 0.35rem; padding: 0.75rem 0.9rem; }
            .corp-card-meta { order: -1; }
        }
        .corp-card-date {
            font-variant-numeric: tabular-nums; font-size: 0.82rem; color: #5b6b73;
        }
        .corp-card-who {
            font-family: ui-serif, Georgia, serif; font-size: 1.0rem; line-height: 1.45;
            color: #14232b; margin: 0;
        }
        .corp-card-who .name { font-weight: 600; }
        .corp-card-who .missing { color: #8a7a4a; font-style: italic; }
        .corp-card-meta {
            display: inline-flex; gap: 0.35rem; flex-wrap: wrap;
            justify-content: flex-end; align-items: center;
        }

        .corp-pill {
            display: inline-block; padding: 0.16rem 0.55rem; border-radius: 999px;
            font-size: 0.7rem; line-height: 1.45; white-space: nowrap;
        }
        .corp-pill-subtype {
            background: #f5f1ea; border: 1px solid #e5e2db; color: #14232b;
        }
        .corp-pill-fund {
            background: #f6f0e6; border: 1px solid #e6d9c2; color: #6b3f00;
            font-weight: 500;
        }
        .corp-pill-receiver { background: #f0eaf6; border: 1px solid #cdbfe2; color: #41306a; }
        .corp-pill-examiner { background: #eef3ec; border: 1px solid #cfe0c8; color: #2c4a23; }
        .corp-pill-icav { background: #fff7e6; border: 1px solid #f0d99b; color: #7a5a00; }
        /* CBI authorisation badge — applied on cards/detail when the
           wound-up entity is itself a CBI-authorised firm. */
        .corp-pill-cbi {
            background: #eef4f7; border: 1px solid #b9d0dc; color: #1f4757;
            font-weight: 500;
            font-variant-numeric: tabular-nums;
        }
        .corp-pill-cbi-ref { color: #5b6b73; font-weight: 400; }

        /* CBI repeat-distress panel — sibling of corp-featured but in a
           cooler regulatory tone to distinguish from the receiver-appointer
           story. Marked as experimental in the kicker. */
        .corp-cbi-panel {
            background: #f4f8fb;
            border: 1px solid #cfdde6;
            border-radius: 10px;
            padding: 1.05rem 1.25rem 1.2rem;
            margin: 0 0 1.6rem;
        }
        .corp-cbi-kicker {
            font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.08em;
            color: #1f4757; font-weight: 600;
            display: inline-flex; align-items: center; gap: 0.45rem;
        }
        .corp-cbi-kicker .corp-cbi-tag {
            background: #fffaeb; border: 1px solid #f0d99b; color: #7a5a00;
            border-radius: 999px; padding: 0.05rem 0.45rem; font-size: 0.65rem;
            letter-spacing: 0.05em; font-weight: 600;
        }
        .corp-cbi-h {
            font-family: ui-serif, Georgia, serif; font-size: 1.18rem;
            line-height: 1.35; margin: 0.25rem 0 0.45rem; color: #14232b;
        }
        .corp-cbi-sub {
            font-size: 0.82rem; color: #5b6b73; margin-bottom: 0.85rem; line-height: 1.45;
        }
        .corp-cbi-row {
            display: grid;
            grid-template-columns: minmax(0, 1.8fr) 4rem 4rem minmax(0, 1.2fr);
            gap: 0.65rem; align-items: baseline;
            padding: 0.45rem 0;
            border-top: 1px solid #e0ebf2;
            font-size: 0.86rem;
        }
        .corp-cbi-row:first-of-type { border-top: none; }
        .corp-cbi-row-head {
            font-size: 0.66rem; text-transform: uppercase; letter-spacing: 0.06em;
            color: #5b6b73; padding-bottom: 0.25rem; border-bottom: 1px solid #cfdde6;
            border-top: none;
        }
        .corp-cbi-name {
            color: #14232b; overflow: hidden; text-overflow: ellipsis;
            white-space: nowrap;
        }
        .corp-cbi-name strong { font-weight: 600; }
        .corp-cbi-name a { color: inherit; text-decoration: none; }
        .corp-cbi-name a:hover { text-decoration: underline; }
        .corp-cbi-count {
            text-align: right; color: #14232b; font-variant-numeric: tabular-nums;
        }
        .corp-cbi-count-routine {
            text-align: right; color: #8a7a4a; font-variant-numeric: tabular-nums;
            font-size: 0.82rem;
        }
        .corp-cbi-reg {
            color: #5b6b73; font-size: 0.78rem; line-height: 1.35;
            overflow: hidden; text-overflow: ellipsis;
        }
        .corp-cbi-reg .corp-cbi-refno {
            font-family: ui-monospace, "SF Mono", Menlo, monospace;
            color: #1f4757;
        }
        @media (max-width: 760px) {
            .corp-cbi-row {
                grid-template-columns: 1fr 3.5rem 3.5rem;
            }
            .corp-cbi-row .corp-cbi-reg {
                grid-column: 1 / -1;
                padding-left: 0;
            }
        }

        /* Detail */
        .corp-detail {
            background: #ffffff; border: 1px solid #e5e2db; border-radius: 10px;
            padding: 1.4rem 1.55rem; margin-top: 0.6rem;
        }
        .corp-detail-ref {
            font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 0.78rem;
            color: #5b6b73;
        }
        .corp-detail-title {
            font-family: ui-serif, Georgia, serif; font-size: 1.45rem; line-height: 1.3;
            color: #14232b; margin: 0.35rem 0 1rem;
        }
        .corp-detail-row {
            display: flex; gap: 0.85rem; padding: 0.55rem 0;
            border-top: 1px solid #f0ece5; align-items: flex-start;
        }
        .corp-detail-row:first-of-type { border-top: none; padding-top: 0; }
        .corp-detail-label {
            width: 170px; flex-shrink: 0; font-size: 0.72rem; text-transform: uppercase;
            letter-spacing: 0.06em; color: #5b6b73; padding-top: 0.18rem;
        }
        .corp-detail-val { flex: 1; font-size: 0.95rem; color: #14232b; line-height: 1.55; }
        .corp-detail-raw {
            background: #f9f5ec; border: 1px solid #ead9b3; border-radius: 6px;
            padding: 0.85rem 1rem; margin-top: 0.8rem;
            font-size: 0.88rem; line-height: 1.6; color: #14232b;
            white-space: pre-wrap;
        }
        .corp-detail-raw-kicker {
            font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.08em;
            color: #7a5a00; margin-bottom: 0.4rem;
        }

        /* Per-company search results panel */
        .corp-company-panel {
            background: #ffffff; border: 1px solid #e5e2db; border-radius: 8px;
            padding: 0.9rem 1.1rem; margin: 0.4rem 0 1rem;
        }
        .corp-company-h {
            font-family: ui-serif, Georgia, serif; font-size: 1.05rem;
            color: #14232b; margin: 0 0 0.2rem;
        }
        .corp-company-sub {
            font-size: 0.78rem; color: #5b6b73; margin-bottom: 0.6rem;
        }

        /* Mobile hero tightening (SI/PA precedent) */
        @media (max-width: 640px) {
            .dt-hero .dt-kicker { display: none; }
            .dt-hero .dt-dek {
                display: -webkit-box; -webkit-line-clamp: 2;
                -webkit-box-orient: vertical; overflow: hidden;
                font-size: 0.82rem; margin-top: 0.25rem;
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
@st.cache_data(show_spinner="Loading Corporate notices…")
def load_corporate() -> pd.DataFrame:
    df = fetch_corporate_notices()
    if df.empty:
        return df
    df["issue_date"] = pd.to_datetime(df["issue_date"], errors="coerce")
    df["year"] = df["issue_date"].dt.year
    df = df.reset_index(drop=True)
    # Stable display-time ref for rows where notice_ref is null (split fragments)
    fallback = pd.Series([f"row-{i}" for i in range(len(df))], index=df.index)
    has_ref = df["notice_ref"].notna() & (df["notice_ref"].astype(str).str.strip() != "")
    df["display_ref"] = df["notice_ref"].where(has_ref, fallback)
    # Materialise an exploded "any-parent-mentioned" column for fund filtering
    # without the page doing list-join business logic on its own. Stored as a
    # comma-joined string for easy str.contains() filtering.
    def _join_list(lst, sep=", "):
        if lst is None or not hasattr(lst, "__iter__"):
            return ""
        items = list(lst)
        return sep.join(str(x) for x in items) if items else ""

    df["_parent_mentions_str"] = df["parent_fund_mentions"].apply(_join_list)
    return df


# Same legal-form strip as pipeline_sandbox/cbi_registers_extract._norm_firm —
# inlined here so the page can normalise entity_name → entity_norm at row time
# (the corporate_notices parquet has no stable per-row primary key we can use
# to pre-join; notice_ref is sparse and shared across many rows).
_NORM_SUFFIX_RE = re.compile(
    r"\b(public limited company|limited liability partnership|limited|ltd\.?|"
    r"plc|llp|sa|nv|gmbh|inc\.?)\b\.?",
    re.I,
)


def _norm_entity(name) -> str:
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return ""
    s = str(name)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").lower()
    s = re.sub(r"\bt/?a\b.*$", "", s)
    s = _NORM_SUFFIX_RE.sub(" ", s)
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


@st.cache_data(show_spinner=False)
def load_cbi_badges() -> list[tuple[str, dict]]:
    """Sorted (entity_norm, badge_info) list for on-card CBI badge lookup.

    Sandbox source — see sql_views/corporate_cbi_distress.sql header for
    provenance. Returned as a list sorted by name-length descending so the
    badge resolver can take the *longest* matching CBI firm name appearing as
    a word-boundary substring of a notice's normalised entity_name. This is
    needed because corporate_notices entity_name often carries notice-text
    prefixes ('presented to the High Court by ...', '... in its capacity as
    trustee') that exact-match-equality misses but substring matching
    correctly handles. The CBI side already requires corporate keywords +
    ≥6 chars in the firm name, so substring matching is safe from noise.
    """
    df = fetch_cbi_notice_matches()
    if df.empty:
        return []
    seen: dict[str, dict] = {}
    for _, r in df.iterrows():
        norm = r.get("entity_norm")
        if not norm or len(str(norm)) < 6:
            continue
        if norm not in seen:
            seen[str(norm)] = {
                "register": str(r.get("primary_register") or ""),
                "ref_no":   str(r.get("primary_ref_no") or ""),
            }
    # Longest-first ordering so longest match wins (avoids "wealth" eating
    # "wealth options trustees").
    return sorted(seen.items(), key=lambda kv: -len(kv[0]))


def _resolve_cbi_badge(entity_name, badges: list[tuple[str, dict]]) -> dict | None:
    """Find the longest CBI firm name appearing as a delimited substring of
    the given notice's normalised entity_name. Returns None when no match."""
    if not badges:
        return None
    ent_norm = _norm_entity(entity_name)
    if not ent_norm or len(ent_norm) < 6:
        return None
    padded = f" {ent_norm} "
    for cand_norm, info in badges:
        if f" {cand_norm} " in padded:
            return info
    return None


@st.cache_data(show_spinner=False)
def load_cbi_repeat_distress() -> pd.DataFrame:
    """Per-firm aggregate for the regulated-firms-in-repeat-distress panel."""
    return fetch_cbi_repeat_distress()


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _safe(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v)


def _pretty_subtype(s: str) -> str:
    return _SUBTYPE_LABEL.get(s, (s or "").replace("_", " ").capitalize() or "—")


def _subtype_pill_class(subtype: str) -> str:
    if subtype == "receivership":
        return "corp-pill-receiver"
    if subtype in ("examinership", "scarp_process_adviser"):
        return "corp-pill-examiner"
    if subtype == "icav_voluntary_strike_off":
        return "corp-pill-icav"
    return "corp-pill-subtype"


def _card_name(row: pd.Series) -> tuple[str, bool]:
    """Return (display_name, was_missing_or_junk). Prefers entity_name; falls back
    to display_title when entity_name matches junk patterns or is null."""
    en = _safe(row.get("entity_name"))
    if en and not pd.Series([en]).str.upper().str.contains(_JUNK_RE, regex=True).iloc[0]:
        return en, False
    dt = _safe(row.get("display_title"))
    if dt and not pd.Series([dt]).str.upper().str.contains(_JUNK_RE, regex=True).iloc[0]:
        return dt, True  # cleaner, but flagged as fallback
    return "Company name not extracted in this notice", True


# ──────────────────────────────────────────────────────────────────────────────
# Filters
# ──────────────────────────────────────────────────────────────────────────────
def _apply_filters(df: pd.DataFrame, years: list[int], subtypes: set | None,
                   fund: str, search: str, type_label: str) -> pd.DataFrame:
    out = df
    if years:
        out = out[out["year"].isin(years)]
    if subtypes:
        out = out[out["notice_subtype"].isin(subtypes)]
    if fund and fund != "All":
        out = out[out["_parent_mentions_str"].astype(str).str.contains(fund, case=False, na=False)]
    if search:
        s = search.strip().lower()
        if s:
            mask = (
                out["entity_name"].fillna("").astype(str).str.lower().str.contains(s, na=False)
                | out["display_title"].fillna("").astype(str).str.lower().str.contains(s, na=False)
                | out["raw_text"].fillna("").astype(str).str.lower().str.contains(s, na=False)
            )
            out = out[mask]
    # type_label tracked in chips even though it's the same as subtypes set
    return out


def _clear_facet(key: str) -> None:
    defaults = {
        "year": ([], "corp_year_filter"),
        "fund": ("All", "corp_fund_filter"),
        "search": ("", "corp_search"),
        "type": (0, "corp_type_idx"),
    }
    if key == "all":
        for v, k in defaults.values():
            st.session_state[k] = v
        return
    if key in defaults:
        v, k = defaults[key]
        st.session_state[k] = v


# ──────────────────────────────────────────────────────────────────────────────
# Featured panel — receiver-appointer ranking + year sparkline
# ──────────────────────────────────────────────────────────────────────────────
def _render_featured(df: pd.DataFrame) -> None:
    """logic_firewall: display_only — ranks by parent fund + a small yearly
    trend. All aggregation here is presentation, not modelling."""
    # Receivership-shaped subset, same definition as the enrichment uses.
    recv = df[
        (df["notice_subtype"] == "receivership")
        | df["raw_text"].fillna("").astype(str).str.contains(
            "APPOINTMENT OF (?:STATUTORY )?RECEIVER|NOTICE OF APPOINTMENT OF RECEIVER",
            case=False, regex=True, na=False,
        )
    ]
    n_recv = len(recv)
    if n_recv == 0:
        return

    # Explode parent_fund_mentions and count per parent (each notice counted once
    # per distinct parent).
    parent_rows: list[dict] = []
    for _, r in recv.iterrows():
        parents = r.get("parent_fund_mentions")
        parents = list(parents) if parents is not None and hasattr(parents, "__iter__") else []
        ftypes = r.get("fund_type_mentions")
        ftypes = list(ftypes) if ftypes is not None and hasattr(ftypes, "__iter__") else []
        for i, p in enumerate(parents):
            if p:
                parent_rows.append({
                    "parent": p,
                    "ftype": (ftypes[i] if i < len(ftypes) else "") or "",
                })
    if not parent_rows:
        st.html(
            '<section class="corp-featured" aria-label="Featured receiver-appointer panel">'
            '<div><div class="corp-featured-kicker">Receiver-appointers</div>'
            '<h2 class="corp-featured-h">Who&apos;s calling in Irish loans</h2>'
            f'<div class="corp-featured-sub">No known major loan-book buyer or Irish bank '
            f'mentioned in the current scope (of {n_recv:,} receivership notices).</div>'
            '</div></section>'
        )
        return

    pdf = pd.DataFrame(parent_rows)
    top = pdf.groupby("parent").agg(n=("parent", "size"), ftype=("ftype", "first")).sort_values("n", ascending=False).head(FEATURED_TOP_N)
    n_tagged = int(recv["parent_fund_mentions"].apply(
        lambda x: bool(x is not None and hasattr(x, "__iter__") and len(list(x)) > 0)
    ).sum())
    coverage_pct = round(100 * n_tagged / max(n_recv, 1))

    max_n = int(top["n"].iloc[0])
    rows_html: list[str] = []
    for parent, row in top.iterrows():
        width = max(8, int(round(100 * (int(row["n"]) / max_n))))
        ftype = row.get("ftype") or ""
        rows_html.append(
            f'<a class="corp-rank-row" href="?fund={html.escape(str(parent), quote=True)}" '
            f'target="_self" style="text-decoration:none;color:inherit;" '
            f'aria-label="Filter to {html.escape(parent, quote=True)} ({int(row["n"])} notices)">'
            f'<div class="corp-rank-name" title="{html.escape(parent)}">'
            f'{html.escape(parent)}'
            f'{(f"<span class=\"corp-rank-type\">{html.escape(ftype)}</span>" if ftype else "")}'
            f'</div>'
            f'<div class="corp-rank-bar"><span style="width:{width}%"></span></div>'
            f'<div class="corp-rank-count">{int(row["n"])}</div>'
            '</a>'
        )

    # Year sparkline of the receiver wave.
    yc = recv["year"].dropna().astype(int).value_counts().sort_index()
    if yc.empty:
        spark_html = ""
    else:
        ymin, ymax = int(yc.index.min()), int(yc.index.max())
        years_full = list(range(ymin, ymax + 1))
        counts = [int(yc.get(y, 0)) for y in years_full]
        spike_threshold = sorted(counts, reverse=True)[2] if len(counts) >= 3 else max(counts)
        peak = max(counts) if counts else 1
        current_years = set(int(y) for y in (st.session_state.get("corp_year_filter") or []))

        bars: list[str] = []
        for y, c in zip(years_full, counts):
            h_pct = max(4, int(round(100 * (c / peak)))) if peak else 4
            klass = ["corp-spark-bar"]
            if spike_threshold > 0 and c >= spike_threshold:
                klass.append("is-spike")
            if y in current_years:
                klass.append("is-current")
            tip = f"{y}: {c} receivership{'s' if c != 1 else ''}"
            bars.append(
                f'<a class="{" ".join(klass)}" '
                f'href="?spark={y}" target="_self" '
                f'style="height:{h_pct}%" '
                f'aria-label="{tip}" title="{tip}"></a>'
            )
        spark_html = (
            '<div class="corp-spark-wrap">'
            '<div class="corp-spark-label">Receivership notices by year</div>'
            f'<div class="corp-spark-row">{"".join(bars)}</div>'
            f'<div class="corp-spark-years"><span>{ymin}</span><span>{ymax}</span></div>'
            '<div class="corp-spark-note">Click a year to filter. Peaks 2016-17 (post-crisis clearouts); dip 2020.</div>'
            '</div>'
        )

    st.markdown(
        '<section class="corp-featured" aria-label="Featured receiver-appointer panel">'
        '<div>'
        '<div class="corp-featured-kicker">Receiver-appointers</div>'
        "<h2 class=\"corp-featured-h\">Who's calling in Irish loans</h2>"
        f'<div class="corp-featured-sub">'
        f'Of <strong>{n_recv:,}</strong> receivership notices, '
        f'<strong>{n_tagged:,}</strong> ({coverage_pct}%) name a known loan-book buyer or '
        f'Irish bank. The rest are appointed by smaller institutions or under private '
        f'debentures where no major fund is named. Click a row to filter the page.'
        f'</div>'
        + "".join(rows_html) +
        '</div>'
        f'<div>{spark_html}</div>'
        '</section>',
        unsafe_allow_html=True,
    )


# ──────────────────────────────────────────────────────────────────────────────
# CBI repeat-distress panel (experimental)
# ──────────────────────────────────────────────────────────────────────────────
def _shorten_register(name: str) -> str:
    """Compact a long CBI register name for the side cell of the panel row.

    Order matters: do the long specific matches FIRST while the original
    "Register of" / "Authorised" wording is still intact, then strip the
    generic prefixes for any register names not covered above.
    """
    if not name:
        return ""
    n = name
    # Drop trailing "as at [date]" — also matches when the trailing text is
    # truncated to just "as at" with no date (source data quirk).
    n = re.sub(r"\s+as at\b.*$", "", n).strip()
    # Long-phrase canonical labels FIRST (depend on prefixes still being present).
    long_map = [
        ("Insurance Distribution Register",
         "Insurance Distribution"),
        ("Register of Insurance Distribution",
         "Insurance Distribution"),
        ("Register of Authorised and Registered Alternative Investment Fund Managers",
         "AIFMs"),
        ("Register of Alternative Investment Fund Managers operating in Ireland on a Branc",
         "AIFMs (Branch/Cross-border)"),
        ("Register of Investment Business Firms authorised under Section 10 of the Investm",
         "Investment Business (S.10)"),
        ("Register of Investment Business Firms deemed to be authorised as Investment Inte",
         "Investment Business (RAIPI)"),
        ("Register of Investment Product Intermediaries Section 31 Register",
         "Investment Product Intermediaries (S.31)"),
        ("Authorised UCITS European Communities Undertakings for Collective Investment in",
         "UCITS"),
        ("Register of UCITS Management Companies operating in Ireland on a Branch or Cross",
         "UCITS Mgmt Cos (Branch/Cross-border)"),
        ("Authorised UCITS Management Companies",
         "UCITS Mgmt Cos"),
        ("Register of Trust or Company Service Providers that are Subsidiaries of Credit o",
         "TCSPs (subsidiaries)"),
        ("Register of Credit Unions maintained by the Central Bank of Ireland pursuant to",
         "Credit Unions (PSD)"),
        ("Register of Credit Unions maintained by the Central Bank of Ireland under Regula",
         "Credit Unions (EMI)"),
        ("Register of Crowdfunding Service Providers",
         "Crowdfunding"),
        ("Register of Authorised Crypto Asset Service Providers Under Article 63 of Regula",
         "Crypto (MiCAR)"),
        ("Register of Virtual Asset Service Providers",
         "VASPs"),
        ("Authorised Irish Collective Asset management Vehicles Irish Collective Asset man",
         "ICAV (authorised)"),
        ("Authorised Designated Investment Companies Companies Act 1990 Part XIII",
         "Designated Investment Companies"),
        ("Authorised Investment Limited Partnerships Investment Limited Partnerships Act 1",
         "Investment Limited Partnerships"),
        ("Authorised Common Contractual Funds Investment Funds Companies and Miscellaneous",
         "Common Contractual Funds"),
        ("Authorised Unit Trust Schemes Unit Trust Act 1990",
         "Unit Trust Schemes"),
        ("Register of Registered Irish Collective Asset management Vehicles ICAV",
         "Registered ICAVs"),
        ("Register of Charges created by Irish Collective Asset management Vehicles ICAV",
         "Charges by ICAVs"),
    ]
    for old, new in long_map:
        if old in n:
            n = n.replace(old, new)
            break
    # Generic prefixes for anything not covered above.
    n = n.replace("Register of ", "").replace("List of ", "")
    return n.strip().rstrip(",")


def _render_cbi_repeat_distress(df_cbi: pd.DataFrame) -> None:
    """logic_firewall: display_only — surfaces v_corporate_cbi_repeat_distress
    as a 7-row panel. No aggregations are done here; HAVING clause in the SQL
    view enforces the distress-vs-routine filter so the page can't drift."""
    if df_cbi is None or df_cbi.empty:
        return

    n_firms = len(df_cbi)
    n_distress_total = int(df_cbi["n_distress"].sum())

    rows_html: list[str] = [
        '<div class="corp-cbi-row corp-cbi-row-head">'
        '<div>Firm (CBI-authorised)</div>'
        '<div style="text-align:right;">Distress<br>notices</div>'
        '<div style="text-align:right;">Total<br>notices</div>'
        '<div>Register</div>'
        '</div>'
    ]

    for _, r in df_cbi.iterrows():
        name = _safe(r.get("entity_name")) or "—"
        primary_reg = _shorten_register(_safe(r.get("primary_register")))
        primary_ref = _safe(r.get("primary_ref_no"))
        n_distress = int(r.get("n_distress") or 0)
        n_total = int(r.get("n_notices_total") or 0)
        # Link the firm name to a search on the existing corp_search box
        link = f"?clear=year&q={html.escape(name, quote=True)}"
        ref_html = (
            f' · <span class="corp-cbi-refno">{html.escape(primary_ref)}</span>'
            if primary_ref else ""
        )
        rows_html.append(
            '<div class="corp-cbi-row">'
            f'<div class="corp-cbi-name" title="{html.escape(name)}">'
            f'<a href="{link}" target="_self"><strong>{html.escape(name)}</strong></a>'
            f'</div>'
            f'<div class="corp-cbi-count">{n_distress}</div>'
            f'<div class="corp-cbi-count-routine">{n_total}</div>'
            f'<div class="corp-cbi-reg">{html.escape(primary_reg)}{ref_html}</div>'
            '</div>'
        )

    st.markdown(
        '<section class="corp-cbi-panel" aria-label="Regulated firms in repeat distress (experimental)">'
        '<div class="corp-cbi-kicker">CBI authorisation '
        '<span class="corp-cbi-tag">EXPERIMENTAL</span></div>'
        "<h2 class=\"corp-cbi-h\">Regulated firms with repeat distress notices</h2>"
        '<div class="corp-cbi-sub">'
        f'<strong>{n_firms}</strong> CBI-authorised firms have appeared in '
        f'<strong>{n_distress_total}</strong> receivership, court winding-up, examinership, '
        f'creditors\' liquidation or SCARP notices since 2016 — and at least twice each. '
        f'Solvent fund wind-ups (Members\' Voluntary Liquidation) are filtered out. '
        f'Match is by exact normalised entity name against CBI register snapshots; '
        f'see the sandbox extract for caveats.'
        '</div>'
        + "".join(rows_html) +
        '</section>',
        unsafe_allow_html=True,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Search + facets
# ──────────────────────────────────────────────────────────────────────────────
def _active_filter_chips(full_df: pd.DataFrame) -> list[tuple[str, str]]:
    chips: list[tuple[str, str]] = []
    all_yrs = set(int(y) for y in full_df["year"].dropna().unique())
    yrs = st.session_state.get("corp_year_filter") or []
    if yrs and set(int(y) for y in yrs) != all_yrs:
        if len(yrs) <= 2:
            for y in sorted(yrs, reverse=True):
                chips.append((str(int(y)), "year"))
        else:
            chips.append((f"Years ({len(yrs)})", "year"))
    if (f := st.session_state.get("corp_fund_filter")) and f != "All":
        chips.append((f"Fund: {f}", "fund"))
    s = (st.session_state.get("corp_search") or "").strip()
    if s:
        chips.append((f'"{s}"', "search"))
    return chips


def _render_facets(full_df: pd.DataFrame) -> int:
    """Renders search + facets and returns the selected type-tab index."""
    if full_df.empty:
        return 0

    # Row 1 — full-record search.
    st.text_input(
        "Search by company name",
        placeholder="Search company name, body of notice, anywhere in the corpus…",
        key="corp_search",
        label_visibility="collapsed",
    )

    # Active-filter chip bar.
    active = _active_filter_chips(full_df)
    if active:
        chip_html = "".join(
            f'<a class="corp-active-chip" href="?clear={k}" target="_self" '
            f'aria-label="Remove filter: {html.escape(l, quote=True)}">'
            f'{html.escape(l)}<span class="corp-active-chip-x" aria-hidden="true">×</span>'
            '</a>'
            for l, k in active
        )
        chip_html += (
            '<a class="corp-active-chip corp-active-chip-all" href="?clear=all" '
            'target="_self" aria-label="Clear all filters">Clear all</a>'
        )
        st.html(
            '<div class="corp-active-bar">'
            '<span class="corp-active-label">Filtered by</span>'
            f'{chip_html}</div>'
        )

    # Row 2 — year pills.
    yrs = sorted((int(y) for y in full_df["year"].dropna().unique()), reverse=True)
    yc = full_df["year"].astype("Int64").value_counts().to_dict()
    current_year = datetime.date.today().year
    st.pills(
        "Year",
        yrs,
        default=[],
        selection_mode="multi",
        key="corp_year_filter",
        format_func=lambda y: (
            f"{y} · {yc.get(y, 0):,} YTD" if y == current_year else f"{y} · {yc.get(y, 0):,}"
        ),
    )

    # Row 3 — fund picker (parent funds known in the data).
    parents_in_data: list[str] = []
    for lst in full_df["parent_fund_mentions"]:
        if lst is not None and hasattr(lst, "__iter__"):
            parents_in_data.extend(list(lst))
    fund_counts = pd.Series(parents_in_data).value_counts()
    fund_opts = ["All"] + fund_counts.index.tolist()
    st.selectbox(
        "Appointing fund / bank",
        fund_opts,
        index=0,
        key="corp_fund_filter",
        format_func=lambda x: "All funds and banks" if x == "All" else f"{x} · {int(fund_counts.get(x, 0)):,}",
    )

    # Row 4 — sub-type tabs.
    tab_labels = [g[0] for g in _TYPE_GROUPS]
    type_idx = st.session_state.get("corp_type_idx", 0) or 0
    selected = st.radio(
        "Type",
        tab_labels,
        index=type_idx,
        horizontal=True,
        key="corp_type_radio",
        label_visibility="collapsed",
    )
    new_idx = tab_labels.index(selected)
    if new_idx != type_idx:
        st.session_state["corp_type_idx"] = new_idx
    return new_idx


# ──────────────────────────────────────────────────────────────────────────────
# Feed
# ──────────────────────────────────────────────────────────────────────────────
def _render_card(row: pd.Series, cbi_badges: list[tuple[str, dict]] | None = None) -> str:
    ref = html.escape(_safe(row.get("display_ref")) or "", quote=True)
    subtype = _safe(row.get("notice_subtype"))
    subtype_label = _pretty_subtype(subtype)
    subtype_cls = _subtype_pill_class(subtype)
    parents = row.get("parent_fund_mentions")
    parents = list(parents) if parents is not None and hasattr(parents, "__iter__") else []

    name, is_missing = _card_name(row)
    date_str = _fmt_date(row.get("issue_date"))

    name_html = (
        f'<span class="missing">{html.escape(name)}</span>'
        if is_missing
        else f'<span class="name">{html.escape(name)}</span>'
    )

    pills = [f'<span class="corp-pill {subtype_cls}">{html.escape(subtype_label)}</span>']
    for p in parents[:2]:
        pills.append(f'<span class="corp-pill corp-pill-fund">{html.escape(str(p))}</span>')
    if len(parents) > 2:
        pills.append(f'<span class="corp-pill corp-pill-fund">+{len(parents) - 2}</span>')

    # Experimental: CBI authorisation badge, when the notice's entity is on
    # a Central Bank register. Substring-resolved so notice-text prefixes
    # ("presented to the High Court by …") don't break the match.
    if cbi_badges:
        info = _resolve_cbi_badge(row.get("entity_name"), cbi_badges)
        if info:
            short_reg = _shorten_register(info.get("register", ""))
            refno = info.get("ref_no") or ""
            tip = f"CBI-authorised: {info.get('register','')}" + (f" · {refno}" if refno else "")
            label = "CBI · " + short_reg if short_reg else "CBI-authorised"
            pills.append(
                f'<span class="corp-pill corp-pill-cbi" title="{html.escape(tip)}">'
                f'{html.escape(label)}'
                + (f' <span class="corp-pill-cbi-ref">{html.escape(refno)}</span>' if refno else "")
                + '</span>'
            )

    return (
        f'<a class="corp-card-link" href="?ref={ref}" target="_self">'
        '<div class="corp-card">'
        f'<div class="corp-card-date">{html.escape(date_str)}</div>'
        f'<div class="corp-card-who">{name_html}</div>'
        f'<div class="corp-card-meta">{"".join(pills)}</div>'
        '</div></a>'
    )


def _render_feed(df: pd.DataFrame, cbi_badges: list[tuple[str, dict]] | None = None) -> None:
    if df.empty:
        empty_state(
            "No notices match these filters",
            "Try widening the year, dropping the fund filter, or clearing the search.",
        )
        return

    total = len(df)
    st.html(
        f'<div style="margin:0.4rem 0 0.2rem;font-size:0.85rem;color:#5b6b73;">'
        f'<strong>{total:,}</strong> notice{"s" if total != 1 else ""} '
        f'match the current filters, sorted newest first.'
        f'</div>'
    )

    page_idx = paginate(total, key_prefix="corp_feed", page_size=PAGE_SIZE)
    visible = df.iloc[page_idx * PAGE_SIZE : (page_idx + 1) * PAGE_SIZE]

    last_month: str | None = None
    parts: list[str] = []
    for _, r in visible.iterrows():
        d = r.get("issue_date")
        try:
            month_key = pd.Timestamp(d).strftime("%B %Y").upper() if pd.notna(d) else "UNDATED"
        except Exception:
            month_key = "UNDATED"
        if month_key != last_month:
            parts.append(f'<div class="corp-month-h">{html.escape(month_key)}</div>')
            last_month = month_key
        parts.append(_render_card(r, cbi_badges))

    st.html("".join(parts))

    pagination_controls(
        total,
        key_prefix="corp_feed",
        page_sizes=(PAGE_SIZE,),
        default_page_size=PAGE_SIZE,
        label="notices",
    )


# ──────────────────────────────────────────────────────────────────────────────
# Detail
# ──────────────────────────────────────────────────────────────────────────────
def _render_detail(row: pd.Series, cbi_badges: list[tuple[str, dict]] | None = None) -> None:
    if back_button("← Back to corporate notices", key="corp_detail"):
        st.session_state.pop("corp_selected_ref", None)
        st.query_params.clear()
        st.rerun()

    subtype = _safe(row.get("notice_subtype"))
    subtype_label = _pretty_subtype(subtype)
    subtype_cls = _subtype_pill_class(subtype)
    entity = _safe(row.get("entity_name"))
    display_title = _safe(row.get("display_title"))
    parents = row.get("parent_fund_mentions")
    parents = list(parents) if parents is not None and hasattr(parents, "__iter__") else []
    raw_text = _safe(row.get("raw_text"))
    src_pdf = _safe(row.get("iris_source_pdf"))
    date_str = _fmt_date(row.get("issue_date"))
    ref = _safe(row.get("notice_ref")) or _safe(row.get("display_ref"))

    name, is_missing = _card_name(row)
    headline = name if not is_missing else (display_title or "—")

    # Detail-header CBI badge (experimental).
    cbi_badge_html = ""
    if cbi_badges:
        info = _resolve_cbi_badge(row.get("entity_name"), cbi_badges)
        if info:
            short_reg = _shorten_register(info.get("register", ""))
            refno = info.get("ref_no") or ""
            tip = f"CBI-authorised: {info.get('register','')}" + (f" · {refno}" if refno else "")
            label = "CBI · " + short_reg if short_reg else "CBI-authorised"
            cbi_badge_html = (
                f'<span class="corp-pill corp-pill-cbi" title="{html.escape(tip)}">'
                f'{html.escape(label)}'
                + (f' <span class="corp-pill-cbi-ref">{html.escape(refno)}</span>' if refno else "")
                + '</span> '
            )

    st.html(
        '<div class="corp-detail">'
        f'<div class="corp-detail-ref">{html.escape(ref)}</div>'
        f'<div class="corp-detail-title">{html.escape(headline)}</div>'
        f'<span class="corp-pill {subtype_cls}">{html.escape(subtype_label)}</span> '
        + "".join(
            f'<span class="corp-pill corp-pill-fund">{html.escape(str(p))}</span> '
            for p in parents
        )
        + cbi_badge_html
        + '</div>'
    )

    rows_html: list[str] = []

    def _row(label: str, value_html: str) -> None:
        rows_html.append(
            f'<div class="corp-detail-row">'
            f'<div class="corp-detail-label">{html.escape(label)}</div>'
            f'<div class="corp-detail-val">{value_html}</div>'
            f'</div>'
        )

    _row("Date", html.escape(date_str))
    _row("Notice type", html.escape(subtype_label))
    if entity:
        _row("Entity (extracted)", html.escape(entity))
    if display_title and display_title != entity:
        _row("Display title", html.escape(display_title))
    if parents:
        _row(
            "Parent fund / bank",
            ", ".join(html.escape(str(p)) for p in parents),
        )
    if src_pdf:
        rows_html.append(
            '<div class="corp-detail-row">'
            '<div class="corp-detail-label">Iris Oifigiúil source</div>'
            f'<div class="corp-detail-val" style="font-family:ui-monospace,Menlo,monospace;'
            f'font-size:0.85rem;color:#5b6b73;">{html.escape(src_pdf)}</div>'
            '</div>'
        )

    st.html('<div class="corp-detail">' + "".join(rows_html) + "</div>")

    if raw_text:
        pretty = raw_text.replace(" // ", "\n").replace(" | ", "\n")
        st.html(
            '<div class="corp-detail-raw">'
            '<div class="corp-detail-raw-kicker">Original notice text</div>'
            f'{html.escape(pretty)}'
            '</div>'
        )


# ──────────────────────────────────────────────────────────────────────────────
# Entry
# ──────────────────────────────────────────────────────────────────────────────
def corporate_page() -> None:
    inject_css()
    _inject_corp_css()

    df = load_corporate()
    cbi_badges = load_cbi_badges()
    cbi_repeat = load_cbi_repeat_distress()

    # URL handlers — run before widgets so session_state is right.
    url_ref = st.query_params.get("ref")
    if url_ref:
        st.session_state["corp_selected_ref"] = url_ref

    url_clear = st.query_params.get("clear")
    if url_clear:
        _clear_facet(url_clear)
        del st.query_params["clear"]
        st.rerun()

    url_spark = st.query_params.get("spark")
    if url_spark:
        try:
            yr = int(url_spark)
            st.session_state["corp_year_filter"] = [yr]
        except ValueError:
            pass
        del st.query_params["spark"]
        st.rerun()

    url_fund = st.query_params.get("fund")
    if url_fund:
        st.session_state["corp_fund_filter"] = url_fund
        del st.query_params["fund"]
        st.rerun()

    # Click-through from the CBI repeat-distress panel — set the search box.
    url_q = st.query_params.get("q")
    if url_q:
        st.session_state["corp_search"] = url_q
        del st.query_params["q"]
        st.rerun()

    # Sidebar
    selected = st.session_state.get("corp_selected_ref")
    with st.sidebar:
        sidebar_page_header("Corporate")
        if not selected:
            sidebar_subtitle("Corporate notices · receiver / examinership / liquidation · Iris Oifigiúil")

    # Detail view
    if selected:
        if df.empty:
            empty_state(
                "Corporate data unavailable",
                "The underlying gold parquet did not load. If this persists, regenerate "
                "data/gold/parquet/corporate_notices.parquet via the corporate enrichment.",
            )
            return
        match = df[(df["notice_ref"] == selected) | (df["display_ref"] == selected)]
        if match.empty:
            empty_state(
                f"Notice {selected!r} isn't in the index",
                "Old bookmark or typed URL? Try the search box or browse the recent feed.",
            )
            if back_button("← Back to corporate notices", key="corp_detail_nf"):
                st.session_state.pop("corp_selected_ref", None)
                st.query_params.clear()
                st.rerun()
            return
        _render_detail(match.iloc[0], cbi_badges)
        return

    # Index
    hero_banner(
        kicker="Iris Oifigiúil · Corporate distress",
        title="Corporate",
        dek=(
            "Who's calling in Irish loans, who's being rescued, who's winding down. "
            "Corporate-side notices from Iris Oifigiúil since 2016, with the major "
            "appointing parties translated to their parent funds."
        ),
    )

    # Constitutional / privacy caveat + acronyms.
    st.html(
        '<p class="corp-context">'
        'Brand-to-parent translation is curated in '
        '<code style="color:#14232b">data/_meta/loan_book_fund_aliases.csv</code>; '
        'long-tail SPVs display under their original brand. The page does not name '
        'wrongdoing — it surfaces who is on the public record as an appointing party.'
        '<span class="corp-privacy">'
        '<strong>Privacy:</strong> personal insolvency (individual bankruptcy notices) is '
        'excluded by policy. Companies only.'
        '</span>'
        '</p>'
    )

    glossary_strip([
        ("SPV", "Special-Purpose Vehicle (a fund structure that holds loan books or assets)"),
        ("ICAV", "Irish Collective Asset-management Vehicle"),
        ("MVL", "Members' Voluntary Liquidation (solvent wind-up)"),
        ("CVL", "Creditors' Voluntary Liquidation (insolvent wind-up)"),
        ("SCARP", "Small Company Administrative Rescue Process"),
    ])

    if df.empty:
        empty_state(
            "Corporate data unavailable",
            "The view returned no rows. Check data/gold/parquet/corporate_notices.parquet "
            "and the corporate SQL view registration.",
        )
        return

    # Featured panel — receiver-appointer ranking, independent of filters.
    _render_featured(df)

    # Experimental — regulated firms in repeat distress (CBI x corporate cross-ref).
    _render_cbi_repeat_distress(cbi_repeat)

    # Facets (search + year + fund + type-tab)
    type_idx = _render_facets(df)
    subtype_filter = _TYPE_GROUPS[type_idx][1]

    # Apply filters
    filtered = _apply_filters(
        df,
        years=st.session_state.get("corp_year_filter") or [],
        subtypes=subtype_filter,
        fund=st.session_state.get("corp_fund_filter") or "All",
        search=st.session_state.get("corp_search") or "",
        type_label=_TYPE_GROUPS[type_idx][0],
    )

    # CSV export
    if not filtered.empty:
        csv_cols = [
            "issue_date", "notice_category", "notice_subtype", "entity_name",
            "display_title", "parent_fund_mentions", "brand_mentions",
            "iris_source_pdf",
        ]
        export_df = filtered[csv_cols].copy()
        # Flatten list columns for CSV friendliness.
        def _csv_join(lst):
            if lst is None or not hasattr(lst, "__iter__"):
                return ""
            items = [str(x) for x in lst]
            return "; ".join(items)
        for c in ("parent_fund_mentions", "brand_mentions"):
            export_df[c] = export_df[c].apply(_csv_join)
        buf = io.StringIO()
        export_df.to_csv(buf, index=False)
        st.download_button(
            label=f"Download {len(filtered):,} notices (CSV)",
            data=buf.getvalue(),
            file_name="dail_tracker_corporate_notices.csv",
            mime="text/csv",
            key="corp_csv_download",
        )

    _render_feed(filtered, cbi_badges)


if __name__ == "__main__":
    corporate_page()
