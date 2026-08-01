from __future__ import annotations

import os
import urllib.parse

import pandas as pd
import streamlit as st

from ui.entity_links import (
    authority_profile_url,
    company_profile_url,
)
from ui.components import (
    year_selector,
)
from functools import partial

from ui.format import coalesce as _coalesce
from ui.format import esc as _esc
from ui.format import eur
from ui.format import truthy as _truthy

_TOP = 60  # cards shown per non-paginated tab (views are pre-ordered DESC)
_SUP_PAGE = 24  # supplier cards per page (multiple of 3 for the grid)
_AWARD_PAGE = 25  # award rows per page on a supplier profile
_LIVE_PAGE = 24  # open-tender cards per page (multiple of 3 for the grid)

# Canonical formatters (ui.format, 2026-07 consolidation). Award amounts dash
# non-positive values: €0 in this register means "not disclosed", not zero.
_eur = partial(eur, dash_nonpositive=True)
_eur_scale = eur  # headline scale label allowing billions: €23.5bn / €4.2m / €0


def _awards_word(n: int) -> str:
    return f"{n:,} award{'s' if n != 1 else ''}"


def _supplier_href(supplier_norm) -> str:
    # Supplier cards open the first-class /company dossier (entity-first flagship).
    # The in-page ?supplier= profile below is kept so existing deep links still work.
    return company_profile_url(str(supplier_norm))


def _authority_href(authority, *, cross_page: bool = False) -> str:
    """Link to a contracting authority's buyer dossier. ``cross_page=True`` returns the
    absolute /rankings-procurement?authority= form for callers on OTHER pages (the company
    dossier, where the panels below are reused) — a full cross-page nav. The default
    relative ``?authority=`` form is intercepted by spa_links for a soft rerun when the
    click happens on the Procurement page itself (no reload, state preserved)."""
    if cross_page:
        return authority_profile_url(str(authority))
    return f"?authority={urllib.parse.quote(str(authority))}"


def _authority_link(authority, *, cross_page: bool = False) -> str:
    """The authority name as a clickable buyer-dossier link (escaped). Used inside plain
    award rows (NOT rows already wrapped in clickable_card_link — no nested anchors)."""
    name = _esc(authority)
    if not name:
        return "—"
    return (
        f'<a class="pr-auth-link" href="{_esc(_authority_href(authority, cross_page=cross_page))}" '
        f'target="_self">{name}</a>'
    )


def _cpv_href(cpv_code) -> str:
    return f"?cpv={urllib.parse.quote(str(cpv_code))}"


def _ted_winner_href(join_norm) -> str:
    return f"?ted_winner={urllib.parse.quote(str(join_norm))}"


def _single_bid_cpv_href(cpv_division) -> str:
    return f"?single_bid_cpv={urllib.parse.quote(str(cpv_division))}"


def _paid_supplier_href(supplier_norm, tier: str = "SPENT") -> str:
    return f"?paid_supplier={urllib.parse.quote(str(supplier_norm))}&paid_tier={urllib.parse.quote(tier)}"


def _paid_pair_href(supplier_norm, publisher_name, tier: str = "SPENT") -> str:
    """Leaf link: the published line items for ONE supplier × public body × tier. Carrying
    BOTH keys is what breaks the old supplier↔body card loop — the router lands on the
    line-item terminus instead of bouncing to another aggregate."""
    return (
        f"?paid_supplier={urllib.parse.quote(str(supplier_norm))}"
        f"&paid_publisher={urllib.parse.quote(str(publisher_name))}"
        f"&paid_tier={urllib.parse.quote(tier)}"
    )


def _sort_toggle(key: str) -> str:
    """Render a 'Most awards / Highest value' segmented control. Returns the
    ``order_by`` key the core query understands ('awards' | 'value'). Award count
    is the honest default; the value lens is sum-safe value only (the dash-heavy
    long tail sinks to the bottom, surfacing the money leaders)."""
    labels = {"Most awards": "awards", "Highest value": "value"}
    choice = st.segmented_control("Rank by", list(labels), default="Most awards", key=key, label_visibility="collapsed")
    return labels.get(choice or "Most awards", "awards")


def _year_pills(years: list[int]) -> int | None:
    """Year-pill filter for the browse rankings. Returns the chosen calendar year, or
    ``None`` for the all-time default. Renders nothing when no years are available."""
    if not years:
        return None
    return year_selector([str(y) for y in years], key="pr_year", include_all=True)


def _year_label(year: int | None) -> str:
    return f" in {year}" if year else ""


def _yr_axis(df: pd.DataFrame, col: str = "year") -> pd.DataFrame:
    """Render a year column as strings for chart x-axes. st.bar_chart treats an integer year
    as a quantitative axis and labels it '2,016' (thousands separator); a string column is
    nominal, so it shows '2016'. Copy-on-write: never mutates the caller's frame."""
    if col not in getattr(df, "columns", ()):
        return df
    out = df.copy()
    out[col] = out[col].map(lambda v: str(int(v)) if pd.notna(v) else "")  # logic_firewall: display_only
    return out


def _award_year_pills(awards: pd.DataFrame, key: str) -> int | None:
    """Year-pill filter for an award-history list. DISPLAY-ONLY (same posture as the supplier
    search and pagination slice): it derives the distinct award years present in the
    already-fetched frame — no aggregation, no rollup — and returns the chosen year, or None for
    the all-time default. Renders nothing when the history spans a single year or has no dates."""
    if "award_date" not in awards.columns:
        return None
    years = sorted({d.year for d in pd.to_datetime(awards["award_date"], errors="coerce").dropna()}, reverse=True)
    if len(years) <= 1:
        return None
    return year_selector([str(y) for y in years], key=key, include_all=True)


def _filter_awards_by_year(awards: pd.DataFrame, year: int | None) -> pd.DataFrame:
    """Display-only row filter — keep awards dated in ``year`` (None = keep all). Mirrors the
    page's existing name-search filter; no aggregation."""
    if year is None:
        return awards
    return awards[pd.to_datetime(awards["award_date"], errors="coerce").dt.year == year]


# ──────────────────────────────────────────────────────────────────────────────
# Top-level section navigation — synced to ?tab= so the chosen section survives a
# drill-down Back, a refresh, and a round-trip to another page. The old st.tabs
# reset to the first tab on every rerun (the cause of "my selection disappeared
# when I came back from a drill-down"); a URL-backed segmented control does not.
# ──────────────────────────────────────────────────────────────────────────────
# EXPERIMENTAL, LOCAL-ONLY gate. The "Should I bid?" signal section is shown only when
# DAIL_EXPERIMENTAL=1 is set in the environment (set on the local box, never in cloud), so
# it ships nowhere until it's been vetted. Whole feature is self-contained — this flag, the
# v_procurement_bid_signal view, one query fn + cached wrapper, and _render_bid_signal below —
# so it can be promoted or deleted in one pass. See the pricing-by-comparable investigation:
# this surfaces FACTS for a bidder to reason from, never a price (no-inference rule).
_EXPERIMENTAL = os.getenv("DAIL_EXPERIMENTAL") == "1"

_SECTION_LABELS = {
    "Who wins contracts?": "wins",
    "Who actually gets paid?": "paid",
    "Open right now": "open",
    "Patterns": "patterns",
}
if _EXPERIMENTAL:
    _SECTION_LABELS["Should I bid? ⚗"] = "bidsignal"


def _section_picker() -> str:
    """Render the section bar and return the active section key. URL is the source of truth on
    entry (Back / deep link / cross-page return); a click writes it back via the on_change
    callback. Keeps the URL authoritative even when a child widget triggers the rerun."""
    rev = {v: k for k, v in _SECTION_LABELS.items()}
    url_tab = st.query_params.get("tab")
    want = url_tab if url_tab in _SECTION_LABELS.values() else "wins"
    want_label = rev[want]

    def _sync() -> None:
        st.query_params["tab"] = _SECTION_LABELS[st.session_state["pr_section"]]

    if "pr_section" not in st.session_state:
        st.session_state["pr_section"] = want_label
    elif url_tab in _SECTION_LABELS.values() and st.session_state["pr_section"] != want_label:
        # Arrived with an explicit ?tab (Back / deep link) that differs from the widget — URL wins.
        st.session_state["pr_section"] = want_label
    st.segmented_control(
        "Section", list(_SECTION_LABELS), key="pr_section", on_change=_sync, label_visibility="collapsed"
    )
    chosen = _SECTION_LABELS[st.session_state["pr_section"]]
    st.query_params["tab"] = chosen  # authoritative even on child-widget reruns
    return chosen


# The register picker inside "Who wins contracts?" had the exact bug the section picker was
# rewritten to fix: it was a plain segmented_control with no URL sync, so a TED or State Aid
# view could not be deep-linked, shared, or returned to after a drill-down — Back always landed
# the reader on the national register. ?reg= makes those three registers reachable (they were
# also unreachable to the screenshot harness, so they had never been reviewed). 2026-08-01.
_REGISTER_LABELS = {
    "National register (eTenders)": "etenders",
    "EU register (TED)": "ted",
    "EU State Aid (grants)": "stateaid",
    "Register overlaps": "overlaps",
}


def _register_picker() -> str:
    """Render the register bar and return the active LABEL. Same URL-authoritative pattern as
    _section_picker: the URL wins on entry, a click writes it back."""
    rev = {v: k for k, v in _REGISTER_LABELS.items()}
    url_reg = st.query_params.get("reg")
    want_label = rev.get(url_reg, "National register (eTenders)")

    def _sync() -> None:
        st.query_params["reg"] = _REGISTER_LABELS[st.session_state["pr_register"]]

    if "pr_register" not in st.session_state:
        st.session_state["pr_register"] = want_label
    elif url_reg in _REGISTER_LABELS.values() and st.session_state["pr_register"] != want_label:
        st.session_state["pr_register"] = want_label
    st.segmented_control(
        "Register", list(_REGISTER_LABELS), key="pr_register", on_change=_sync, label_visibility="collapsed"
    )
    chosen = st.session_state["pr_register"]
    st.query_params["reg"] = _REGISTER_LABELS[chosen]
    return chosen


def _return_to_browse(section: str) -> None:
    """Back-button action for every drill-down: clear the drill keys but land on the section the
    drill came from (so the reader returns to context, not to the first section)."""
    st.query_params.clear()
    st.query_params["tab"] = section
    st.rerun()


# ──────────────────────────────────────────────────────────────────────────────
# Card builders (all CSS in shared_css.py — pr-* family)
# ──────────────────────────────────────────────────────────────────────────────
def _card(name_html: str, meta: str, pills: list[str], *, rank: int | None = None) -> str:
    rank_html = f'<span class="pr-rank">#{rank}</span>' if rank else ""
    pills_html = "".join(pills)
    pills_sec = f'<div class="pr-pills">{pills_html}</div>' if pills_html else ""
    return (
        f'<div class="pr-card"><div class="pr-card-head">{rank_html}'
        f'<div class="pr-name">{name_html}</div></div>'
        f'<div class="pr-meta">{_esc(meta)}</div>{pills_sec}</div>'
    )


def _value_pill(val) -> str:
    return f'<span class="pr-pill pr-pill-val">{_eur(val)} awarded</span>'


def _afs_bar_row(label: str, amount, max_amount: float, *, fig_html: str, note: str, accent: str) -> str:
    """One horizontal labelled bar for the AFS lanes (net cost / capital by service).

    Width is a pure DISPLAY scaling of ``amount`` against the lane's own max (no aggregation —
    the rows arrive pre-summed and pre-ordered from the view). ``fig_html`` is the right-aligned
    figure, ``note`` the muted sub-label (e.g. self-funding), ``accent`` the bar fill colour."""
    try:
        frac = max(0.0, min(1.0, float(amount) / max_amount)) if max_amount > 0 else 0.0
    except (TypeError, ValueError):
        frac = 0.0
    pct = max(2.0, frac * 100) if frac > 0 else 0.0  # 2% floor so a tiny non-zero bar stays visible
    note_html = f'<span class="pr-afsbar-note">{_esc(note)}</span>' if note else ""
    return (
        '<div class="pr-afsbar">'
        f'<div class="pr-afsbar-top"><span class="pr-afsbar-label">{_esc(label)}</span>'
        f'<span class="pr-afsbar-fig">{fig_html}</span></div>'
        f'<div class="pr-afsbar-track"><div class="pr-afsbar-fill" style="width:{pct:.1f}%;background:{accent}"></div></div>'
        f"{note_html}</div>"
    )


def _cro_pill(row) -> str:
    if not _truthy(getattr(row, "company_num", None)):
        return ""
    status = _esc(_coalesce(getattr(row, "company_status", None)) or "matched")
    return f'<span class="pr-pill pr-pill-cro">CRO: {status}</span>'


def _lobby_pill(row) -> str:
    if not _truthy(getattr(row, "on_lobbying_register", None)):
        return ""
    return '<span class="pr-pill pr-pill-lob">also on lobbying register</span>'
