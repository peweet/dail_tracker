"""Public Procurement — read-only explorer over the registered ``v_procurement_*``
views (eTenders / national procurement open data).

Surfacing only: every aggregation, CRO join and value-gate already lives in the
SQL views; this page reads pre-aggregated rows and renders cards. It does NO
modelling — no value_counts / groupby / merge / parquet reads (the logic firewall
checker scans this file). The supplier search is a display-only name filter over
the already-fetched ranking; pagination is a display-only slice.

Honesty rails (non-negotiable, see doc/REVIEW_SYNTHESIS.md):
  * "Awarded value, not actual spend" — the page never sums the corpus into a
    headline € figure and only ever shows ``awarded_value_safe_eur`` (the view's
    sum-safe column), per row. Framework/DPS ceilings are labelled as ceilings.
  * Lobbying overlap is co-occurrence disclosure, never causation — copy says
    "appears in both registers", never "influenced" / "won because"; the chip is
    neutral grey, never an alarm colour.
  * Source-state aware: a missing view / parquet shows "data unavailable", not a
    silent empty list (uses the QueryResult ok/unavailable distinction).

Layout: browse view (caveat → Suppliers / Authorities / Categories / Lobbying
overlap tabs → provenance) with a ``?supplier=<norm>`` drill-down to a single
supplier's profile and full award history. CSS lives in shared_css.py (``pr-*``).
"""

from __future__ import annotations

import html
import sys
import urllib.parse
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.freshness_data import freshness_line
from data_access.procurement_data import (
    fetch_afs_by_division_result,
    fetch_afs_capital_by_division_result,
    fetch_afs_capital_by_year_result,
    fetch_afs_total_by_year_result,
    fetch_afs_vs_po_coverage_result,
    fetch_authority_summary_result,
    fetch_available_years,
    fetch_call_offs_for_supplier_result,
    fetch_charity_overlap_result,
    fetch_council_summary_result,
    fetch_awards_for_authority,
    fetch_awards_for_cpv,
    fetch_awards_for_supplier,
    fetch_competition_by_cpv_result,
    fetch_coverage_stats_result,
    fetch_cpv_summary_result,
    fetch_dependency_for_supplier_result,
    fetch_dependency_top_result,
    fetch_entity_chain_for_company_result,
    fetch_entity_search_result,
    fetch_incumbency_for_supplier_result,
    fetch_incumbency_top_result,
    fetch_lobbying_overlap_result,
    fetch_awards_by_year_result,
    fetch_new_entrants_result,
    fetch_payment_lines_for_pair_result,
    fetch_payments_corpus_stats_result,
    fetch_payments_for_publisher_result,
    fetch_payments_for_supplier_result,
    fetch_payments_by_year_result,
    fetch_payments_publisher_profile_result,
    fetch_payments_publisher_summary_result,
    fetch_payments_publishers_for_supplier_result,
    fetch_payments_supplier_header_result,
    fetch_payments_supplier_summary_result,
    fetch_quarter_profile_top_result,
    fetch_quarter_totals_result,
    fetch_sector_breadth_top_result,
    fetch_single_bid_baseline_result,
    fetch_single_bid_notices_for_cpv_result,
    fetch_supplier_concentration_result,
    fetch_supplier_single_bid_result,
    fetch_supplier_summary_result,
    fetch_ted_awards_by_year_result,
    fetch_ted_competition_stats_result,
    fetch_ted_corpus_stats_result,
    fetch_ted_for_supplier_result,
    fetch_ted_notices_for_supplier_result,
    fetch_ted_supplier_summary_result,
    fetch_expiring_contracts_result,
    fetch_expiring_contracts_stats_result,
    fetch_expiring_etenders_result,
    fetch_live_tender_sectors_result,
    fetch_live_tenders_result,
    fetch_live_tenders_stats_result,
    fetch_ted_tender_sectors_result,
    fetch_ted_tenders_result,
    fetch_ted_tenders_stats_result,
)
from shared_css import inject_css  # noqa: F401  (kept parallel to other pages)
from ui.entity_links import company_profile_url
from ui.components import (
    back_button,
    clickable_card_link,
    empty_state,
    finding_lede,
    fmt_civic_date,
    glossary_strip,
    hero_banner,
    hide_sidebar,
    paginate,
    pagination_controls,
    year_selector,
)

_TOP = 60  # cards shown per non-paginated tab (views are pre-ordered DESC)
_SUP_PAGE = 24  # supplier cards per page (multiple of 3 for the grid)
_AWARD_PAGE = 25  # award rows per page on a supplier profile


def _esc(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    return html.escape(str(val))


def _eur(val) -> str:
    """Compact euro label: €1.2m / €345k / €1,234 / — ."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    try:
        n = float(val)
    except (TypeError, ValueError):
        return "—"
    if n <= 0:
        return "—"
    if n >= 1_000_000:
        return f"€{n / 1_000_000:.1f}m"
    if n >= 1_000:
        return f"€{n / 1_000:.0f}k"
    return f"€{n:,.0f}"


def _n(val) -> int:
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


def _truthy(val) -> bool:
    """Safe truthiness for possibly-NA pandas cells — ``bool(pd.NA)`` raises."""
    if val is None:
        return False
    try:
        if pd.isna(val):
            return False
    except (TypeError, ValueError):
        pass
    return bool(val)


def _coalesce(*vals) -> str:
    """First non-NA, non-empty value as a stripped string, else ''. Avoids the
    ``pd.NA or x`` truthiness error when coalescing nullable columns."""
    for v in vals:
        if v is None:
            continue
        try:
            if pd.isna(v):
                continue
        except (TypeError, ValueError):
            pass
        s = str(v).strip()
        if s:
            return s
    return ""


def _awards_word(n: int) -> str:
    return f"{n:,} award{'s' if n != 1 else ''}"


def _eur_scale(val) -> str:
    """Headline scale label allowing billions: €23.5bn / €4.2m / €0 ."""
    try:
        n = float(val)
    except (TypeError, ValueError):
        return "—"
    if n >= 1_000_000_000:
        return f"€{n / 1_000_000_000:.1f}bn"
    if n >= 1_000_000:
        return f"€{n / 1_000_000:.1f}m"
    if n >= 1_000:
        return f"€{n / 1_000:.0f}k"
    return f"€{n:,.0f}"


def _supplier_href(supplier_norm) -> str:
    # Supplier cards open the first-class /company dossier (entity-first flagship).
    # The in-page ?supplier= profile below is kept so existing deep links still work.
    return company_profile_url(str(supplier_norm))


def _authority_href(authority) -> str:
    return f"?authority={urllib.parse.quote(str(authority))}"


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
_SECTION_LABELS = {
    "Who wins contracts?": "wins",
    "Who actually gets paid?": "paid",
    "Open right now": "open",
    "Patterns": "patterns",
}


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


# ──────────────────────────────────────────────────────────────────────────────
# Tab: Suppliers (search + pagination + clickable drill-down)
# ──────────────────────────────────────────────────────────────────────────────
def _concentration_and_trend() -> None:
    """Award-count trend over time, collapsed. The concentration sentence that used to
    sit here moved into the page lede (_page_lede) — stating it twice on one page was
    clutter, and the lede is where the market-shape finding belongs."""
    tr = fetch_awards_by_year_result()
    if tr.ok and not tr.data.empty and len(tr.data) > 1:
        with st.expander("Award activity over time"):
            st.bar_chart(tr.data, x="year", y="n_awards", x_label="Year", y_label="Awards", height=200, color="#9c5b2e")


def _render_suppliers(year: int | None) -> None:
    if not (st.session_state.get("pr_sup_q") or "").strip():
        _concentration_and_trend()
    order = _sort_toggle("pr_sup_sort")
    res = fetch_supplier_summary_result(limit=None, order_by=order, year=year)
    if not res.ok:
        empty_state("Supplier data isn't available right now", "A source/pipeline issue, not an empty result.")
        return
    df = res.data

    # Global rank in the CURRENT sort (df is pre-ordered by the view) — kept so a
    # card's "#N" reflects its true overall position even after a search filter.
    ranks = {str(r.supplier_norm): i for i, r in enumerate(df.itertuples(), start=1)}

    q = st.text_input(
        "Search suppliers",
        placeholder="Search by company name…",
        key="pr_sup_q",
        label_visibility="collapsed",
    )
    view = df
    qs = (q or "").strip()
    if qs:
        view = df[df["supplier"].str.contains(qs, case=False, na=False)]

    total = len(view)
    ranked_by = "sum-safe awarded value" if order == "value" else "number of contract awards"
    st.caption(
        f"{total:,} suppliers{_year_label(year)}"
        + (f' matching "{qs}"' if qs else f" ranked by {ranked_by}")
        + ". Value shown is awarded value, not spend — click a supplier for its full award history."
    )
    if total == 0:
        empty_state(
            "No suppliers match",
            "Try a shorter search term" + (f" or a different year than {year}." if year else "."),
        )
        return

    page_idx = paginate(total, key_prefix="pr_sup", page_size=_SUP_PAGE)
    page = view.iloc[page_idx * _SUP_PAGE : (page_idx + 1) * _SUP_PAGE]

    cards = []
    for r in page.itertuples():
        meta = (
            f"{_awards_word(_n(r.n_awards))} · "
            f"{_n(r.n_authorities):,} authorit{'ies' if _n(r.n_authorities) != 1 else 'y'}"
        )
        pills = [_value_pill(r.awarded_value_safe_eur)]
        pills += [p for p in (_cro_pill(r), _lobby_pill(r)) if p]
        inner = _card(f"<span>{_esc(r.supplier)}</span>", meta, pills, rank=ranks.get(str(r.supplier_norm)))
        cards.append(
            clickable_card_link(
                href=_supplier_href(r.supplier_norm),
                inner_html=inner,
                aria_label=f"View the award history of {r.supplier}",
            )
        )
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html('<div class="pr-sp-md"></div>')
    pagination_controls(
        total,
        key_prefix="pr_sup",
        page_sizes=(_SUP_PAGE,),
        default_page_size=_SUP_PAGE,
        label="suppliers",
    )


# ──────────────────────────────────────────────────────────────────────────────
# Tab: Contracting authorities / Categories (ranked; each card drills down to that
# entity's award list via ?authority= / ?cpv=). Both honour the year + sort lens.
# ──────────────────────────────────────────────────────────────────────────────
def _render_authorities(year: int | None) -> None:
    order = _sort_toggle("pr_auth_sort")
    res = fetch_authority_summary_result(limit=_TOP, order_by=order, year=year)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        empty_state("No contracting authorities", f"No authority has awards{_year_label(year)}.")
        return
    by = "sum-safe awarded value" if order == "value" else "number of awards"
    st.caption(f"Top {len(df):,} contracting authorities{_year_label(year)} by {by}. Click one for its awards.")
    cards = []
    for i, r in enumerate(df.head(_TOP).itertuples(), start=1):
        meta = f"{_awards_word(_n(r.n_awards))} · {_n(r.n_suppliers):,} supplier{'s' if _n(r.n_suppliers) != 1 else ''}"
        inner = _card(
            f"<span>{_esc(r.contracting_authority)}</span>", meta, [_value_pill(r.awarded_value_safe_eur)], rank=i
        )
        cards.append(
            clickable_card_link(
                href=_authority_href(r.contracting_authority),
                inner_html=inner,
                aria_label=f"View the awards made by {r.contracting_authority}",
            )
        )
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')


def _render_cpv(year: int | None) -> None:
    order = _sort_toggle("pr_cpv_sort")
    res = fetch_cpv_summary_result(limit=_TOP, order_by=order, year=year)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        empty_state("No categories", f"No category has awards{_year_label(year)}.")
        return
    by = "sum-safe awarded value" if order == "value" else "number of awards"
    st.caption(
        f"Top {len(df):,} procurement categories (CPV){_year_label(year)} by {by}. "
        "“Typical award” is the middle 50% (p25–median–p75) of the real, sum-safe awarded "
        "values in that category — a factual benchmark of what contracts here cost, not spend. "
        "Click a category for its awards."
    )
    cards = []
    for i, r in enumerate(df.head(_TOP).itertuples(), start=1):
        title = _esc(r.cpv_description) or _esc(r.cpv_code) or "—"
        meta = (
            f"CPV {_esc(r.cpv_code)} · {_awards_word(_n(r.n_awards))} · "
            f"{_n(r.n_suppliers):,} supplier{'s' if _n(r.n_suppliers) != 1 else ''}"
        )
        # Factual award-value benchmark — only when enough awards carry a sum-safe value
        # that a "typical" range is meaningful (a median over 2–3 awards would mislead).
        valued = _n(getattr(r, "n_awards_valued", 0))
        if valued >= 8 and getattr(r, "median_award_eur", None):
            meta += (
                f" · typical award {_eur_scale(r.p25_award_eur)}–{_eur_scale(r.p75_award_eur)} "
                f"(median {_eur_scale(r.median_award_eur)}, {valued} valued)"
            )
        inner = _card(f"<span>{title}</span>", meta, [_value_pill(r.awarded_value_safe_eur)], rank=i)
        cards.append(
            clickable_card_link(
                href=_cpv_href(r.cpv_code),
                inner_html=inner,
                aria_label=f"View the awards in category {title}",
            )
        )
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')


# ──────────────────────────────────────────────────────────────────────────────
# Tab: Lobbying overlap (clickable → supplier profile)
# ──────────────────────────────────────────────────────────────────────────────
def _render_overlap(df: pd.DataFrame, year: int | None = None) -> None:
    st.caption(
        "Organisations that appear on BOTH the procurement and lobbying registers. "
        "This is a co-occurrence disclosure only — it does not imply that lobbying "
        "influenced any award." + (" Shown across all years — the lobbying register isn't dated here." if year else "")
    )
    if df.empty:
        empty_state("No overlap rows", "No organisation currently appears on both registers.")
        return
    cards = []
    for i, r in enumerate(df.head(_TOP).itertuples(), start=1):
        name = _esc(_coalesce(getattr(r, "supplier", None), getattr(r, "lobby_name", None))) or "—"
        pills = [
            _value_pill(r.awarded_value_safe_eur),
            f'<span class="pr-pill pr-pill-lob">{_n(r.n_lobby_returns):,} lobbying returns</span>',
        ]
        meta = f"{_n(r.n_award_rows):,} award row{'s' if _n(r.n_award_rows) != 1 else ''} · appears in both registers"
        inner = _card(f"<span>{name}</span>", meta, pills, rank=i)
        norm = _coalesce(getattr(r, "supplier_norm", None))
        if norm:
            cards.append(
                clickable_card_link(
                    href=_supplier_href(norm),
                    inner_html=inner,
                    aria_label=f"View the award history of {name}",
                )
            )
        else:
            cards.append(inner)
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')


# ──────────────────────────────────────────────────────────────────────────────
# Tab: Charities on the register (clickable → supplier profile)
# Registered charities that also win public contracts — linked on the SHARED CRO
# company number (a hard id, not a fuzzy name match). Co-occurrence disclosure
# only, never a claim about the charity (same honesty rail as lobbying overlap).
# ──────────────────────────────────────────────────────────────────────────────
def _gov_share_pill(val) -> str:
    """The charity's latest government-funded income share (0–1) as a neutral
    context chip — its own annual-return figure, shown as context, not a judgement."""
    try:
        pct = float(val) * 100
    except (TypeError, ValueError):
        return ""
    if pct <= 0:
        return ""
    return f'<span class="pr-pill pr-pill-lob">{pct:.0f}% government-funded</span>'


def _render_charity_overlap(df: pd.DataFrame) -> None:
    st.caption(
        "Registered charities that also appear on the procurement award register, linked by a "
        "shared Companies Registration Office number (a hard identifier — the charity's declared "
        "company number equals the supplier's). This is a co-occurrence of public records only — "
        "it is not a claim about the charity or any award. Government-funded share is the charity's "
        "own latest annual-return figure. Shown across all years; click a card for the award history."
    )
    if df.empty:
        empty_state(
            "No charities on the register",
            "No registered charity currently matches a procurement supplier by CRO number.",
        )
        return
    cards = []
    for i, r in enumerate(df.head(_TOP).itertuples(), start=1):
        name = _esc(_coalesce(getattr(r, "registered_charity_name", None))) or "—"
        n_auth = _n(r.n_authorities)
        meta = (
            f"{_awards_word(_n(r.n_awards))} · {n_auth:,} authorit{'ies' if n_auth != 1 else 'y'} · registered charity"
        )
        pills = [_value_pill(r.awarded_value_safe_eur)]
        pills += [p for p in (_gov_share_pill(getattr(r, "gov_funded_share_latest", None)),) if p]
        inner = _card(f"<span>{name}</span>", meta, pills, rank=i)
        norm = _coalesce(getattr(r, "supplier_norm", None))
        if norm:
            cards.append(
                clickable_card_link(
                    href=_supplier_href(norm),
                    inner_html=inner,
                    aria_label=f"View the award history of {name}",
                )
            )
        else:
            cards.append(inner)
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html(
        '<div class="pr-foot"><strong>Source:</strong> Charities Regulator register, '
        "cross-referenced to the eTenders procurement register on the shared Companies "
        "Registration Office number. A charity shown here is the same registered company that "
        "won the contract — a co-occurrence of public records, never an implication of "
        "wrongdoing. Values are awarded value, not money paid.</div>"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Tab: Money actually paid — public-body PAYMENTS (SPENT/COMMITTED), a different grain
# from awards, never summed with them. Suppliers named per published source.
# ──────────────────────────────────────────────────────────────────────────────
def _tier_toggle(key: str) -> str:
    """'Paid' (SPENT) vs 'Ordered' (COMMITTED) — two lifecycle tiers, never blended."""
    labels = {"Paid (actual spend)": "SPENT", "Ordered (purchase orders)": "COMMITTED"}
    choice = st.segmented_control(
        "Tier", list(labels), default="Paid (actual spend)", key=key, label_visibility="collapsed"
    )
    return labels.get(choice or "Paid (actual spend)", "SPENT")


def _paid_verb(tier: str) -> str:
    return "ordered" if tier == "COMMITTED" else "paid"


def _paid_pill(val, tier: str) -> str:
    if _eur(val) == "—":
        return ""
    return f'<span class="pr-pill pr-pill-val">{_eur(val)} {_paid_verb(tier)}</span>'


def _paid_publisher_href(name, tier: str = "SPENT") -> str:
    """Buyer-dossier link carrying the tier so a council linked from the 'Ordered' ranking
    lands on its ordered (purchase-order) dossier, not an empty 'paid' one."""
    return f"?paid_publisher={urllib.parse.quote(str(name))}&paid_tier={urllib.parse.quote(tier)}"


def _render_payments() -> None:
    stats_res = fetch_payments_corpus_stats_result()
    if not stats_res.ok or stats_res.data.empty:
        empty_state(
            "Payment data isn't available right now",
            "The public-body payment views couldn't be loaded — a source/pipeline issue, not an empty result.",
        )
        return
    s = stats_res.data.iloc[0]
    span = f"{_n(s.get('min_year'))}–{_n(s.get('max_year'))}"
    st.html(
        '<div class="pr-caveat"><strong>Money actually paid — a different thing from awards.</strong> '
        f"These are payments and purchase orders {_n(s.get('n_publishers')):,} public bodies "
        f"<em>published themselves</em> (their over-€20,000 lists, {span}), to "
        f"{_n(s.get('n_suppliers')):,} suppliers. At least <strong>{_eur_scale(s.get('spent_safe_eur'))} "
        f"paid</strong> and {_eur_scale(s.get('committed_safe_eur'))} ordered — an indicative floor, "
        "not an audited total (bodies use different VAT bases, so totals are never summed across them, "
        "and these are <em>never</em> added to the award figures above — a paid invoice and a contract "
        "ceiling are different stages of public money).</div>"
    )

    c1, c2 = st.columns([1, 1])
    with c1:
        tier = _tier_toggle("pr_pay_tier")
    with c2:
        view_labels = {"Top suppliers": "supplier", "Top public bodies": "publisher"}
        view = view_labels.get(
            st.segmented_control(
                "View", list(view_labels), default="Top suppliers", key="pr_pay_view", label_visibility="collapsed"
            )
            or "Top suppliers",
            "supplier",
        )

    if view == "supplier":
        _render_paid_suppliers(tier)
    else:
        _render_paid_publishers(tier)
    st.html(
        '<div class="pr-foot"><strong>Source:</strong> each public body\'s own published '
        "purchase-order / payments-over-€20,000 disclosures (Circular 07/2012 / FOI), consolidated and "
        "matched to the Companies Registration Office. Suppliers are named as published. "
        "Paid (actual spend) and ordered (purchase orders) are different stages and are never summed "
        "together; totals are never summed across bodies with different VAT bases; never added to award values.</div>"
    )


def _render_paid_suppliers(tier: str) -> None:
    res = fetch_payments_supplier_summary_result(tier=tier, limit=_TOP)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        empty_state("No payments", f"No supplier has {_paid_verb(tier)} records in this tier.")
        return
    st.caption(
        f"Top {len(df):,} suppliers by money {_paid_verb(tier)} (sum-safe). Names as published by the body. "
        "Click a company for the public bodies that paid it."
    )
    cards = []
    for i, r in enumerate(df.head(_TOP).itertuples(), start=1):
        np_ = _n(r.n_publishers)
        meta = f"{_n(r.n_payments):,} payment{'s' if _n(r.n_payments) != 1 else ''} · {np_:,} public bod{'ies' if np_ != 1 else 'y'}"
        if _truthy(getattr(r, "vat_mixed", None)):
            meta += " · mixed VAT bases (floor)"
        pills = [
            p
            for p in (
                _paid_pill(r.total_safe_eur, tier),
                _cro_pill_from(getattr(r, "cro_company_num", None), getattr(r, "cro_company_status", None)),
            )
            if p
        ]
        inner = _card(f"<span>{_esc(r.supplier)}</span>", meta, pills, rank=i)
        # Company-class only: composing one individual / sole trader's cross-body payment footprint is
        # profile-building (same quarantine as the awards drill-down). Their card stays non-clickable —
        # the single published line is already public, but the cross-register roll-up is not surfaced.
        if _coalesce(getattr(r, "supplier_class", None)) == "company":
            cards.append(
                clickable_card_link(
                    href=_paid_supplier_href(r.supplier_normalised, tier),
                    inner_html=inner,
                    aria_label=f"View the public bodies that {_paid_verb(tier)} {r.supplier}",
                )
            )
        else:
            cards.append(inner)
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')


def _render_paid_publishers(tier: str) -> None:
    res = fetch_payments_publisher_summary_result(tier=tier, limit=None)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        empty_state("No public bodies", f"No body has {_paid_verb(tier)} records in this tier.")
        return
    # Local-authority lens (display-only slice over the fetched ranking — the per-council buyer
    # view ProZorro-style). Councils mostly publish purchase ORDERS, so they cluster in the
    # 'Ordered' tier; the toggle just narrows the list, it computes nothing.
    n_la = int((df["publisher_type"] == "local_authority").sum()) if "publisher_type" in df.columns else 0
    if n_la:
        only_la = st.toggle(
            "Local authorities only",
            value=False,
            key=f"pr_pay_la_{tier}",
            help=f"{n_la} of the {len(df):,} bodies in this tier are county / city councils.",
        )
        if only_la:
            df = df[df["publisher_type"] == "local_authority"]
    st.caption(f"Public bodies by money {_paid_verb(tier)} (sum-safe within each body). Click one for its suppliers.")
    cards = []
    for i, r in enumerate(df.head(_TOP).itertuples(), start=1):
        meta = (
            f"{_n(r.n_suppliers):,} supplier{'s' if _n(r.n_suppliers) != 1 else ''} · {_n(r.min_year)}–{_n(r.max_year)}"
        )
        vat = _coalesce(getattr(r, "vat_status", None))
        pills = [_paid_pill(r.total_safe_eur, tier)]
        if _coalesce(getattr(r, "publisher_type", None)) == "local_authority":
            pills.append('<span class="pr-pill pr-pill-lob">local authority</span>')
        if vat == "incl_vat":
            pills.append('<span class="pr-pill pr-pill-lob">VAT-inclusive</span>')
        inner = _card(f"<span>{_esc(r.publisher_name)}</span>", meta, [p for p in pills if p], rank=i)
        cards.append(
            clickable_card_link(
                href=_paid_publisher_href(r.publisher_name, tier),
                inner_html=inner,
                aria_label=f"View the suppliers {_paid_verb(tier)} by {r.publisher_name}",
            )
        )
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')


def _council_tier_pills(row) -> list[str]:
    """The lifecycle pill(s) a council carries: solid 'paid' (actual payments, the firmest
    fact) and/or dashed 'ordered' (purchase-order commitments, provisional). Different stages
    of public money — shown side by side, NEVER summed. The verb is the accessible carrier
    (colour-/border-independent); the dashed/solid contrast is the visual one. In this corpus
    a council has exactly one, but both are handled so the view stays honest if that changes."""
    pills = []
    if _n(getattr(row, "n_paid", 0)) > 0:
        pills.append(f'<span class="pr-pill pr-pill-paid">{_eur(row.paid_safe_eur)} paid</span>')
    if _n(getattr(row, "n_ordered", 0)) > 0:
        pills.append(f'<span class="pr-pill pr-pill-ordered">{_eur(row.ordered_safe_eur)} ordered</span>')
    return pills


def _render_councils() -> None:
    """The "Your council" index — Ireland's publishing local authorities as a civic directory,
    grouped North->South by province, each card linking to its existing per-council dossier
    (?paid_publisher=). Surfacing-only: v_procurement_council_summary is pre-aggregated and
    pre-ordered; this selects and renders, computing nothing. No rank chips — a directory
    ("find your council"), not a league table. 'ordered' and 'paid' are different lifecycle
    stages and are never added together."""
    res = fetch_council_summary_result()
    if not res.ok:
        empty_state(
            "Council spending isn't available right now",
            "The public-body payment views couldn't be loaded — a source/pipeline issue, not an empty result.",
        )
        return
    df = res.data
    if df.empty:
        empty_state("No councils", "No local authority has published payment records yet.")
        return

    n_councils = len(df)
    span = f"{_n(df['min_year'].min())}–{_n(df['max_year'].max())}"
    st.html(
        '<div class="pr-caveat"><strong>What your county and city councils spend.</strong> '
        f"The {n_councils} local authorities that publish their purchase orders and payments "
        f"over €20,000 ({span}), grouped by province. Each council shows money <em>ordered</em> "
        "(purchase-order commitments) or <em>paid</em> (actual payments) — different stages of "
        "public money, shown per council and <strong>never added together</strong>. Click a "
        "council for its suppliers and audited-accounts context.</div>"
    )

    # Province bands, North->South via province_order; councils pre-ordered by scale within
    # each band. The band header is a semantic <h3> (heading-navigable). Geography is the
    # fixed band order, not colour.
    for prov_order in sorted(df["province_order"].unique()):
        band = df[df["province_order"] == prov_order]
        prov = _esc(band.iloc[0]["province"])
        n = len(band)
        # <h2>: direct section heading under the page <h1> hero (no h2→h3 skip), so the
        # province bands are screen-reader heading-navigable. CSS sets the visual size.
        st.html(
            f'<h2 class="pr-region-head"><span class="pr-region-name">{prov}</span>'
            f'<span class="pr-region-count">{n} council{"s" if n != 1 else ""} publishing</span></h2>'
        )
        cards = []
        for r in band.itertuples():
            n_sup = _n(r.n_suppliers)
            # Guard the year span: a council whose source carries no usable year (e.g. Mayo)
            # would otherwise render "0–0". Drop the span rather than show a sentinel.
            yr_span = f" · {_n(r.min_year)}–{_n(r.max_year)}" if _n(r.min_year) and _n(r.max_year) else ""
            meta = f"{n_sup:,} supplier{'s' if n_sup != 1 else ''}{yr_span}"
            # Land the dossier on the tier the council actually publishes, so it opens populated.
            tier = "SPENT" if _n(r.n_paid) > 0 else "COMMITTED"
            inner = _card(f"<span>{_esc(r.council)}</span>", meta, _council_tier_pills(r))
            cards.append(
                clickable_card_link(
                    href=_paid_publisher_href(r.council, tier),
                    inner_html=inner,
                    aria_label=f"View {r.council} council's suppliers and audited accounts",
                )
            )
        st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html(_PAY_FOOT_HTML)


def _lane_header(tag: str, head: str, dek_html: str) -> str:
    """A bold lane band that opens one of the dossier's three honest grains (Running / Building /
    Paying). ``tag`` is the small caps stratum, ``head`` the section <h2>, ``dek_html`` the prose."""
    return (
        '<div class="pr-lane">'
        f'<div class="pr-lane-tag">{_esc(tag)}</div>'
        f'<h2 class="pr-lane-head">{_esc(head)}</h2>'
        f'<p class="pr-lane-dek">{dek_html}</p></div>'
    )


def _net_cost_label(net) -> str:
    """Net cost of a service to the local taxpayer. A non-positive net means the service's own
    income/grants cover it (housing rents, water recoupment) — say so rather than print '—'."""
    try:
        n = float(net)
    except (TypeError, ValueError):
        return "—"
    return f"{_eur(n)} net cost" if n > 0 else "covered by its own income"


def _self_funded_note(pct, division: str) -> str:
    """Muted sub-label: the share of a service the council recovers itself vs. funds from rates/LPT."""
    try:
        p = float(pct)
    except (TypeError, ValueError):
        return ""
    if division == "Miscellaneous Services":
        return "carries the rates / Local Property Tax allocation — not a like-for-like service"
    if p >= 100:
        return "fully covered by its own income & grants"
    return f"{p:.0f}% funded by its own charges & grants — the rest by you (rates, LPT, State grant)"


def _render_council_running_lane(council: str, active_tier: str, *, po_max_year: int | None) -> int | None:
    """LANE 1 — RUNNING THE SERVICES (audited revenue account). Leads with NET COST by service
    (what the local taxpayer actually funds, the strongest civic figure), then the spend-over-time
    spine and the indicative named-supplier traceability bridge to the PAYING lane below.

    ⚠️ BUDGET grain — a SIBLING fact, NEVER summed with the purchase-order euros. All figures are
    pre-aggregated/pre-ordered in the views; the page selects and renders, computing no metric.
    Returns the latest accounts year (so the BUILDING lane can align its coverage note), or None
    when this council has no audited AFS in the fact yet."""
    by_year = fetch_afs_total_by_year_result(council)
    if not by_year.ok or by_year.data.empty:
        return None
    ay = by_year.data
    years_present = {int(y) for y in ay["year"].dropna()}
    earliest, latest = min(years_present), max(years_present)
    span = f"{earliest}–{latest}" if len(years_present) > 1 else str(latest)

    st.html(
        _lane_header(
            "RUNNING THE SERVICES · revenue account, audited",
            "Where your money goes",
            "Every council publishes an audited <strong>Annual Financial Statement</strong> — its "
            "end-of-year accounts for running each service. Below is the <strong>net cost</strong> of "
            f"each service to {_esc(council)} ({_esc(span)}): what’s left for the local taxpayer "
            "(rates, Local Property Tax, State grant) to fund <em>after</em> the service’s own income "
            "and grants. This is the council’s <strong>whole operating spend</strong> — a separate, "
            "broader measure from the over-€20,000 purchase orders, and <strong>never added to them</strong>.",
        )
    )

    # Coverage flag — the audited AFS is filed in arrears (and the odd year can be missing from a
    # council's own publication run), so be explicit rather than let a gap read as "stopped spending".
    flag_bits: list[str] = []
    if po_max_year and latest < po_max_year:
        flag_bits.append(
            f"audited accounts run to <strong>{latest}</strong>, but the purchase orders below reach "
            f"<strong>{po_max_year}</strong> — councils publish their audited AFS in arrears, so the "
            "most recent year or two isn’t available yet"
        )
    missing = [y for y in range(earliest, latest + 1) if y not in years_present]
    if missing:
        flag_bits.append(f"no published statement for {', '.join(str(y) for y in missing)} in this series")
    if flag_bits:
        st.html(f'<div class="pr-caveat"><strong>Coverage:</strong> {"; ".join(flag_bits)}.</div>')

    # HERO — net cost by service (largest first; the view pre-orders by net DESC). Bar width is a
    # display scaling against the lane's own largest net cost (no aggregation here).
    bd = fetch_afs_by_division_result(council, latest)
    if bd.ok and not bd.data.empty:
        st.caption(f"Net cost to the local taxpayer by service, {latest} — bar width = net cost")
        nets = [float(r.net_expenditure_eur) for r in bd.data.itertuples() if _truthy(r.net_expenditure_eur)]
        max_net = max([n for n in nets if n > 0], default=0.0)
        has_misc = False
        rows = []
        for r in bd.data.itertuples():
            net = getattr(r, "net_expenditure_eur", None)
            pct = getattr(r, "pct_self_funded", None)
            if r.division == "Miscellaneous Services":
                has_misc = True
            fig = f'<strong>{_eur(net)}</strong>' if _truthy(net) and float(net) > 0 else '<span class="pr-afsbar-zero">income covers it</span>'
            rows.append(
                _afs_bar_row(
                    r.division,
                    net if _truthy(net) and float(net) > 0 else 0,
                    max_net,
                    fig_html=fig,
                    note=_self_funded_note(pct, r.division),
                    accent="#3a6b7e",
                )
            )
        st.html(f'<div class="pr-afsbars">{"".join(rows)}</div>')
        if has_misc:
            st.caption(
                "“Miscellaneous Services” carries the council’s rates / Local Property Tax income, so it "
                "can show as fully covered — it isn’t a single service."
            )

    # Spend-over-time spine — distinct teal from the PO chart's brown (a different grain).
    if len(ay) > 1:
        st.caption("Total operating spending per year (revenue account, audited gross €)")
        st.bar_chart(ay, x="year", y="gross_expenditure_eur", x_label="Year", y_label="Audited € spent", height=180, color="#3a6b7e")

    # Traceability bridge to the PAYING lane — the latest year present in both accounts + active PO tier.
    cov = fetch_afs_vs_po_coverage_result(council)
    if cov.ok and not cov.data.empty:
        pct_col = "pct_spent_of_gross" if active_tier == "SPENT" else "pct_committed_of_gross"
        po_col = "po_spent_safe_eur" if active_tier == "SPENT" else "po_committed_safe_eur"
        usable = cov.data[cov.data[pct_col].notna()]
        if not usable.empty:
            crow = usable.sort_values("year").iloc[-1]
            yr, gross, po, pct = (_n(crow.get("year")), crow.get("afs_gross_eur"), crow.get(po_col), crow.get(pct_col))
            verb = _paid_verb(active_tier)  # 'paid' / 'ordered'
            st.html(
                '<div class="pr-afs-trace">'
                f'<div class="pr-afs-trace-fig"><strong>{_eur(gross)}</strong> spent (accounts, {yr})'
                f" · <strong>{_eur(po)}</strong> traceable to named suppliers"
                f" · <strong>{float(pct):g}%</strong></div>"
                f'<div class="pr-afs-trace-cap">Indicative coverage only. The accounts figure is the '
                "council’s full audited operating spend; the supplier figure counts only purchases over "
                f"the €20,000 publication threshold ({verb} via purchase orders). Different thresholds and "
                "stages — a coverage signal, not a reconciliation.</div></div>"
            )
    return latest


def _render_council_building_lane(council: str, *, accounts_latest: int | None) -> None:
    """LANE 2 — BUILDING (audited capital account). What the council is investing in / acquiring —
    the homes, roads and facilities being built. A THIRD, DISTINCT grain: the revenue account shows
    housing netting to ~€0 (rents/HAP recoupment pass through), so the real housing investment only
    shows up here. NEVER summed with the revenue net cost or the purchase-order euros.

    Silently omitted for a council whose capital appendix isn't in the fact yet."""
    by_year = fetch_afs_capital_by_year_result(council)
    if not by_year.ok or by_year.data.empty:
        return
    cy = by_year.data
    cap_years = {int(y) for y in cy["year"].dropna()}
    cap_latest = max(cap_years)
    span = f"{min(cap_years)}–{cap_latest}" if len(cap_years) > 1 else str(cap_latest)

    st.html(
        _lane_header(
            "BUILDING · capital account, audited",
            "What your council is building",
            f"Beyond running services day to day, {_esc(council)} invests in <strong>building and "
            "acquiring</strong> — housing, roads, libraries, water infrastructure. This is the audited "
            f"<strong>capital programme</strong> ({_esc(span)}), funded largely by central-government "
            "grants and loans. It is a <strong>different kind of money</strong> from the running costs "
            "above — investment, not operating spend — and the two are <strong>never added together</strong>.",
        )
    )
    if accounts_latest and cap_latest < accounts_latest:
        st.html(
            f'<div class="pr-caveat"><strong>Coverage:</strong> the capital appendix runs to '
            f"<strong>{cap_latest}</strong> here.</div>"
        )

    # Capital invested per year — a DISTINCT green (a third grain after brown PO + teal revenue).
    if len(cy) > 1:
        st.caption("Capital invested per year (audited €)")
        st.bar_chart(cy, x="year", y="capital_expenditure_eur", x_label="Year", y_label="€ invested", height=180, color="#2f7d5b")

    # Capital by service in the latest year — bars, largest investment first (view pre-orders).
    bd = fetch_afs_capital_by_division_result(council, cap_latest)
    if bd.ok and not bd.data.empty:
        st.caption(f"What it built in {cap_latest} — capital investment by service")
        capex = [float(r.capital_expenditure_eur) for r in bd.data.itertuples() if _truthy(r.capital_expenditure_eur)]
        max_cap = max(capex, default=0.0)
        rows = [
            _afs_bar_row(
                r.division,
                getattr(r, "capital_expenditure_eur", None),
                max_cap,
                fig_html=f'<strong>{_eur(getattr(r, "capital_expenditure_eur", None))}</strong>',
                note="",
                accent="#2f7d5b",
            )
            for r in bd.data.itertuples()
        ]
        st.html(f'<div class="pr-afsbars">{"".join(rows)}</div>')


def _render_council_accounts_context(council: str, active_tier: str, *, po_max_year: int | None = None) -> None:
    """The two AUDITED-ACCOUNTS lanes of a local-authority dossier, in civic reading order:
    RUNNING THE SERVICES (revenue net cost) then BUILDING (capital investment). Both are BUDGET
    grain — sibling facts, each pre-aggregated in its view, NEVER summed with each other or with the
    purchase-order euros in the PAYING lane. ``po_max_year`` lets the running lane flag the AFS
    arrears lag against the PO data."""
    accounts_latest = _render_council_running_lane(council, active_tier, po_max_year=po_max_year)
    _render_council_building_lane(council, accounts_latest=accounts_latest)


def _render_payments_publisher_profile(publisher_name: str, tier: str = "SPENT") -> None:
    """Per-buyer dossier (the per-council profile): which tiers the body publishes, both totals
    shown side by side (never summed), and its top suppliers in the active tier. Councils mostly
    publish purchase ORDERS, so this falls back to whichever tier the body actually has."""
    if back_button("← Back to procurement", key="prpaypub"):
        _return_to_browse("paid")

    prof = fetch_payments_publisher_profile_result(publisher_name)
    prow = prof.data.iloc[0] if (prof.ok and not prof.data.empty) else None
    n_paid = _n(prow.get("n_paid_lines")) if prow is not None else 0
    n_ordered = _n(prow.get("n_ordered_lines")) if prow is not None else 0
    tiers_present = [t for t, c in (("SPENT", n_paid), ("COMMITTED", n_ordered)) if c]

    is_la = prow is not None and _coalesce(prow.get("publisher_type")) == "local_authority"
    kicker = "LOCAL AUTHORITY" if is_la else "PUBLIC BODY"
    sector = _coalesce(prow.get("sector")) if prow is not None else ""
    n_sup = _n(prow.get("n_suppliers")) if prow is not None else 0
    span = ""
    if prow is not None and _n(prow.get("min_year")):
        span = f"{_n(prow.get('min_year'))}–{_n(prow.get('max_year'))}"
    sub_parts = [f"{n_sup:,} supplier{'s' if n_sup != 1 else ''} over €20,000"]
    if span:
        sub_parts.append(span)
    kick = kicker + (f" · {sector.upper()}" if sector and not is_la else "")
    st.html(
        f'<div class="pr-prof-head"><div class="pr-prof-kicker">{_esc(kick)}</div>'
        f'<h1 class="pr-prof-name">{_esc(publisher_name)}</h1>'
        f'<div class="pr-prof-sub">{_esc(" · ".join(sub_parts))}</div></div>'
    )
    # Both lifecycle tiers side by side — distinct stages of public money, NEVER summed.
    if prow is not None:
        tier_pills = []
        if n_ordered:
            tier_pills.append(_paid_pill(prow.get("ordered_safe_eur"), "COMMITTED"))
        if n_paid:
            tier_pills.append(_paid_pill(prow.get("paid_safe_eur"), "SPENT"))
        tier_pills = [p for p in tier_pills if p]
        if tier_pills:
            st.html(f'<div class="pr-pills" style="margin:0.1rem 0 0.6rem">{"".join(tier_pills)}</div>')

    if not tiers_present:
        empty_state("No payments found", "This body has no sum-safe records, or the link didn't match.")
        return

    # Active tier: honour the requested one; else the tier the body actually has. If it has
    # both, a toggle switches the supplier list (the headline pills always show both).
    active = tier if tier in tiers_present else tiers_present[0]
    if len(tiers_present) > 1:
        labels = {"Paid (actual spend)": "SPENT", "Ordered (purchase orders)": "COMMITTED"}
        default = next(k for k, v in labels.items() if v == active)
        choice = st.segmented_control(
            "Tier", list(labels), default=default, key="pr_paypub_tier", label_visibility="collapsed"
        )
        active = labels.get(choice or default, active)

    # Local-authority dossiers lead with the two AUDITED-ACCOUNTS lanes — RUNNING THE SERVICES
    # (revenue net cost) then BUILDING (capital investment) — the council's whole-budget context.
    # BUDGET grain: siblings, never summed with each other or with the purchase-order euros below.
    # Pass the PO data's max year so the running lane can flag the AFS arrears lag.
    if is_la:
        _render_council_accounts_context(publisher_name, active, po_max_year=_n(prow.get("max_year")))
        # LANE 3 — PAYING: the named suppliers over €20,000. The narrowest, most granular slice of
        # council money (most spend never passes through a tendered PO), but the only one named to a
        # firm. A DIFFERENT grain again — never added to the audited-accounts lanes above.
        st.html(
            _lane_header(
                "PAYING · purchase orders over €20,000",
                "Who it pays",
                "The suppliers the council reports paying or ordering more than €20,000 (Circular "
                "07/2012 / FOI disclosures). This is the <strong>named-supplier</strong> slice — most "
                "council money never passes through a tendered purchase order, so it is far narrower "
                "than the audited accounts above, and <strong>never added to them</strong>.",
            )
        )

    # Spend-over-time spine — one tier only (never stack ordered+paid, which would read as a sum).
    # Meaningful now the council payment data is a decade deep (2016–2026).
    by_year = fetch_payments_by_year_result(publisher_name, tier=active)
    if by_year.ok and len(by_year.data) > 1:
        st.caption(f"Money {_paid_verb(active)} per year (sum-safe)")
        st.bar_chart(by_year.data, x="year", y="total_safe_eur", x_label="Year", y_label="€ (sum-safe)", height=200, color="#9c5b2e")

    res = fetch_payments_for_publisher_result(publisher_name, tier=active)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        empty_state("No suppliers in this tier", f"This body has no sum-safe {_paid_verb(active)} records.")
        st.html(_FOOT_HTML)
        return
    st.caption(
        f"Top {len(df):,} suppliers by money {_paid_verb(active)} (sum-safe). Names as published by the body; "
        "amounts are the body's own reported figures, not award ceilings. Click a company to see every public "
        "body that paid it."
    )
    cards = []
    for i, r in enumerate(df.itertuples(), start=1):
        meta = f"{_n(r.n_payments):,} {_paid_verb(active)} line{'s' if _n(r.n_payments) != 1 else ''} · {_n(r.min_year)}–{_n(r.max_year)}"
        pills = [
            p
            for p in (_paid_pill(r.total_safe_eur, active), _cro_pill_from(getattr(r, "cro_company_num", None), None))
            if p
        ]
        inner = _card(f"<span>{_esc(r.supplier)}</span>", meta, pills, rank=i)
        # Only company-class suppliers drill down: the supplier dossier composes a firm's
        # cross-body footprint, which for an individual / sole trader would be profile-building
        # (the same privacy quarantine the dossier itself enforces). Individuals stay static.
        if _coalesce(getattr(r, "supplier_class", None)) == "company":
            # Drill to the LEAF (this body's published line items naming this firm), not to the
            # firm's own aggregate profile — the mutual linking is what made the drill-down loop.
            cards.append(
                clickable_card_link(
                    href=_paid_pair_href(r.supplier_normalised, publisher_name, active),
                    inner_html=inner,
                    aria_label=f"See the published {_paid_verb(active)} line items from {publisher_name} to {r.supplier}",
                )
            )
        else:
            cards.append(inner)
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html(_FOOT_HTML)


_PAY_FOOT_HTML = (
    '<div class="pr-foot"><strong>Source:</strong> each public body\'s own published '
    "purchase-order / payments-over-€20,000 disclosures (Circular 07/2012 / FOI), consolidated and "
    "matched to the Companies Registration Office. Suppliers and bodies are named as published. "
    "Paid (actual spend) and ordered (purchase orders) are different stages and are never summed "
    "together; totals are never summed across bodies with different VAT bases; never added to award values.</div>"
)


def _render_payments_supplier_profile(supplier_norm: str, tier: str = "SPENT") -> None:
    """Paid-supplier drill-down — the public bodies that paid (SPENT) or ordered (COMMITTED)
    from one firm: the exact mirror of the per-body dossier (which lists a body's suppliers).
    A later lifecycle stage than awards (never added to award totals) and the two payment tiers
    are shown side by side, never blended. Company-class only (cross-body footprints of an
    individual are profile-building — the same quarantine as the awards drill-down)."""
    if back_button("← Back to procurement", key="prpaysup"):
        _return_to_browse("paid")

    hdr = fetch_payments_supplier_header_result(supplier_norm)
    hrow = hdr.data.iloc[0] if (hdr.ok and not hdr.data.empty) else None
    if hrow is None:
        if not hdr.ok:
            empty_state("Payment data isn't available right now", "A source/pipeline issue, not an empty result.")
        else:
            empty_state("Supplier not found", "That link didn't match a paid supplier. Use Back to return.")
        return
    if _coalesce(hrow.get("supplier_class")) != "company":
        empty_state(
            "Not available",
            "Cross-body payment footprints are shown for companies only — composing one individual's is "
            "profile-building. Use Back to return.",
        )
        return

    name = _esc(_coalesce(hrow.get("supplier"))) or "—"
    n_paid, n_ordered = _n(hrow.get("n_paid_lines")), _n(hrow.get("n_ordered_lines"))
    tiers_present = [t for t, c in (("SPENT", n_paid), ("COMMITTED", n_ordered)) if c]
    np_ = _n(hrow.get("n_publishers"))
    span = f"{_n(hrow.get('min_year'))}–{_n(hrow.get('max_year'))}" if _n(hrow.get("min_year")) else ""
    sub_parts = [f"{np_:,} public bod{'ies' if np_ != 1 else 'y'} (over €20,000)"]
    if span:
        sub_parts.append(span)
    st.html(
        f'<div class="pr-prof-head"><div class="pr-prof-kicker">MONEY ACTUALLY PAID</div>'
        f'<h1 class="pr-prof-name">{name}</h1>'
        f'<div class="pr-prof-sub">{_esc(" · ".join(sub_parts))}</div></div>'
    )
    # Both lifecycle tiers side by side — distinct stages of public money, NEVER summed.
    tier_pills = []
    if n_ordered:
        tier_pills.append(_paid_pill(hrow.get("ordered_safe_eur"), "COMMITTED"))
    if n_paid:
        tier_pills.append(_paid_pill(hrow.get("paid_safe_eur"), "SPENT"))
    if _truthy(hrow.get("vat_mixed")):
        tier_pills.append('<span class="pr-pill pr-pill-lob">mixed VAT bases (floor)</span>')
    cro = _cro_pill_from(hrow.get("cro_company_num"), hrow.get("cro_company_status"))
    pills = [p for p in (*tier_pills, cro) if p]
    if pills:
        st.html(f'<div class="pr-pills" style="margin:0.1rem 0 0.6rem">{"".join(pills)}</div>')

    if not tiers_present:
        empty_state("No payments found", "This firm has no sum-safe payment records.")
        st.html(_PAY_FOOT_HTML)
        return

    active = tier if tier in tiers_present else tiers_present[0]
    if len(tiers_present) > 1:
        labels = {"Paid (actual spend)": "SPENT", "Ordered (purchase orders)": "COMMITTED"}
        default = next(k for k, v in labels.items() if v == active)
        choice = st.segmented_control(
            "Tier", list(labels), default=default, key="pr_paysup_tier", label_visibility="collapsed"
        )
        active = labels.get(choice or default, active)

    res = fetch_payments_publishers_for_supplier_result(supplier_norm, tier=active)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        empty_state("No bodies in this tier", f"No public body has {_paid_verb(active)} records for this firm.")
        st.html(_PAY_FOOT_HTML)
        return
    st.caption(
        f"Public bodies that {_paid_verb(active)} this firm (sum-safe within each body). Names and amounts "
        "as the body published them, not award ceilings. Click a body for its own supplier list."
    )
    cards = []
    for i, r in enumerate(df.itertuples(), start=1):
        meta = (
            f"{_n(r.n_payments):,} {_paid_verb(active)} line{'s' if _n(r.n_payments) != 1 else ''} · "
            f"{_n(r.min_year)}–{_n(r.max_year)}"
        )
        row_pills = [_paid_pill(r.total_safe_eur, active)]
        if _coalesce(getattr(r, "publisher_type", None)) == "local_authority":
            row_pills.append('<span class="pr-pill pr-pill-lob">local authority</span>')
        inner = _card(f"<span>{_esc(r.publisher_name)}</span>", meta, [p for p in row_pills if p], rank=i)
        # Drill to the LEAF (this body's published line items naming this firm), not to the body's
        # own aggregate profile — that mutual linking is what made the drill-down loop endlessly
        # without ever showing a record.
        cards.append(
            clickable_card_link(
                href=_paid_pair_href(supplier_norm, r.publisher_name, active),
                inner_html=inner,
                aria_label=f"See the published {_paid_verb(active)} line items from {r.publisher_name} to this firm",
            )
        )
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html(_PAY_FOOT_HTML)


def _payment_line_row(r, tier: str) -> str:
    """One published payment line as a list row (the leaf of the payments drill-down): the
    period, the body's own description, PO number, and the amount — with a link to the body's
    source file where it published one. Display-only; the amount is the body's reported figure."""
    period = _esc(_coalesce(getattr(r, "period", None))) or _esc(_n(getattr(r, "year", None)) or "")
    desc = _esc(_coalesce(getattr(r, "description", None)))
    title_html = f'<div class="pr-award-title">{desc}</div>' if desc else ""
    meta_parts = []
    po = _coalesce(getattr(r, "po_number", None))
    if po:
        meta_parts.append(f"PO {_esc(po)}")
    if not _truthy(getattr(r, "value_safe_to_sum", None)):
        meta_parts.append("not sum-safe")
    src = _coalesce(getattr(r, "source_file_url", None))
    if src.startswith("http"):
        meta_parts.append(f'<a href="{_esc(src)}" target="_blank" rel="noopener">source ↗</a>')
    meta = " · ".join(p for p in meta_parts if p)
    val = _eur(getattr(r, "amount_eur", None))
    val_html = (
        f'<div class="pr-award-val">{val}<small>{_paid_verb(tier)}</small></div>' if val != "—" else ""
    )
    return (
        f'<div class="pr-award"><div class="pr-award-body">'
        f'<div class="pr-award-auth">{period or "—"}</div>{title_html}'
        f'<div class="pr-award-meta">{meta or "—"}</div></div>{val_html}</div>'
    )


def _render_payment_lines(supplier_norm: str, publisher_name: str, tier: str = "SPENT") -> None:
    """LEAF view — the published payment line items for one supplier × public body × tier.
    The terminus that ends the old supplier↔body loop: instead of bouncing between aggregate
    cards, the reader lands here on the individual records the body published (period,
    description, PO number, amount), each linked to the body's own source file. Company-class
    entry points only (same privacy quarantine as the rest of the payments drill-down)."""
    if back_button("← Back", key="prpayline"):
        # Return to the supplier's payers list (the natural parent), preserving the tier.
        st.query_params.clear()
        st.query_params["paid_supplier"] = supplier_norm
        st.query_params["paid_tier"] = tier
        st.rerun()

    hdr = fetch_payments_supplier_header_result(supplier_norm)
    hrow = hdr.data.iloc[0] if (hdr.ok and not hdr.data.empty) else None
    sup_name = _esc(_coalesce(hrow.get("supplier"))) if hrow is not None else _esc(supplier_norm)

    st.html(
        f'<div class="pr-prof-head"><div class="pr-prof-kicker">PUBLISHED PAYMENT RECORDS · '
        f'{_esc(_paid_verb(tier).upper())}</div>'
        f'<h1 class="pr-prof-name">{sup_name or "—"}</h1>'
        f'<div class="pr-prof-sub">as {_esc(_paid_verb(tier))} by {_esc(publisher_name)}</div></div>'
    )

    res = fetch_payment_lines_for_pair_result(supplier_norm, publisher_name, tier)
    if not res.ok:
        empty_state("Payment data isn't available right now", "A source/pipeline issue, not an empty result.")
        return
    df = res.data
    if df.empty:
        empty_state(
            "No line items in this tier",
            f"This body published no {_paid_verb(tier)} lines naming this firm, or the link didn't match.",
        )
        return
    st.caption(
        f"{len(df):,} published {_paid_verb(tier)} line{'s' if len(df) != 1 else ''} naming this firm, biggest first. "
        "Each is the body's own reported figure (over €20,000), not an award ceiling — never summed across "
        "bodies with different VAT bases. Open a line's source ↗ for the body's published disclosure."
    )
    st.html("".join(_payment_line_row(r, tier) for r in df.itertuples()))
    st.html(_PAY_FOOT_HTML)


def _render_supplier_register_footprint(company_num) -> None:
    """Cross-register footprint for a CRO-matched firm: which of the three public-money
    registers (eTenders / TED / public-body payments) the same legal entity appears in, with
    each register's own headline figure side by side. The unified backbone over
    ``v_procurement_entity_chain`` — hard CRO company-number match only (no fuzzy name joins).

    ⚠️ The figures are DIFFERENT GRAINS (award ceilings vs realised payments): shown labelled,
    side by side, and NEVER summed. Absence from a register is coverage, not missing money (only
    a fraction of State spend is published in the payments lists). Skipped when the firm has no
    CRO match or appears in only one register (the rest of the profile already covers that)."""
    if not _truthy(company_num):
        return
    res = fetch_entity_chain_for_company_result(str(company_num))
    if not res.ok or res.data.empty:
        return
    r = res.data.iloc[0]
    n_reg = _n(r.get("n_registers"))
    if n_reg < 2:
        return  # eTenders only — nothing here the awards section above doesn't already show

    items: list[str] = []
    if _truthy(r.get("in_etenders")):
        v = _eur(r.get("etenders_awarded_value_safe_eur"))
        auth = _n(r.get("etenders_n_authorities"))
        val_pre = f"{v} awarded across " if v != "—" else ""
        items.append(
            f"<li><strong>eTenders (national)</strong> — {val_pre}{auth:,} contracting "
            f"authorit{'y' if auth == 1 else 'ies'} "
            '<span class="pr-notice-tag">award ceiling</span></li>'
        )
    if _truthy(r.get("in_ted")):
        v = _eur(r.get("ted_value_safe_eur"))
        nb = _n(r.get("ted_awards"))
        val_pre = f"{v} awarded across " if v != "—" else ""
        items.append(
            f"<li><strong>TED (EU Official Journal)</strong> — {val_pre}{nb:,} award "
            f"notice{'' if nb == 1 else 's'} "
            '<span class="pr-notice-tag">award ceiling</span></li>'
        )
    if _truthy(r.get("in_payments")):
        paid, comm = _eur(r.get("paid_safe_eur")), _eur(r.get("committed_safe_eur"))
        npub = _n(r.get("payments_n_publishers"))
        money = " · ".join(x for x in (f"{paid} paid" if paid != "—" else "", f"{comm} ordered" if comm != "—" else "") if x)
        money = money or "present"
        items.append(
            f"<li><strong>Public-body payments</strong> — {money} by {npub:,} "
            f"bod{'y' if npub == 1 else 'ies'} "
            '<span class="pr-notice-tag">realised spend</span></li>'
        )
    st.html(
        '<div class="pr-ted-xref">'
        '<div class="pr-ted-xref-h">Register footprint — the same firm across public money</div>'
        '<div class="pr-ted-xref-b">Matched by Companies Registration Office number, this firm appears '
        f"in <strong>{n_reg} of 3</strong> public-money registers:"
        f'<ul class="pr-notice-list">{"".join(items)}</ul>'
        "These are <strong>different stages</strong> — an award ceiling is what a contract <em>could</em> "
        "be worth; realised spend is what a body <em>reported paying</em>. They are shown separately and "
        "<strong>never added together</strong>. Absence from a register is coverage, not missing money "
        "(only a fraction of State spend is published in the payments lists).</div></div>"
    )


def _render_paid_supplier_panel(supplier_norm: str) -> None:
    """Cross-reference on an eTenders supplier profile: what public bodies actually PAID this
    firm (a later lifecycle stage than the awards above — never added to them)."""
    res = fetch_payments_for_supplier_result(supplier_norm)
    if not res.ok or res.data.empty:
        return
    parts = []
    for r in res.data.itertuples():
        val = _eur(getattr(r, "total_safe_eur", None))
        if val == "—":
            continue
        verb = _paid_verb(getattr(r, "realisation_tier", "SPENT"))
        floor = " (indicative floor — mixed VAT bases)" if _truthy(getattr(r, "vat_mixed", None)) else ""
        parts.append(
            f"<strong>{val} {verb}</strong> by {_n(r.n_publishers):,} public "
            f"bod{'ies' if _n(r.n_publishers) != 1 else 'y'}{floor}"
        )
    if not parts:
        return
    st.html(
        '<div class="pr-ted-xref"><div class="pr-ted-xref-h">Money actually paid (public-body disclosures)</div>'
        f'<div class="pr-ted-xref-b">This firm was {", and ".join(parts)} (over €20k, self-published). '
        "A later stage than the awards above — these are <em>not</em> added to the award totals.</div></div>"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Tab: EU-level awards (TED) — a SEPARATE register, never summed with eTenders.
# ──────────────────────────────────────────────────────────────────────────────
def _ted_value_pill(val) -> str:
    """Sum-safe TED value pill; omitted when the firm has no summable value (all its EU
    notices are framework ceilings) so the card shows the trustworthy count instead of '—'."""
    if _eur(val) == "—":
        return ""
    return f'<span class="pr-pill pr-pill-val">{_eur(val)} awarded (EU)</span>'


def _ted_competition_strip() -> None:
    """Neutral competition-intensity facts from the eForms award notices: how many received
    only one tender, ran without an open call, or were awarded on lowest price alone. Framed
    strictly as disclosure (no-inference rule) — never 'uncompetitive'/'rigged' as a verdict."""
    res = fetch_ted_competition_stats_result()
    if not res.ok or res.data.empty:
        return
    s = res.data.iloc[0]
    with_t = _n(s.get("notices_with_tenders"))
    single = _n(s.get("single_bid_notices"))
    if not with_t and not _n(s.get("uncompetitive_notices")):
        return
    parts = []
    if with_t:
        parts.append(
            f"<strong>{single:,}</strong> of {with_t:,} award notices that report a tender count "
            f"received <strong>only one tender</strong> on at least one lot ({100 * single / with_t:.0f}%)"
        )
    unc = _n(s.get("uncompetitive_notices"))
    if unc:
        parts.append(f"<strong>{unc:,}</strong> were awarded without an open competitive call")
    po = _n(s.get("price_only_notices"))
    if po:
        parts.append(f"<strong>{po:,}</strong> were awarded on lowest price alone")
    st.html(
        '<p class="pr-cap"><strong>Competition signals (eForms, 2024+):</strong> '
        + "; ".join(parts)
        + ". These are factual disclosures recorded in the notices themselves — a single tender or "
        "a negotiated procedure is a matter of record, not evidence of wrongdoing. Competition detail "
        "is only recorded from 2024 (eForms); earlier years show winners and value only.</p>"
    )


def _render_ted() -> None:
    stats_res = fetch_ted_corpus_stats_result()
    if not stats_res.ok or stats_res.data.empty:
        empty_state(
            "EU-level award data isn't available right now",
            "The TED register couldn't be loaded — a source/pipeline issue, not an empty result.",
        )
        return
    s = stats_res.data.iloc[0]

    show_pan_eu = st.toggle(
        "Include pan-EU research frameworks",
        value=False,
        key="pr_ted_paneu",
        help="375 notices (e.g. GÉANT) where Ireland is one of dozens of participants. Their "
        "vast shared ceilings are never summable, so this only changes the notice count.",
    )
    n_shown = _n(s.get("n_notices")) if show_pan_eu else _n(s.get("n_notices_ex_pan_eu"))
    span = f"{_n(s.get('min_year'))}–{_n(s.get('max_year'))}"
    caption = (
        f"{n_shown:,} EU Official Journal award notices ({span}), from {_n(s.get('n_buyers')):,} "
        f"Irish public buyers. {_eur_scale(s.get('value_safe_eur'))} in summable awarded value — "
        "a different register from eTenders (some firms appear in both; the two are never added "
        "together)."
    )
    if show_pan_eu:
        caption += (
            f" Including the {_n(s.get('n_pan_eu')):,} pan-EU frameworks adds "
            f"{_eur_scale(s.get('pan_eu_ceiling_eur'))} of <em>shared</em> ceilings — a mirage like "
            "the €570bn headline, never real Irish spend."
        )
    else:
        caption += f" {_n(s.get('n_pan_eu')):,} pan-EU research frameworks are excluded from totals."
    st.html(f'<p class="pr-cap">{caption}</p>')

    _ted_competition_strip()

    # EU awards over time (2016–2026) — the payoff of the legacy backfill. Collapsed so the
    # winner ranking stays the first thing on the tab (matches the eTenders trend pattern).
    tr = fetch_ted_awards_by_year_result()
    if tr.ok and not tr.data.empty and len(tr.data) > 1:
        with st.expander("EU awards over time"):
            st.bar_chart(tr.data, x="year", y="n_awards", x_label="Year", y_label="Awards", height=200, color="#9c5b2e")

    res = fetch_ted_supplier_summary_result(limit=_TOP, order_by="awards")
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        empty_state("No TED winners", "The EU register loaded but returned no company-class winners.")
        return
    st.caption(
        f"Top {len(df):,} firms by number of EU award notices won. Value is awarded value, not spend. "
        "Click a firm to open its individual EU notices."
    )
    cards = []
    for i, r in enumerate(df.head(_TOP).itertuples(), start=1):
        meta = f"{_awards_word(_n(r.n_awards))} · {_n(r.n_buyers):,} buyer{'s' if _n(r.n_buyers) != 1 else ''}"
        cro = _cro_pill_from(getattr(r, "cro_company_num", None), getattr(r, "cro_company_status", None))
        pills = [p for p in (_ted_value_pill(r.ted_value_safe_eur), cro) if p]
        inner = _card(f"<span>{_esc(r.winner_name)}</span>", meta, pills, rank=i)
        cards.append(
            clickable_card_link(
                href=_ted_winner_href(r.winner_join_norm),
                inner_html=inner,
                aria_label=f"View the EU award notices won by {r.winner_name}",
            )
        )
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html(
        '<div class="pr-foot"><strong>Source:</strong> TED — Tenders Electronic Daily, the EU '
        'Official Journal of public procurement (<a href="https://ted.europa.eu" target="_blank" '
        'rel="noopener">ted.europa.eu ↗</a>), winners matched to the Companies Registration Office. '
        "2024+ from the TED API; 2016–2023 winner detail recovered from the per-notice Official Journal "
        "XML (the API omits it for pre-2024 notices). Award notices, not payments; a separate register "
        "from the national eTenders data — never summed.</div>"
    )


def _cro_pill_from(company_num, status) -> str:
    """CRO chip from explicit num/status (TED rows expose these directly, not as a row attr)."""
    if not _truthy(company_num):
        return ""
    label = _esc(_coalesce(status) or "matched")
    return f'<span class="pr-pill pr-pill-cro">CRO: {label}</span>'


def _render_ted_supplier_panel(supplier_norm: str) -> None:
    """Cross-reference block on an eTenders supplier profile: the same firm's TED (EU-level)
    footprint, matched on the normalised name. Clearly a separate register — never added to
    the eTenders headline (honesty rail; 66% of TED winners also appear in eTenders)."""
    res = fetch_ted_for_supplier_result(supplier_norm)
    if not res.ok or res.data.empty:
        return
    r = res.data.iloc[0]
    n = _n(r.get("n_awards"))
    if n <= 0:
        return
    val = _eur(r.get("ted_value_safe_eur"))
    val_clause = f" worth {val} in summable awarded value" if val != "—" else ""
    st.html(
        '<div class="pr-ted-xref"><div class="pr-ted-xref-h">Also in the EU register (TED)</div>'
        f'<div class="pr-ted-xref-b">This firm also won <strong>{n:,} EU Official Journal award '
        f"notice{'' if n == 1 else 's'}</strong>{val_clause}, from {_n(r.get('n_buyers')):,} buyers "
        "(2016–2026). A separate register — these are <em>not</em> added to the national total above.</div></div>"
    )

    # The conduit: route the reader to the AUTHORITATIVE notice. The tracker stores a thin
    # slice of each award (winner, buyer, a value-kind tag); the full deliverable, the real
    # framework ceiling and the award criteria live in the EU Official Journal notice itself.
    # The core query also rolls up CLOSELY-NAMED winners (shared brand stem) so a merged/
    # renamed entity's notices surface here; we keep exact-name and variant rows in SEPARATE,
    # labelled sections so a name match is never passed off as a verified same-company claim.
    # Display-only — every link points at the source; nothing here is computed or inferred.
    exact_html, variant_html, total = _ted_notices_sections(supplier_norm)
    if total:
        with st.expander(f"Open the {total:,} authoritative EU notice{'' if total == 1 else 's'} on TED ↗"):
            st.html(_TED_NOTICES_INTRO + exact_html)
            if variant_html:
                st.html(variant_html)


def _ted_notice_li(nr, *, show_name: bool) -> str:
    """One TED notice as a source-linked list item. ``show_name`` leads with the winner's own
    published name (used on variant rows so a name-based grouping is never hidden)."""
    url = _coalesce(getattr(nr, "notice_url", None))
    if not url:
        return ""
    date = _coalesce(getattr(nr, "dispatch_date", None))[:10]
    buyer = _esc(_coalesce(getattr(nr, "buyer_name", None)) or "—")
    is_fw = _coalesce(getattr(nr, "value_kind", None)) == "framework_or_dps_ceiling"
    tag = "framework — shared ceiling, not a payment" if is_fw else "contract award"
    name_pre = f"<strong>{_esc(_coalesce(getattr(nr, 'winner_name', None)))}</strong> — " if show_name else ""
    return (
        f'<li class="pr-notice"><a href="{_esc(url)}" target="_blank" rel="noopener">'
        f'{name_pre}{buyer} · {date} ↗</a> <span class="pr-notice-tag">{tag}</span></li>'
    )


_TED_NOTICES_INTRO = (
    '<p class="pr-cap">The tracker stores a thin slice of each award. Each notice below opens '
    "the full Official Journal record on TED — where the authority publishes what is actually "
    "being built, the real framework ceiling and the award criteria. The source, not our summary.</p>"
)


def _ted_notices_sections(supplier_norm: str) -> tuple[str, str, int]:
    """Build one winner's TED notice list, split into an exact-name ``<ul>`` and a labelled
    closely-named-variant section. Returns ``(exact_html, variant_html, total_count)`` —
    both blocks empty and total 0 when the firm has no linkable notices. Shared by the
    supplier-profile cross-reference panel and the EU-register winner drill-down so the
    name-match honesty copy can never drift between them."""
    notices_res = fetch_ted_notices_for_supplier_result(supplier_norm)
    ndf = notices_res.data if notices_res.ok else pd.DataFrame()
    exact_li = [
        li
        for nr in ndf.itertuples()
        if _truthy(getattr(nr, "is_exact_name", False))
        for li in (_ted_notice_li(nr, show_name=False),)
        if li
    ]
    variant_li = [
        li
        for nr in ndf.itertuples()
        if not _truthy(getattr(nr, "is_exact_name", False))
        for li in (_ted_notice_li(nr, show_name=True),)
        if li
    ]
    exact_html = f'<ul class="pr-notice-list">{"".join(exact_li)}</ul>' if exact_li else ""
    variant_html = ""
    if variant_li:
        variant_html = (
            '<p class="pr-cap" style="margin-top:0.8rem"><strong>Closely-named winners.</strong> '
            f"{len(variant_li):,} further notice{'' if len(variant_li) == 1 else 's'} won under a "
            "<em>similar</em> name (shared name stem — e.g. a renamed or merged company). Grouped by "
            "name only; these <em>may be different legal entities</em> — confirm via the CRO number on "
            "each notice before treating them as one firm.</p>"
            f'<ul class="pr-notice-list">{"".join(variant_li)}</ul>'
        )
    return exact_html, variant_html, len(exact_li) + len(variant_li)


def _render_ted_winner_profile(join_norm: str) -> None:
    """Drill-down for one EU-register (TED) winner: the firm's individual Official Journal
    award notices as line items, each linking to the authoritative source. The EU register's
    counterpart to the national supplier profile — reached from the TED winner ranking, and
    NOT gated on the firm appearing in the national eTenders register (most TED-only winners
    don't). A separate register, never summed with the national award totals."""
    if back_button("← Back to procurement", key="prtedwin"):
        _return_to_browse("wins")

    res = fetch_ted_for_supplier_result(join_norm)
    row = res.data.iloc[0] if (res.ok and not res.data.empty) else None
    if row is None:
        if not res.ok:
            empty_state(
                "EU register isn't available right now",
                "The TED views couldn't be loaded — a source/pipeline issue, not an empty result.",
            )
        else:
            empty_state("EU winner not found", "That link didn't match a firm in the EU register. Use Back to return.")
        return

    name = _esc(_coalesce(row.get("winner_name"))) or "—"
    n_awards, n_buyers = _n(row.get("n_awards")), _n(row.get("n_buyers"))
    sub = f"{_awards_word(n_awards)} from {n_buyers:,} EU public buyer{'s' if n_buyers != 1 else ''} · 2016–2026"
    st.html(
        f'<div class="pr-prof-head"><div class="pr-prof-kicker">EU REGISTER · TED</div>'
        f'<h1 class="pr-prof-name">{name}</h1><div class="pr-prof-sub">{sub}</div></div>'
    )
    pills = [
        p
        for p in (
            _ted_value_pill(row.get("ted_value_safe_eur")),
            _cro_pill_from(row.get("cro_company_num"), row.get("cro_company_status")),
        )
        if p
    ]
    if pills:
        st.html(f'<div class="pr-pills" style="margin:0.1rem 0 0.6rem">{"".join(pills)}</div>')

    st.caption(
        "EU Official Journal (TED) award notices won by this firm — a separate register from the national "
        "eTenders data, and never summed with it. Award notices, not payments; framework rows are shared "
        "ceilings, not money paid."
    )
    exact_html, variant_html, total = _ted_notices_sections(join_norm)
    if not total:
        empty_state(
            "No linkable notices", "This firm is in the EU ranking but none of its notices carry a source link."
        )
    else:
        st.html(_TED_NOTICES_INTRO + exact_html)
        if variant_html:
            st.html(variant_html)

    # Same firm's lot-level competition context (TED 2024+), shown with its no-inference caveat.
    _render_supplier_competition_panel(join_norm)

    # If the firm is ALSO on the national register, route to its full cross-register dossier.
    sup = fetch_supplier_summary_result(limit=None)
    if sup.ok and not sup.data.empty and bool((sup.data["supplier_norm"] == join_norm).any()):
        st.html(
            f'<div style="margin:1rem 0"><a class="dt-entity-cta" href="{_esc(company_profile_url(join_norm))}" '
            'target="_self">See this firm’s full public-money dossier (national awards + payments) →</a></div>'
        )

    st.html(
        '<div class="pr-foot"><strong>Source:</strong> TED — Tenders Electronic Daily, the EU '
        'Official Journal of public procurement (<a href="https://ted.europa.eu" target="_blank" '
        'rel="noopener">ted.europa.eu ↗</a>), winners matched to the Companies Registration Office. '
        "Award notices, not payments; a separate register from the national eTenders data — never summed.</div>"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Supplier-profile context panels (shared by the in-page ?supplier= profile and the
# /company dossier). All pre-aggregated in the registered views; factual structure
# signals with their caveats — never verdicts (no-inference rule).
# ──────────────────────────────────────────────────────────────────────────────
def _render_supplier_competition_panel(supplier_norm: str) -> None:
    """Lot-level single-bid context for one firm (TED 2024+, sole-winner notices only)
    against the national baseline — OpenTender-style competition context on the profile.
    Omitted below 5 bid-counted lots (a rate over 2 lots would mislead)."""
    res = fetch_supplier_single_bid_result(supplier_norm)
    if not res.ok or res.data.empty:
        return
    r = res.data.iloc[0]
    lots, single = _n(r.get("n_lots_with_bidcount")), _n(r.get("n_single_bid_lots"))
    if lots < 5:
        return
    pct = r.get("single_bid_lot_pct")
    base_res = fetch_single_bid_baseline_result()
    base_clause = ""
    if base_res.ok and not base_res.data.empty:
        b = base_res.data.iloc[0].get("single_bid_lot_pct")
        if b is not None:
            base_clause = f" The national rate across all EU-notice lots is <strong>{float(b):g}%</strong>."
    excl = _n(r.get("n_multi_winner_notices_excluded"))
    excl_clause = (
        f" ({excl:,} multi-winner notice{'' if excl == 1 else 's'} excluded — their lot counts "
        "can't be attributed to one winner.)"
        if excl
        else ""
    )
    st.html(
        '<div class="pr-ted-xref"><div class="pr-ted-xref-h">Competition context (EU notices, 2024+)</div>'
        f'<div class="pr-ted-xref-b">Of the <strong>{lots:,}</strong> contract lots this firm won outright '
        f"that report a bid count, <strong>{single:,}</strong> drew a single bid "
        f"(<strong>{float(pct):g}%</strong>).{base_clause} A single bid is recorded fact, often wholly "
        f"legitimate (a niche specialism, genuine urgency) — context to look at, never evidence of "
        f"wrongdoing.{excl_clause}</div></div>"
    )


def _render_supplier_relationships_panel(supplier_norm: str) -> None:
    """The firm's repeat buyers (distinct-years spans) + its top-buyer share — structure
    facts from the awards register. Central-purchasing buyers (OGP / EPS) are badged:
    a streak with them is repeated central-framework success, not a bilateral relationship."""
    inc = fetch_incumbency_for_supplier_result(supplier_norm)
    idf = inc.data if inc.ok else pd.DataFrame()
    idf = idf[idf["n_awards"] >= 2] if not idf.empty else idf
    dep = fetch_dependency_for_supplier_result(supplier_norm)
    drow = dep.data.iloc[0] if (dep.ok and not dep.data.empty) else None
    if idf.empty and drow is None:
        return

    parts = []
    if drow is not None and _n(drow.get("total_awards")) >= 5:
        share = drow.get("top_authority_share_pct")
        cp = (
            " — the Office of Government Procurement buys on behalf of the whole public service, "
            "so this reflects central frameworks, not one bilateral customer"
            if _truthy(drow.get("top_authority_is_central_purchasing"))
            else ""
        )
        parts.append(
            f"<strong>{_n(drow.get('awards_from_top_authority')):,}</strong> of its "
            f"<strong>{_n(drow.get('total_awards')):,}</strong> recorded awards "
            f"({float(share):g}%) came from <strong>{_esc(drow.get('top_authority'))}</strong>{cp}."
        )
    if parts:
        st.html(
            '<div class="pr-ted-xref"><div class="pr-ted-xref-h">Buyer relationships</div>'
            f'<div class="pr-ted-xref-b">{" ".join(parts)} A repeat relationship is a structure fact — '
            "durable incumbency is often the procurement system working (framework renewals, "
            "specialist capability).</div></div>"
        )
    if not idf.empty and len(idf) >= 1:
        rows = []
        for r in idf.itertuples():
            yrs = _n(r.n_distinct_years)
            span = (
                f"{_n(r.first_year)}–{_n(r.last_year)}"
                if _n(r.first_year) != _n(r.last_year)
                else str(_n(r.first_year))
            )
            badge = (
                ' <span class="pr-pill pr-pill-lob">central purchasing body</span>'
                if _truthy(r.authority_is_central_purchasing)
                else ""
            )
            rows.append(
                f'<div class="pr-award"><div class="pr-award-body">'
                f'<div class="pr-award-auth">{_esc(r.contracting_authority)}{badge}</div>'
                f'<div class="pr-award-meta">{_awards_word(_n(r.n_awards))} across '
                f"{yrs:,} year{'s' if yrs != 1 else ''} ({span})</div></div></div>"
            )
        with st.expander(f"Repeat buyers ({len(idf):,})"):
            st.html("".join(rows))


def _render_supplier_call_offs_panel(supplier_norm: str) -> None:
    """The firm's call-off awards (drawdowns under a framework/DPS) with the parent
    agreement named where its notice exists in the corpus — the framework nesting, made
    visible. An unresolved parent is disclosed, never hidden; a parent ceiling is context,
    never added to the call-off's own value."""
    res = fetch_call_offs_for_supplier_result(supplier_norm)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        return
    resolved = df[df["parent_in_corpus"] == True]  # noqa: E712 — pandas mask
    n_unresolved = len(df) - len(resolved)
    with st.expander(f"Framework drawdowns ({len(df):,} call-offs)"):
        st.html(
            '<p class="pr-cap">These awards are <strong>call-offs</strong> — drawdowns under a '
            "framework or dynamic purchasing system. Where the parent agreement's notice is in the "
            "published corpus it is named below; its ceiling is the framework's spending limit, "
            "never this call-off's value and never added to it.</p>"
        )
        rows = []
        for r in resolved.head(15).itertuples():
            parent_bits = [f"under agreement {_esc(r.parent_agreement_id)}"]
            pv = _eur(getattr(r, "parent_value_eur", None))
            if pv != "—" and _coalesce(getattr(r, "parent_value_kind", None)) == "framework_or_dps_ceiling":
                parent_bits.append(f"ceiling {pv}")
            n_ps = _n(getattr(r, "parent_n_suppliers", None))
            if n_ps > 1:
                parent_bits.append(f"{n_ps:,} suppliers on the framework")
            rows.append(
                f'<div class="pr-award"><div class="pr-award-body">'
                f'<div class="pr-award-auth">{_esc(r.contracting_authority)}</div>'
                f'<div class="pr-award-meta">{fmt_civic_date(getattr(r, "award_date", None))} · '
                f"{' · '.join(parent_bits)}</div></div>{_award_value_html(r)}</div>"
            )
        if rows:
            st.html("".join(rows))
        if n_unresolved:
            st.html(
                f'<p class="pr-cap">{n_unresolved:,} further call-off{"" if n_unresolved == 1 else "s"} '
                "name a parent agreement whose own notice is <strong>not in the published corpus</strong> — "
                "a coverage gap in the source register, disclosed rather than hidden.</p>"
            )


# ──────────────────────────────────────────────────────────────────────────────
# Tab: Tender pipeline (TED cn-standard) — a THIRD grain (pre-award), never summed.
# ──────────────────────────────────────────────────────────────────────────────
def _render_ted_tenders() -> None:
    stats_res = fetch_ted_tenders_stats_result()
    if not stats_res.ok or stats_res.data.empty:
        empty_state(
            "Tender-pipeline data isn't available right now",
            "The TED competition-notice view couldn't be loaded — a source/pipeline issue, not an empty result.",
        )
        return
    s = stats_res.data.iloc[0]
    span = f"{_n(s.get('min_year'))}–{_n(s.get('max_year'))}"
    st.html(
        '<div class="pr-caveat"><strong>The tender pipeline — opportunities, not awards.</strong> '
        f"{_n(s.get('n_notices')):,} EU-journal <em>competition</em> notices ({span}) from "
        f"{_n(s.get('n_buyers')):,} Irish public buyers — what is being put out to tender. The estimated "
        "value shown is a <em>buyer estimate recorded before any award</em>: never a contract value, never "
        "a payment, and never added to the award or payment figures elsewhere on this page.</div>"
    )
    # Two facets, side by side: an open-by-deadline DATE gate, and a SECTOR (CPV division) filter.
    # Unlike the national feed above, TED notices carry a CPV code, so sector filtering is possible here.
    fcol1, fcol2 = st.columns([1, 1])
    with fcol1:
        only_open = st.toggle(
            "Only tenders still open by deadline",
            value=False,
            key="pr_ted_open",
            help=f"{_n(s.get('n_still_open')):,} of {_n(s.get('n_notices')):,} have a submission deadline still in the future.",
        )
    sector = None
    with fcol2:
        # Sector option list carries a per-division count (the "facet counts in parentheses"
        # convention); counts track the open toggle so they match the list below.
        sectors_res = fetch_ted_tender_sectors_result(only_open=only_open)
        sec_df = sectors_res.data if sectors_res.ok else pd.DataFrame()
        if not sec_df.empty:
            label_to_sector = {f"{r.sector} ({int(r.n):,})": r.sector for r in sec_df.itertuples()}
            choice = st.selectbox(
                "Sector (CPV division)",
                ["All sectors", *label_to_sector.keys()],
                index=0,
                key="pr_ted_sector",
            )
            sector = label_to_sector.get(choice)
    res = fetch_ted_tenders_result(only_open=only_open, limit=_TOP, sector=sector)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        if sector:
            empty_state("No tenders in that sector", f"No competition notice in “{sector}” for this filter.")
        else:
            empty_state("No tenders", "No still-open competition notice." if only_open else "The view returned no rows.")
        return
    sector_label = f" in {sector}" if sector else ""
    st.caption(
        f"{len(df):,} most-recent competition notices{' still open' if only_open else ''}{sector_label}. "
        "Estimated value is a pre-award buyer estimate — not an award and not a payment. "
        "Click a notice to open the full tender on TED."
    )
    cards = []
    for r in df.head(_TOP).itertuples():
        meta_parts = [
            _esc(_coalesce(getattr(r, "cpv_division", None))),
            _esc(_coalesce(getattr(r, "procedure_type", None))),
        ]
        dl = _coalesce(getattr(r, "submission_deadline", None))
        if dl:
            meta_parts.append(f"deadline {fmt_civic_date(dl)}")
        meta = " · ".join(p for p in meta_parts if p)
        pills = []
        ev = _eur(getattr(r, "estimated_value_eur", None))
        if ev != "—":
            pills.append(f'<span class="pr-pill pr-pill-val">{ev} est. value</span>')
        if _truthy(getattr(r, "is_still_open", None)):
            pills.append('<span class="pr-pill pr-pill-lob">still open</span>')
        if _truthy(getattr(r, "is_uncompetitive_procedure", None)):
            pills.append('<span class="pr-pill pr-pill-lob">no open call</span>')
        buyer = _coalesce(getattr(r, "buyer_name", None))
        inner = _card(f"<span>{_esc(buyer) or '—'}</span>", meta, pills)
        # Each card IS one line item (a single notice) — make the whole card open the
        # authoritative EU Official Journal record, the closest thing to a pre-award detail page.
        url = _coalesce(getattr(r, "notice_url", None))
        if url.startswith("http"):
            cards.append(
                clickable_card_link(
                    href=url,
                    inner_html=inner,
                    aria_label=f"Open the EU tender notice from {buyer or 'this buyer'} on TED",
                    target="_blank",
                )
            )
        else:
            cards.append(inner)
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html(
        '<div class="pr-foot"><strong>Source:</strong> TED — Tenders Electronic Daily, EU Official Journal '
        'competition notices (<a href="https://ted.europa.eu" target="_blank" rel="noopener">ted.europa.eu ↗</a>). '
        "Pre-award opportunities; estimated values are buyer estimates — never awards or payments, and never summed.</div>"
    )


# Human label for how the estimated end date was derived (carried from the view; the
# basis is part of the fact being presented, not a UI judgement).
_END_BASIS_LABEL = {
    "explicit_end_date": "end date on the notice",
    "start_plus_duration": "start date + advertised duration",
    "conclusion_plus_duration": "signed date + advertised duration",
}


def _render_expiring_contracts() -> None:
    stats_res = fetch_expiring_contracts_stats_result()
    if not stats_res.ok or stats_res.data.empty:
        empty_state(
            "Contract-term data isn't available right now",
            "The advertised-term view couldn't be loaded — a source/pipeline issue, not an empty result.",
        )
        return
    s = stats_res.data.iloc[0]
    st.html(
        '<div class="pr-caveat"><strong>Advertised contract terms — when current contracts are due to end.</strong> '
        f"{_n(s.get('n_with_estimate')):,} TED award notices state a contract term (an explicit end date on "
        f"{_n(s.get('n_explicit')):,} of them; otherwise the signed/start date plus the advertised duration). "
        "These are the terms <em>as advertised on the award notice</em> — a contract can end early or run "
        "longer through renewal options, which are shown separately and never folded in.</div>"
    )
    window = st.segmented_control(
        "Ending within",
        ["6 months", "12 months", "24 months"],
        default="12 months",
        key="pr_expiring_window",
        label_visibility="collapsed",
    )
    months = int((window or "12 months").split()[0])
    res = fetch_expiring_contracts_result(months_ahead=months, limit=_TOP)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        empty_state("No contracts in this window", "No advertised term ends in the selected period.")
        return
    st.caption(
        f"{len(df):,} contracts whose advertised term ends within {months} months, soonest first. "
        "Values are award/ceiling figures shown for context — never totals. "
        "Click a contract to open its award notice on TED."
    )
    cards = []
    for r in df.itertuples():
        meta_parts = [_esc(_coalesce(getattr(r, "cpv_division", None)))]
        winners = _coalesce(getattr(r, "winners_display", None))
        if winners:
            meta_parts.append(_esc(winners))
        dur = getattr(r, "contract_duration_months", None)
        if dur is not None and not pd.isna(dur):
            dur_i = int(dur) if float(dur).is_integer() else dur
            meta_parts.append(f"{dur_i}-month term")
        meta = " · ".join(p for p in meta_parts if p)
        pills = []
        end = _coalesce(getattr(r, "contract_end_date_est", None))
        if end:
            pills.append(f'<span class="pr-pill pr-pill-val">ends {fmt_civic_date(end)}</span>')
        ev = _eur(getattr(r, "award_value_eur", None))
        if ev != "—":
            kind = _coalesce(getattr(r, "value_kind", None))
            pills.append(f'<span class="pr-pill">{ev}{" ceiling" if kind == "framework_or_dps_ceiling" else ""}</span>')
        renew = getattr(r, "renewal_max", None)
        if renew is not None and not pd.isna(renew) and int(renew) > 0:
            pills.append(f'<span class="pr-pill pr-pill-lob">up to {int(renew)} renewals</span>')
        basis = _END_BASIS_LABEL.get(_coalesce(getattr(r, "contract_end_basis", None)))
        if basis:
            pills.append(f'<span class="pr-pill">{basis}</span>')
        buyer = _coalesce(getattr(r, "buyer_name", None))
        inner = _card(f"<span>{_esc(buyer) or '—'}</span>", meta, pills)
        url = _coalesce(getattr(r, "notice_url", None))
        if url.startswith("http"):
            cards.append(
                clickable_card_link(
                    href=url,
                    inner_html=inner,
                    aria_label=f"Open the EU award notice from {buyer or 'this buyer'} on TED",
                    target="_blank",
                )
            )
        else:
            cards.append(inner)
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html(
        '<div class="pr-foot"><strong>Source:</strong> TED — Tenders Electronic Daily, EU Official Journal award '
        'notices (<a href="https://ted.europa.eu" target="_blank" rel="noopener">ted.europa.eu ↗</a>), eForms '
        "contract-term fields (BT-36/BT-145/BT-536/BT-537). The end date is the advertised term, not a verified "
        "event; ~36% of award notices state a term. Winner names follow the published notice; sole traders and "
        "individuals are not shown.</div>"
    )


# ──────────────────────────────────────────────────────────────────────────────
# National (eTenders) forward lenses — the sub-EU-threshold mass TED can't see.
# Rendered ABOVE the TED lens in each "Open right now" segment as its own register,
# never value-merged with TED (two registers, never summed). Snapshot-based, so it
# carries an explicit freshness line + staleness guard.
# ──────────────────────────────────────────────────────────────────────────────
def _national_freshness_html(retrieved) -> str:
    """Point-in-time freshness line for the live national snapshot. Display-only — formats
    the snapshot stamp and flags it when older than 3 days (a stale open-tenders list whose
    deadlines have passed would mislead; this is the one real risk of a scraped snapshot)."""
    ts = pd.to_datetime(retrieved, errors="coerce", utc=True)
    if pd.isna(ts):
        return ""
    age_days = (pd.Timestamp.now(tz="UTC") - ts).days
    stale = (
        ' <strong class="pr-cap-stale">— this snapshot may be out of date; '
        "some deadlines below may already have passed.</strong>"
        if age_days > 3
        else ""
    )
    return f'<p class="pr-cap">National opportunities as of {fmt_civic_date(ts)}.{stale}</p>'


def _national_sector_facet(within_days: int | None) -> str | None:
    """Sector (CPV division) facet for the national open-tenders list. Returns the chosen sector,
    or None when 'All sectors' is picked OR the snapshot has no CPV yet (the sectors query is
    unavailable pre-enrichment, so the facet is simply omitted — the date filter still works)."""
    res = fetch_live_tender_sectors_result(within_days)
    if not res.ok or res.data.empty:
        return None
    label_to_sector = {f"{r.sector} ({int(r.n):,})": r.sector for r in res.data.itertuples()}
    choice = st.selectbox(
        "Sector (CPV division)",
        ["All sectors", *label_to_sector.keys()],
        index=0,
        key="pr_live_sector",
    )
    return label_to_sector.get(choice)


def _render_national_open_tenders() -> None:
    """Open NATIONAL tenders (etenders.gov.ie), PLANNED tier, soonest-closing first. A separate
    register from TED above — sub-EU-threshold opportunities (schools, councils, water schemes)."""
    stats_res = fetch_live_tenders_stats_result()
    if not stats_res.ok or stats_res.data.empty or _n(stats_res.data.iloc[0].get("n_open")) == 0:
        # Silent absence is honest here: the snapshot may simply not be polled yet. Show a quiet
        # note rather than an error so the TED lens below still reads as the primary content.
        st.html(
            '<div class="pr-foot"><strong>National (eTenders) live tenders:</strong> no current snapshot '
            "loaded. The national opportunities feed is refreshed separately from the EU-journal data above.</div>"
        )
        return
    s = stats_res.data.iloc[0]
    # The data's horizon: the furthest submission deadline in the open set. Surfacing it (and
    # making "All open" the default + reachable window) answers "project to the furthest date" —
    # the list was never capped at 30 days, but the largest pill was, which read as a cap.
    last_closing = s.get("last_closing")
    horizon = ""
    if not pd.isna(last_closing):
        horizon = f" The furthest deadline currently open is <strong>{fmt_civic_date(last_closing)}</strong>."
    st.html(
        '<div class="pr-caveat"><strong>National opportunities — open right now on eTenders.</strong> '
        f"{_n(s.get('n_open')):,} tenders currently accepting bids from {_n(s.get('n_buyers')):,} Irish public "
        f"buyers ({_n(s.get('closing_within_14d')):,} close within 14 days).{horizon} The sub-EU-threshold national "
        "picture the EU-journal feed above cannot show. The estimated value shown is a <em>buyer estimate "
        "recorded before any award</em>: never a contract value, never a payment, and never summed.</div>"
    )
    st.html(_national_freshness_html(s.get("retrieved_utc")))
    # Forward DATE facet: narrow to soonest-closing windows, or "All open" to project to the
    # furthest deadline in the data. The national eTenders snapshot carries a CPV division only
    # after the detail-page enrichment (added below when present); the TED lens always has one.
    max_days = _n(s.get("max_days"))
    windows = ["All open", "7 days", "14 days", "30 days", "90 days"]
    if max_days > 90:
        windows.append("180 days")
    window = st.segmented_control(
        "Closing within",
        windows,
        default="All open",
        key="pr_live_window",
        label_visibility="collapsed",
    )
    sel = window or "All open"
    within_days = None if sel == "All open" else int(sel.split()[0])
    # Sector facet — only when the snapshot carries a CPV division (post-enrichment). Degrades
    # silently to date-only on an un-enriched snapshot (the column is simply absent).
    sector = _national_sector_facet(within_days)
    res = fetch_live_tenders_result(limit=_TOP, within_days=within_days, sector=sector)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        if sector:
            empty_state("No national tenders in that sector", f"No open national tender in “{sector}” for this window.")
        elif within_days is not None:
            empty_state(
                "No national tenders closing that soon",
                f"No open national tender closes within {within_days} days. Try a wider window.",
            )
        else:
            empty_state("No open national tenders", "The national live-tender view returned no rows.")
        return
    window_label = "soonest-closing" if within_days is None else f"closing within {within_days} days"
    sector_label = f" in {sector}" if sector else ""
    st.caption(
        f"{len(df):,} {window_label} national tenders{sector_label}. Estimated value is a pre-award buyer estimate — "
        "not an award and not a payment. Click a tender to open it on eTenders."
    )
    cards = []
    for r in df.head(_TOP).itertuples():
        meta_parts = [_esc(_coalesce(getattr(r, "procedure", None)))]
        dl = _coalesce(getattr(r, "submission_deadline", None))
        if dl:
            meta_parts.append(f"closes {fmt_civic_date(dl)}")
        meta = " · ".join(p for p in meta_parts if p)
        pills = []
        days = getattr(r, "days_to_deadline", None)
        if days is not None and not pd.isna(days):
            d = int(days)
            label = "closes today" if d == 0 else f"{d} day{'s' if d != 1 else ''} left"
            cls = "pr-pill pr-pill-lob" if d <= 14 else "pr-pill"
            pills.append(f'<span class="{cls}">{label}</span>')
        ev = _eur(getattr(r, "estimated_value_eur", None))
        if ev != "—":
            pills.append(f'<span class="pr-pill pr-pill-val">{ev} est. value</span>')
        buyer = _coalesce(getattr(r, "buyer", None))
        title = _coalesce(getattr(r, "title", None))
        name_html = f"<span>{_esc(buyer) or '—'}</span>"
        if title:
            name_html += f'<span class="pr-sub">{_esc(title)}</span>'
        inner = _card(name_html, meta, pills)
        url = _coalesce(getattr(r, "detail_url", None))
        if url.startswith("http"):
            cards.append(
                clickable_card_link(
                    href=url,
                    inner_html=inner,
                    aria_label=f"Open the national tender from {buyer or 'this buyer'} on eTenders",
                    target="_blank",
                )
            )
        else:
            cards.append(inner)
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html(
        '<div class="pr-foot"><strong>Source:</strong> eTenders — the national public-procurement platform '
        '(<a href="https://www.etenders.gov.ie" target="_blank" rel="noopener">etenders.gov.ie ↗</a>), live '
        "request-for-tender notices captured as a point-in-time snapshot. Open opportunities only; estimated "
        "values are pre-award buyer estimates — never awards or payments, and never summed.</div>"
    )


def _render_national_expiring() -> None:
    """NATIONAL (eTenders) contracts whose advertised term is due to end — the re-tender pipeline,
    reconstructed from award date + advertised duration. A term, never a verified end event."""
    window = st.segmented_control(
        "National contracts ending within",
        ["12 months", "24 months", "36 months"],
        default="24 months",
        key="pr_expiring_etenders_window",
        label_visibility="collapsed",
    )
    months = int((window or "24 months").split()[0])
    res = fetch_expiring_etenders_result(months_ahead=months, limit=_TOP)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        st.html(
            '<div class="pr-foot"><strong>National (eTenders) contract terms:</strong> no national contracts '
            "with an advertised term ending in this window, or the award corpus isn't loaded.</div>"
        )
        return
    st.html(
        '<div class="pr-caveat"><strong>National contract terms due to end — the re-tender pipeline.</strong> '
        "Reconstructed from each national award's date plus its <em>advertised duration</em> — the term as "
        "stated, not a verified end event; contracts can end early or run longer through renewals (not folded "
        "in). Frameworks are excluded. The value shown is an award/ceiling figure for context — never summed.</div>"
    )
    st.caption(
        f"{len(df):,} national contracts whose advertised term ends within {months} months, soonest first. "
        "Advertised terms only; values are award/ceiling figures, never totals."
    )
    cards = []
    for r in df.itertuples():
        meta_parts = [_esc(_coalesce(getattr(r, "spend_category", None), getattr(r, "cpv_code", None)))]
        winner = _coalesce(getattr(r, "winner_display", None))
        if winner:
            meta_parts.append(_esc(winner))
        dur = getattr(r, "duration_months", None)
        if dur is not None and not pd.isna(dur):
            meta_parts.append(f"{int(dur)}-month term")
        meta = " · ".join(p for p in meta_parts if p)
        pills = []
        end = _coalesce(getattr(r, "est_end_date", None))
        if end:
            pills.append(f'<span class="pr-pill pr-pill-val">ends {fmt_civic_date(end)}</span>')
        ev = _eur(getattr(r, "award_value_eur", None))
        if ev != "—":
            pills.append(f'<span class="pr-pill">{ev} award value</span>')
        buyer = _coalesce(getattr(r, "buyer_name", None))
        contract = _coalesce(getattr(r, "contract_name", None))
        name_html = f"<span>{_esc(buyer) or '—'}</span>"
        if contract:
            name_html += f'<span class="pr-sub">{_esc(contract)}</span>'
        cards.append(_card(name_html, meta, pills))
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html(
        '<div class="pr-foot"><strong>Source:</strong> eTenders national award notices (estimated end = award '
        "date + advertised contract duration). The end date is the advertised term, not a verified event; ~43% "
        "of national awards state a duration, and frameworks / dynamic purchasing systems (DPS) are excluded. "
        "Sole-trader and individual winner "
        "names are not shown; the contract itself stays listed as public record.</div>"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Drill-down: a single supplier's profile + full award history (?supplier=)
# ──────────────────────────────────────────────────────────────────────────────
def _award_value_html(r) -> str:
    """The right-hand value block of an award row, ceiling-aware (honesty rail)."""
    is_ceiling = _coalesce(getattr(r, "value_kind", None)) == "framework_or_dps_ceiling"
    val = _eur(getattr(r, "value_eur", None))
    if val == "—":
        return ""
    sub = "framework ceiling — not a payment" if is_ceiling else "contract award value"
    cls = "pr-award-val ceiling" if is_ceiling else "pr-award-val"
    return f'<div class="{cls}">{val}<small>{sub}</small></div>'


def _award_notice_url(r) -> str:
    """The best path to the AUTHORITATIVE notice for one award row, in preference order:
    the EU Official Journal contract-award notice (TED CAN), then the TED contract notice,
    then the national eTenders notice (templated from the Tender ID). Empty when none
    resolve — the row then renders un-clickable rather than linking to a dead page."""
    for cand in (
        _coalesce(getattr(r, "ted_can_link", None)),
        _coalesce(getattr(r, "ted_notice_link", None)),
        _coalesce(getattr(r, "etenders_notice_url", None)),
    ):
        if cand.startswith("http"):
            return cand
    return ""


def _award_row(head: str, meta_parts: list[str], r) -> str:
    meta = " · ".join(p for p in meta_parts if p and p != "—")
    # The published contract title (100% filled in the source) — without it a line item's
    # only description is its generic CPV label ("IT services: consulting, …").
    title = _esc(_coalesce(getattr(r, "tender_title", None)))
    title_html = f'<div class="pr-award-title">{title}</div>' if title else ""
    inner = (
        f'<div class="pr-award"><div class="pr-award-body">'
        f'<div class="pr-award-auth">{head or "—"}</div>{title_html}'
        f'<div class="pr-award-meta">{meta or "—"}</div></div>{_award_value_html(r)}</div>'
    )
    # The guiding rail: every line item is a click away from its authoritative notice. Where a
    # link resolves, the whole row opens it (new tab — it's an external source); otherwise the
    # row stays static rather than pointing at a dead URL.
    url = _award_notice_url(r)
    if url:
        return clickable_card_link(
            href=url,
            inner_html=inner,
            aria_label=f"Open the source notice for this award to {head or 'this supplier'} ↗",
            target="_blank",
        )
    return inner


def _award_detail_meta(r) -> list[str]:
    """Detail meta fragments shared by every award row: procedure, contract term, bid
    count. The notice deep link is no longer inlined here — the whole award row is now a
    link to its authoritative notice (see _award_row / _award_notice_url). All values come
    straight from the view — display formatting only."""
    parts = [_esc(_coalesce(getattr(r, "procedure_type", None)))]
    months = _n(getattr(r, "contract_duration_months", None))
    if months > 0:
        parts.append(f"{months}-month term")
    bids = _n(getattr(r, "n_bids_received", None))
    if bids > 0:
        parts.append(f"{bids:,} bid{'' if bids == 1 else 's'} received")
    return parts


def _award_row_html(r) -> str:
    """Supplier-profile award row — headlines the contracting authority. Call-off rows are
    tagged: a drawdown under a framework/DPS, the nesting the register otherwise hides."""
    # category_label is the view's display fallback (CPV description, else OGP spend
    # category) — Main CPV is filled on only ~30% of award rows.
    cat = (
        _coalesce(getattr(r, "category_label", None))
        or _coalesce(getattr(r, "cpv_description", None))
        or _coalesce(getattr(r, "cpv_code", None))
    )
    meta = [
        fmt_civic_date(getattr(r, "award_date", None)),
        _esc(cat),
        _coalesce(getattr(r, "competition_type", None)),
        *_award_detail_meta(r),
    ]
    if _truthy(getattr(r, "is_call_off", None)):
        meta.append("framework call-off")
    return _award_row(_esc(r.contracting_authority) or "—", meta, r)


def _supplier_head(r) -> str:
    """Supplier name for an authority/category award row. Sole traders / individuals ARE
    named (owner decision 2026-06-06): eTenders is published procurement data, so a supplier
    name on a public contract is already public and shown in a business capacity — consistent
    with the 'Money actually paid' tab. Only the published name is shown; no other PII."""
    return _esc(getattr(r, "supplier", None)) or "—"


def _award_row_by_supplier(r) -> str:
    """Authority-profile award row — headlines the supplier who won it."""
    cat = (
        _coalesce(getattr(r, "category_label", None))
        or _coalesce(getattr(r, "cpv_description", None))
        or _coalesce(getattr(r, "cpv_code", None))
    )
    return _award_row(
        _supplier_head(r),
        [
            fmt_civic_date(getattr(r, "award_date", None)),
            _esc(cat),
            _coalesce(getattr(r, "competition_type", None)),
            *_award_detail_meta(r),
        ],
        r,
    )


def _award_row_cpv(r) -> str:
    """Category-profile award row — headlines the supplier, authority in the meta.
    No category fragment: every row here is already inside one CPV category."""
    return _award_row(
        _supplier_head(r),
        [
            fmt_civic_date(getattr(r, "award_date", None)),
            _esc(_coalesce(getattr(r, "contracting_authority", None))),
            _coalesce(getattr(r, "competition_type", None)),
            *_award_detail_meta(r),
        ],
        r,
    )


def _supplier_awards_section(row, supplier_norm: str) -> None:
    """Paginated eTenders award history for one firm, with the headline-reconciliation
    caption. Shared by the in-page supplier profile here and the /company dossier page
    (pages_code/company.py) so the honesty copy can never drift between the two."""
    awards = fetch_awards_for_supplier(supplier_norm)
    if awards is None or awards.empty:
        empty_state("No itemised awards", "The supplier is in the ranking but no award rows were returned.")
        return

    # Reconcile the headline with the rows the user is about to see: the sum-safe total
    # is composed ONLY of contract-award rows (never a ceiling), but the most recent
    # rows are often framework/DPS ceilings shown in rust — so a user can read "€134.6m
    # awarded" then scroll past a screen of "not a payment" rows and wonder where the
    # money is. The split counts (from the view, not computed here) close that gap.
    all_total = len(awards)
    # Year pills — jump to one year's awards (the contract-history-over-time ask). Display-only
    # filter + slice; the years come from the already-fetched frame.
    year = _award_year_pills(awards, key=f"pr_awyr_{supplier_norm}")
    awards = _filter_awards_by_year(awards, year)
    total = len(awards)
    n_safe = _n(row.get("n_value_safe_awards"))
    n_ceil = _n(row.get("n_ceiling_notices"))
    recon = (
        f"The {_eur(row.get('awarded_value_safe_eur'))} headline is the sum of {n_safe:,} contract "
        f"award{'' if n_safe == 1 else 's'} that carry a sum-safe value (all years)."
    )
    if n_ceil:
        recon += (
            f" A further {n_ceil:,} framework / DPS ceiling notice{'' if n_ceil == 1 else 's'} "
            "are listed below in rust — spending limits a buyer may draw down against, not payments, "
            "and never added to the headline."
        )
    if year is None:
        st.caption(f"Every recorded contract award to this supplier ({total:,} in total), most recent first. " + recon)
    else:
        st.caption(
            f"{total:,} award{'' if total == 1 else 's'} dated {year} "
            f"(of {all_total:,} all-time), most recent first. " + recon
        )
    if total == 0:
        empty_state("No awards in this year", f"This supplier has no recorded award dated {year}.")
        return
    key = f"pr_aw_{supplier_norm}_{year or 'all'}"
    page_idx = paginate(total, key_prefix=key, page_size=_AWARD_PAGE)
    page = awards.iloc[page_idx * _AWARD_PAGE : (page_idx + 1) * _AWARD_PAGE]
    st.html("".join(_award_row_html(r) for r in page.itertuples()))
    st.html('<div class="pr-sp-sm"></div>')
    pagination_controls(
        total,
        key_prefix=key,
        page_sizes=(_AWARD_PAGE,),
        default_page_size=_AWARD_PAGE,
        label="awards",
    )


def _render_supplier_profile(supplier_norm: str) -> None:
    if back_button("← Back to procurement", key="prsupprof"):
        _return_to_browse("wins")

    sup = fetch_supplier_summary_result(limit=None)
    if not sup.ok:
        empty_state(
            "Supplier data isn't available right now",
            "The procurement views couldn't be loaded — a source/pipeline issue, not an empty result.",
        )
        return

    match = sup.data[sup.data["supplier_norm"] == supplier_norm] if not sup.data.empty else sup.data
    if match.empty:
        empty_state(
            "Supplier not found",
            "That link didn't match a supplier in the ranking. Use Back to return to the register.",
        )
        return
    row = match.iloc[0]

    sub = f"{_awards_word(_n(row.get('n_awards')))} from {_n(row.get('n_authorities')):,} contracting authorities"
    st.html(
        f'<div class="pr-prof-head"><h1 class="pr-prof-name">{_esc(row.get("supplier"))}</h1>'
        f'<div class="pr-prof-sub">{sub}</div></div>'
    )

    pills = [_value_pill(row.get("awarded_value_safe_eur"))]
    pills += [p for p in (_cro_pill(row), _lobby_pill(row)) if p]
    st.html(f'<div class="pr-pills" style="margin:0.1rem 0 0.6rem">{"".join(pills)}</div>')

    _supplier_awards_section(row, supplier_norm)
    _render_supplier_call_offs_panel(supplier_norm)

    # Cross-references for the same firm — each a separate register/stage, never summed.
    # The footprint leads: a CRO-unified one-glance summary of which registers it's in, framing
    # the per-register detail panels that follow (rendered only when CRO-matched + multi-register).
    _render_supplier_register_footprint(row.get("company_num"))
    _render_paid_supplier_panel(supplier_norm)
    _render_ted_supplier_panel(supplier_norm)
    _render_supplier_competition_panel(supplier_norm)
    _render_supplier_relationships_panel(supplier_norm)

    st.html(
        '<div class="pr-foot"><strong>Source:</strong> eTenders / national procurement open data '
        '(<a href="https://data.gov.ie/dataset/contract-notices-published-on-etenders" '
        'target="_blank" rel="noopener">data.gov.ie ↗</a>). Values are awarded contract values, not '
        "actual payments; framework / DPS rows are ceilings a buyer may draw down against, not money paid.</div>"
    )


_FOOT_HTML = (
    '<div class="pr-foot"><strong>Source:</strong> eTenders / national procurement open data '
    '(<a href="https://data.gov.ie/dataset/contract-notices-published-on-etenders" '
    'target="_blank" rel="noopener">data.gov.ie ↗</a>). Values are awarded contract values, not '
    "actual payments; framework / DPS rows are ceilings a buyer may draw down against, not money paid. "
    "Suppliers shown are company-class registrations — sole traders and individuals are excluded.</div>"
)


def _render_award_list(awards: pd.DataFrame, *, key: str, row_fn) -> None:
    """Paginated award-row list shared by the supplier / authority / category profiles, with a
    display-only year-pill filter (the contract-history-over-time ask)."""
    all_total = len(awards)
    year = _award_year_pills(awards, key=f"{key}_yr")
    awards = _filter_awards_by_year(awards, year)
    total = len(awards)
    if year is None:
        st.caption(
            f"Every recorded contract award ({total:,} in total), most recent first. "
            "Framework / DPS ceilings are shown in rust and are not actual payments."
        )
    else:
        st.caption(
            f"{total:,} award{'' if total == 1 else 's'} dated {year} (of {all_total:,} all-time), most recent first. "
            "Framework / DPS ceilings are shown in rust and are not actual payments."
        )
    if total == 0:
        empty_state("No awards in this year", f"Nothing dated {year} here.")
        st.html(_FOOT_HTML)
        return
    pkey = f"{key}_{year or 'all'}"
    page_idx = paginate(total, key_prefix=pkey, page_size=_AWARD_PAGE)
    page = awards.iloc[page_idx * _AWARD_PAGE : (page_idx + 1) * _AWARD_PAGE]
    st.html("".join(row_fn(r) for r in page.itertuples()))
    st.html('<div class="pr-sp-sm"></div>')
    pagination_controls(total, key_prefix=pkey, page_sizes=(_AWARD_PAGE,), default_page_size=_AWARD_PAGE, label="awards")
    st.html(_FOOT_HTML)


def _render_authority_profile(authority: str) -> None:
    if back_button("← Back to procurement", key="prauthprof"):
        _return_to_browse("wins")

    res = fetch_authority_summary_result(limit=None)
    if not res.ok:
        empty_state("Authority data isn't available right now", "A source/pipeline issue, not an empty result.")
        return
    match = res.data[res.data["contracting_authority"] == authority] if not res.data.empty else res.data
    if match.empty:
        empty_state("Authority not found", "That link didn't match a contracting authority. Use Back to return.")
        return
    row = match.iloc[0]

    n_sup = _n(row.get("n_suppliers"))
    sub = f"{_awards_word(_n(row.get('n_awards')))} to {n_sup:,} supplier{'s' if n_sup != 1 else ''}"
    st.html(
        f'<div class="pr-prof-head"><h1 class="pr-prof-name">{_esc(authority)}</h1>'
        f'<div class="pr-prof-sub">{sub}</div></div>'
        f'<div class="pr-pills" style="margin:0.1rem 0 0.6rem">{_value_pill(row.get("awarded_value_safe_eur"))}</div>'
    )

    awards = fetch_awards_for_authority(authority)
    if awards is None or awards.empty:
        empty_state("No itemised awards", "This authority is in the ranking but no award rows were returned.")
        return
    _render_award_list(awards, key=f"pr_auth_{authority}", row_fn=_award_row_by_supplier)


def _render_cpv_profile(cpv_code: str) -> None:
    if back_button("← Back to procurement", key="prcpvprof"):
        _return_to_browse("wins")

    res = fetch_cpv_summary_result(limit=None)
    if not res.ok:
        empty_state("Category data isn't available right now", "A source/pipeline issue, not an empty result.")
        return
    match = res.data[res.data["cpv_code"] == cpv_code] if not res.data.empty else res.data
    if match.empty:
        empty_state("Category not found", "That link didn't match a CPV category. Use Back to return.")
        return
    row = match.iloc[0]

    title = _esc(_coalesce(row.get("cpv_description"))) or _esc(cpv_code)
    n_sup = _n(row.get("n_suppliers"))
    sub = f"CPV {_esc(cpv_code)} · {_awards_word(_n(row.get('n_awards')))} to {n_sup:,} supplier{'s' if n_sup != 1 else ''}"
    st.html(
        f'<div class="pr-prof-head"><h1 class="pr-prof-name">{title}</h1>'
        f'<div class="pr-prof-sub">{sub}</div></div>'
        f'<div class="pr-pills" style="margin:0.1rem 0 0.6rem">{_value_pill(row.get("awarded_value_safe_eur"))}</div>'
    )

    awards = fetch_awards_for_cpv(cpv_code)
    if awards is None or awards.empty:
        empty_state("No itemised awards", "This category is in the ranking but no award rows were returned.")
        return
    _render_award_list(awards, key=f"pr_cpv_{cpv_code}", row_fn=_award_row_cpv)


def _entity_search_hero() -> None:
    """Search-first entry (USAspending lesson: people type a NAME first). One box over
    suppliers + public bodies + categories — the reader never needs to know which register
    answers their question. DISPLAY-ONLY name filter over the pre-built search corpus
    (v_procurement_entity_search); renders nothing until the user types."""
    res = fetch_entity_search_result()
    if not res.ok or res.data.empty:
        return
    q = st.text_input(
        "Search procurement",
        placeholder="Search a company, public body or category…",
        key="pr_hero_q",
        label_visibility="collapsed",
    )
    qs = (q or "").strip()
    if not qs:
        return
    df = res.data
    hits = df[df["display_name"].str.contains(qs, case=False, na=False)].head(12)
    if hits.empty:
        empty_state("No matches", "Try a shorter term — names are matched as published.")
        return
    kind_label = {"supplier": "COMPANY", "authority": "PUBLIC BODY", "cpv": "CATEGORY"}
    aria = {
        "supplier": "Open the public-money dossier of",
        "authority": "View the awards made by",
        "cpv": "View the awards in category",
    }
    cards = []
    for r in hits.itertuples():
        kind = str(r.entity_kind)
        meta = _awards_word(_n(r.n_records))
        nc = _n(r.n_counterparties)
        if kind == "supplier":
            meta += f" · {nc:,} public bod{'ies' if nc != 1 else 'y'}"
        else:
            meta += f" · {nc:,} supplier{'s' if nc != 1 else ''}"
        pills = [f'<span class="pr-pill pr-pill-lob">{kind_label.get(kind, kind)}</span>']
        if _eur(r.awarded_value_safe_eur) != "—":
            pills.append(_value_pill(r.awarded_value_safe_eur))
        # Paid figure is a DIFFERENT grain (realised payments) — its own label, never merged.
        if kind == "supplier" and _eur(getattr(r, "paid_safe_eur", None)) != "—":
            pills.append(f'<span class="pr-pill pr-pill-val">{_eur(r.paid_safe_eur)} paid (where published)</span>')
        if _truthy(getattr(r, "on_lobbying_register", None)):
            pills.append('<span class="pr-pill pr-pill-lob">also on lobbying register</span>')
        href = {
            "supplier": _supplier_href(r.url_key),
            "authority": _authority_href(r.url_key),
            "cpv": _cpv_href(r.url_key),
        }[kind]
        inner = _card(f"<span>{_esc(r.display_name)}</span>", meta, pills)
        cards.append(clickable_card_link(href=href, inner_html=inner, aria_label=f"{aria[kind]} {r.display_name}"))
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.caption(
        "Awarded values are contract ceilings at the point of award; a company's paid figure is a "
        "separate, later stage (public bodies' own >€20k lists) — the two are never added together."
    )


# ──────────────────────────────────────────────────────────────────────────────
# Tab: Patterns — factual structure signals from the derived views
# (doc/PROCUREMENT_NUGGETS.md). Every card is an observable shape in the public
# record with its caveat attached; prompts to look, never verdicts (no-inference).
# ──────────────────────────────────────────────────────────────────────────────
def _render_single_bid_cpv(cpv_division: str) -> None:
    """Drill-down for one market's single-bid award notices (reached from the Patterns single-bid
    card). Each notice opens the authoritative EU Official Journal record. A single bid is a
    recorded fact — often wholly legitimate (a niche market with few capable suppliers) — never
    presented as evidence of wrongdoing (no-inference rail)."""
    if back_button("← Back to procurement", key="prsbcpv"):
        _return_to_browse("patterns")

    st.html(
        f'<div class="pr-prof-head"><div class="pr-prof-kicker">SINGLE-BID NOTICES · EU NOTICES 2024+</div>'
        f'<h1 class="pr-prof-name">{_esc(cpv_division)}</h1>'
        '<div class="pr-prof-sub">Contract-award notices in this market that drew a single tender</div></div>'
    )

    res = fetch_single_bid_notices_for_cpv_result(cpv_division)
    if not res.ok:
        empty_state("Competition data isn't available right now", "A source/pipeline issue, not an empty result.")
        return
    df = res.data
    if df.empty:
        empty_state(
            "No single-bid notices in this market",
            "No 2024+ EU award notice in this CPV division recorded a single tender.",
        )
        return
    st.caption(
        f"{len(df):,} EU Official Journal award notice{'s' if len(df) != 1 else ''} in “{_esc(cpv_division)}” that "
        "received a single tender (2024+ eForms, soonest first). A single bid is a matter of record — niche "
        "markets often have few capable suppliers — and is never, on its own, evidence of wrongdoing. "
        "Click a notice to open the authoritative record on TED."
    )
    cards = []
    for r in df.itertuples():
        date = _coalesce(getattr(r, "dispatch_date", None))[:10]
        winner = _esc(_coalesce(getattr(r, "winner_name", None))) or "—"
        meta_parts = [_esc(_coalesce(getattr(r, "buyer_name", None)))]
        if date:
            meta_parts.append(date)
        meta = " · ".join(p for p in meta_parts if p)
        pills = ['<span class="pr-pill pr-pill-lob">single bid</span>']
        if _coalesce(getattr(r, "value_kind", None)) == "framework_or_dps_ceiling":
            pills.append('<span class="pr-pill">framework ceiling</span>')
        inner = _card(f"<span>{winner}</span>", meta, pills)
        url = _coalesce(getattr(r, "notice_url", None))
        if url.startswith("http"):
            cards.append(
                clickable_card_link(
                    href=url,
                    inner_html=inner,
                    aria_label=f"Open the EU award notice won by {winner} on TED",
                    target="_blank",
                )
            )
        else:
            cards.append(inner)
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html(
        '<div class="pr-foot"><strong>Source:</strong> TED — Tenders Electronic Daily, EU Official Journal award '
        'notices (<a href="https://ted.europa.eu" target="_blank" rel="noopener">ted.europa.eu ↗</a>), eForms '
        "single-tender field (2024+). A single bid is recorded fact, not a verdict.</div>"
    )


def _render_patterns() -> None:
    st.html(
        '<div class="pr-caveat"><strong>Patterns are facts about the register, not findings about '
        "anyone.</strong> Each panel below shows a structure in the published record — how often one "
        "bid wins, how long the same firms keep winning, when orders are placed. Any of these shapes "
        "can be wholly legitimate; they are starting points for a reader's own questions.</div>"
    )

    # 1. Single-bid by market (lot-level, TED 2024+)
    comp = fetch_competition_by_cpv_result()
    if comp.ok and not comp.data.empty:
        base = fetch_single_bid_baseline_result()
        base_pct = (
            float(base.data.iloc[0].get("single_bid_lot_pct"))
            if base.ok and not base.data.empty and base.data.iloc[0].get("single_bid_lot_pct") is not None
            else None
        )
        st.html('<h2 class="pr-section-h">How often does one bid win, by market?</h2>')
        cap = (
            "Share of contract lots that drew a single bid, per category (EU award notices, 2024+; "
            "lots with a reported bid count)."
        )
        if base_pct is not None:
            cap += f" National rate: {base_pct:g}%."
        cap += " A single bid is often legitimate — niche markets have few capable suppliers."
        cap += " Click a market to see the individual single-bid notices inside it."
        st.caption(cap)
        cards = []
        for r in comp.data.head(12).itertuples():
            pct = r.single_bid_lot_pct
            meta = (
                f"{_n(r.n_single_bid_lots):,} of {_n(r.n_lots_with_bidcount):,} lots single-bid · "
                f"{_n(r.n_buyers):,} buyers"
            )
            pill = f'<span class="pr-pill pr-pill-val">{float(pct):g}% single-bid</span>' if pct is not None else ""
            inner = _card(f"<span>{_esc(r.cpv_division)}</span>", meta, [pill] if pill else [])
            cards.append(
                clickable_card_link(
                    href=_single_bid_cpv_href(r.cpv_division),
                    inner_html=inner,
                    aria_label=f"See the single-bid award notices in {r.cpv_division}",
                )
            )
        st.html(f'<div class="pr-grid">{"".join(cards)}</div>')

    # 2. New entrants per year
    ne = fetch_new_entrants_result()
    if ne.ok and not ne.data.empty:
        shown = ne.data[ne.data["is_left_censored"] == False]  # noqa: E712 — pandas mask
        if len(shown) > 1:
            st.html('<h2 class="pr-section-h">Who gets in — first-time winners</h2>')
            first_y, last_y = _n(shown.iloc[0].get("year")), _n(shown.iloc[-1].get("year"))
            first_pct, last_pct = (
                shown.iloc[0].get("pct_awards_to_new_entrants"),
                shown.iloc[-1].get("pct_awards_to_new_entrants"),
            )
            st.caption(
                f"Share of each year's contract awards won by suppliers with no earlier award in the register: "
                f"{float(first_pct):g}% in {first_y} → {float(last_pct):g}% in {last_y}. A falling entry rate is a "
                "market shape — consistent with consolidation, central frameworks, or a maturing register; the "
                "register only began in 2013, so earlier years are not comparable and are omitted."
            )
            st.bar_chart(
                shown,
                x="year",
                y="pct_awards_to_new_entrants",
                x_label="Year",
                y_label="New-entrant share (%)",
                height=200,
                color="#9c5b2e",
            )

    # 3. Longest-running relationships
    inc = fetch_incumbency_top_result()
    if inc.ok and not inc.data.empty:
        st.html('<h2 class="pr-section-h">The longest-running winners</h2>')
        st.caption(
            "Supplier–buyer pairs with awards in six or more different years. Durable incumbency is often "
            "the system working (framework renewals, specialist capability) — a record of persistence, not "
            "an accusation. Office of Government Procurement rows reflect central frameworks for the whole "
            "public service."
        )
        cards = []
        for r in inc.data.head(12).itertuples():
            yrs = _n(r.n_distinct_years)
            badge = (
                '<span class="pr-pill pr-pill-lob">central purchasing body</span>'
                if _truthy(r.authority_is_central_purchasing)
                else ""
            )
            meta = f"{_awards_word(_n(r.n_awards))} from {_esc(r.contracting_authority)} · {_n(r.first_year)}–{_n(r.last_year)}"
            pills = [f'<span class="pr-pill pr-pill-val">{yrs} winning years</span>'] + ([badge] if badge else [])
            inner = _card(f"<span>{_esc(r.supplier)}</span>", meta, pills)
            cards.append(
                clickable_card_link(
                    href=_supplier_href(r.supplier_norm),
                    inner_html=inner,
                    aria_label=f"Open the public-money dossier of {r.supplier}",
                )
            )
        st.html(f'<div class="pr-grid">{"".join(cards)}</div>')

    # 4. One-buyer suppliers (central purchasing excluded in the query)
    dep = fetch_dependency_top_result()
    if dep.ok and not dep.data.empty:
        st.html('<h2 class="pr-section-h">Suppliers with one main buyer</h2>')
        st.caption(
            "Firms that won at least 80% of their recorded awards (10+) from a single public body. "
            "A specialist serving the one body that buys its specialism is the market working — this is "
            "a structure fact, not a risk score. Central purchasing bodies are excluded (winning via OGP "
            "frameworks is how the system is designed)."
        )
        cards = []
        for r in dep.data.head(12).itertuples():
            meta = f"{_n(r.awards_from_top_authority):,} of {_n(r.total_awards):,} awards from {_esc(r.top_authority)}"
            pills = [f'<span class="pr-pill pr-pill-val">{float(r.top_authority_share_pct):g}% one buyer</span>']
            inner = _card(f"<span>{_esc(r.supplier)}</span>", meta, pills)
            cards.append(
                clickable_card_link(
                    href=_supplier_href(r.supplier_norm),
                    inner_html=inner,
                    aria_label=f"Open the public-money dossier of {r.supplier}",
                )
            )
        st.html(f'<div class="pr-grid">{"".join(cards)}</div>')

    # 5. Year-end ordering shape (COMMITTED tier only)
    qt = fetch_quarter_totals_result()
    if qt.ok and len(qt.data) == 4:
        st.html('<h2 class="pr-section-h">When orders are placed</h2>')
        st.caption(
            "Purchase-order lines by quarter across all publishing bodies (ordered tier only — never mixed "
            "with payments). A year-end rise is a known public-finance seasonality; invoicing cycles, grant "
            "schedules and works seasons all contribute. The shape is the fact; the reason is not asserted."
        )
        st.bar_chart(qt.data, x="quarter", y="n_lines", x_label="Quarter", y_label="Order lines", height=200, color="#9c5b2e")
        skew = fetch_quarter_profile_top_result()
        if skew.ok and not skew.data.empty:
            cards = []
            for r in skew.data.head(6).itertuples():
                meta = f"{_n(r.n_lines):,} of its order lines fall in Q4"
                pills = [f'<span class="pr-pill pr-pill-val">{float(r.pct_of_publisher_lines):g}% in Q4</span>']
                inner = _card(f"<span>{_esc(r.publisher_name)}</span>", meta, pills)
                cards.append(
                    clickable_card_link(
                        href=_paid_publisher_href(r.publisher_name, "COMMITTED"),
                        inner_html=inner,
                        aria_label=f"View the suppliers ordered by {r.publisher_name}",
                    )
                )
            st.html(f'<div class="pr-grid">{"".join(cards)}</div>')

    # 6. Sector breadth (paid corpus)
    sb = fetch_sector_breadth_top_result()
    if sb.ok and not sb.data.empty:
        st.html('<h2 class="pr-section-h">Firms paid across the most of the State</h2>')
        st.caption(
            "Suppliers appearing in the published payment lists of bodies across the most public-service "
            "sectors (health, councils, justice, …) — reach, as published. Grouped by the published name; "
            "totals are the usual indicative floors, never audited sums."
        )
        cards = []
        for r in sb.data.head(6).itertuples():
            meta = f"{_n(r.n_sectors)} sectors · {_n(r.n_publishers):,} public bodies"
            pills = []
            if _eur(getattr(r, "paid_safe_eur", None)) != "—":
                pills.append(f'<span class="pr-pill pr-pill-val">{_eur(r.paid_safe_eur)} paid (floor)</span>')
            cards.append(_card(f"<span>{_esc(r.supplier_normalised)}</span>", meta, pills))
        st.html(f'<div class="pr-grid">{"".join(cards)}</div>')

    st.html(
        '<div class="pr-foot"><strong>Method:</strong> every panel reads a registered, documented view '
        "(doc/PROCUREMENT_NUGGETS.md) over the same published registers as the rest of this page — "
        "eTenders awards, EU Official Journal notices, and public bodies' own payment lists. Counts and "
        "shares only within one register and one grain; nothing here mixes award ceilings with payments.</div>"
    )


def _page_lede(stats) -> None:
    """The page's opening findings (findings-not-filters,
    doc/APP_REDESIGN_SWEEP_2026_06_10.md). DISPLAY-ONLY: the top-winner row,
    the concentration row and the corpus counts all arrive pre-aggregated from
    the registered views; this assembles sentences and renders."""
    sentences: list[str] = []

    top = fetch_supplier_summary_result(limit=1)
    min_y, max_y = _n(stats.get("min_year")), _n(stats.get("max_year"))
    span = f"{min_y}–{max_y}" if min_y and max_y else "recent years"
    if top.ok and not top.data.empty:
        t = top.data.iloc[0]
        sentences.append(
            f"{_esc(t.get('supplier'))} has won more public contracts than any other firm — "
            f"<strong>{_n(t.get('n_awards')):,}</strong> since {min_y or 'records began'}, "
            f"from <strong>{_n(t.get('n_authorities')):,}</strong> public bodies."
        )

    con = fetch_supplier_concentration_result()
    if con.ok and not con.data.empty:
        c = con.data.iloc[0]
        share, n_sup_c = c.get("top_n_share_pct"), _n(c.get("n_suppliers"))
        if share is not None and n_sup_c:
            shape = "a long tail, not a closed shop" if float(share) < 25 else "a concentrated market"
            sentences.append(
                f"Contract-winning is {shape}: across <strong>{n_sup_c:,}</strong> companies, "
                f"the top {_n(c.get('top_n'))} firms hold <strong>{float(share):g}%</strong> "
                f"of all {_n(c.get('total_awards')):,} awards."
            )

    sentences.append(
        f"<strong>{_n(stats.get('n_suppliers')):,}</strong> suppliers and "
        f"<strong>{_n(stats.get('n_authorities')):,}</strong> public bodies appear on the "
        f"register, {_esc(span)}. Rankings count awards — the trustworthy metric — "
        "never naive euro totals."
    )
    finding_lede(sentences)


def _data_completeness_note() -> None:
    """Collapsed "How complete is this data?" honesty note. Static, sourced editorial prose
    (no live metric — the firewall keeps computation in the view layer); the coverage figures
    are documented point-in-time estimates from the 2026-06-08 coverage analysis, stated with
    their caveats so a reader never mistakes this corpus for the whole of public spending."""
    with st.expander("How complete is this data?"):
        st.markdown(
            "**Short answer: this is what public bodies publish — not the whole picture.** "
            "Treat every total here as a *floor* (at least this much, from the records we can see), "
            "never an audited figure.\n\n"
            "- **Awards** (eTenders, TED) name almost every public buyer (~1,950 bodies), but only a "
            "fraction of the euro value can be summed — most contracts fall below the publication "
            "threshold or run through frameworks whose ceilings aren't real spend.\n"
            "- **Money actually paid** comes from the over-€20,000 lists bodies publish themselves — and "
            "only about **1 in 40 public buyers (~3%)** does so. Against the State's estimated "
            "**€15–22 billion a year** of procurement, what's traceable here works out at roughly "
            "**7% of the money spent overall** — rising to the **mid-teens (%) in recent years** as more "
            "bodies began publishing, and under 2% before 2021. So on the order of **90%+ of actual "
            "spend is not yet visible** here.\n"
            "- The three records **aren't linked**: a contract's notice, its award, and the eventual "
            "payments sit in separate registers with no shared key, so *awarded* and *paid* can never "
            "be reconciled for the same deal.\n\n"
            "For scale, Ukraine's **Prozorro** publishes 100% of public procurement, full lifecycle, in "
            "one system — the standard Ireland has no equivalent of. National-spend estimate: OECD / "
            "US trade.gov country guide."
        )


# ╔══════════════════════════════════════════════════════════════════════════════════════╗
# ║ EXPERIMENTAL QS VALUATION FEATURE — self-contained & easily removable.                ║
# ║ To delete: remove this whole marked block (down to the matching ╚ marker) AND the      ║
# ║ `_render_qs_valuation()` call marked in procurement_page(). Nothing else depends on it. ║
# ║ The only non-UI parts it uses (dail_tracker_core/qs_valuation.py, the two data/_meta    ║
# ║ benchmark CSVs, test/dail_tracker_core/test_qs_valuation.py) are likewise standalone.   ║
# ║ Owner-greenlit 2026-06-11 as the ONE place inference is surfaced in the citizen app;    ║
# ║ it NEVER reports a disclosed figure (always wrapped in "estimate, not disclosed").      ║
# ╚══════════════════════════════════════════════════════════════════════════════════════╝
def _render_qs_valuation() -> None:
    """Quantity-surveyor style indicative construction valuation, year-adjusted via the
    SCSI Tender Price Index. Inference, display-only; logic in dail_tracker_core.qs_valuation.
    doc/PROCUREMENT_SURFACING_PLAN.md."""
    try:
        from dail_tracker_core import qs_valuation as qs  # lazy: keeps the feature self-contained
    except Exception:
        return  # if the module/data is absent, the page renders without this experimental panel
    with st.expander("Indicative construction valuation (experimental)"):
        st.html(
            '<div class="pr-caveat"><strong>Experimental — an estimate, not a disclosed figure.</strong> '
            "This applies published Irish €/m² cost benchmarks to a deliverable you describe — the way a "
            "quantity surveyor sizes a project before bills of quantities exist — and adjusts to the award "
            "year via the SCSI Tender Price Index. It is inference, shown with its method and sources; never "
            "read it as a contract’s actual value.</div>"
        )
        subs = qs.list_subtypes()
        labels = {s["subtype"]: s["label"] for s in subs}
        is_m2 = {s["subtype"]: s["unit"] == "per_m2" for s in subs}
        c1, c2, c3 = st.columns(3)
        with c1:
            subtype = st.selectbox(
                "Building type", [s["subtype"] for s in subs], format_func=lambda x: labels[x], key="qs_subtype"
            )
        with c2:
            units = st.number_input("How many?", min_value=1, value=1, step=1, key="qs_units")
        with c3:
            per_m2 = is_m2.get(subtype, False)
            area = st.number_input(
                "Floor area each (m²)",
                min_value=0,
                value=95 if per_m2 else 0,
                step=5,
                key="qs_area",
                disabled=not per_m2,
                help="Required for per-m² building types.",
            )
        c4, c5 = st.columns(2)
        with c4:
            yr = st.selectbox(
                "Award year (optional)",
                [None, *range(2025, 2012, -1)],
                format_func=lambda y: "current costs" if y is None else str(y),
                key="qs_year",
            )
        with c5:
            ceiling = st.number_input(
                "Framework / DPS ceiling € (optional)", min_value=0, value=0, step=100_000, key="qs_ceiling"
            )
        est = qs.estimate(
            subtype,
            units=int(units),
            area_m2=float(area) or None,
            award_year=yr,
            framework_ceiling_eur=float(ceiling) or None,
        )
        if not est.ok:
            st.caption(est.message)
            return
        p = est.payload
        v = p["value_eur"]
        st.html(
            '<div style="background:#ffffff;border:1px solid var(--border);border-radius:10px;'
            'padding:0.8rem 1rem;margin:0.6rem 0;max-width:48rem">'
            '<div style="font-size:0.7rem;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;'
            'color:var(--text-meta)">Indicative build value · inference</div>'
            f'<div style="font-size:1.5rem;font-weight:750;color:var(--ink-strong);'
            f'font-variant-numeric:tabular-nums">{_esc(_eur_scale(v["low"]))} – {_esc(_eur_scale(v["high"]))}</div>'
            f'<div style="font-size:0.82rem;color:var(--text-secondary)">midpoint '
            f"{_esc(_eur_scale(v['mid']))} · {_esc(p['read_as']['per_unit_basis'])} × {p['read_as']['units']} · "
            f"{_esc(p['year_adjustment'])}</div></div>"
        )
        if "ceiling_reading" in p:
            st.html(f'<div class="pr-caveat">{_esc(p["ceiling_reading"])}</div>')
        st.caption(p["caveat"])
        st.caption(
            f"Method: {p['method']}. Basis: {p['basis']['source']} {p['basis']['basis_period']}, "
            f"{p['basis']['vat']} (excludes {p['basis']['excludes']}). Sources: {', '.join(p['sources'])}."
        )


# ╔══════════════════════════════════════════════════════════════════════════════════════╗
# ║ END EXPERIMENTAL QS VALUATION FEATURE — delete up to the matching ╔ marker above.       ║
# ╚══════════════════════════════════════════════════════════════════════════════════════╝


# ──────────────────────────────────────────────────────────────────────────────
def procurement_page() -> None:
    hide_sidebar()

    # Drill-downs — full-width detail views with back nav.
    params = st.query_params
    if params.get("supplier"):
        _render_supplier_profile(params.get("supplier"))
        return
    if params.get("authority"):
        _render_authority_profile(params.get("authority"))
        return
    if params.get("cpv"):
        _render_cpv_profile(params.get("cpv"))
        return
    if params.get("paid_supplier") and params.get("paid_publisher"):
        # LEAF: both keys present → the published line items for that supplier × body pair (the
        # terminus that breaks the supplier↔body card loop). Checked before the single-key branches.
        req_tier = (params.get("paid_tier") or "SPENT").upper()
        _render_payment_lines(
            params.get("paid_supplier"),
            params.get("paid_publisher"),
            req_tier if req_tier in ("SPENT", "COMMITTED") else "SPENT",
        )
        return
    if params.get("paid_publisher"):
        req_tier = (params.get("paid_tier") or "SPENT").upper()
        _render_payments_publisher_profile(
            params.get("paid_publisher"), req_tier if req_tier in ("SPENT", "COMMITTED") else "SPENT"
        )
        return
    if params.get("paid_supplier"):
        req_tier = (params.get("paid_tier") or "SPENT").upper()
        _render_payments_supplier_profile(
            params.get("paid_supplier"), req_tier if req_tier in ("SPENT", "COMMITTED") else "SPENT"
        )
        return
    if params.get("ted_winner"):
        _render_ted_winner_profile(params.get("ted_winner"))
        return
    if params.get("single_bid_cpv"):
        _render_single_bid_cpv(params.get("single_bid_cpv"))
        return

    # coverage_stats is the source-state gate AND the scale anchor: a missing view /
    # parquet / DuckDB error is NOT "no results".
    stats_res = fetch_coverage_stats_result()
    if not stats_res.ok:
        hero_banner(
            kicker="PUBLIC MONEY",
            title="Public Procurement",
            dek="Contract awards published on eTenders / national procurement open data.",
        )
        empty_state(
            "Procurement data isn't available right now",
            "The procurement views couldn't be loaded — the gold parquet may be missing "
            "or a view failed to register. This is a source/pipeline issue, not an empty result.",
        )
        return

    stats = stats_res.data.iloc[0]

    # Hero carries no stat badges: the corpus counts + the top-winner / market-shape
    # findings live in the single _page_lede below, so the data isn't pushed off-screen
    # by a second stat block and the sum-safe total is shown exactly once.
    hero_banner(
        kicker="PUBLIC MONEY",
        title="Public Procurement",
        dek="Contract awards published on eTenders and the national procurement open data — "
        "who was awarded public contracts, by which bodies, in which categories.",
    )

    # Search-first entry: one box across companies / public bodies / categories (renders
    # results only when the user types; the lenses below are untouched otherwise).
    _entity_search_hero()

    # Caveat trimmed to its two load-bearing honesty rails (awarded ≠ paid; no-inference). The
    # ceilings explanation moved to the "What these terms mean" expander (no duplication), and the
    # "€570bn" contrast panel was removed 2026-06-08 — both cut above-the-fold weight.
    st.html(
        '<div class="pr-caveat"><strong>Awarded value, not money paid.</strong> '
        "These are values at the point of award — see <em>Money actually paid</em> for real "
        "payments. A contract award is a public record of a procurement decision, not evidence "
        "of influence or wrongdoing.</div>"
    )
    _page_lede(stats)
    # Glossary tucked into a collapsed expander (declutter 2026-06-08) — there for first-time
    # readers, but no longer a permanent block between the hero and the rankings.
    with st.expander("What these terms mean"):
        glossary_strip(
            [
                ("Award value", "the contract value at the point of award — not money actually paid out"),
                ("Framework / DPS", "an agreement a buyer may draw down against — the ceiling is not a payment"),
                ("CPV", "Common Procurement Vocabulary — the EU category code for what was bought"),
                ("CRO", "Companies Registration Office — a matched company registration number"),
            ]
        )

    _data_completeness_note()

    if _n(stats.get("n_suppliers")) == 0:
        empty_state("No supplier records", "The procurement views are loaded but returned no rows.")
        return

    # Four top-level sections, phrased as the questions a reader actually brings
    # (doc/APP_REDESIGN_SWEEP_2026_06_10.md §1 + doc/PROCUREMENT_UI_BRIEF.md: registers →
    # questions). "Who wins contracts?" holds the award-stage registers (eTenders national /
    # TED EU) plus the register-overlap disclosures behind one register picker; "Who actually
    # gets paid?" is the payment stage; "Open right now" promotes the pre-award tender
    # pipeline to a first-class lens (the forward-looking view, no longer buried two pickers
    # deep); "Patterns" is the factual signal feed. The section bar is a ?tab=-synced segmented
    # control (NOT st.tabs, which reset to the first tab on every rerun — losing the reader's
    # place on a drill-down Back or a cross-page round-trip). Surfacing-only: every lens calls a
    # _render_* function; no logic moves into this layer.
    section = _section_picker()

    if section == "wins":
        register = st.segmented_control(
            "Register",
            ["National register (eTenders)", "EU register (TED)", "Register overlaps"],
            default="National register (eTenders)",
            key="pr_register",
            label_visibility="collapsed",
        )
        if register == "EU register (TED)":
            # TED contract awards WON (2016–2026). The pre-award tender pipeline moved to
            # the top-level "Open right now" section (different grain, never summed).
            _render_ted()
        elif register == "Register overlaps":
            # Co-occurrence disclosures (same pattern, two registers). All-time scope.
            ov_lens = st.segmented_control(
                "View",
                ["Lobbying", "Charities"],
                default="Lobbying",
                key="pr_overlap_lens",
                label_visibility="collapsed",
            )
            if ov_lens == "Charities":
                charity_overlap = fetch_charity_overlap_result()
                _render_charity_overlap(charity_overlap.data if charity_overlap.ok else pd.DataFrame())
            else:
                overlap = fetch_lobbying_overlap_result()
                _render_overlap(overlap.data if overlap.ok else pd.DataFrame(), None)
        else:
            # Year pills + lens picker live with the rankings they scope (national register).
            st.caption("Filter the rankings by award year:")
            year = _year_pills(fetch_available_years())
            awards_lens = st.segmented_control(
                "View awards by",
                ["By supplier", "By authority", "By category"],
                default="By supplier",
                key="pr_awards_lens",
                label_visibility="collapsed",
            )
            if awards_lens == "By authority":
                _render_authorities(year)
            elif awards_lens == "By category":
                _render_cpv(year)
            else:
                _render_suppliers(year)

    elif section == "paid":
        _render_payments()

    elif section == "open":
        # Two forward-looking lenses, same grain discipline: open competition notices
        # (pre-award) and advertised contract terms due to end (post-award fact — when
        # the contracted period runs out, as stated on the notice; never summed).
        fwd_lens = st.segmented_control(
            "View",
            ["Open tenders", "Contract terms ending"],
            default="Open tenders",
            key="pr_forward_lens",
            label_visibility="collapsed",
        )
        # Two registers, rendered as separate sections (never value-merged): the national
        # eTenders feed (the sub-EU-threshold mass) first, then the EU-journal (TED) feed.
        if fwd_lens == "Contract terms ending":
            _render_national_expiring()
            st.html('<div class="pr-register-rule"><span>EU Official Journal (TED)</span></div>')
            _render_expiring_contracts()
        else:
            _render_national_open_tenders()
            st.html('<div class="pr-register-rule"><span>EU Official Journal (TED)</span></div>')
            _render_ted_tenders()

    else:  # "patterns"
        _render_patterns()

    # ╔═══ EXPERIMENTAL QS VALUATION — REMOVE THIS LINE + the marked block below to delete ═══╗
    _render_qs_valuation()
    # ╚════════════════════════════════════════════════════════════════════════════════════╝

    st.html(
        '<div class="pr-foot"><strong>Source:</strong> eTenders / national procurement open data '
        '(<a href="https://data.gov.ie/dataset/contract-notices-published-on-etenders" '
        'target="_blank" rel="noopener">data.gov.ie ↗</a>), cross-referenced to the Companies '
        "Registration Office and the Register of Lobbying. Values are awarded contract values, not "
        "actual payments; only sum-safe award values are shown. Suppliers shown are company-class "
        "registrations — sole traders and individuals are excluded.</div>"
    )
    _fresh = freshness_line("procurement")
    if _fresh:
        # The OGP open-data export itself publishes with a lag of several months,
        # so the newest notice held legitimately predates the latest pipeline run.
        st.caption(f"{_fresh} The national export publishes with a lag of several months.")
