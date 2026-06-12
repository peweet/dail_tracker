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
    fetch_afs_total_by_year_result,
    fetch_afs_vs_po_coverage_result,
    fetch_authority_summary_result,
    fetch_available_years,
    fetch_call_offs_for_supplier_result,
    fetch_charity_overlap_result,
    fetch_awards_for_authority,
    fetch_awards_for_cpv,
    fetch_awards_for_supplier,
    fetch_competition_by_cpv_result,
    fetch_coverage_stats_result,
    fetch_cpv_summary_result,
    fetch_dependency_for_supplier_result,
    fetch_dependency_top_result,
    fetch_entity_search_result,
    fetch_incumbency_for_supplier_result,
    fetch_incumbency_top_result,
    fetch_lobbying_overlap_result,
    fetch_awards_by_year_result,
    fetch_new_entrants_result,
    fetch_payments_corpus_stats_result,
    fetch_payments_for_publisher_result,
    fetch_payments_for_supplier_result,
    fetch_payments_by_year_result,
    fetch_payments_publisher_profile_result,
    fetch_payments_publisher_summary_result,
    fetch_payments_supplier_summary_result,
    fetch_quarter_profile_top_result,
    fetch_quarter_totals_result,
    fetch_sector_breadth_top_result,
    fetch_single_bid_baseline_result,
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
            st.bar_chart(tr.data, x="year", y="n_awards", height=200, color="#9c5b2e")


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
    st.html('<div style="height:1rem"></div>')
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
    st.caption(f"Top {len(df):,} suppliers by money {_paid_verb(tier)} (sum-safe). Names as published by the body.")
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
        cards.append(_card(f"<span>{_esc(r.supplier)}</span>", meta, pills, rank=i))
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


def _render_council_accounts_context(council: str, active_tier: str) -> None:
    """AFS enrichment on a LOCAL-AUTHORITY dossier — the council's audited accounts as the
    "complete spending" context the named-supplier PO/payment data sits inside.

    ⚠️ BUDGET grain — a SIBLING fact, NEVER summed with the purchase-order euros above it.
    Three additive pieces, all pre-aggregated in the views (the page selects + renders, never
    computes a metric): (1) audited revenue spend per year; (2) the latest year's spending by
    service division; (3) an indicative traceability line — how much of that audited spend is
    tied to named suppliers in the >€20k purchase-order register. Silently omitted for a council
    whose audited AFS isn't in the fact yet (so the dossier just shows the PO data, as before)."""
    by_year = fetch_afs_total_by_year_result(council)
    if not by_year.ok or by_year.data.empty:
        return
    ay = by_year.data
    latest = int(ay["year"].max())
    span = f"{int(ay['year'].min())}–{latest}" if len(ay) > 1 else str(latest)

    st.html(
        '<div class="pr-afs">'
        '<div class="pr-afs-head">Council accounts — all spending (audited)</div>'
        f'<p class="pr-cap" style="margin-top:0">From the council’s own audited Annual Financial '
        f"Statement (revenue account, {_esc(span)}): spending by service. This is the council’s "
        "<strong>whole</strong> operating spend — a broader, separate measure from the purchase-order "
        "figures above, and <strong>never added to them</strong>.</p></div>"
    )
    # (1) revenue spend per year — a DISTINCT colour from the PO chart's brown to signal a different grain.
    st.caption("Operating spending per year (revenue account, audited €)")
    st.bar_chart(ay, x="year", y="gross_expenditure_eur", height=200, color="#3a6b7e")

    # (3) traceability line — the latest year present in BOTH the accounts and the active PO tier.
    cov = fetch_afs_vs_po_coverage_result(council)
    if cov.ok and not cov.data.empty:
        pct_col = "pct_spent_of_gross" if active_tier == "SPENT" else "pct_committed_of_gross"
        po_col = "po_spent_safe_eur" if active_tier == "SPENT" else "po_committed_safe_eur"
        usable = cov.data[cov.data[pct_col].notna()]
        if not usable.empty:
            crow = usable.sort_values("year").iloc[-1]
            yr, gross, po, pct = (
                _n(crow.get("year")),
                crow.get("afs_gross_eur"),
                crow.get(po_col),
                crow.get(pct_col),
            )
            verb = _paid_verb(active_tier)  # 'paid' / 'ordered'
            st.html(
                '<div class="pr-afs-trace">'
                f'<div class="pr-afs-trace-fig"><strong>{_eur(gross)}</strong> spent (accounts, {yr})'
                f" · <strong>{_eur(po)}</strong> traceable to named suppliers"
                f" · <strong>{float(pct):g}%</strong></div>"
                f'<div class="pr-afs-trace-cap">Indicative coverage only. The accounts figure is the '
                f"council’s full audited operating spend; the supplier figure counts only purchases "
                f"over the €20,000 publication threshold ({verb} via purchase orders). Different "
                "thresholds and stages — a coverage signal, not a reconciliation.</div></div>"
            )

    # (2) latest-year by-division breakdown — compact cards, largest service first.
    bd = fetch_afs_by_division_result(council, latest)
    if bd.ok and not bd.data.empty:
        st.caption(f"Where it went in {latest} — spending by service (revenue account)")
        cards = []
        for r in bd.data.itertuples():
            net = getattr(r, "net_expenditure_eur", None)
            meta = f"{_eur(net)} net cost after income & grants"
            pill = f'<span class="pr-pill pr-pill-val">{_eur(getattr(r, "gross_expenditure_eur", None))} spent</span>'
            cards.append(_card(f"<span>{_esc(r.division)}</span>", meta, [pill]))
        st.html(f'<div class="pr-grid">{"".join(cards)}</div>')


def _render_payments_publisher_profile(publisher_name: str, tier: str = "SPENT") -> None:
    """Per-buyer dossier (the per-council profile): which tiers the body publishes, both totals
    shown side by side (never summed), and its top suppliers in the active tier. Councils mostly
    publish purchase ORDERS, so this falls back to whichever tier the body actually has."""
    if back_button("← Back to procurement", key="prpaypub"):
        st.query_params.clear()
        st.rerun()

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

    # Spend-over-time spine — one tier only (never stack ordered+paid, which would read as a sum).
    # Meaningful now the council payment data is a decade deep (2016–2026).
    by_year = fetch_payments_by_year_result(publisher_name, tier=active)
    if by_year.ok and len(by_year.data) > 1:
        st.caption(f"Money {_paid_verb(active)} per year (sum-safe)")
        st.bar_chart(by_year.data, x="year", y="total_safe_eur", height=200, color="#9c5b2e")

    # Local-authority dossiers gain the audited-accounts context (the "complete spend" denominator
    # the named-supplier PO data sits inside). BUDGET grain — a sibling, never summed with the above.
    if is_la:
        _render_council_accounts_context(publisher_name, active)

    res = fetch_payments_for_publisher_result(publisher_name, tier=active)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        empty_state("No suppliers in this tier", f"This body has no sum-safe {_paid_verb(active)} records.")
        st.html(_FOOT_HTML)
        return
    st.caption(
        f"Top {len(df):,} suppliers by money {_paid_verb(active)} (sum-safe). Names as published by the body; "
        "amounts are the body's own reported figures, not award ceilings."
    )
    cards = []
    for i, r in enumerate(df.itertuples(), start=1):
        meta = f"{_n(r.n_payments):,} {_paid_verb(active)} line{'s' if _n(r.n_payments) != 1 else ''} · {_n(r.min_year)}–{_n(r.max_year)}"
        pills = [
            p
            for p in (_paid_pill(r.total_safe_eur, active), _cro_pill_from(getattr(r, "cro_company_num", None), None))
            if p
        ]
        cards.append(_card(f"<span>{_esc(r.supplier)}</span>", meta, pills, rank=i))
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html(_FOOT_HTML)


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
            st.bar_chart(tr.data, x="year", y="n_awards", height=200, color="#9c5b2e")

    res = fetch_ted_supplier_summary_result(limit=_TOP, order_by="awards")
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        empty_state("No TED winners", "The EU register loaded but returned no company-class winners.")
        return
    st.caption(f"Top {len(df):,} firms by number of EU award notices won. Value is awarded value, not spend.")
    cards = []
    for i, r in enumerate(df.head(_TOP).itertuples(), start=1):
        meta = f"{_awards_word(_n(r.n_awards))} · {_n(r.n_buyers):,} buyer{'s' if _n(r.n_buyers) != 1 else ''}"
        cro = _cro_pill_from(getattr(r, "cro_company_num", None), getattr(r, "cro_company_status", None))
        pills = [p for p in (_ted_value_pill(r.ted_value_safe_eur), cro) if p]
        cards.append(_card(f"<span>{_esc(r.winner_name)}</span>", meta, pills, rank=i))
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
    notices_res = fetch_ted_notices_for_supplier_result(supplier_norm)
    ndf = notices_res.data if notices_res.ok else pd.DataFrame()

    def _notice_li(nr, *, show_name: bool) -> str:
        url = _coalesce(getattr(nr, "notice_url", None))
        if not url:
            return ""
        date = _coalesce(getattr(nr, "dispatch_date", None))[:10]
        buyer = _esc(_coalesce(getattr(nr, "buyer_name", None)) or "—")
        is_fw = _coalesce(getattr(nr, "value_kind", None)) == "framework_or_dps_ceiling"
        tag = "framework — shared ceiling, not a payment" if is_fw else "contract award"
        # On variant rows, lead with the winner's own published name so the reader sees the
        # grouping is name-based, not a hidden identity assertion.
        name_pre = f"<strong>{_esc(_coalesce(getattr(nr, 'winner_name', None)))}</strong> — " if show_name else ""
        return (
            f'<li class="pr-notice"><a href="{_esc(url)}" target="_blank" rel="noopener">'
            f'{name_pre}{buyer} · {date} ↗</a> <span class="pr-notice-tag">{tag}</span></li>'
        )

    exact_li = [
        li
        for nr in ndf.itertuples()
        if _truthy(getattr(nr, "is_exact_name", False))
        for li in (_notice_li(nr, show_name=False),)
        if li
    ]
    variant_li = [
        li
        for nr in ndf.itertuples()
        if not _truthy(getattr(nr, "is_exact_name", False))
        for li in (_notice_li(nr, show_name=True),)
        if li
    ]

    if exact_li or variant_li:
        total = len(exact_li) + len(variant_li)
        with st.expander(f"Open the {total:,} authoritative EU notice{'' if total == 1 else 's'} on TED ↗"):
            st.html(
                '<p class="pr-cap">The tracker stores a thin slice of each award. Each notice below opens '
                "the full Official Journal record on TED — where the authority publishes what is actually "
                "being built, the real framework ceiling and the award criteria. The source, not our summary.</p>"
                + (f'<ul class="pr-notice-list">{"".join(exact_li)}</ul>' if exact_li else "")
            )
            if variant_li:
                st.html(
                    '<p class="pr-cap" style="margin-top:0.8rem"><strong>Closely-named winners.</strong> '
                    f"{len(variant_li):,} further notice{'' if len(variant_li) == 1 else 's'} won under a "
                    "<em>similar</em> name (shared name stem — e.g. a renamed or merged company). Grouped by "
                    "name only; these <em>may be different legal entities</em> — confirm via the CRO number on "
                    "each notice before treating them as one firm.</p>"
                    f'<ul class="pr-notice-list">{"".join(variant_li)}</ul>'
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
    only_open = st.toggle(
        "Only tenders still open by deadline",
        value=False,
        key="pr_ted_open",
        help=f"{_n(s.get('n_still_open')):,} of {_n(s.get('n_notices')):,} have a submission deadline still in the future.",
    )
    res = fetch_ted_tenders_result(only_open=only_open, limit=_TOP)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        empty_state("No tenders", "No still-open competition notice." if only_open else "The view returned no rows.")
        return
    st.caption(
        f"{len(df):,} most-recent competition notices{' still open' if only_open else ''}. "
        "Estimated value is a pre-award buyer estimate — not an award and not a payment."
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
        cards.append(_card(f"<span>{_esc(getattr(r, 'buyer_name', None))}</span>", meta, pills))
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
        "Values are award/ceiling figures shown for context — never totals."
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
        cards.append(_card(f"<span>{_esc(getattr(r, 'buyer_name', None))}</span>", meta, pills))
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html(
        '<div class="pr-foot"><strong>Source:</strong> TED — Tenders Electronic Daily, EU Official Journal award '
        'notices (<a href="https://ted.europa.eu" target="_blank" rel="noopener">ted.europa.eu ↗</a>), eForms '
        "contract-term fields (BT-36/BT-145/BT-536/BT-537). The end date is the advertised term, not a verified "
        "event; ~36% of award notices state a term. Winner names follow the published notice; sole traders and "
        "individuals are not shown.</div>"
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


def _award_row(head: str, meta_parts: list[str], r) -> str:
    meta = " · ".join(p for p in meta_parts if p and p != "—")
    # The published contract title (100% filled in the source) — without it a line item's
    # only description is its generic CPV label ("IT services: consulting, …").
    title = _esc(_coalesce(getattr(r, "tender_title", None)))
    title_html = f'<div class="pr-award-title">{title}</div>' if title else ""
    return (
        f'<div class="pr-award"><div class="pr-award-body">'
        f'<div class="pr-award-auth">{head or "—"}</div>{title_html}'
        f'<div class="pr-award-meta">{meta or "—"}</div></div>{_award_value_html(r)}</div>'
    )


def _award_detail_meta(r) -> list[str]:
    """Detail meta fragments shared by every award row: procedure, contract term, bid
    count, and the deep link to the EU Official Journal notice (above-EU-threshold
    subset only). All values come straight from the view — display formatting only."""
    parts = [_esc(_coalesce(getattr(r, "procedure_type", None)))]
    months = _n(getattr(r, "contract_duration_months", None))
    if months > 0:
        parts.append(f"{months}-month term")
    bids = _n(getattr(r, "n_bids_received", None))
    if bids > 0:
        parts.append(f"{bids:,} bid{'' if bids == 1 else 's'} received")
    ted_url = _coalesce(getattr(r, "ted_can_link", None)) or _coalesce(getattr(r, "ted_notice_link", None))
    if ted_url.startswith("http"):
        parts.append(f'<a href="{_esc(ted_url)}" target="_blank" rel="noopener">TED notice ↗</a>')
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
    total = len(awards)
    n_safe = _n(row.get("n_value_safe_awards"))
    n_ceil = _n(row.get("n_ceiling_notices"))
    recon = (
        f"The {_eur(row.get('awarded_value_safe_eur'))} headline is the sum of {n_safe:,} contract "
        f"award{'' if n_safe == 1 else 's'} that carry a sum-safe value."
    )
    if n_ceil:
        recon += (
            f" A further {n_ceil:,} framework / DPS ceiling notice{'' if n_ceil == 1 else 's'} "
            "are listed below in rust — spending limits a buyer may draw down against, not payments, "
            "and never added to the headline."
        )
    st.caption(f"Every recorded contract award to this supplier ({total:,} in total), most recent first. " + recon)
    page_idx = paginate(total, key_prefix=f"pr_aw_{supplier_norm}", page_size=_AWARD_PAGE)
    page = awards.iloc[page_idx * _AWARD_PAGE : (page_idx + 1) * _AWARD_PAGE]
    st.html("".join(_award_row_html(r) for r in page.itertuples()))
    st.html('<div style="height:0.6rem"></div>')
    pagination_controls(
        total,
        key_prefix=f"pr_aw_{supplier_norm}",
        page_sizes=(_AWARD_PAGE,),
        default_page_size=_AWARD_PAGE,
        label="awards",
    )


def _render_supplier_profile(supplier_norm: str) -> None:
    if back_button("← Back to procurement", key="prsupprof"):
        st.query_params.clear()
        st.rerun()

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
    """Paginated award-row list shared by the supplier / authority / category profiles."""
    total = len(awards)
    st.caption(
        f"Every recorded contract award ({total:,} in total), most recent first. "
        "Framework / DPS ceilings are shown in rust and are not actual payments."
    )
    page_idx = paginate(total, key_prefix=key, page_size=_AWARD_PAGE)
    page = awards.iloc[page_idx * _AWARD_PAGE : (page_idx + 1) * _AWARD_PAGE]
    st.html("".join(row_fn(r) for r in page.itertuples()))
    st.html('<div style="height:0.6rem"></div>')
    pagination_controls(total, key_prefix=key, page_sizes=(_AWARD_PAGE,), default_page_size=_AWARD_PAGE, label="awards")
    st.html(_FOOT_HTML)


def _render_authority_profile(authority: str) -> None:
    if back_button("← Back to procurement", key="prauthprof"):
        st.query_params.clear()
        st.rerun()

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
        st.query_params.clear()
        st.rerun()

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
        st.html('<div class="pr-ted-xref-h" style="margin-top:0.4rem">How often does one bid win, by market?</div>')
        cap = (
            "Share of contract lots that drew a single bid, per category (EU award notices, 2024+; "
            "lots with a reported bid count)."
        )
        if base_pct is not None:
            cap += f" National rate: {base_pct:g}%."
        cap += " A single bid is often legitimate — niche markets have few capable suppliers."
        st.caption(cap)
        cards = []
        for r in comp.data.head(12).itertuples():
            pct = r.single_bid_lot_pct
            meta = (
                f"{_n(r.n_single_bid_lots):,} of {_n(r.n_lots_with_bidcount):,} lots single-bid · "
                f"{_n(r.n_buyers):,} buyers"
            )
            pill = f'<span class="pr-pill pr-pill-val">{float(pct):g}% single-bid</span>' if pct is not None else ""
            cards.append(_card(f"<span>{_esc(r.cpv_division)}</span>", meta, [pill] if pill else []))
        st.html(f'<div class="pr-grid">{"".join(cards)}</div>')

    # 2. New entrants per year
    ne = fetch_new_entrants_result()
    if ne.ok and not ne.data.empty:
        shown = ne.data[ne.data["is_left_censored"] == False]  # noqa: E712 — pandas mask
        if len(shown) > 1:
            st.html('<div class="pr-ted-xref-h" style="margin-top:1rem">Who gets in — first-time winners</div>')
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
            st.bar_chart(shown, x="year", y="pct_awards_to_new_entrants", height=200, color="#9c5b2e")

    # 3. Longest-running relationships
    inc = fetch_incumbency_top_result()
    if inc.ok and not inc.data.empty:
        st.html('<div class="pr-ted-xref-h" style="margin-top:1rem">The longest-running winners</div>')
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
        st.html('<div class="pr-ted-xref-h" style="margin-top:1rem">Suppliers with one main buyer</div>')
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
        st.html('<div class="pr-ted-xref-h" style="margin-top:1rem">When orders are placed</div>')
        st.caption(
            "Purchase-order lines by quarter across all publishing bodies (ordered tier only — never mixed "
            "with payments). A year-end rise is a known public-finance seasonality; invoicing cycles, grant "
            "schedules and works seasons all contribute. The shape is the fact; the reason is not asserted."
        )
        st.bar_chart(qt.data, x="quarter", y="n_lines", height=200, color="#9c5b2e")
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
        st.html('<div class="pr-ted-xref-h" style="margin-top:1rem">Firms paid across the most of the State</div>')
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
    with st.expander("🧪 Indicative construction valuation (experimental)"):
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
    if params.get("paid_publisher"):
        req_tier = (params.get("paid_tier") or "SPENT").upper()
        _render_payments_publisher_profile(
            params.get("paid_publisher"), req_tier if req_tier in ("SPENT", "COMMITTED") else "SPENT"
        )
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

    # Four top-level tabs, phrased as the questions a reader actually brings
    # (doc/APP_REDESIGN_SWEEP_2026_06_10.md §1 + doc/PROCUREMENT_UI_BRIEF.md: registers →
    # questions). "Who wins contracts?" holds the award-stage registers (eTenders national /
    # TED EU) plus the register-overlap disclosures behind one register picker; "Who actually
    # gets paid?" is the payment stage; "Open right now" promotes the pre-award tender
    # pipeline to a first-class lens (the forward-looking view, no longer buried two pickers
    # deep); "Patterns" is the factual signal feed. Surfacing-only: every lens calls a
    # _render_* function; no logic moves into this layer.
    tabs = st.tabs(["Who wins contracts?", "Who actually gets paid?", "Open right now", "Patterns"])

    with tabs[0]:
        register = st.segmented_control(
            "Register",
            ["National register (eTenders)", "EU register (TED)", "Register overlaps"],
            default="National register (eTenders)",
            key="pr_register",
            label_visibility="collapsed",
        )
        if register == "EU register (TED)":
            # TED contract awards WON (2016–2026). The pre-award tender pipeline moved to
            # the top-level "Open right now" tab (different grain, never summed).
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

    with tabs[1]:
        _render_payments()

    with tabs[2]:
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
        if fwd_lens == "Contract terms ending":
            _render_expiring_contracts()
        else:
            _render_ted_tenders()

    with tabs[3]:
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
