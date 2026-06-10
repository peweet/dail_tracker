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

from data_access.procurement_data import (
    fetch_afs_by_division_result,
    fetch_afs_total_by_year_result,
    fetch_afs_vs_po_coverage_result,
    fetch_authority_summary_result,
    fetch_available_years,
    fetch_charity_overlap_result,
    fetch_awards_for_authority,
    fetch_awards_for_cpv,
    fetch_awards_for_supplier,
    fetch_coverage,
    fetch_coverage_stats_result,
    fetch_cpv_summary_result,
    fetch_lobbying_overlap_result,
    fetch_awards_by_year_result,
    fetch_payments_corpus_stats_result,
    fetch_payments_for_publisher_result,
    fetch_payments_for_supplier_result,
    fetch_payments_by_year_result,
    fetch_payments_publisher_profile_result,
    fetch_payments_publisher_summary_result,
    fetch_payments_supplier_summary_result,
    fetch_supplier_concentration_result,
    fetch_supplier_summary_result,
    fetch_ted_awards_by_year_result,
    fetch_ted_competition_stats_result,
    fetch_ted_corpus_stats_result,
    fetch_ted_for_supplier_result,
    fetch_ted_notices_for_supplier_result,
    fetch_ted_supplier_summary_result,
    fetch_ted_tenders_result,
    fetch_ted_tenders_stats_result,
)
from shared_css import inject_css  # noqa: F401  (kept parallel to other pages)
from ui.components import (
    back_button,
    clickable_card_link,
    empty_state,
    fmt_civic_date,
    glossary_strip,
    hero_banner,
    hide_sidebar,
    paginate,
    pagination_controls,
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
    return f"?supplier={urllib.parse.quote(str(supplier_norm))}"


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
    options = ["All years"] + [str(y) for y in years]
    choice = st.pills("Filter by year", options, default="All years", key="pr_year", label_visibility="collapsed")
    if not choice or choice == "All years":
        return None
    try:
        return int(choice)
    except (TypeError, ValueError):
        return None


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
    """Market-shape context above the supplier ranking: how concentrated contract-winning is
    (top-N share — answers 'do a few firms dominate?') and the award-count trend over time.
    Both are pre-aggregated in the view/core layer; the page only renders them."""
    con = fetch_supplier_concentration_result()
    if con.ok and not con.data.empty:
        c = con.data.iloc[0]
        share = c.get("top_n_share_pct")
        n_sup, total, topn = _n(c.get("n_suppliers")), _n(c.get("total_awards")), _n(c.get("top_n"))
        if share is not None and n_sup and total:
            verb = "a broad market" if float(share) < 25 else "a concentrated market"
            st.html(
                f'<p class="pr-cap">Across <strong>{n_sup:,}</strong> companies, the top {topn} firms '
                f"hold <strong>{float(share):g}%</strong> of all {total:,} contract awards — {verb}. "
                "Rankings count awards (the trustworthy metric), not euro value.</p>"
            )
    # Trend chart tucked into a collapsed expander so the supplier ranking is the first thing
    # the reader sees on the Suppliers tab (declutter 2026-06-08); still one click away.
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
    st.caption(f"Top {len(df):,} procurement categories (CPV){_year_label(year)} by {by}. Click one for its awards.")
    cards = []
    for i, r in enumerate(df.head(_TOP).itertuples(), start=1):
        title = _esc(r.cpv_description) or _esc(r.cpv_code) or "—"
        meta = (
            f"CPV {_esc(r.cpv_code)} · {_awards_word(_n(r.n_awards))} · "
            f"{_n(r.n_suppliers):,} supplier{'s' if _n(r.n_suppliers) != 1 else ''}"
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
            f"{_awards_word(_n(r.n_awards))} · "
            f"{n_auth:,} authorit{'ies' if n_auth != 1 else 'y'} · registered charity"
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
    choice = st.segmented_control("Tier", list(labels), default="Paid (actual spend)", key=key, label_visibility="collapsed")
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
            st.segmented_control("View", list(view_labels), default="Top suppliers", key="pr_pay_view", label_visibility="collapsed")
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
        pills = [p for p in (_paid_pill(r.total_safe_eur, tier),
                             _cro_pill_from(getattr(r, "cro_company_num", None), getattr(r, "cro_company_status", None))) if p]
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
        meta = f"{_n(r.n_suppliers):,} supplier{'s' if _n(r.n_suppliers) != 1 else ''} · {_n(r.min_year)}–{_n(r.max_year)}"
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
        f'Statement (revenue account, {_esc(span)}): spending by service. This is the council’s '
        '<strong>whole</strong> operating spend — a broader, separate measure from the purchase-order '
        'figures above, and <strong>never added to them</strong>.</p></div>'
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
                _n(crow.get("year")), crow.get("afs_gross_eur"), crow.get(po_col), crow.get(pct_col),
            )
            verb = _paid_verb(active_tier)  # 'paid' / 'ordered'
            st.html(
                '<div class="pr-afs-trace">'
                f'<div class="pr-afs-trace-fig"><strong>{_eur(gross)}</strong> spent (accounts, {yr})'
                f' · <strong>{_eur(po)}</strong> traceable to named suppliers'
                f' · <strong>{float(pct):g}%</strong></div>'
                f'<div class="pr-afs-trace-cap">Indicative coverage only. The accounts figure is the '
                f'council’s full audited operating spend; the supplier figure counts only purchases '
                f'over the €20,000 publication threshold ({verb} via purchase orders). Different '
                'thresholds and stages — a coverage signal, not a reconciliation.</div></div>'
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
        pills = [p for p in (_paid_pill(r.total_safe_eur, active),
                             _cro_pill_from(getattr(r, "cro_company_num", None), None)) if p]
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
        meta = (
            f"{_awards_word(_n(r.n_awards))} · {_n(r.n_buyers):,} buyer{'s' if _n(r.n_buyers) != 1 else ''}"
        )
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
    # Display-only — every link points at the source; nothing here is computed or inferred.
    notices_res = fetch_ted_notices_for_supplier_result(supplier_norm)
    ndf = notices_res.data if notices_res.ok else pd.DataFrame()
    links = []
    for nr in ndf.itertuples():
        url = _coalesce(getattr(nr, "notice_url", None))
        if not url:
            continue
        date = _coalesce(getattr(nr, "dispatch_date", None))[:10]
        buyer = _esc(_coalesce(getattr(nr, "buyer_name", None)) or "—")
        is_fw = _coalesce(getattr(nr, "value_kind", None)) == "framework_or_dps_ceiling"
        tag = "framework — shared ceiling, not a payment" if is_fw else "contract award"
        links.append(
            f'<li class="pr-notice"><a href="{_esc(url)}" target="_blank" rel="noopener">'
            f"{buyer} · {date} ↗</a> <span class=\"pr-notice-tag\">{tag}</span></li>"
        )
    if links:
        with st.expander(f"Open the {len(links):,} authoritative EU notice{'' if len(links) == 1 else 's'} on TED ↗"):
            st.html(
                '<p class="pr-cap">The tracker stores a thin slice of each award. Each notice below opens '
                "the full Official Journal record on TED — where the authority publishes what is actually "
                "being built, the real framework ceiling and the award criteria. The source, not our summary.</p>"
                f'<ul class="pr-notice-list">{"".join(links)}</ul>'
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
        meta_parts = [_esc(_coalesce(getattr(r, "cpv_division", None))), _esc(_coalesce(getattr(r, "procedure_type", None)))]
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
    return (
        f'<div class="pr-award"><div class="pr-award-body">'
        f'<div class="pr-award-auth">{head or "—"}</div>'
        f'<div class="pr-award-meta">{meta or "—"}</div></div>{_award_value_html(r)}</div>'
    )


def _award_row_html(r) -> str:
    """Supplier-profile award row — headlines the contracting authority."""
    cpv = _coalesce(getattr(r, "cpv_description", None)) or _coalesce(getattr(r, "cpv_code", None))
    return _award_row(
        _esc(r.contracting_authority) or "—",
        [fmt_civic_date(getattr(r, "award_date", None)), _esc(cpv), _coalesce(getattr(r, "competition_type", None))],
        r,
    )


def _supplier_head(r) -> str:
    """Supplier name for an authority/category award row. Sole traders / individuals ARE
    named (owner decision 2026-06-06): eTenders is published procurement data, so a supplier
    name on a public contract is already public and shown in a business capacity — consistent
    with the 'Money actually paid' tab. Only the published name is shown; no other PII."""
    return _esc(getattr(r, "supplier", None)) or "—"


def _award_row_by_supplier(r) -> str:
    """Authority-profile award row — headlines the supplier who won it."""
    cpv = _coalesce(getattr(r, "cpv_description", None)) or _coalesce(getattr(r, "cpv_code", None))
    return _award_row(
        _supplier_head(r),
        [fmt_civic_date(getattr(r, "award_date", None)), _esc(cpv), _coalesce(getattr(r, "competition_type", None))],
        r,
    )


def _award_row_cpv(r) -> str:
    """Category-profile award row — headlines the supplier, authority in the meta."""
    return _award_row(
        _supplier_head(r),
        [
            fmt_civic_date(getattr(r, "award_date", None)),
            _esc(_coalesce(getattr(r, "contracting_authority", None))),
            _coalesce(getattr(r, "competition_type", None)),
        ],
        r,
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

    # Cross-references for the same firm — each a separate register/stage, never summed.
    _render_paid_supplier_panel(supplier_norm)
    _render_ted_supplier_panel(supplier_norm)

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


def _stats_strip(stats, cov: dict) -> None:
    """Compact scale strip: just the plain-English corpus counts a reader can use. Decluttered
    2026-06-08 — dropped the internal data-quality chips ("N carry a sum-safe value", "X% matched
    to a CRO company") which were jargon, not public value; no euro figure here (awarded value is
    labelled per row)."""
    min_y, max_y = _n(stats.get("min_year")), _n(stats.get("max_year"))
    span = f"{min_y}–{max_y}" if min_y and max_y else "—"
    chips = [
        (f"{_n(stats.get('n_suppliers')):,}", "companies"),
        (f"{_n(stats.get('n_authorities')):,}", "public bodies"),
        (f"{_n(stats.get('n_categories')):,}", "categories"),
        (span, "award years"),
    ]
    items = "".join(
        f'<div class="pr-stat"><span class="pr-stat-num">{_esc(num)}</span>'
        f'<span class="pr-stat-lbl">{_esc(lbl)}</span></div>'
        for num, lbl in chips
    )
    st.html(f'<div class="pr-stats">{items}</div>')


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
    cov = fetch_coverage()

    # Hero carries no stat badges: the corpus counts + sum-safe total live in the
    # single _stats_strip below, so the data isn't pushed off-screen by a second
    # stat strip and the sum-safe total is shown exactly once (audit 2026-06-06).
    hero_banner(
        kicker="PUBLIC MONEY",
        title="Public Procurement",
        dek="Contract awards published on eTenders and the national procurement open data — "
        "who was awarded public contracts, by which bodies, in which categories.",
    )

    # Caveat trimmed to its two load-bearing honesty rails (awarded ≠ paid; no-inference). The
    # ceilings explanation moved to the "What these terms mean" expander (no duplication), and the
    # "€570bn" contrast panel was removed 2026-06-08 — both cut above-the-fold weight.
    st.html(
        '<div class="pr-caveat"><strong>Awarded value, not money paid.</strong> '
        "These are values at the point of award — see <em>Money actually paid</em> for real "
        "payments. A contract award is a public record of a procurement decision, not evidence "
        "of influence or wrongdoing.</div>"
    )
    _stats_strip(stats, cov)
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

    # Four top-level tabs (decluttered 2026-06-08 from eight). Each groups its related lenses
    # behind a segmented control — the same in-tab pattern the "Money actually paid" tab already
    # uses — so the page presents four clear doors, not a wrapping tab bar. Surfacing-only: every
    # lens calls an existing _render_* function; no logic moves into this layer.
    overlap = fetch_lobbying_overlap_result()
    charity_overlap = fetch_charity_overlap_result()
    tabs = st.tabs(["Contract awards", "Money actually paid", "EU awards (TED)", "Register overlaps"])

    with tabs[0]:
        # Year pills + lens picker live with the rankings they scope (national eTenders register).
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
        # Two TED grains behind one control: contract awards WON (2016–2026) vs the pre-award
        # tender pipeline (opportunities). Clearly different grains, never summed.
        ted_lens = st.segmented_control(
            "View",
            ["Awards won", "Open tenders"],
            default="Awards won",
            key="pr_ted_lens",
            label_visibility="collapsed",
        )
        if ted_lens == "Open tenders":
            _render_ted_tenders()
        else:
            _render_ted()

    with tabs[3]:
        # Co-occurrence disclosures (same pattern, two registers). Both all-time (not year-scoped).
        ov_lens = st.segmented_control(
            "View",
            ["Lobbying", "Charities"],
            default="Lobbying",
            key="pr_overlap_lens",
            label_visibility="collapsed",
        )
        if ov_lens == "Charities":
            _render_charity_overlap(charity_overlap.data if charity_overlap.ok else pd.DataFrame())
        else:
            _render_overlap(overlap.data if overlap.ok else pd.DataFrame(), None)

    st.html(
        '<div class="pr-foot"><strong>Source:</strong> eTenders / national procurement open data '
        '(<a href="https://data.gov.ie/dataset/contract-notices-published-on-etenders" '
        'target="_blank" rel="noopener">data.gov.ie ↗</a>), cross-referenced to the Companies '
        "Registration Office and the Register of Lobbying. Values are awarded contract values, not "
        "actual payments; only sum-safe award values are shown. Suppliers shown are company-class "
        "registrations — sole traders and individuals are excluded.</div>"
    )
