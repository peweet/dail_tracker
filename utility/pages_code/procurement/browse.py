from __future__ import annotations


import pandas as pd
import streamlit as st

from data_access.procurement_data import (
    fetch_authority_summary_result,
    fetch_cpv_summary_result,
    fetch_cpv_summary_real_result,
    fetch_inflation_indices,
    fetch_awards_by_year_result,
    fetch_supplier_summary_result,
)
from ui.components import (
    clickable_card_link,
    empty_state,
    paginate,
    pagination_controls,
    text_search_mask,
)

from ui.format import coalesce as _coalesce
from ui.format import esc as _esc
from ui.format import to_int as _n

from ._shared import (
    _EXPERIMENTAL,
    _TOP,
    _SUP_PAGE,
    _eur_scale,
    _awards_word,
    _supplier_href,
    _authority_href,
    _cpv_href,
    _sort_toggle,
    _year_label,
    _yr_axis,
    _card,
    _value_pill,
    _cro_pill,
    _lobby_pill,
)


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
            st.bar_chart(
                _yr_axis(tr.data), x="year", y="n_awards", x_label="Year", y_label="Awards", height=200, color="#9c5b2e"
            )


def _render_suppliers(year: int | None) -> None:
    # Role-clarity (Money nav declutter Phase 2): this tab is the AWARDS league
    # table; the whole-firm cross-register view is the company dossier each card opens.
    st.caption(
        "The awards league table — every firm on the national contract-award register, ranked. "
        "A company card opens its full cross-register dossier."
    )
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
        view = df[text_search_mask(df, qs, ["supplier"])]

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


def _render_real_terms_rail(index_code: str) -> None:
    """Shared caveat rail + 'how is this adjusted?' popover for any real-terms lens. Reads the
    index's label/source/caveat from the deflation registry (services.deflator, via the cached
    wrapper) so the page states nothing it cannot cite. EXPERIMENTAL — only shown when the lens
    toggle is on, which is itself gated to the local box."""
    meta = next((i for i in fetch_inflation_indices() if i["code"] == index_code), None)
    label = meta["label"] if meta else index_code
    st.warning(
        f"**Shown in today's money ({label}).** This re-expresses *past disclosed values* in "
        "current purchasing power — it is **not** what the work would cost to buy today, and "
        "**not** a recommended bid price. General consumer-price inflation is **not** the same as "
        "construction, building-materials, labour-rate or tender-price inflation, which move at "
        "very different rates.",
        icon="🧮",
    )
    with st.popover("ⓘ How is this adjusted?"):
        st.markdown(
            f"**Index:** {label}  \n"
            f"**Source:** {meta['source'] if meta else '—'}  \n"
            "**Method:** each award is multiplied by the index ratio from its award year to the "
            "base year — the standard rebasing statistical agencies use.\n\n"
            f"{meta['caveat'] if meta else ''}\n\n"
            "Framework/DPS ceilings, awards whose year falls outside the index, and implausible "
            "values are left in nominal terms and counted separately — never silently adjusted."
        )
    st.caption("⚗ Experimental · local only — not shown in the published app.")


def _render_cpv(year: int | None) -> None:
    order = _sort_toggle("pr_cpv_sort")
    res = fetch_cpv_summary_result(limit=_TOP, order_by=order, year=year)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        empty_state("No categories", f"No category has awards{_year_label(year)}.")
        return
    # EXPERIMENTAL real-terms lens (local only). Deflates the typical-award band to today's
    # money via CPI. All-time only — deflating within a single selected year is trivial (every
    # award that year shares one factor). The deflation lives in v_procurement_cpv_summary_real
    # + services/deflator.py; the page only looks the real band up by CPV and renders it beside
    # the nominal one, and never computes a figure.
    real_lookup: dict[str, object] = {}
    show_real = False
    if _EXPERIMENTAL and year is None:
        show_real = st.toggle(
            "Show the typical-award band in today's money (2025 prices)",
            value=False,
            key="pr_cpv_real",
            help="Re-expresses past award values using the CSO Consumer Price Index — purchasing "
            "power only, not a current cost and not a bid price.",
        )
        if show_real:
            rres = fetch_cpv_summary_real_result()
            if rres.ok and not rres.data.empty:
                real_lookup = {str(rr.cpv_code): rr for rr in rres.data.itertuples()}
            _render_real_terms_rail("CSO_CPA07_CPI")
            st.caption(
                "Construction categories (CPV 45/71) are shown in **tender prices** (SCSI index — "
                "construction costs rose far faster than CPI); every other category uses CPI. "
                "Each band names the index it used."
            )
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
            # Real-terms companion band, looked up by CPV (only when its own real sample is deep
            # enough to be meaningful). Shown beside the nominal band, never replacing it.
            if show_real:
                rr = real_lookup.get(str(r.cpv_code))
                # Sector-aware band: construction CPVs (45*/71*) use the SCSI tender-price index
                # (the right "cost to procure" lens — construction rose far faster than CPI),
                # every other category uses CPI. deflator_index_sector names the index used.
                rn = _n(getattr(rr, "n_awards_valued_real_sector", 0)) if rr is not None else 0
                if rr is not None and rn >= 8 and getattr(rr, "median_award_real_sector_eur", None):
                    idx = getattr(rr, "deflator_index_sector", "") or ""
                    lens = "2025 tender prices" if idx == "SCSI_TPI_CONSTRUCTION" else "2025 prices"
                    meta += (
                        f" · in {lens} {_eur_scale(rr.p25_award_real_sector_eur)}–"
                        f"{_eur_scale(rr.p75_award_real_sector_eur)} "
                        f"(median {_eur_scale(rr.median_award_real_sector_eur)})"
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
