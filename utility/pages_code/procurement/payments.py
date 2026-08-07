from __future__ import annotations

import urllib.parse

import pandas as pd
import streamlit as st

from data_access.procurement_data import (
    fetch_payments_real_trend_result,
    fetch_payments_corpus_stats_result,
    fetch_payments_publisher_summary_result,
    fetch_payments_supplier_summary_result,
)
from ui.components import (
    clickable_card_link,
    empty_state,
    paginate,
    pagination_controls,
)

from ui.format import coalesce as _coalesce
from ui.format import esc as _esc
from ui.format import to_int as _n
from ui.format import truthy as _truthy

from ._shared import (
    _EXPERIMENTAL,
    _SUP_PAGE,
    _eur,
    _eur_scale,
    _paid_supplier_href,
    _yr_axis,
    _card,
)

from .browse import _render_real_terms_rail


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
        f"<em>published themselves</em> (mostly their over-€20,000 lists — some bodies use a different "
        f"threshold, e.g. €25,000; {span}), to "
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
        if _EXPERIMENTAL:  # local-only real-terms trend lens (gov-consumption deflator)
            view_labels["In real terms ⚗"] = "realtrend"
        view = view_labels.get(
            st.segmented_control(
                "View", list(view_labels), default="Top suppliers", key="pr_pay_view", label_visibility="collapsed"
            )
            or "Top suppliers",
            "supplier",
        )

    if view == "supplier":
        _render_paid_suppliers(tier)
    elif view == "realtrend":
        _render_payments_real_trend(tier)
    else:
        _render_paid_publishers(tier)
    st.html(
        '<div class="pr-foot"><strong>Source:</strong> each public body\'s own published '
        "purchase-order / payments disclosures — most under the FOI Act 2014 s.8 model publication "
        "scheme (origin: Circular FIN 07/12), some published voluntarily; consolidated and "
        "matched to the Companies Registration Office. Not every public body has this obligation, and "
        "thresholds differ by body. Suppliers are named as published. "
        "Paid (actual spend) and ordered (purchase orders) are different stages and are never summed "
        "together; totals are never summed across bodies with different VAT bases; never added to award values.</div>"
    )


def _render_payments_bridge() -> None:
    """Compact lifecycle bridge for the "Who actually gets paid?" section (Money nav
    declutter Phase 2.5, doc/archive/MONEY_NAV_DECLUTTER_PLAN.md §15). The awards→paid pivot
    stays on this page, but the FULL payments browse now lives in one place — the
    Public Payments hub — so this section renders the honest corpus framing, a
    top-few teaser, and the two doors onward instead of duplicating that browse.
    Follow the Money keeps the full ``_render_payments()`` landing (it IS the payment
    graph), and the in-page ``?paid_*`` drills stay routable via the page router, so
    search results and existing deep links still land on their ledgers."""
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
        f"<em>published themselves</em> (mostly their over-€20,000 lists — some bodies use a different "
        f"threshold, e.g. €25,000; {span}), to "
        f"{_n(s.get('n_suppliers')):,} suppliers. At least <strong>{_eur_scale(s.get('spent_safe_eur'))} "
        f"paid</strong> and {_eur_scale(s.get('committed_safe_eur'))} ordered — an indicative floor, "
        "not an audited total (bodies use different VAT bases, so totals are never summed across them, "
        "and these are <em>never</em> added to the award figures above — a paid invoice and a contract "
        "ceiling are different stages of public money).</div>"
    )
    # Teaser: the top few paid firms (actual payments), a display-only head-slice of
    # the same cached ranking the full browse uses. Cards keep the in-page drill and
    # the company-class-only clickability quarantine (same as _render_paid_suppliers).
    res = fetch_payments_supplier_summary_result(tier="SPENT", limit=None)
    df = res.data if res.ok else pd.DataFrame()
    if not df.empty:
        st.caption("The biggest recipients of actual payments (sum-safe), as published:")
        cards = []
        for i, r in enumerate(df.head(5).itertuples(), start=1):
            np_ = _n(r.n_publishers)
            meta = (
                f"{_n(r.n_payments):,} payment{'s' if _n(r.n_payments) != 1 else ''} · "
                f"{np_:,} public bod{'ies' if np_ != 1 else 'y'}"
            )
            pills = [p for p in (_paid_pill(r.total_safe_eur, "SPENT"),) if p]
            inner = _card(f"<span>{_esc(r.supplier)}</span>", meta, pills, rank=i)
            if _coalesce(getattr(r, "supplier_class", None)) == "company":
                cards.append(
                    clickable_card_link(
                        href=_paid_supplier_href(r.supplier_normalised, "SPENT"),
                        inner_html=inner,
                        aria_label=f"View the public bodies that paid {r.supplier}",
                    )
                )
            else:
                cards.append(inner)
        st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    # The two doors onward — the same whole-card family as the Public Payments hub
    # cards, so the "go deeper" pattern reads identically on both sides of the bridge.
    st.html(
        '<div class="pp-deeper">'
        '<a class="mf-featured" href="/rankings-public-payments" target="_self" '
        'aria-label="Browse the full payments register">'
        '<div class="mf-featured-kick">FULL REGISTER</div>'
        '<div class="mf-featured-name">Browse the full payments register</div>'
        '<div class="mf-featured-blurb">Every publishing body and supplier — rankings, '
        "categories and coverage. The payments home.</div></a>"
        '<a class="mf-featured" href="/follow-the-money" target="_self" '
        'aria-label="Trace a payment chain">'
        '<div class="mf-featured-kick">GO DEEPER</div>'
        '<div class="mf-featured-name">Trace a payment chain</div>'
        '<div class="mf-featured-blurb">Follow one body\'s money to the companies it pays, '
        "line by line — and step back through the trail.</div></a>"
        "</div>"
    )
    st.html(
        '<div class="pr-foot"><strong>Source:</strong> each public body\'s own published '
        "purchase-order / payments disclosures — most under the FOI Act 2014 s.8 model publication "
        "scheme (origin: Circular FIN 07/12), some published voluntarily. Paid and ordered are "
        "different stages, never summed together; never added to award values.</div>"
    )


def _render_payments_real_trend(tier: str) -> None:
    """EXPERIMENTAL real-terms trend for public spend, deflated by the government-consumption index
    (the agency-standard for public money). Shows the per-year nominal-vs-real gap — the honest use
    of this lens: nominal figures increasingly understate OLDER spend, and the effect on recent
    years is small. Years the National Accounts deflator can't yet reach (2025+) are shown in
    nominal terms and flagged, never blended into the real series. The page computes nothing — the
    rollup + per-year uplift live in v_procurement_payments_real_trend."""
    _render_real_terms_rail("CSO_GOV_CONSUMPTION")
    res = fetch_payments_real_trend_result(tier=tier)
    if not res.ok or res.data.empty:
        empty_state("Real-terms trend unavailable", "The real-terms payments view did not load.")
        return
    df = res.data
    base = _n(df["real_base_year"].dropna().iloc[0]) if df["real_base_year"].notna().any() else "the base year"
    verb = _paid_verb(tier)
    adj = df[df["real_uplift_pct"].notna()]  # years the deflator reaches
    unadj = df[df["real_uplift_pct"].isna()]  # the 2025+ coverage cliff
    if not adj.empty:
        first = adj.iloc[0]  # earliest adjustable year — the widest gap (rows are year-ordered)
        st.markdown(
            f"**In today's money, older public spend is bigger than it looks.** {verb.capitalize()} in "
            f"**{_n(first['year'])}** is worth **+{first['real_uplift_pct']:.0f}%** more in {base} prices; "
            f"the gap narrows to ~0% by {base}. Each bar is how much more that year's {verb} spend is "
            f"worth once re-expressed in {base} prices (government-consumption deflator) — so the same "
            "spending compares like-for-like across years, not left understated in older money."
        )
        # A SINGLE series — the per-year uplift %. Deliberately NOT nominal-vs-real absolute bars:
        # those stack into a false "sum", and the real story (big uplift on OLD years) is invisible
        # because older years are tiny in absolute €. The uplift ratio is VAT-independent.
        # Plain field name + y_label for the human axis title — a field name with spaces/"%"
        # silently renders an empty Vega chart (matches the working awards-by-year chart pattern).
        chart = adj[["year", "real_uplift_pct"]]
        st.bar_chart(
            _yr_axis(chart),
            x="year",
            y="real_uplift_pct",
            x_label="Year",
            y_label="% more in today's money (vs nominal)",
            height=280,
            color="#9c5b2e",
        )
    if not unadj.empty:
        yrs = ", ".join(str(_n(y)) for y in sorted(unadj["year"].tolist()))
        st.info(
            f"**{yrs} not yet adjustable.** The CSO government-consumption deflator currently ends "
            f"{base} — National Accounts for later years aren't published yet — so {verb} spend for "
            "these years is shown in nominal terms only, never blended into the real series.",
            icon="🕓",
        )
    st.caption(
        "Government-consumption deflator (CSO National Accounts) — the index statistical agencies "
        "use for public spending, not consumer prices. Real-terms re-expresses purchasing power; it "
        "is not a current cost. Paid and ordered are never summed; neither is added to award values."
    )


def _render_paid_suppliers(tier: str) -> None:
    # Deferred: .ted imports from this module, so this reverse edge is call-time.
    from .ted import _cro_pill_from

    res = fetch_payments_supplier_summary_result(tier=tier, limit=None)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        empty_state("No payments", f"No supplier has {_paid_verb(tier)} records in this tier.")
        return
    total = len(df)
    st.caption(
        f"{total:,} suppliers by money {_paid_verb(tier)} (sum-safe), biggest first. Names as published by the body. "
        "Click a company for the public bodies that paid it."
    )
    pg_key = f"pr_pay_sup_{tier}"
    page_idx = paginate(total, key_prefix=pg_key, page_size=_SUP_PAGE)
    page = df.iloc[page_idx * _SUP_PAGE : (page_idx + 1) * _SUP_PAGE]
    cards = []
    for offset, r in enumerate(page.itertuples()):
        i = page_idx * _SUP_PAGE + offset + 1
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
    st.html('<div class="pr-sp-md"></div>')
    pagination_controls(
        total,
        key_prefix=pg_key,
        page_sizes=(_SUP_PAGE,),
        default_page_size=_SUP_PAGE,
        label="suppliers",
    )


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
    total = len(df)
    st.caption(f"Public bodies by money {_paid_verb(tier)} (sum-safe within each body). Click one for its suppliers.")
    # Keyed by the LA toggle too: filtering changes total, so the page index must not carry over.
    pg_key = f"pr_pay_pub_{tier}_{'la' if (n_la and only_la) else 'all'}"
    page_idx = paginate(total, key_prefix=pg_key, page_size=_SUP_PAGE)
    page = df.iloc[page_idx * _SUP_PAGE : (page_idx + 1) * _SUP_PAGE]
    cards = []
    for offset, r in enumerate(page.itertuples()):
        i = page_idx * _SUP_PAGE + offset + 1
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
    st.html('<div class="pr-sp-md"></div>')
    pagination_controls(
        total,
        key_prefix=pg_key,
        page_sizes=(_SUP_PAGE,),
        default_page_size=_SUP_PAGE,
        label="public bodies",
    )
