from __future__ import annotations


import pandas as pd
import streamlit as st

from data_access.procurement_data import (
    resolve_buyer_identity,
    fetch_authority_summary_result,
    fetch_awards_for_authority,
    fetch_awards_for_cpv,
    fetch_awards_for_supplier,
    fetch_cpv_summary_result,
    fetch_supplier_summary_result,
    fetch_supplier_year_trend_result,
)
from ui.entity_links import (
    buyer_dossier_cta_html,
    company_link_html,
    source_link_html,
)
from ui.components import (
    back_button,
    empty_state,
    fmt_civic_date,
    paginate,
    pagination_controls,
)

from ui.format import coalesce as _coalesce
from ui.format import esc as _esc
from ui.format import to_int as _n
from ui.format import truthy as _truthy

from ._shared import (
    _AWARD_PAGE,
    _eur,
    _awards_word,
    _yr_axis,
    _award_year_pills,
    _filter_awards_by_year,
    _return_to_browse,
    _value_pill,
    _cro_pill,
    _lobby_pill,
    _authority_link,
)

# NB: .ted and .pay_profiles both import from THIS module (award-row / panel helpers),
# so the reverse edge is deferred to call time (see _render_supplier_profile) to avoid a
# circular top-level import within the payments/ted/profiles/pay_profiles/councils cluster.


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
    # Keep entity navigation and source evidence as sibling links. Wrapping the
    # whole row in the external notice anchor would make a linked buyer/supplier a
    # nested anchor, which is invalid HTML and leaves only one of the two journeys
    # usable with a keyboard.
    url = _award_notice_url(r)
    if url:
        meta_parts = [
            *meta_parts,
            source_link_html(
                url,
                "Source notice",
                aria_label="Open the authoritative procurement notice in a new tab",
            ),
        ]
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
    return inner


def _award_detail_meta(r) -> list[str]:
    """Detail meta fragments shared by every award row: procedure, contract term, bid
    count. :func:`_award_row` adds the source-notice link beside these fragments so an
    entity link can coexist without invalid nested anchors. All values come straight
    from the view; this function only formats them for display."""
    parts = [_esc(_coalesce(getattr(r, "procedure_type", None)))]
    months = _n(getattr(r, "contract_duration_months", None))
    if months > 0:
        parts.append(f"{months}-month term")
    bids = _n(getattr(r, "n_bids_received", None))
    if bids > 0:
        parts.append(f"{bids:,} bid{'' if bids == 1 else 's'} received")
    return parts


def _award_row_html(r, *, cross_page: bool = False) -> str:
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
    return _award_row(
        _authority_link(r.contracting_authority, cross_page=cross_page),
        meta,
        r,
    )


def _supplier_head(r) -> str:
    """Supplier name for an authority/category award row. Sole traders / individuals ARE
    named (owner decision 2026-06-06): eTenders is published procurement data, so a supplier
    name on a public contract is already public and shown in a business capacity — consistent
    with the 'Money actually paid' tab. Only the published name is shown; no other PII."""
    name = _coalesce(getattr(r, "supplier", None))
    if not name:
        return "—"
    supplier_norm = str(_coalesce(getattr(r, "supplier_norm", None)))
    resolves_to_company = (
        _coalesce(getattr(r, "supplier_class", None)) == "company"
        and len(supplier_norm) >= 4
        and supplier_norm.upper() != "NULL"
        and not _truthy(getattr(r, "name_truncated", None))
    )
    if resolves_to_company:
        return company_link_html(supplier_norm, name, css_class="pr-auth-link")
    return _esc(name)


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
            _authority_link(getattr(r, "contracting_authority", None)),
            _coalesce(getattr(r, "competition_type", None)),
            *_award_detail_meta(r),
        ],
        r,
    )


def _supplier_secured_trend(supplier_norm: str) -> None:
    """Public-sector work SECURED per year — the firm's public order-book trend (the market-
    intelligence ask: 'is this competitor's public workload rising or thinning?'). DISPLAY-ONLY:
    the per-(supplier, year) rows arrive pre-aggregated and value-gated from
    v_procurement_supplier_year_summary; the page charts them, computing no metric (no groupby
    here — the logic firewall forbids it).

    The public-only framing is the non-negotiable honesty rail: this is contracts won on the
    public procurement register, NEVER the company's turnover. A single year is not a trend, so the
    panel is shown only for firms with awards in ≥2 years (the award list below still shows the rest)."""
    res = fetch_supplier_year_trend_result(supplier_norm)
    if not res.ok or res.data.empty or len(res.data) < 2:
        return
    df = res.data
    st.html(
        '<div class="pr-caveat"><strong>Public-sector work secured, year by year.</strong> '
        "The value of public contracts this firm <em>won</em> on the national procurement register "
        "each year — <strong>not its turnover</strong>. It shows only the public-sector slice of the "
        "business: a private company may earn most of its income from private clients, which never "
        "appears here. Figures are <em>awarded</em> contract value (sum-safe — framework/DPS ceilings "
        "excluded), not money paid.</div>"
    )
    st.caption("Sum-safe awarded value secured per year (€)")
    st.bar_chart(
        _yr_axis(df),
        x="year",
        y="awarded_value_safe_eur",
        x_label="Year",
        y_label="€ awarded (sum-safe)",
        height=200,
        color="#9c5b2e",
    )


def _supplier_awards_section(row, supplier_norm: str, *, cross_page: bool = True) -> None:
    """Paginated eTenders award history for one firm, with the headline-reconciliation
    caption. Shared by the in-page supplier profile here and the /company dossier page
    (pages_code/company.py) so the honesty copy can never drift between the two.

    The external ``/company`` reuse is the default and emits absolute authority links.
    The legacy in-procurement profile passes ``cross_page=False`` for same-page soft
    navigation.
    """
    awards = fetch_awards_for_supplier(supplier_norm)
    if awards is None or awards.empty:
        empty_state("No itemised awards", "The supplier is in the ranking but no award rows were returned.")
        return

    # Public-work-secured-per-year trend + the public-only framing banner, ABOVE the itemised
    # rows (both the procurement profile and the /company dossier inherit it through this shared
    # component, so the "not turnover" honesty copy can never drift between the two surfaces).
    _supplier_secured_trend(supplier_norm)

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
    st.html("".join(_award_row_html(r, cross_page=cross_page) for r in page.itertuples()))
    st.html('<div class="pr-sp-sm"></div>')
    pagination_controls(
        total,
        key_prefix=key,
        page_sizes=(_AWARD_PAGE,),
        default_page_size=_AWARD_PAGE,
        label="awards",
    )


def _render_supplier_profile(supplier_norm: str) -> None:
    # Deferred (call-time) imports: .ted and .pay_profiles both import award-row helpers
    # from this module, so a top-level import here would be circular.
    from .pay_profiles import _render_paid_supplier_panel, _render_supplier_register_footprint
    from .ted import (
        _render_supplier_call_offs_panel,
        _render_ted_supplier_panel,
        _render_supplier_competition_panel,
        _render_supplier_relationships_panel,
    )

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

    _supplier_awards_section(row, supplier_norm, cross_page=False)
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
    pagination_controls(
        total, key_prefix=pkey, page_sizes=(_AWARD_PAGE,), default_page_size=_AWARD_PAGE, label="awards"
    )
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

    # Forward edge into the body's canonical /body dossier (awards + payments unified).
    # GATED: only the ~90 crosswalk bodies resolve, so an authority not in it gets NO link
    # rather than a "not found" dead end (never a false hand-off).
    if resolve_buyer_identity(authority):
        st.html(buyer_dossier_cta_html(authority))

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
