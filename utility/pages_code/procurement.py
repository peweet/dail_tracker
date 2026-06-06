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
    fetch_authority_summary_result,
    fetch_available_years,
    fetch_awards_for_authority,
    fetch_awards_for_cpv,
    fetch_awards_for_supplier,
    fetch_coverage,
    fetch_coverage_stats_result,
    fetch_cpv_summary_result,
    fetch_lobbying_overlap_result,
    fetch_supplier_summary_result,
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
def _render_suppliers(year: int | None) -> None:
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
    """Supplier name for an authority/category award row, with individuals masked.
    Sole-trader / individual awardees are personal data — the award is disclosed but
    the name is withheld (privacy rail); organisations are shown in full."""
    if _coalesce(getattr(r, "supplier_class", None)) == "sole_trader_or_individual":
        return "Individual / sole trader (name withheld)"
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
    # TODO_PIPELINE_VIEW_REQUIRED: the "€X awarded" pill can be composed ENTIRELY of
    # framework/DPS ceilings (e.g. Deloitte: 330/330 award rows are ceilings the page
    # itself labels "not a payment"), so the headline reads as money when none is. The
    # honest marker needs a per-supplier ceiling share on v_procurement_supplier_summary
    # (e.g. ceiling_value_eur / n_ceiling_rows); computing it here would be a Streamlit
    # aggregation (firewall breach). Until the column exists, add a "mostly ceilings"
    # variant of _value_pill driven by that field.
    st.html(f'<div class="pr-pills" style="margin:0.1rem 0 0.6rem">{"".join(pills)}</div>')

    awards = fetch_awards_for_supplier(supplier_norm)
    if awards is None or awards.empty:
        empty_state("No itemised awards", "The supplier is in the ranking but no award rows were returned.")
        return

    total = len(awards)
    st.caption(
        f"Every recorded contract award to this supplier ({total:,} in total), most recent first. "
        "Framework / DPS ceilings are shown in rust and are not actual payments."
    )
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
    """Scale anchor + trust strip: the corpus's real magnitude and what's in / out.
    Honest by construction — only the sum-safe total is shown, never the naive sum."""
    safe_total = _eur_scale(stats.get("value_safe_total_eur"))
    min_y, max_y = _n(stats.get("min_year")), _n(stats.get("max_year"))
    span = f"{min_y}–{max_y}" if min_y and max_y else "—"
    n_rows = _n(stats.get("n_award_rows"))
    n_safe = _n(stats.get("n_safe_rows"))
    # Corpus counts lead the strip (previously hero badges) so this is the single
    # scale anchor on the page — the sum-safe total appears here and nowhere else.
    chips = [
        (f"{_n(stats.get('n_suppliers')):,}", "suppliers"),
        (f"{_n(stats.get('n_authorities')):,}", "authorities"),
        (f"{_n(stats.get('n_categories')):,}", "categories"),
        (safe_total, "sum-safe awarded value"),
        (span, "award years"),
        (f"{n_rows:,}", "award records"),
        (f"{n_safe:,}", "carry a sum-safe value"),
    ]
    cro_pct = cov.get("cro_exact_unique_pct_of_company")
    if cro_pct:
        chips.append((f"{cro_pct:g}%", "matched to a CRO company"))
    quarantined = _n(cov.get("sole_trader_quarantined"))
    if quarantined:
        chips.append((f"{quarantined:,}", "private names withheld"))
    items = "".join(
        f'<div class="pr-stat"><span class="pr-stat-num">{_esc(num)}</span>'
        f'<span class="pr-stat-lbl">{_esc(lbl)}</span></div>'
        for num, lbl in chips
    )
    st.html(f'<div class="pr-stats">{items}</div>')


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

    st.html(
        '<div class="pr-caveat"><strong>Awarded value, not actual spend.</strong> '
        "These are contract <em>award</em> values reported on eTenders — the value at award, "
        "not money actually paid. Framework and dynamic-purchasing ceilings can overstate what "
        "is ever drawn down, so the page only ever shows the <em>sum-safe</em> awarded value: the "
        "total below sums only the rows that carry one, never a naive total of every reported "
        "figure. A contract award is a public record of a procurement decision, not evidence of "
        "influence or wrongdoing.</div>"
    )
    _stats_strip(stats, cov)
    glossary_strip(
        [
            ("Award value", "the contract value at the point of award — not money actually paid out"),
            ("Framework / DPS", "an agreement a buyer may draw down against — the ceiling is not a payment"),
            ("CPV", "Common Procurement Vocabulary — the EU category code for what was bought"),
            ("CRO", "Companies Registration Office — a matched company registration number"),
        ]
    )

    if _n(stats.get("n_suppliers")) == 0:
        empty_state("No supplier records", "The procurement views are loaded but returned no rows.")
        return

    # Year pills scope the Suppliers / Authorities / Categories rankings. The lobbying
    # overlap register isn't dated here, so that tab stays all-time (noted in-tab).
    st.caption("Filter the rankings by award year:")
    year = _year_pills(fetch_available_years())

    overlap = fetch_lobbying_overlap_result()
    tabs = st.tabs(["Suppliers", "Contracting authorities", "Categories", "Lobbying overlap"])
    with tabs[0]:
        _render_suppliers(year)
    with tabs[1]:
        _render_authorities(year)
    with tabs[2]:
        _render_cpv(year)
    with tabs[3]:
        _render_overlap(overlap.data if overlap.ok else pd.DataFrame(), year)

    st.html(
        '<div class="pr-foot"><strong>Source:</strong> eTenders / national procurement open data '
        '(<a href="https://data.gov.ie/dataset/contract-notices-published-on-etenders" '
        'target="_blank" rel="noopener">data.gov.ie ↗</a>), cross-referenced to the Companies '
        "Registration Office and the Register of Lobbying. Values are awarded contract values, not "
        "actual payments; only sum-safe award values are shown. Suppliers shown are company-class "
        "registrations — sole traders and individuals are excluded.</div>"
    )
