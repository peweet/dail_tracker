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
    fetch_awards_for_supplier,
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


def _supplier_href(supplier_norm) -> str:
    return f"?supplier={urllib.parse.quote(str(supplier_norm))}"


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
def _render_suppliers(df: pd.DataFrame) -> None:
    # Global rank by award count (df is pre-ordered DESC by the view) — kept so a
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
    st.caption(
        f"{total:,} suppliers"
        + (f' matching "{qs}"' if qs else " ranked by number of contract awards")
        + ". Value shown is awarded value, not spend — click a supplier for its full award history."
    )
    if total == 0:
        empty_state("No suppliers match", "Try a shorter or different search term.")
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
        inner = _card(
            f"<span>{_esc(r.supplier)}</span>", meta, pills, rank=ranks.get(str(r.supplier_norm))
        )
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
# Tab: Contracting authorities / Categories (ranked, non-clickable — no per-entity
# detail view exists in core, so these stay as a read-only ranking)
# ──────────────────────────────────────────────────────────────────────────────
def _render_authorities(df: pd.DataFrame) -> None:
    if df.empty:
        empty_state("No contracting authorities", "The authority view returned no rows.")
        return
    st.caption(f"Top {min(_TOP, len(df)):,} contracting authorities by number of awards.")
    cards = []
    for i, r in enumerate(df.head(_TOP).itertuples(), start=1):
        meta = (
            f"{_awards_word(_n(r.n_awards))} · "
            f"{_n(r.n_suppliers):,} supplier{'s' if _n(r.n_suppliers) != 1 else ''}"
        )
        cards.append(
            _card(f"<span>{_esc(r.contracting_authority)}</span>", meta, [_value_pill(r.awarded_value_safe_eur)], rank=i)
        )
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')


def _render_cpv(df: pd.DataFrame) -> None:
    if df.empty:
        empty_state("No categories", "The CPV category view returned no rows.")
        return
    st.caption(f"Top {min(_TOP, len(df)):,} procurement categories (CPV) by number of awards.")
    cards = []
    for i, r in enumerate(df.head(_TOP).itertuples(), start=1):
        title = _esc(r.cpv_description) or _esc(r.cpv_code) or "—"
        meta = (
            f"CPV {_esc(r.cpv_code)} · {_awards_word(_n(r.n_awards))} · "
            f"{_n(r.n_suppliers):,} supplier{'s' if _n(r.n_suppliers) != 1 else ''}"
        )
        cards.append(_card(f"<span>{title}</span>", meta, [_value_pill(r.awarded_value_safe_eur)], rank=i))
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')


# ──────────────────────────────────────────────────────────────────────────────
# Tab: Lobbying overlap (clickable → supplier profile)
# ──────────────────────────────────────────────────────────────────────────────
def _render_overlap(df: pd.DataFrame) -> None:
    st.caption(
        "Organisations that appear on BOTH the procurement and lobbying registers. "
        "This is a co-occurrence disclosure only — it does not imply that lobbying "
        "influenced any award."
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
def _award_row_html(r) -> str:
    auth = _esc(r.contracting_authority) or "—"
    cpv = _coalesce(getattr(r, "cpv_description", None)) or _coalesce(getattr(r, "cpv_code", None))
    date = fmt_civic_date(getattr(r, "award_date", None))
    comp = _coalesce(getattr(r, "competition_type", None))
    meta = " · ".join(p for p in (date, _esc(cpv), _esc(comp)) if p and p != "—")

    is_ceiling = _coalesce(getattr(r, "value_kind", None)) == "framework_or_dps_ceiling"
    val = _eur(getattr(r, "value_eur", None))
    sub = "framework ceiling — not a payment" if is_ceiling else "contract award value"
    cls = "pr-award-val ceiling" if is_ceiling else "pr-award-val"
    val_html = f'<div class="{cls}">{val}<small>{sub}</small></div>' if val != "—" else ""
    return (
        f'<div class="pr-award"><div class="pr-award-body">'
        f'<div class="pr-award-auth">{auth}</div>'
        f'<div class="pr-award-meta">{meta or "—"}</div></div>{val_html}</div>'
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


# ──────────────────────────────────────────────────────────────────────────────
def procurement_page() -> None:
    hide_sidebar()

    # Drill-down — a single supplier's award history, full width, with back nav.
    supplier_norm = st.query_params.get("supplier")
    if supplier_norm:
        _render_supplier_profile(supplier_norm)
        return

    sup = fetch_supplier_summary_result(limit=None)
    auth = fetch_authority_summary_result(limit=_TOP)
    cpv = fetch_cpv_summary_result(limit=_TOP)
    overlap = fetch_lobbying_overlap_result()

    # Source-state: a missing view / parquet / DuckDB error is NOT "no results".
    if not sup.ok:
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

    suppliers = sup.data
    n_auth = len(auth.data) if auth.ok else 0
    n_cpv = len(cpv.data) if cpv.ok else 0

    hero_banner(
        kicker="PUBLIC MONEY",
        title="Public Procurement",
        dek="Contract awards published on eTenders and the national procurement open data — "
        "who was awarded public contracts, by which bodies, in which categories.",
        badges=[
            f"{len(suppliers):,} suppliers",
            f"{n_auth:,} authorities",
            f"{n_cpv:,} categories",
        ],
    )

    st.html(
        '<div class="pr-caveat"><strong>Awarded value, not actual spend.</strong> '
        "These are contract <em>award</em> values reported on eTenders — the value at award, "
        "not money actually paid. Framework and dynamic-purchasing ceilings can overstate what "
        "is ever drawn down, so the page only ever shows the sum-safe awarded value per row and "
        "never totals the corpus into a single headline figure. A contract award is a public "
        "record of a procurement decision, not evidence of influence or wrongdoing.</div>"
    )
    glossary_strip(
        [
            ("Award value", "the contract value at the point of award — not money actually paid out"),
            ("Framework / DPS", "an agreement a buyer may draw down against — the ceiling is not a payment"),
            ("CPV", "Common Procurement Vocabulary — the EU category code for what was bought"),
            ("CRO", "Companies Registration Office — a matched company registration number"),
        ]
    )

    if suppliers.empty:
        empty_state("No supplier records", "The procurement views are loaded but returned no rows.")
        return

    tabs = st.tabs(["Suppliers", "Contracting authorities", "Categories", "Lobbying overlap"])
    with tabs[0]:
        _render_suppliers(suppliers)
    with tabs[1]:
        _render_authorities(auth.data if auth.ok else pd.DataFrame())
    with tabs[2]:
        _render_cpv(cpv.data if cpv.ok else pd.DataFrame())
    with tabs[3]:
        _render_overlap(overlap.data if overlap.ok else pd.DataFrame())

    st.html(
        '<div class="pr-foot"><strong>Source:</strong> eTenders / national procurement open data '
        '(<a href="https://data.gov.ie/dataset/contract-notices-published-on-etenders" '
        'target="_blank" rel="noopener">data.gov.ie ↗</a>), cross-referenced to the Companies '
        "Registration Office and the Register of Lobbying. Values are awarded contract values, not "
        "actual payments; only sum-safe award values are shown. Suppliers shown are company-class "
        "registrations — sole traders and individuals are excluded.</div>"
    )
