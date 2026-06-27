"""Public-body payments — read-only explorer over the registered ``v_public_payments*``
views (purchase-order / payment disclosures over €20k by departments, semi-states,
health and education bodies, plus HSE & Tusla).

Surfacing only: every aggregation, value-gate and privacy-gate already lives in the SQL
views; this page reads pre-aggregated rows and renders cards. It does NO modelling — no
value_counts / groupby / merge / parquet reads (the logic firewall checker scans this file).
Search and pagination are display-only slices over already-fetched rows.

Honesty rails (non-negotiable):
  * "Ordered or paid, not a single 'spend' figure" — the page only ever shows the view's
    sum-safe value (``value_safe_to_sum``), which excludes intergovernmental transfers/grants
    and non-positive amounts. Per-publisher rows never blend purchase-order commitments
    ("ordered") with actual payments ("paid"); the corpus total combines both and is labelled
    as such, and is NEVER added to eTenders/TED award ceilings (a different register).
  * Privacy: likely-personal suppliers (sole traders / individuals — e.g. HSE locum clinicians,
    Tusla carers) are withheld at the view boundary (public_display); they never reach this page.
  * Source-state aware: a missing view / parquet shows "data unavailable", not a silent empty
    list (uses the QueryResult ok/unavailable distinction).

Layout: browse view (caveat → Publishers / Suppliers tabs → provenance) with ``?publisher=<id>``
and ``?supplier=<norm>`` drill-downs to a single entity's payment lines. CSS reuses the
procurement ``pr-*`` family in shared_css.py.
"""

from __future__ import annotations

import html
import sys
import urllib.parse
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.public_payments_data import (
    fetch_available_years_result,
    fetch_categories_result,
    fetch_category_publishers_result,
    fetch_category_suppliers_result,
    fetch_coverage,
    fetch_coverage_stats_result,
    fetch_publisher_lines_result,
    fetch_publisher_summary_result,
    fetch_supplier_lines_result,
    fetch_supplier_quarter_totals_result,
    fetch_supplier_summary_result,
)
from shared_css import inject_css  # noqa: F401  (kept parallel to other pages)
from ui.components import (
    back_button,
    card_sources_html,
    clickable_card_link,
    empty_state,
    finding_lede,
    glossary_strip,
    hero_banner,
    hide_sidebar,
    paginate,
    pagination_controls,
    render_national_finance_context,
    text_search_mask,
)
from ui.entity_links import company_profile_url, entity_cta_html, source_link_html

# Shared council audited-accounts (AFS by-division) context block. Cross-page
# import mirrors member_overview → lobbying_3; the renderer and its fetches are
# self-contained and silently no-op when a council has no AFS rows.
from pages_code.procurement import _render_council_accounts_context as render_council_accounts_context

_TOP = 60  # ranked cards per browse tab (views are pre-ordered DESC)
_PUB_PAGE = 24  # publisher cards per page (multiple of 3 for the grid)
_LINE_PAGE = 25  # payment-line rows per page on a drill-down
_Q_PAGE = 8  # quarter sections per page on the supplier drill-down


# ──────────────────────────────────────────────────────────────────────────────
# Formatting helpers (mirrors procurement.py — possibly-NA safe)
# ──────────────────────────────────────────────────────────────────────────────
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


def _eur_scale(val) -> str:
    """Headline scale label allowing billions: €6.4bn / €4.2m / €0 ."""
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


def _n(val) -> int:
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


def _coalesce(*vals) -> str:
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


def _lines_word(n: int) -> str:
    return f"{n:,} line{'s' if n != 1 else ''}"


def _semantics_label(sem: str) -> str:
    """Plain-English value kind: po_committed -> 'ordered', payment_actual -> 'paid'."""
    return {"po_committed": "ordered", "payment_actual": "paid"}.get(_coalesce(sem), "value")


def _publisher_href(publisher_id) -> str:
    return f"?publisher={urllib.parse.quote(str(publisher_id))}"


def _supplier_href(supplier_norm) -> str:
    return f"?supplier={urllib.parse.quote(str(supplier_norm))}"


def _sort_toggle(key: str) -> str:
    """'Most records / Highest value' segmented control. Returns the order_by key the core
    query understands ('lines' | 'value'). Record count is the neutral default; the value lens
    is sum-safe value only."""
    labels = {"Most records": "lines", "Highest value": "value"}
    choice = st.segmented_control(
        "Rank by", list(labels), default="Most records", key=key, label_visibility="collapsed"
    )
    return labels.get(choice or "Most records", "lines")


# ──────────────────────────────────────────────────────────────────────────────
# Card builders (CSS reuses the procurement pr-* family)
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


def _value_pill(val, sem: str) -> str:
    """Sum-safe value pill labelled by semantics ('ordered'/'paid'); omitted when no
    summable value so the card shows the trustworthy record count instead of '—'."""
    if _eur(val) == "—":
        return ""
    return f'<span class="pr-pill pr-pill-val">{_eur(val)} {_semantics_label(sem)}</span>'


def _class_pill(supplier_class) -> str:
    if _coalesce(supplier_class) == "public_body":
        return '<span class="pr-pill pr-pill-cro">public body — transfer, not summed</span>'
    return ""


# ──────────────────────────────────────────────────────────────────────────────
# Tab: Publishers (ranked; each card drills down to that body's payment lines)
# ──────────────────────────────────────────────────────────────────────────────
def _render_publishers() -> None:
    order = _sort_toggle("pp_pub_sort")
    res = fetch_publisher_summary_result(order_by=order)
    df = res.data if res.ok else pd.DataFrame()
    if not res.ok:
        empty_state("Publisher data isn't available right now", "A source/pipeline issue, not an empty result.")
        return
    if df.empty:
        empty_state("No publishers", "No public body has payment records yet.")
        return
    by = "sum-safe value" if order == "value" else "number of records"
    st.caption(
        f"{len(df):,} publishers ranked by {by}. Value is purchase-order commitments ('ordered') "
        "or actual payments ('paid') — shown per body, never blended. Click a body for its lines."
    )
    cards = []
    for i, r in enumerate(df.head(_TOP).itertuples(), start=1):
        span = f"{_n(r.first_year)}–{_n(r.last_year)}" if _n(r.first_year) else "—"
        meta = (
            f"{_lines_word(_n(r.n_lines))} · {_n(r.n_suppliers):,} supplier"
            f"{'s' if _n(r.n_suppliers) != 1 else ''} · {span}"
        )
        pills = [p for p in (_value_pill(r.total_safe_eur, r.amount_semantics),) if p]
        sector = _coalesce(getattr(r, "sector", None))
        if sector:
            pills.append(f'<span class="pr-pill">{_esc(sector)}</span>')
        inner = _card(f"<span>{_esc(r.publisher_name)}</span>", meta, pills, rank=i)
        cards.append(
            clickable_card_link(
                href=_publisher_href(r.publisher_id),
                inner_html=inner,
                aria_label=f"View the payment lines published by {r.publisher_name}",
            )
        )
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')


# ──────────────────────────────────────────────────────────────────────────────
# Tab: Suppliers (search + pagination + clickable drill-down)
# ──────────────────────────────────────────────────────────────────────────────
def _render_suppliers() -> None:
    order = _sort_toggle("pp_sup_sort")
    res = fetch_supplier_summary_result(order_by=order)
    if not res.ok:
        empty_state("Supplier data isn't available right now", "A source/pipeline issue, not an empty result.")
        return
    df = res.data
    if df.empty:
        empty_state("No suppliers", "The views are loaded but returned no rows.")
        return
    ranks = {str(r.supplier_normalised): i for i, r in enumerate(df.itertuples(), start=1)}

    q = st.text_input(
        "Search suppliers",
        placeholder="Search by company / body name…",
        key="pp_sup_q",
        label_visibility="collapsed",
    )
    view = df
    qs = (q or "").strip()
    if qs:
        view = df[text_search_mask(df, qs, ["supplier"])]

    total = len(view)
    by = "sum-safe value" if order == "value" else "number of records"
    st.caption(
        f"{total:,} suppliers"
        + (f' matching "{qs}"' if qs else f" ranked by {by}")
        + ". Personal names are withheld; click a supplier for every body that ordered from or paid it."
    )
    if total == 0:
        empty_state("No suppliers match", "Try a shorter search term.")
        return

    page_idx = paginate(total, key_prefix="pp_sup", page_size=_PUB_PAGE)
    page = view.iloc[page_idx * _PUB_PAGE : (page_idx + 1) * _PUB_PAGE]
    cards = []
    for r in page.itertuples():
        meta = (
            f"{_lines_word(_n(r.n_lines))} · {_n(r.n_publishers):,} publisher{'s' if _n(r.n_publishers) != 1 else ''}"
        )
        # supplier_summary blends ordered+paid across bodies, so the value pill is unlabelled-safe:
        # show the sum-safe euro with a neutral 'sum-safe' label rather than asserting ordered/paid.
        pills = []
        if _eur(r.total_safe_eur) != "—":
            pills.append(f'<span class="pr-pill pr-pill-val">{_eur(r.total_safe_eur)} sum-safe</span>')
        cls = _class_pill(getattr(r, "supplier_class", None))
        if cls:
            pills.append(cls)
        inner = _card(f"<span>{_esc(r.supplier)}</span>", meta, pills, rank=ranks.get(str(r.supplier_normalised)))
        cards.append(
            clickable_card_link(
                href=_supplier_href(r.supplier_normalised),
                inner_html=inner,
                aria_label=f"View the public-body payment lines for {r.supplier}",
            )
        )
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html('<div style="height:1rem"></div>')
    pagination_controls(
        total, key_prefix="pp_sup", page_sizes=(_PUB_PAGE,), default_page_size=_PUB_PAGE, label="suppliers"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Drill-downs
# ──────────────────────────────────────────────────────────────────────────────
def _line_row_html(r) -> str:
    """One payment/PO line as a row card."""
    head = _esc(_coalesce(getattr(r, "supplier", None))) or "—"
    period = _coalesce(getattr(r, "period", None)) or (_coalesce(getattr(r, "year", None)) or "")
    desc = _esc(_coalesce(getattr(r, "description", None)))
    meta_parts = [p for p in (_esc(period), desc) if p]
    val = _eur(getattr(r, "amount_eur", None))
    sem = _semantics_label(getattr(r, "amount_semantics", None))
    val_html = f'<span class="pr-pill pr-pill-val">{val} {sem}</span>' if val != "—" else ""
    meta = " · ".join(meta_parts) if meta_parts else ""
    # Conduit (S-4): link the line to the actual published source PDF so a reader can
    # verify the detail behind the number. source_file_url is on every line (100%
    # coverage); source_link_html no-ops on a missing/non-http URL so this is safe.
    src = card_sources_html([source_link_html(_coalesce(getattr(r, "source_file_url", None)), "View published source")])
    return (
        f'<div class="pr-card"><div class="pr-card-head"><div class="pr-name">{head}</div></div>'
        f'<div class="pr-meta">{meta}</div>'
        f'<div class="pr-pills">{val_html}</div>{src}</div>'
    )


def _render_line_list(df: pd.DataFrame, *, key: str) -> None:
    total = len(df)
    page_idx = paginate(total, key_prefix=key, page_size=_LINE_PAGE)
    page = df.iloc[page_idx * _LINE_PAGE : (page_idx + 1) * _LINE_PAGE]
    cards = [_line_row_html(r) for r in page.itertuples()]
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html('<div style="height:1rem"></div>')
    pagination_controls(total, key_prefix=key, page_sizes=(_LINE_PAGE,), default_page_size=_LINE_PAGE, label="lines")


def _quarter_header_html(period: str, subtotal, n_lines: int) -> str:
    """Full-width section divider for one quarter: the period on the left, its sum-safe
    subtotal + line count on the right. Inline-styled (a one-off section rule, not a
    reusable card) to keep all the reusable CSS in shared_css.py."""
    sub = _eur(subtotal)
    right = (
        f"{sub} sum-safe · {_lines_word(n_lines)}" if sub != "—" else _lines_word(n_lines)
    )
    return (
        '<div style="display:flex;justify-content:space-between;align-items:baseline;gap:0.75rem;'
        'margin:1.4rem 0 0.6rem;padding-bottom:0.35rem;border-bottom:2px solid #e3ddd1;">'
        f'<span style="font-weight:700;font-size:1.05rem;color:#1a1a1a;">{_esc(period)}</span>'
        f'<span style="font-weight:600;color:#555;white-space:nowrap;">{_esc(right)}</span>'
        "</div>"
    )


def _render_supplier_quarters(lines: pd.DataFrame, quarters: pd.DataFrame, *, key: str) -> None:
    """Render a supplier's lines grouped into quarter sections (newest first), each with a
    pre-computed sum-safe subtotal header. Aggregation already happened in the registered
    quarter-totals query; here we only slice the line frame per period (display-only) and
    paginate over the quarter sections. Falls back to a flat list if the rollup is absent."""
    if quarters is None or quarters.empty:
        _render_line_list(lines, key=key)
        return
    total_q = len(quarters)
    page_idx = paginate(total_q, key_prefix=key, page_size=_Q_PAGE)
    page = quarters.iloc[page_idx * _Q_PAGE : (page_idx + 1) * _Q_PAGE]
    blocks: list[str] = []
    for qr in page.itertuples():
        per = getattr(qr, "period", None)
        period_label = _coalesce(per) or "Period not stated"
        # Display-only slice of the already-fetched lines for this period (null-safe so
        # annual/undated rows still group). No aggregation — the subtotal is from the query.
        if per is None or (isinstance(per, float) and pd.isna(per)):
            qlines = lines[lines["period"].isna()]
        else:
            qlines = lines[lines["period"] == per]
        line_cards = "".join(_line_row_html(r) for r in qlines.itertuples())
        blocks.append(
            _quarter_header_html(period_label, getattr(qr, "total_safe_eur", None), _n(getattr(qr, "n_lines", None)))
            + f'<div class="pr-grid">{line_cards}</div>'
        )
    st.html("".join(blocks))
    st.html('<div style="height:1rem"></div>')
    pagination_controls(total_q, key_prefix=key, page_sizes=(_Q_PAGE,), default_page_size=_Q_PAGE, label="quarters")


def _render_publisher_profile(publisher_id: str) -> None:
    back_button("← All public-body payments", "?")
    res = fetch_publisher_lines_result(publisher_id, order_by="value", limit=2000)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        empty_state("No lines", "This publisher has no payment lines, or the source is unavailable.")
        return
    name = _esc(_coalesce(df.iloc[0].get("publisher_name"))) or _esc(publisher_id)
    sector = _esc(_coalesce(df.iloc[0].get("sector")))
    safe = df[df["value_safe_to_sum"] == True] if "value_safe_to_sum" in df.columns else df  # noqa: E712
    safe_total = _eur_scale(safe["amount_eur"].sum()) if not safe.empty else "—"
    hero_banner(
        kicker="PUBLIC BODY" + (f" · {sector.upper()}" if sector else ""),
        title=name,
        dek=f"{len(df):,} purchase-order / payment lines over €20,000. "
        f"{safe_total} in sum-safe value (excludes transfers to other public bodies).",
    )

    # Council dossiers gain the audited-accounts breakdown — where the
    # council's whole operating spend goes, by service division (Housing,
    # Roads, Environment…). BUDGET grain, pre-aggregated in the AFS views —
    # a SIBLING fact to the per-line register below, never summed with it.
    # Shared renderer with the Procurement council dossier; it silently
    # no-ops for a body whose audited AFS isn't in the fact.
    if sector == "local_government":
        sems = df["amount_semantics"].dropna().unique().tolist() if "amount_semantics" in df.columns else []
        afs_tier = "SPENT" if "payment_actual" in sems else "COMMITTED"
        render_council_accounts_context(str(_coalesce(df.iloc[0].get("publisher_name")) or publisher_id), afs_tier)

    st.caption("Lines shown highest-value first. Value is the body's reported amount per line — 'ordered' or 'paid'.")
    _render_line_list(df, key="pp_pub_lines")
    _provenance_footer()


def _render_supplier_profile(supplier_norm: str) -> None:
    back_button("← All public-body payments", "?")
    res = fetch_supplier_lines_result(supplier_norm, order_by="recent", limit=2000)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        empty_state("No lines", "This supplier has no payment lines, or the source is unavailable.")
        return
    name = _esc(_coalesce(df.iloc[0].get("supplier"))) or _esc(supplier_norm)
    n_pub = df["publisher_id"].nunique() if "publisher_id" in df.columns else 0
    safe = df[df["value_safe_to_sum"] == True] if "value_safe_to_sum" in df.columns else df  # noqa: E712
    safe_total = _eur_scale(safe["amount_eur"].sum()) if not safe.empty else "—"
    hero_banner(
        kicker="SUPPLIER",
        title=name,
        dek=f"{len(df):,} payment / purchase-order lines from {n_pub:,} public "
        f"bod{'ies' if n_pub != 1 else 'y'}. {safe_total} in sum-safe value.",
    )
    # Contextual edge into the canonical company dossier — the same firm's
    # eTenders/TED awards, lobbying co-occurrence and CRO status, which this
    # payments-only view doesn't carry. Closes the Public-Payments → Company
    # cul-de-sac (the supplier_norm param resolves directly on /company). The
    # dossier degrades gracefully for a body with no award/CRO footprint.
    st.html(
        '<div style="margin:-0.25rem 0 1rem">'
        + entity_cta_html(
            company_profile_url(supplier_norm),
            "View full company dossier — awards, lobbying & CRO →",
        )
        + "</div>"
    )
    st.caption(
        "Grouped by quarter, newest first; each quarter shows its sum-safe subtotal, with the "
        "larger figures leading. A line is a purchase order or payment record, not evidence of "
        "influence or wrongdoing."
    )
    qres = fetch_supplier_quarter_totals_result(supplier_norm)
    quarters = qres.data if qres.ok else pd.DataFrame()
    _render_supplier_quarters(df, quarters, key="pp_sup_lines")
    _provenance_footer()


# ──────────────────────────────────────────────────────────────────────────────
def _stats_strip(stats, cov: dict) -> None:
    """Opening findings lede — the canonical stat-strip replacement
    (finding_lede; doc/archive/APP_REDESIGN_SWEEP_2026_06_10.md S-1, work order #3).
    Top publisher + register scale + the withheld-names transparency count, all
    from registered views; ordered commitments and actual payments are never
    blended. Display-only — renders pre-computed figures, never derives."""
    safe_total = _eur_scale(stats.get("total_safe_eur"))
    first_y, last_y = _n(stats.get("first_year")), _n(stats.get("last_year"))
    span = f"{first_y}–{last_y}" if first_y and last_y else "recent years"
    n_pub = _n(stats.get("n_publishers"))
    n_sup = _n(stats.get("n_suppliers"))
    n_lines = _n(stats.get("n_lines"))
    # Withheld personal-data counts from both registers' coverage JSONs (transparency).
    withheld = _n((cov.get("public_payments") or {}).get("rows_quarantined")) + _n(
        (cov.get("hse_tusla_payments") or {}).get("rows_quarantined")
    )
    sentences: list[str] = []
    top_pub = fetch_publisher_summary_result(order_by="value")
    if top_pub.ok and not top_pub.data.empty:
        p = top_pub.data.iloc[0]
        sentences.append(
            f"<strong>{_esc(str(p['publisher_name']))}</strong> has published the largest "
            f"sum-safe total, <strong>{_eur_scale(p['total_safe_eur'])}</strong>."
        )
    sentences.append(
        f"<strong>{n_pub:,}</strong> public bodies have published purchase orders and payments "
        f"over €20,000, naming <strong>{n_sup:,}</strong> suppliers across "
        f"<strong>{n_lines:,}</strong> records, {_esc(span)}. Adding up only the lines that are "
        f"safe to total comes to <strong>{_esc(safe_total)}</strong> — see how that's counted "
        "just below."
    )
    if withheld:
        sentences.append(f"<strong>{withheld:,}</strong> likely-personal supplier names are withheld.")
    finding_lede(sentences)
    _how_counted(safe_total)


def _how_counted(safe_total: str) -> None:
    """Plain-language build-down for the headline total — collapsed by default so it
    explains how the figure is reached without crowding the lede. Deliberately NOT a
    number-waterfall: the 'everything published' gross would blend ordered commitments
    with actual payments (a sum the app never shows), so the steps are qualitative."""
    with st.expander("How is this total counted?", expanded=False):
        st.markdown(
            "Start with **every purchase order and payment** a public body published "
            "over €20,000.\n\n"
            "Then we add up **only the lines that are safe to total**, leaving out:\n\n"
            "- **Money moving between public bodies** (a department's grant to a council, "
            "say) — counting it would tally the same euro twice as it passes down the chain.\n"
            "- **Blank, subtotal and placeholder lines** — page headings or running totals a "
            "document scan picked up, with no real named supplier behind them.\n\n"
            "We also **never mix _ordered_ with _paid_**: a purchase-order commitment and an "
            "actual payment are different stages, so they're never added together.\n\n"
            f"What's left, added up, is the **{safe_total}** shown above."
        )


def _provenance_footer() -> None:
    st.html(
        '<div class="pr-foot"><strong>Source:</strong> purchase-order / payment disclosures over '
        "€20,000 published by individual public bodies under Circular 07/2012, plus the HSE and "
        "Tusla FOI model-publication PDFs. Two registers unioned here; figures are the bodies' own "
        "reported amounts (purchase-order commitments or actual payments), never added to eTenders / "
        "TED award values. Likely-personal suppliers (sole traders / individuals) are withheld. "
        "A line is a procurement record, not evidence of influence or wrongdoing.</div>"
    )


# ──────────────────────────────────────────────────────────────────────────────
# "What the money buys" — spend-category lens (doc/PAYMENTS_CATEGORY_LENS_DESIGN.md).
# Browse public-body spend by the bodies' OWN published purpose → drill to the named
# vendors → out to each vendor's Company dossier. Tussell-style profiles over
# imperfect aggregate data: coverage honesty is a first-class element, never a footer.
# ──────────────────────────────────────────────────────────────────────────────
_TIER_LABEL = {"SPENT": "paid", "COMMITTED": "ordered"}
_UNCAT = "Uncategorised"


def _tier_label(tier) -> str:
    return _TIER_LABEL.get(_coalesce(tier).upper(), "value")


def _category_href(category) -> str:
    return f"?category={urllib.parse.quote(str(category))}"


def _is_uncat(category) -> bool:
    return _coalesce(category).lower().startswith("uncategor")


def _cat_tier_picker(df: pd.DataFrame, key: str) -> str:
    """Tier selector for the category lens. Returns the realisation_tier code ('SPENT' /
    'COMMITTED'); defaults to whichever tier carries the most sum-safe value so the busiest
    lens shows first. One tier at a time — paid and ordered are never blended into one bar."""
    tiers = [t for t in ("SPENT", "COMMITTED") if t in set(df["realisation_tier"])]
    if len(tiers) < 2:
        return tiers[0] if tiers else "SPENT"
    # Default to 'paid' (the concrete tier) when present — no aggregation in the page; the
    # view is already ordered by value so neither tier is privileged by row order.
    default_code = "SPENT" if "SPENT" in tiers else tiers[0]
    labels = {"Paid (actual)": "SPENT", "Ordered (committed)": "COMMITTED"}
    default_label = next(k for k, v in labels.items() if v == default_code)
    choice = st.segmented_control(
        "Tier", list(labels), default=default_label, key=key, label_visibility="collapsed"
    )
    return labels.get(choice or default_label, default_code)


def _cat_bar_card_html(r, max_val: float, rank: int) -> str:
    """A ranked category row: name + a width-scaled bar + the sum-safe figure & counts.
    Bar width is display scaling against the lens's own largest category (no aggregation).
    'Uncategorised' is flagged ⚠ and shown plainly — never hidden (it is ~15% of lines)."""
    val = getattr(r, "total_safe_eur", None)
    try:
        pct = max(2.0, 100.0 * float(val) / max_val) if max_val and float(val) > 0 else 0.0
    except (TypeError, ValueError):
        pct = 0.0
    uncat = _is_uncat(r.spend_category)
    name = _esc(r.spend_category) + (' <span title="purpose not published">⚠</span>' if uncat else "")
    bodies = f"{_n(r.n_bodies):,} bod{'y' if _n(r.n_bodies) == 1 else 'ies'}"
    vendors = f"{_n(r.n_suppliers):,} vendor{'s' if _n(r.n_suppliers) != 1 else ''}"
    meta = f"{_lines_word(_n(r.n_lines))} · {bodies} · {vendors}"
    accent = "#9a8f78" if uncat else "#3a6b7e"
    return (
        '<div class="pr-afsbar">'
        f'<div class="pr-afsbar-top"><span class="pr-afsbar-label"><span class="pr-rank">#{rank}</span> {name}</span>'
        f'<span class="pr-afsbar-fig"><strong>{_eur_scale(val)}</strong> {_tier_label(r.realisation_tier)}</span></div>'
        f'<div class="pr-afsbar-track"><div class="pr-afsbar-fill" style="width:{pct:.1f}%;background:{accent}"></div></div>'
        f'<div class="pr-afsbar-note">{_esc(meta)}</div>'
        "</div>"
    )


def _render_categories() -> None:
    """The 'What the money buys' tab: coverage honesty first, then a ranked-bar overview of
    spend by the publishing bodies' own purpose categories, each linking to its profile."""
    res = fetch_categories_result()
    if not res.ok:
        empty_state("Category data isn't available right now", "A source/pipeline issue, not an empty result.")
        return
    df = res.data
    if df.empty:
        empty_state("No categories", "The lens view loaded but returned no rows.")
        return

    st.html(
        '<div class="pr-caveat"><strong>What this shows — and doesn\'t.</strong> Over-€20,000 purchase '
        "orders &amp; payments that public bodies have <em>published</em>, grouped by the purpose each body "
        "wrote on the line. Not “all public spending”: it leaves out sub-€20k, bodies that don’t publish, and "
        "withheld personal-name rows. Lines without a stated purpose show as <strong>Uncategorised</strong> — "
        "never hidden. Categories are the bodies’ <strong>own words</strong> (de-noised), not a standard taxonomy.</div>"
    )

    tier = _cat_tier_picker(df, "pp_cat_tier")
    view = df[df["realisation_tier"] == tier].sort_values("total_safe_eur", ascending=False)
    if view.empty:
        empty_state("Nothing in this tier", "Try the other tier (paid / ordered).")
        return
    verb = _tier_label(tier)
    total_cats = len(view)
    shown = view.head(_TOP)
    st.caption(
        f"The {len(shown)} largest of {total_cats:,} published purpose labels ({verb}) — bar width = "
        "sum-safe value within this tier. Click a category for its vendors. Categories are the bodies’ "
        "own free-text purpose, so the long tail is many small one-off labels."
    )

    max_val = float(pd.to_numeric(shown["total_safe_eur"], errors="coerce").max() or 0.0)
    cards = []
    for i, r in enumerate(shown.itertuples(), start=1):
        inner = _cat_bar_card_html(r, max_val, i)
        cards.append(
            clickable_card_link(
                href=_category_href(r.spend_category),
                inner_html=inner,
                aria_label=f"View the vendors {verb} under {r.spend_category}",
            )
        )
    st.html(f'<div class="pr-afsbars pp-cat-bars">{"".join(cards)}</div>')
    if total_cats > len(shown):
        st.caption(
            f"+ {total_cats - len(shown):,} smaller purpose labels not shown — most are one-off "
            "descriptions a single body used a handful of times."
        )


def _render_category_profile(category: str) -> None:
    """Drill-down for one spend category: tier totals (never blended), the bodies that drove it,
    and the named vendors paid/ordered — each linking to its Company dossier. Vendors are shown
    as published (CRO surfaced but NOT operator-merged)."""
    back_button("← What the money buys", "?")
    cats = fetch_categories_result()
    rows = cats.data[cats.data["spend_category"] == category] if cats.ok else pd.DataFrame()
    if rows.empty:
        empty_state("Category not found", "It may have been renamed, or the link is stale.")
        return

    span_lo = _n(rows["first_year"].min())
    span_hi = _n(rows["last_year"].max())
    span = f"{span_lo}–{span_hi}" if span_lo and span_hi and span_lo != span_hi else str(span_hi or span_lo or "")
    uncat = _is_uncat(category)
    st.html(
        f'<div class="pr-prof-head"><div class="pr-prof-kicker">WHAT THE MONEY BUYS</div>'
        f'<h1 class="pr-prof-name">{_esc(category)}</h1>'
        f'<div class="pr-prof-sub">{_esc(span)} · the publishing bodies’ own purpose words</div></div>'
    )
    if uncat:
        st.html(
            '<div class="pr-caveat">These are published lines whose <strong>purpose text was blank, '
            "truncated or a placeholder</strong> — real spend, but not attributable to a service here.</div>"
        )

    # Tier totals — one pill per tier present, NEVER blended into one figure.
    pills = []
    for tr in rows.sort_values("total_safe_eur", ascending=False).itertuples():
        if _eur(tr.total_safe_eur) == "—":
            continue
        pills.append(
            f'<span class="pr-pill pr-pill-val">{_eur_scale(tr.total_safe_eur)} {_tier_label(tr.realisation_tier)}'
            f' · {_lines_word(_n(tr.n_lines))}</span>'
        )
    if pills:
        st.html(f'<div class="pr-pills" style="margin:0.2rem 0 0.8rem">{"".join(pills)}</div>')

    # Which bodies drove it (attribution) — small ranked list.
    pubs = fetch_category_publishers_result(category)
    if pubs.ok and not pubs.data.empty:
        pv = pubs.data.sort_values("total_safe_eur", ascending=False).head(6)
        chips = []
        for pr in pv.itertuples():
            href = _publisher_href(pr.publisher_id)
            chips.append(
                f'<a class="pp-cat-chip" href="{href}" target="_self">{_esc(pr.publisher_name)} '
                f"· {_eur_scale(pr.total_safe_eur)}</a>"
            )
        st.caption("Published by")
        st.html(f'<div class="pp-cat-chips">{"".join(chips)}</div>')

    # Named vendors paid/ordered within the category (the drill) — cards link to Company dossier.
    sup = fetch_category_suppliers_result(category, limit=_TOP)
    if sup.ok and not sup.data.empty:
        sv = sup.data.sort_values("total_safe_eur", ascending=False)
        st.caption("Vendors paid or ordered under this purpose — shown as published, not merged")
        cards = []
        for i, r in enumerate(sv.head(_TOP).itertuples(), start=1):
            cro = _coalesce(getattr(r, "cro_company_num", None))
            meta_bits = [f"{_lines_word(_n(r.n_lines))} · {_n(r.n_bodies):,} bod{'y' if _n(r.n_bodies) == 1 else 'ies'}"]
            if cro:
                meta_bits.append(f"CRO {cro}")
            tier_pill = (
                f'<span class="pr-pill pr-pill-val">{_eur_scale(r.total_safe_eur)} {_tier_label(r.realisation_tier)}</span>'
                if _eur(r.total_safe_eur) != "—"
                else ""
            )
            inner = _card(f"<span>{_esc(r.supplier)}</span>", " · ".join(meta_bits), [tier_pill] if tier_pill else [], rank=i)
            cards.append(
                clickable_card_link(
                    href=company_profile_url(r.supplier_normalised),
                    inner_html=inner,
                    aria_label=f"View the cross-register dossier for {r.supplier}",
                )
            )
        st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
        st.html(
            '<div class="pr-foot">Vendors are shown <strong>as published — not operator-merged</strong>: '
            "“Mosney” and “Mosney Holidays” can appear separately and we don’t assert they’re one company. "
            "The CRO number (where matched) is surfaced for optional roll-up. This is paid/ordered history — "
            "<strong>not a contract price or a “going rate”</strong>; for what’s tendered &amp; awarded, see the "
            '<a class="dt-source-link" href="/rankings-procurement" target="_self">eTenders contracts register →</a>.</div>'
        )


# ──────────────────────────────────────────────────────────────────────────────
def public_payments_page() -> None:
    hide_sidebar()

    params = st.query_params
    if params.get("publisher"):
        _render_publisher_profile(params.get("publisher"))
        return
    if params.get("supplier"):
        _render_supplier_profile(params.get("supplier"))
        return
    if params.get("category"):
        _render_category_profile(params.get("category"))
        return

    stats_res = fetch_coverage_stats_result()
    if not stats_res.ok:
        hero_banner(
            kicker="PUBLIC MONEY",
            title="Public-Body Payments",
            dek="Purchase orders and payments over €20,000 published by public bodies.",
        )
        empty_state(
            "Public-body payment data isn't available right now",
            "The views couldn't be loaded — the gold parquet may be missing or a view failed to "
            "register. This is a source/pipeline issue, not an empty result.",
        )
        return

    stats = stats_res.data.iloc[0]
    cov = fetch_coverage()

    hero_banner(
        kicker="PUBLIC MONEY",
        title="Public-Body Payments",
        dek="Purchase orders and payments over €20,000 published by government departments, "
        "semi-states, health and education bodies — including the HSE and Tusla. Who they "
        "ordered from or paid, and how much.",
    )
    st.html(
        '<div class="pr-caveat"><strong>Ordered or paid — not a single "spend" figure.</strong> '
        "Each line is a purchase-order commitment (<em>ordered</em>) or an actual payment "
        "(<em>paid</em>) a public body published itself. Totals only ever add up the "
        "<em>sum-safe</em> value (explained below the headline). These figures are a "
        "different register from eTenders / TED contract awards and are <strong>never added to "
        "them</strong>. A line is a procurement record, not evidence of influence or wrongdoing.</div>"
    )
    _stats_strip(stats, cov)
    # National-scale anchor from the (previously orphaned) gov-finance series — a denominator
    # to eyeball these figures against, explicitly NOT a share they sum into (different bases).
    render_national_finance_context(
        note="The published payments below are one slice of this, not the whole."
    )
    glossary_strip(
        [
            ("Ordered", "a purchase-order commitment — money the body committed to spend, not yet paid"),
            ("Paid", "an actual payment made — money that left the body"),
            ("Sum-safe value", "only the rows safe to total: excludes transfers to other public bodies"),
            ("Over €20,000", "the publication threshold set by Circular 07/2012"),
        ]
    )

    if _n(stats.get("n_publishers")) == 0:
        empty_state("No payment records", "The views are loaded but returned no rows.")
        return

    yrs = fetch_available_years_result()
    if yrs.ok and not yrs.data.empty:
        years = [_n(y) for y in yrs.data["year"].tolist() if _n(y)]
        if years:
            st.caption(f"Covering {min(years)}–{max(years)}. Rankings are all-time; click any row for its lines.")

    tabs = st.tabs(["Public bodies", "Suppliers", "What the money buys"])
    with tabs[0]:
        _render_publishers()
    with tabs[1]:
        _render_suppliers()
    with tabs[2]:
        _render_categories()

    _provenance_footer()
