"""Public Procurement — read-only explorer over the registered ``v_procurement_*``
views (eTenders / national procurement open data).

Surfacing only: every aggregation, CRO join and value-gate already lives in the
SQL views; this page reads pre-aggregated rows and renders cards. It does NO
modelling — no value_counts / groupby / merge / parquet reads (the logic firewall
checker scans this file).

Honesty rails (non-negotiable, see doc/REVIEW_SYNTHESIS.md):
  * "Awarded value, not actual spend" — the page never sums the corpus into a
    headline € figure and only ever shows ``awarded_value_safe_eur`` (the view's
    sum-safe column), per row.
  * Lobbying overlap is co-occurrence disclosure, never causation — copy says
    "appears in both registers", never "influenced" / "won because".
  * Source-state aware: a missing view / parquet shows "data unavailable", not a
    silent empty list (uses the QueryResult ok/unavailable distinction).

Sections: caveat panel → Suppliers / Contracting authorities / Categories /
Lobbying overlap (tabs) → provenance footer.
"""

from __future__ import annotations

import html
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.procurement_data import (
    fetch_authority_summary_result,
    fetch_cpv_summary_result,
    fetch_lobbying_overlap_result,
    fetch_supplier_summary_result,
)
from shared_css import inject_css  # noqa: F401  (kept parallel to other pages)
from ui.components import empty_state, glossary_strip, hero_banner, hide_sidebar

_TOP = 60  # cards shown per tab (views are pre-ordered DESC by award count)


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


# ──────────────────────────────────────────────────────────────────────────────
def _inject_pr_css() -> None:
    st.markdown(
        """
        <style>
        .pr-caveat {
            background: #fffaf2; border: 1px solid #f0e3cf; border-left: 3px solid #c98a2b;
            border-radius: 8px; padding: 0.7rem 0.95rem; margin: 0.4rem 0 0.9rem;
            font-size: 0.86rem; color: #4a3c25; line-height: 1.55; max-width: 64rem;
        }
        .pr-caveat strong { color: #7a5a1e; }
        .pr-grid {
            display: grid; grid-template-columns: repeat(auto-fill, minmax(18rem, 1fr));
            gap: 0.7rem; margin-top: 0.5rem;
        }
        .pr-card {
            background: #ffffff; border: 1px solid #e4e9ec; border-radius: 10px;
            padding: 0.7rem 0.85rem; display: flex; flex-direction: column; gap: 0.35rem;
        }
        .pr-name { font-weight: 650; color: #14232b; font-size: 0.93rem; line-height: 1.3; }
        .pr-meta { font-size: 0.78rem; color: #6b7b83; }
        .pr-pills { display: flex; flex-wrap: wrap; gap: 0.3rem; margin-top: 0.1rem; }
        .pr-pill {
            font-size: 0.72rem; font-weight: 600; padding: 0.08rem 0.5rem; border-radius: 999px;
            background: #eef2f4; color: #33474f;
        }
        .pr-pill-val { background: #e8f0fe; color: #1a4b8f; }
        .pr-pill-cro { background: #e9f6ec; color: #1d6b34; }
        .pr-pill-lob { background: #fdeef0; color: #9a2740; }
        .pr-foot {
            font-size: 0.8rem; color: #5b6b73; line-height: 1.55;
            margin-top: 1.4rem; padding-top: 0.7rem; border-top: 1px solid #e4e9ec; max-width: 64rem;
        }
        .pr-foot a { color: #1a4b8f; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _card(name_html: str, meta: str, pills: list[str]) -> str:
    pills_html = "".join(pills)
    pills_sec = f'<div class="pr-pills">{pills_html}</div>' if pills_html else ""
    return (
        f'<div class="pr-card"><div class="pr-name">{name_html}</div>'
        f'<div class="pr-meta">{_esc(meta)}</div>{pills_sec}</div>'
    )


def _render_suppliers(df: pd.DataFrame) -> None:
    st.caption(
        f"Top {min(_TOP, len(df)):,} suppliers by number of contract awards "
        f"({len(df):,} in the dataset). Value shown is awarded value, not spend."
    )
    cards = []
    for r in df.head(_TOP).itertuples():
        pills = [f'<span class="pr-pill pr-pill-val">{_eur(r.awarded_value_safe_eur)} awarded</span>']
        if getattr(r, "company_num", None):
            status = _esc(getattr(r, "company_status", "") or "matched")
            pills.append(f'<span class="pr-pill pr-pill-cro">CRO: {status}</span>')
        if getattr(r, "on_lobbying_register", False):
            pills.append('<span class="pr-pill pr-pill-lob">also on lobbying register</span>')
        meta = (f"{_n(r.n_awards):,} award{'s' if _n(r.n_awards) != 1 else ''} · "
                f"{_n(r.n_authorities):,} authorit{'ies' if _n(r.n_authorities) != 1 else 'y'}")
        cards.append(_card(f'<span>{_esc(r.supplier)}</span>', meta, pills))
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')


def _render_authorities(df: pd.DataFrame) -> None:
    if df.empty:
        empty_state("No contracting authorities", "The authority view returned no rows.")
        return
    st.caption(f"Top {min(_TOP, len(df)):,} contracting authorities by number of awards.")
    cards = []
    for r in df.head(_TOP).itertuples():
        pills = [f'<span class="pr-pill pr-pill-val">{_eur(r.awarded_value_safe_eur)} awarded</span>']
        meta = (f"{_n(r.n_awards):,} award{'s' if _n(r.n_awards) != 1 else ''} · "
                f"{_n(r.n_suppliers):,} supplier{'s' if _n(r.n_suppliers) != 1 else ''}")
        cards.append(_card(f'<span>{_esc(r.contracting_authority)}</span>', meta, pills))
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')


def _render_cpv(df: pd.DataFrame) -> None:
    if df.empty:
        empty_state("No categories", "The CPV category view returned no rows.")
        return
    st.caption(f"Top {min(_TOP, len(df)):,} procurement categories (CPV) by number of awards.")
    cards = []
    for r in df.head(_TOP).itertuples():
        title = _esc(r.cpv_description) or _esc(r.cpv_code) or "—"
        pills = [f'<span class="pr-pill pr-pill-val">{_eur(r.awarded_value_safe_eur)} awarded</span>']
        meta = (f"CPV {_esc(r.cpv_code)} · {_n(r.n_awards):,} award{'s' if _n(r.n_awards) != 1 else ''} · "
                f"{_n(r.n_suppliers):,} supplier{'s' if _n(r.n_suppliers) != 1 else ''}")
        cards.append(_card(f"<span>{title}</span>", meta, pills))
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')


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
    for r in df.head(_TOP).itertuples():
        name = _esc(getattr(r, "supplier", "") or getattr(r, "lobby_name", "")) or "—"
        pills = [
            f'<span class="pr-pill pr-pill-val">{_eur(r.awarded_value_safe_eur)} awarded</span>',
            f'<span class="pr-pill pr-pill-lob">{_n(r.n_lobby_returns):,} lobbying returns</span>',
        ]
        meta = (f"{_n(r.n_award_rows):,} award row{'s' if _n(r.n_award_rows) != 1 else ''} · "
                f"appears in both registers")
        cards.append(_card(f"<span>{name}</span>", meta, pills))
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')


# ──────────────────────────────────────────────────────────────────────────────
def procurement_page() -> None:
    _inject_pr_css()
    hide_sidebar()

    sup = fetch_supplier_summary_result(limit=400)
    auth = fetch_authority_summary_result(limit=_TOP)
    cpv = fetch_cpv_summary_result(limit=_TOP)
    overlap = fetch_lobbying_overlap_result()

    # Source-state: a missing view / parquet / DuckDB error is NOT "no results".
    if not sup.ok:
        hero_banner(kicker="PUBLIC MONEY", title="Public Procurement",
                    dek="Contract awards published on eTenders / national procurement open data.")
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
    glossary_strip([
        ("Award value", "the contract value at the point of award — not money actually paid out"),
        ("Framework / DPS", "an agreement a buyer may draw down against — the ceiling is not a payment"),
        ("CPV", "Common Procurement Vocabulary — the EU category code for what was bought"),
        ("CRO", "Companies Registration Office — a matched company registration number"),
    ])

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
