"""Your Council — ONE consolidated dossier per local authority (Phase 1 of the council-pages
consolidation; see the design sketch in chat / project_council_spending_rebuild memory).

The council is the spine. Three concerns that used to be three separate pages — "Who Runs Your
County" (the appointed Chief Executive + accountability indicators), "Your Councillors" (the elected
side, sandbox/preview data) and "Council Spending" (audited accounts + purchase orders) — are
recomposed here into ONE index → ONE dossier with a section switcher.

SURFACING-ONLY / NO NEW LOGIC. This page imports and orchestrates the existing, already-tested
render functions from local_government.py and procurement.py (the same pattern council_spending.py
already uses). It computes no metric of its own. Phase 1 fully embeds the two GOLD concerns
(Who runs it · Spending); the SANDBOX councillor flow is cross-linked, not deep-embedded, until that
data is promoted (then it becomes a third inline section — Phase 2).

Key alignment that makes this possible: la_chief_executives.local_authority == la_afs_divisions.council
== payments publisher_name (plain names, e.g. "Dublin City", "Dun Laoghaire-Rathdown"). One ?council=
param keys all three.
"""

from __future__ import annotations

import sys
from html import escape as _h
from pathlib import Path
from urllib.parse import quote

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.local_government_data import (
    fetch_chief_executive_result,
    fetch_chief_executives_result,
)
from pages_code.local_government import (
    _render_ce_hero,
    _render_performance,
    _render_power_explainer,
)
from pages_code.procurement import (
    _council_summary_row,
    _render_payment_lines,
    _render_payments_publisher_profile,
    _render_payments_supplier_profile,
)
from ui.components import (
    back_button,
    clickable_card_link,
    empty_state,
    hero_banner,
    hide_sidebar,
    info_card,
    subsection_heading,
)

_SECTIONS = ["Who runs it", "Your councillors", "Spending"]


# ── routing helpers ───────────────────────────────────────────────────────────
def _go(council: str | None = None, *, section: str | None = None) -> None:
    """Navigate to the council hub (or the index when ``council`` is None), optionally landing on a
    section. Clears drill keys so a leaf's Back returns to clean hub state."""
    st.query_params.clear()
    if council:
        st.query_params["council"] = council
    if section:
        st.session_state["yc_section"] = section
    st.rerun()


def _tier_from(params) -> str:
    t = (params.get("paid_tier") or "COMMITTED").upper()
    return t if t in ("SPENT", "COMMITTED") else "COMMITTED"


# ── index (the directory) ─────────────────────────────────────────────────────
def _render_index() -> None:
    hero_banner(
        kicker="YOUR AREA",
        title="Your Council",
        dek="Your county or city council in one place — who runs it (the appointed Chief Executive), "
        "the councillors you elect, and what it spends. Pick a council.",
    )
    res = fetch_chief_executives_result()
    if not res.ok or res.data.empty:
        empty_state(
            "Councils aren't available right now",
            "The local-authority roster couldn't be loaded — a source/pipeline issue, not an empty result.",
        )
        return
    df = res.data.sort_values("council_name")
    cards = []
    for r in df.itertuples():
        la = str(r.local_authority)
        ce = _h(str(getattr(r, "chief_executive", "") or "—"))
        title = _h(str(getattr(r, "head_title", "") or "Chief Executive"))
        inner = (
            f'<div class="con-card-inner"><div class="con-card-name">{_h(str(getattr(r, "council_name", la)))}</div>'
            f'<div class="con-card-meta">{title}: <strong>{ce}</strong></div>'
            f'<div class="con-card-sub">Who runs it · councillors · spending</div></div>'
        )
        cards.append(
            clickable_card_link(
                href=f"?council={quote(la)}",
                inner_html=inner,
                aria_label=f"Open {la} council",
            )
        )
    subsection_heading("Every council")
    st.html(f'<div class="con-card-grid">{"".join(cards)}</div>')


# ── the three sections ────────────────────────────────────────────────────────
def _section_who_runs_it(council: str) -> None:
    res = fetch_chief_executive_result(council)
    if not res.ok or res.data.empty:
        empty_state("Not available", f"No Chief Executive record for “{council}”.")
        return
    _render_ce_hero(council, res.data.iloc[0])
    _render_power_explainer(council)
    _render_performance(council)
    st.caption(
        "Performance figures are each council's published whole-area numbers, shown beside the "
        "national benchmark — not apportioned, never summed across measures. Sources: NOAC "
        "Performance Indicator Report · An Bord Pleanála · Dept of Housing Derelict Sites return."
    )


def _section_councillors(council: str) -> None:
    # Cross-link, not deep-embed: councillor data is sandbox/preview (roster ~96%, named votes only
    # where a council records roll-calls). Once promoted it becomes a third inline section (Phase 2).
    subsection_heading("The councillors you elect")
    info_card(
        "Councillors hold the <b>reserved functions</b> — the county development plan, the annual "
        "budget and the rates — while the appointed Chief Executive holds the executive functions "
        "(staff, contracts, planning permissions). See your full roster, how meetings and agendas "
        "work, and any recorded votes on the dedicated councillor tools.",
        border_left_color="#3a6b7e",
    )
    st.html(
        f'<a class="dt-source-link" href="/your-councillors?clr_county={quote(council)}" target="_self">'
        f"Open councillor tools for {_h(council)} →</a>"
    )
    st.caption(
        "Preview data: the councillor roster is ~96% complete (sourced from public listings); named "
        "votes exist only for councils that record roll-calls. Each section there states its own coverage."
    )


def _section_spending(council: str) -> None:
    if _council_summary_row(council) is None:
        empty_state(
            "No published spending yet",
            f"{council} doesn't publish a machine-readable purchase-order list or audited accounts we "
            "can read yet, so there's nothing to show in this section — not that it has no spending.",
        )
        return
    # The shared per-council dossier (RUNNING / BUILDING / PAYING lanes). Its own Back returns to the
    # council directory. Supplier drill-downs inside it are handled by this page's leaf dispatch.
    _render_payments_publisher_profile(
        council, "COMMITTED", on_back=lambda: _go(), back_label="← All councils"
    )


# ── the hub ───────────────────────────────────────────────────────────────────
def _render_hub(council: str) -> None:
    if back_button("← All councils", key="yc_hub_back"):
        _go()
    summ = _council_summary_row(council)
    province = str(summ.get("province")) if summ and summ.get("province") else ""
    ce_res = fetch_chief_executive_result(council)
    ce_nm = ""
    council_name = council
    if ce_res.ok and not ce_res.data.empty:
        row = ce_res.data.iloc[0]
        ce_nm = str(row.get("chief_executive") or "")
        council_name = str(row.get("council_name") or council)
    dek_bits = []
    if ce_nm:
        dek_bits.append(f"Run day-to-day by {ce_nm} (appointed Chief Executive)")
    if province:
        dek_bits.append(province)
    hero_banner(kicker="YOUR COUNCIL", title=council_name, dek=" · ".join(dek_bits))

    default = st.session_state.get("yc_section", _SECTIONS[0])
    if default not in _SECTIONS:
        default = _SECTIONS[0]
    section = st.segmented_control(
        "Section", _SECTIONS, default=default, key="yc_section", label_visibility="collapsed"
    )
    section = section or default
    if section == "Spending":
        _section_spending(council)
    elif section == "Your councillors":
        _section_councillors(council)
    else:
        _section_who_runs_it(council)


def your_council_page() -> None:
    hide_sidebar()
    p = st.query_params

    # ── spending drill-down LEAVES (leaf-first, mirrors council_spending.py routing order) ──
    # A supplier tile inside a council dossier links with BOTH ?paid_supplier= and ?paid_publisher=
    # (the published line items from this council to this supplier). Check before the supplier-only
    # branch so it isn't shadowed.
    if p.get("paid_supplier") and p.get("paid_publisher"):
        name = p.get("paid_publisher")
        _render_payment_lines(
            p.get("paid_supplier"), name, _tier_from(p),
            on_back=lambda: _go(name, section="Spending"), back_label=f"← Back to {name}",
        )
        return
    if p.get("paid_supplier"):
        _render_payments_supplier_profile(
            p.get("paid_supplier"), _tier_from(p), on_back=lambda: _go(), back_label="← All councils"
        )
        return

    council = p.get("council")
    # Legacy / cross-page deep links land the reader on the hub's Spending section.
    if not council and p.get("paid_publisher"):
        _go(p.get("paid_publisher"), section="Spending")
        return

    if council:
        _render_hub(council)
    else:
        _render_index()
