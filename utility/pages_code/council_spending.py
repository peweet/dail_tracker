"""Council Spending — a civic directory of what Ireland's county and city councils spend.

A standalone page (not a Procurement sub-tab) because the per-council dossier carries the
council's audited Annual Financial Statement: its WHOLE operating budget by service
(housing / roads / environment …), which is local-government finance, broader than
procurement. Most council spend never passes through a tendered purchase order.

Surfacing-only: the council index (``_render_councils``) and the per-council dossier
(``_render_payments_publisher_profile``) both live in ``pages_code/procurement.py``,
co-located with their ``v_procurement_*`` views and ``pr-*`` helpers. This page is a thin
shell — it sets the hero and dispatches the ``?paid_publisher=`` drill-down to the shared
dossier renderer (the same per-council profile reachable from Procurement's "Who actually
gets paid?" tab). No modelling here; it renders pre-aggregated rows.
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.freshness_data import freshness_line
from pages_code.procurement import (
    _render_afs_national,
    _render_councils,
    _render_payment_lines,
    _render_payments_publisher_profile,
    _render_payments_supplier_profile,
)
from ui.components import dt_page, hero_banner
from ui.source_pdfs import provenance_expander


@dt_page
def council_spending_page() -> None:
    # Drill-down: a council card sets ?paid_publisher= (+ &paid_tier=). The dossier renderer
    # is shared with Procurement's "Who actually gets paid?" tab — the same per-council
    # profile (suppliers + audited-accounts / AFS context). Councils mostly publish purchase
    # ORDERS, so the default tier is COMMITTED (actual payments are the exception: only a
    # couple of councils publish them).
    params = st.query_params
    # Leaf FIRST: a supplier tile inside a council dossier links with BOTH ?paid_supplier=
    # and ?paid_publisher= — that pair means "show the published line items from this council
    # to this supplier". It must be checked before the publisher-only branch, otherwise the
    # paid_publisher match below shadows it and the click just re-renders the council dossier
    # (mirrors the routing order in procurement.py).
    if params.get("paid_supplier") and params.get("paid_publisher"):
        req_tier = (params.get("paid_tier") or "COMMITTED").upper()
        _render_payment_lines(
            params.get("paid_supplier"),
            params.get("paid_publisher"),
            req_tier if req_tier in ("SPENT", "COMMITTED") else "COMMITTED",
        )
        return
    if params.get("paid_publisher"):
        req_tier = (params.get("paid_tier") or "COMMITTED").upper()
        _render_payments_publisher_profile(
            params.get("paid_publisher"), req_tier if req_tier in ("SPENT", "COMMITTED") else "COMMITTED"
        )
        return
    # A supplier tile inside a council dossier links here: drill into the firm's cross-body
    # footprint (which other public bodies paid it). Same renderer as Procurement's paid-supplier
    # profile; company-class only (the renderer quarantines individuals).
    if params.get("paid_supplier"):
        req_tier = (params.get("paid_tier") or "COMMITTED").upper()
        _render_payments_supplier_profile(
            params.get("paid_supplier"), req_tier if req_tier in ("SPENT", "COMMITTED") else "COMMITTED"
        )
        return

    hero_banner(
        kicker="PUBLIC MONEY",
        title="Council Spending",
        dek="What Ireland's county and city councils spend — the suppliers they pay and "
        "their audited accounts, council by council.",
    )
    # National frame FIRST — the complete audited picture of what all 31 councils spend by
    # service (amalgamated AFS), before the per-council index. A BUDGET grain, never summed
    # with the over-€20k purchase orders the per-council dossiers carry.
    _render_afs_national()
    _render_councils()

    provenance_expander(
        sections=[
            "**What this shows.** Each council's published **purchase orders and payments "
            "over €20,000** — the supplier, the council's own description, PO number and "
            "amount — alongside its audited **Annual Financial Statement** for whole-budget "
            "context (housing, roads, environment …). Most council spend never passes through "
            "a tendered purchase order, so the AFS is the broader picture.",
            "**Validate any figure at source.** Drill into a council, then a supplier, to reach "
            "the individual published lines; each line carries a **source ↗** link to the "
            "council's own published disclosure where it provided one.",
            "**Different grains — never summed.** A line is the body's own reported figure "
            "(a purchase-order *commitment* or an actual *payment*), not an award ceiling, and "
            "is never added across councils with different VAT bases. A record is a procurement "
            "disclosure, not evidence of influence or wrongdoing.",
        ],
        source_caption=(
            "Data: each local authority's over-€20,000 purchase-order / payment lists "
            "(published under Circular 07/2012) and audited Annual Financial Statements. "
            "Councils publish at their own cadence."
        ),
        freshness=freshness_line("procurement"),
    )
