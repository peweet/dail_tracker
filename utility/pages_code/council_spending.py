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
from pages_code.procurement import _render_councils, _render_payments_publisher_profile
from ui.components import hero_banner, hide_sidebar


def council_spending_page() -> None:
    hide_sidebar()

    # Drill-down: a council card sets ?paid_publisher= (+ &paid_tier=). The dossier renderer
    # is shared with Procurement's "Who actually gets paid?" tab — the same per-council
    # profile (suppliers + audited-accounts / AFS context). Councils mostly publish purchase
    # ORDERS, so the default tier is COMMITTED (actual payments are the exception: only a
    # couple of councils publish them).
    params = st.query_params
    if params.get("paid_publisher"):
        req_tier = (params.get("paid_tier") or "COMMITTED").upper()
        _render_payments_publisher_profile(
            params.get("paid_publisher"), req_tier if req_tier in ("SPENT", "COMMITTED") else "COMMITTED"
        )
        return

    hero_banner(
        kicker="PUBLIC MONEY",
        title="Council Spending",
        dek="What Ireland's county and city councils spend — the suppliers they pay and "
        "their audited accounts, council by council.",
    )
    _render_councils()

    _fresh = freshness_line("procurement")
    if _fresh:
        st.caption(f"{_fresh} Councils publish their over-€20,000 lists at their own cadence.")
