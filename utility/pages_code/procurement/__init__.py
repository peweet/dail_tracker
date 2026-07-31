"""Public Procurement — read-only explorer over the registered ``v_procurement_*``
views (eTenders / national procurement open data).

Surfacing only: every aggregation, CRO join and value-gate already lives in the
SQL views; this page reads pre-aggregated rows and renders cards. It does NO
modelling — no value_counts / groupby / merge / parquet reads (the logic firewall
checker scans this file). The supplier search is a display-only name filter over
the already-fetched ranking; pagination is a display-only slice.

Honesty rails (non-negotiable, see doc/archive/REVIEW_SYNTHESIS.md):
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

Package map (mechanical split of the former 4,665-line procurement.py, 2026-07):
  * _shared.py       — import header, constants, card/pill/link helpers.
  * browse.py        — Suppliers / Authorities / Categories / overlap tabs.
  * payments.py      — "Who actually gets paid?" bridge + real-terms trend.
  * councils.py      — per-council budget / running / building lanes.
  * pay_profiles.py  — payments publisher/supplier drill-down profiles.
  * national.py      — national AFS amalgamation + EU State Aid (TAM).
  * ted.py           — EU register (TED) browse + supplier/winner panels.
  * tenders.py       — open-right-now / expiring-contract lenses.
  * profiles.py      — supplier/authority/CPV award-history profiles.
  * patterns.py      — entity search hero, single-bid CPV, bid-signal.
  * page.py          — ``procurement_page`` — the drill-down router.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from shared_css import inject_css  # noqa: F401  (kept parallel to other pages)

from .page import procurement_page

# Re-exports for pages that import procurement internals directly (your_council,
# public_payments, follow_the_money, council_spending, company, body).
from ._shared import (  # noqa: F401
    _awards_word,
    _card,
    _cro_pill,
    _lobby_pill,
    _value_pill,
)
from .councils import (  # noqa: F401
    _council_summary_row,
    _lane_header,
    _render_council_accounts_context,
    _render_council_budget_lane,
    _render_councils,
)
from .national import _render_afs_national  # noqa: F401
from .pay_profiles import (  # noqa: F401
    _PAY_FOOT_HTML,
    _render_paid_supplier_panel,
    _render_payment_lines,
    _render_payments_publisher_profile,
    _render_payments_supplier_profile,
)
from .payments import _paid_pill, _paid_verb, _render_payments  # noqa: F401
from .profiles import _supplier_awards_section  # noqa: F401
from .ted import (  # noqa: F401
    _cro_pill_from,
    _render_epa_credentials_panel,
    _render_supplier_call_offs_panel,
    _render_supplier_competition_panel,
    _render_supplier_relationships_panel,
    _render_ted_supplier_panel,
)

__all__ = ["procurement_page"]
