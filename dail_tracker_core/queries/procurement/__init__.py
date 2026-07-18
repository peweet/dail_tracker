"""Procurement (eTenders/TED/payments/AFS) retrieval — grain-split package.

One module per MONEY GRAIN (never-sum boundaries = module boundaries):
- ``awards`` — eTenders AWARD notices — the award-ceiling grain (never summed with payments/TED)
- ``payments`` — public-body PAYMENTS/PO lines — the SPENT/COMMITTED tiers (realised money; never summed with award ceilings)
- ``ted`` — TED (EU Official Journal) notices — a SEPARATE award register (never-sum with national)
- ``tenders_live`` — national LIVE tender pipeline (etenders.gov.ie) — planned/open competitions, not money
- ``afs`` — per-LA audited Annual Financial Statements + adopted budgets — the BUDGET/OUTTURN grain
- ``signals`` — cross-register + derived competition signals (single-bid, incumbency, dependency, overlaps)

The flat import surface is preserved: every function is re-exported here, so
``from dail_tracker_core.queries import procurement as _q`` keeps working.
"""

from __future__ import annotations

from dail_tracker_core.queries.procurement.afs import (  # noqa: F401
    afs_by_division,
    afs_capital_by_division,
    afs_capital_by_year,
    afs_coverage_by_council,
    afs_national_by_division,
    afs_national_by_year,
    afs_total_by_year,
    afs_vs_po_coverage,
    la_budget_by_division,
    la_budget_vs_actual,
)
from dail_tracker_core.queries.procurement.awards import (  # noqa: F401
    authority_summary,
    available_years,
    awards_by_year,
    awards_for_authority,
    awards_for_cpv,
    awards_for_supplier,
    bid_signal,
    competition,
    coverage_stats,
    cpv_summary,
    cpv_summary_real,
    supplier_concentration,
    supplier_summary,
    supplier_year_trend,
    value_contrast,
)
from dail_tracker_core.queries.procurement.payments import (  # noqa: F401
    _tier,  # noqa: F401
    council_summary,
    entity_chain_for_company,
    payment_group_header,
    payment_group_members,
    payment_lines_for_pair,
    payment_lines_for_supplier,
    payments_by_year,
    payments_corpus_stats,
    payments_for_publisher,
    payments_for_supplier,
    payments_publisher_profile,
    payments_publisher_summary,
    payments_publishers_for_supplier,
    payments_real_by_year,
    payments_real_trend,
    payments_supplier_header,
    payments_supplier_summary,
    supplier_payments_by_year,
)
from dail_tracker_core.queries.procurement.signals import (  # noqa: F401
    call_offs_for_supplier,
    charity_overlap,
    competition_by_cpv,
    dependency_for_supplier,
    dependency_top,
    entity_search,
    epa_compliance_for_supplier,
    epa_supplier_index,
    eu_tam_state_aid,
    eu_tam_state_aid_count,
    incumbency_for_supplier,
    incumbency_top,
    lobbying_overlap,
    new_entrants_by_year,
    quarter_profile_top,
    quarter_totals,
    sector_breadth_top,
    single_bid_baseline,
    single_bid_notices_for_cpv,
    supplier_single_bid,
)
from dail_tracker_core.queries.procurement.ted import (  # noqa: F401
    expiring_contracts,
    expiring_contracts_stats,
    ted_awards_by_year,
    ted_competition_stats,
    ted_corpus_stats,
    ted_for_supplier,
    ted_notices_for_supplier,
    ted_supplier_summary,
    ted_tender_sectors,
    ted_tenders,
    ted_tenders_stats,
)
from dail_tracker_core.queries.procurement.tenders_live import (  # noqa: F401
    expiring_contracts_etenders,
    live_tender_sectors,
    live_tenders,
    live_tenders_stats,
)
