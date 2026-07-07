"""Public-finance resource — CSO general-government revenue / expenditure / balance (GFA01).

The authoritative "share of total public spend" denominator: a national-accounts aggregate,
never summed with transaction-level award or payment registers (caveat rides in ``head``).
"""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends

from api.deps import get_cursor
from dail_tracker_core import dossiers

router = APIRouter(tags=["public-finance"])


@router.get(
    "/public-finance/government-finance",
    summary="General-government revenue/expenditure/balance per year (CSO GFA01)",
)
def government_finance(cur: duckdb.DuckDBPyConnection = Depends(get_cursor)) -> dict:
    return dossiers.government_finance(cur)
