"""Shared procurement constants (ORDER-BY allowlists, column lists).

Values are allow-listed SQL fragments/identifiers chosen by dict KEY — a raw
caller string can never reach the SQL (the injection-safety pattern).
"""

# Display-ordering options exposed to the page. The page never builds SQL — it
# passes one of these keys and the safe ORDER BY fragment is chosen here, so a
# raw string can never reach the query. "awards" is the trustworthy default
# (counts); "value" surfaces the money leaders (sum-safe awarded value only,
# ties broken by award count).
_SUPPLIER_ORDER = {
    "awards": "n_awards DESC",
    "value": "awarded_value_safe_eur DESC, n_awards DESC",
}

_RANK_ORDER = {  # authority + cpv summaries share the same column shape
    "awards": "n_awards DESC",
    "value": "awarded_value_safe_eur DESC, n_awards DESC",
}

_COMPETITION_ORDER = {  # buyer competition ranking
    "single_bid": "single_bid_lot_pct DESC NULLS LAST, n_lots_with_bidcount DESC",
    "lots": "n_lots_with_bidcount DESC",
}

_SUPPLIER_COLS = (
    "supplier, supplier_norm, n_awards, n_authorities, awarded_value_safe_eur,"
    " n_value_safe_awards, n_ceiling_notices,"
    " company_num, company_status, cro_match_method,"
    " on_lobbying_register, lobbying_returns, is_lobbying_registrant, is_lobbying_client"
)

# ---------------------------------------------------------------------------
# Inflation-adjusted (real-terms) lenses — EXPERIMENTAL. The deflation math lives in
# the views (v_procurement_*_real) and services/deflator.py; these are retrieval-only
# pass-throughs that carry the chosen index + caveat columns up to the page. NOTHING is
# computed here — the page must gate consumption behind DAIL_EXPERIMENTAL.
# ---------------------------------------------------------------------------

_PAYMENTS_REAL_TIERS = {"SPENT", "COMMITTED"}  # whitelist — no raw tier string ever reaches SQL

# ── TED (EU Official Journal award notices) — a SEPARATE award register ───────────
# Award grain, never summed with eTenders. pan-EU outliers (GÉANT-type frameworks) are
# excluded from value totals by default; the page's toggle re-includes them.
_TED_ORDER = {
    "awards": "n_awards DESC",
    "value": "ted_value_safe_eur DESC, n_awards DESC",
}

# ── Public-body PAYMENTS (the SPENT / COMMITTED tiers) — a DIFFERENT grain from awards ──
# Never summed with eTenders/TED. One lifecycle tier at a time; only value_safe_to_sum sums,
# never across vat_status. Suppliers named per published source (see the view headers).
_PAYMENT_TIERS = {"SPENT": "SPENT", "COMMITTED": "COMMITTED"}  # whitelist (no raw string in SQL)

