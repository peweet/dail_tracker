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

# Publishability floor for a per-CPV "typical award" band (median + p25–p75).
# A median over 2–3 awards reads as a benchmark but is noise, so the band is only
# a fact worth stating once the category carries this many sum-safe valued awards.
#
# THE ONE DEFINITION. It used to be written four times with two different values —
# `>= 8` twice in the page, `min_valued=1` as the core/data-access default, and
# `ge=1` on the API — so an API caller could request a published median computed
# over a single award. Queries derive `has_reliable_award_band` from this constant
# and consumers render that boolean; nobody re-states the number.
#
# Kept here rather than in the views because three views compute a band
# (v_procurement_cpv_summary, _cpv_year_summary, _cpv_summary_real) and a SQL
# literal in each is the duplication this replaces.
MIN_AWARDS_FOR_BAND = 8

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
}

# ── Public-body PAYMENTS (the SPENT / COMMITTED tiers) — a DIFFERENT grain from awards ──
# Never summed with eTenders/TED. One lifecycle tier at a time; only value_safe_to_sum sums,
# never across vat_status. Suppliers named per published source (see the view headers).
_PAYMENT_TIERS = {"SPENT": "SPENT", "COMMITTED": "COMMITTED"}  # whitelist (no raw string in SQL)
