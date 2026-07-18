"""Data-integrity contracts for high-value gold tables that were previously UNGUARDED.

Uses the shared invariant vocabulary (test/contracts/_invariants.py). Runs in the @sql lane
against committed gold. Focus = the money / cross-reference tables whose numbers get quoted,
plus the documented sum-traps that today only have a human-readable warning, not a test.

Every invariant below was verified to currently HOLD on real gold before being asserted.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).parent))
import _invariants as inv  # noqa: E402

pytestmark = pytest.mark.sql

_GOLD = Path("data/gold/parquet")


def _rel(table: str) -> str:
    return f"read_parquet('{(_GOLD / (table + '.parquet')).as_posix()}')"


@pytest.fixture(scope="module")
def conn():
    c = duckdb.connect()
    yield c
    c.close()


# ── procurement_lobbying_overlap — pin the famous dedup-SUM trap ─────────────────────────
# Rows are exploded per lobby-name, so awarded_value_safe_eur REPEATS per supplier. Summing
# rows double-counts (measured: ~€2.0bn naive vs ~€1.34bn honest). The structural guarantee
# that makes the honest read possible is: one award value per supplier.
def test_plo_award_value_functionally_determined_by_supplier(conn):
    inv.functionally_determined(conn, _rel("procurement_lobbying_overlap"), "supplier_norm", "awarded_value_safe_eur")


def test_plo_nonneg_and_side_vocab(conn):
    r = _rel("procurement_lobbying_overlap")
    inv.nonneg(conn, r, "awarded_value_safe_eur", "n_lobby_returns", "n_award_rows", "n_authorities")
    inv.in_vocab(conn, r, "lobby_side", {"client", "registrant"})


def test_plo_row_sum_overcounts_distinct_supplier_sum(conn):
    # documents (and locks) the trap: the naive row-sum MUST exceed the distinct-supplier sum,
    # i.e. duplicate rows exist → never sum awarded_value_safe_eur across rows.
    r = _rel("procurement_lobbying_overlap")
    naive = conn.execute(f"SELECT sum(awarded_value_safe_eur) FROM {r}").fetchone()[0]
    honest = conn.execute(
        f"SELECT sum(v) FROM (SELECT supplier_norm, any_value(awarded_value_safe_eur) v FROM {r} GROUP BY 1)"
    ).fetchone()[0]
    assert honest < naive, "expected duplicated supplier rows (the dedup-sum trap) — has the grain changed?"


# ── charities_enriched ──────────────────────────────────────────────────────────────────
def test_charities_rcn_unique_and_money_nonneg(conn):
    r = _rel("charities_enriched")
    inv.unique_key(conn, r, "rcn")
    inv.nonneg(conn, r, "gross_income_latest_eur", "gross_expenditure_latest_eur", "total_assets_latest_eur")
    inv.no_sentinels(conn, r, "registered_charity_name")


# ── corporate_notices_enriched ──────────────────────────────────────────────────────────
def test_corporate_notices_flags_vocab_and_year(conn):
    r = _rel("corporate_notices_enriched")
    inv.flag_consistent(conn, r, "has_receiver_firm", "receiver_firms IS NOT NULL AND len(receiver_firms) > 0")
    inv.in_vocab(
        conn,
        r,
        "notice_category",
        {"corporate_insolvency", "corporate_notice", "corporate_rescue", "investment_vehicle_register_notice"},
    )
    bad_year = conn.execute(
        f"SELECT count(*) FROM {r} WHERE year IS NOT NULL AND (year < 2010 OR year > 2030)"
    ).fetchone()[0]
    assert bad_year == 0, f"{bad_year} corporate notices with an implausible year"
    inv.no_sentinels(conn, r, "entity_name")


# ── PSA allowance payments (Dáil + Seanad) — the members' expenses money lane ───────────
# payment_kind vocabulary is the classifier's designed output (payments/payments_full_psa_etl.py
# docstring); the (0, 100k] amount window is that ETL's own validity rule — rows outside it
# are diverted to *_quarantine, so one INSIDE gold means the split broke.
PSA_PAYMENT_KINDS = {"TAA", "PSA_DUBLIN", "PRA", "PRA_MIN", "PRA_FLAG_ONLY"}


@pytest.mark.parametrize("table", ["payments_full_psa", "seanad_payments_full_psa"])
def test_psa_payments_kind_vocab_and_amount_window(conn, table):
    r = _rel(table)
    inv.in_vocab(conn, r, "payment_kind", PSA_PAYMENT_KINDS)
    bad = conn.execute(
        f"SELECT count(*) FROM {r} WHERE amount IS NULL OR amount <= 0 OR amount > 100000"
    ).fetchone()[0]
    assert bad == 0, f"{table}: {bad} rows outside the ETL's own (0, 100k] validity window"
    inv.no_sentinels(conn, r, "member_name")


# ── enrichment money facts (EU TAM / ISIF / CBI) — the never-union rail ─────────────────
# These are COMMITTED investments / AWARDED aid / sanction fines in mixed currencies and
# "up to" phrasings: every row is stamped value_safe_to_sum=FALSE by design
# (enrichment_promote_to_gold docstring). A single True row is the drift that lets one of
# them leak into a payment/award SUM. value_kind/realisation_tier are single-value stamps.
def _never_summable(conn, r: str) -> None:
    n = conn.execute(f"SELECT count(*) FROM {r} WHERE value_safe_to_sum").fetchone()[0]
    assert n == 0, f"{r}: {n} rows claim value_safe_to_sum — this fact must NEVER be summable"


def test_eu_tam_state_aid_semantics(conn):
    r = _rel("eu_tam_state_aid")
    inv.in_vocab(conn, r, "value_kind", {"grant_awarded"})
    inv.in_vocab(conn, r, "realisation_tier", {"AWARDED"})
    inv.nonneg(conn, r, "nominal_amount_value", "aid_element_value")
    _never_summable(conn, r)


def test_isif_portfolio_semantics(conn):
    r = _rel("isif_portfolio")
    inv.in_vocab(conn, r, "value_kind", {"investment_commitment"})
    inv.in_vocab(conn, r, "realisation_tier", {"COMMITTED"})
    inv.nonneg(conn, r, "amount_stated")
    _never_summable(conn, r)


def test_cbi_enforcement_semantics(conn):
    r = _rel("cbi_enforcement_actions")
    inv.in_vocab(conn, r, "value_kind", {"sanction_fine"})
    inv.nonneg(conn, r, "fine_amount_eur")
    _never_summable(conn, r)


# ── dceidy_ipas_legacy_spend — asylum accommodation payments (both streams) ─────────────
# The FACT carries both classifier streams (dceidy_ipas_legacy_extract_experimental:143-145);
# the "Ukraine EXCLUDED" rule lives downstream in ipas_promote_to_gold's filtered view.
def test_ipas_legacy_spend_money_and_stream(conn):
    r = _rel("dceidy_ipas_legacy_spend")
    inv.nonneg(conn, r, "amount_eur")
    inv.in_vocab(conn, r, "stream", {"International Protection", "Ukraine"})
    inv.no_sentinels(conn, r, "provider")
    bad_year = conn.execute(
        f"SELECT count(*) FROM {r} WHERE year IS NOT NULL AND (year < 2000 OR year > 2030)"
    ).fetchone()[0]
    assert bad_year == 0, f"{bad_year} IPAS legacy rows with an implausible year"


# ═══════════════════════════════════════════════════════════════════════════════════════
# Tier-B join-drivers — a duplicate key here silently multiplies every LEFT JOIN that reads
# the table, inflating downstream counts/money. The uniqueness assertions ARE the contract.
# ═══════════════════════════════════════════════════════════════════════════════════════

# ── procurement_supplier_cro_match — the supplier→CRO bridge (read by 3+ procurement views)
# supplier_norm is the join key those views use; a duplicate would fan out every supplier row.
def test_supplier_cro_match_key_unique_and_vocab(conn):
    r = _rel("procurement_supplier_cro_match")
    inv.unique_key(conn, r, "supplier_norm")
    inv.in_vocab(conn, r, "match_method", {"exact_unique", "exact_ambiguous", "no_match"})
    inv.nonneg(conn, r, "n_cro")
    # match_confidence is the discrete score behind match_method — a stray value means the
    # scorer changed without this contract. Float column, so compare numerically.
    bad = conn.execute(f"SELECT count(*) FROM {r} WHERE match_confidence NOT IN (0.0, 0.5, 0.9)").fetchone()[0]
    assert bad == 0, f"procurement_supplier_cro_match: {bad} rows with an unknown match_confidence"


def test_supplier_cro_match_method_consistent_with_company_num(conn):
    # The whole point of the table: a 'no_match' must carry no company number, and an
    # 'exact_unique' must carry one. A break here means a supplier joins to the wrong CRO
    # entity (or a real match is dropped) — the failure mode this bridge exists to prevent.
    r = _rel("procurement_supplier_cro_match")
    leaked = conn.execute(f"SELECT count(*) FROM {r} WHERE match_method='no_match' AND company_num IS NOT NULL").fetchone()[0]
    assert leaked == 0, f"{leaked} 'no_match' rows carry a company_num"
    missing = conn.execute(
        f"SELECT count(*) FROM {r} WHERE match_method='exact_unique' AND company_num IS NULL"
    ).fetchone()[0]
    assert missing == 0, f"{missing} 'exact_unique' rows have no company_num"


# ── attendance_by_td_year — per-TD-year participation (read by 3 attendance views) ──────
# member_id+year is the reliable grain; unique_member_code+year is NOT unique (13 collisions
# on real gold, 2026-07-18) so a view joining on the code would double-count — pin member_id.
def test_attendance_by_td_year_grain_and_daycounts(conn):
    r = _rel("attendance_by_td_year")
    # one row per (member_id, year)
    n, d = conn.execute(f"SELECT count(*), count(DISTINCT (member_id || '|' || year)) FROM {r}").fetchone()
    assert n == d, f"attendance_by_td_year: (member_id, year) not unique ({n} rows, {d} distinct)"
    inv.nonneg(conn, r, "sitting_days", "other_days", "total_days")
    split = conn.execute(f"SELECT count(*) FROM {r} WHERE total_days <> sitting_days + other_days").fetchone()[0]
    assert split == 0, f"{split} rows where total_days != sitting_days + other_days"


# ── current_dail_vote_history — the per-member vote fact under the turnout views ─────────
# (vote_id, member) is the grain; a duplicate would double-count a TD's vote in every turnout.
def test_vote_history_grain_and_vocab(conn):
    r = _rel("current_dail_vote_history")
    n, d = conn.execute(
        f"SELECT count(*), count(DISTINCT (vote_id || '|' || unique_member_code)) FROM {r}"
    ).fetchone()
    assert n == d, f"current_dail_vote_history: (vote_id, member) not unique ({n} rows, {d} distinct)"
    inv.in_vocab(conn, r, "vote_type", {"Voted Yes", "Voted No", "Abstained"})
    inv.in_vocab(conn, r, "vote_outcome", {"Carried", "Lost", "_"})


# ── cro_xref_corporate_notices — company↔notice matches (read by corporate_cro_match.sql)
# Deliberately NO uniqueness assertion: (notice_ref, company_num) legitimately repeats
# (14,384 dup rows on real gold) — a notice can name a company more than once. Guard the
# closed classification vocabularies instead; a new value is an unclassified notice.
def test_cro_xref_notice_vocab(conn):
    r = _rel("cro_xref_corporate_notices")
    inv.in_vocab(
        conn, r, "notice_category",
        {"corporate_insolvency", "corporate_notice", "corporate_rescue", "investment_vehicle_register_notice"},
    )
    inv.in_vocab(conn, r, "status_pill_value", {"active", "dead", "in_distress", "other"})
    inv.in_vocab(
        conn, r, "notice_subtype",
        {
            "companies_act_notice", "court_winding_up", "creditors_voluntary_liquidation", "examinership",
            "icav_voluntary_strike_off", "liquidation_unspecified", "members_voluntary_liquidation",
            "receivership", "scarp_process_adviser", "voluntary_liquidation_unspecified",
        },
    )
