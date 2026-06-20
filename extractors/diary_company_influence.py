"""Company-influence cross-reference: ministerial ACCESS x public MONEY x lobbying.

The single highest-value join in the corpus (validated 2026-06-20): of the companies that
appear in ministers' published diaries, which also won public contracts, were paid public
money, and lobbied the register — in one table. Anchored on the diary org universe (a company
that never met a minister is out of scope here; the procurement tools cover those).

Sources (all gold):
  ministerial_diary_org_overlap   diary side — meetings, ministers_met, lobbying returns, corroboration
  procurement_awards              awards won (supplier_class='company', value_safe_to_sum)
  procurement_payments_fact       public payments received (public_display, value_safe_to_sum)

MATCHING (transparent + guarded, NOT modelling): a deterministic name FOLD (lower, strip legal
suffixes/ Ireland/ group, collapse spaces) applied to BOTH sides, exact-equality join. We keep
`matched_supplier` (the procurement name it hit) so a human can verify, require a fold key of
>=5 chars (kills generic single-word collisions), drop state/semi-state bodies, and only count
company-class suppliers. It is a coarse matcher by design — it UNDER-matches (e.g. acronyms,
trading names) rather than inventing links. Misses are false negatives, not false claims.

HONEST FRAMING ([[feedback_no_inference_in_app]]): co-occurrence, NEVER causation. "Met + lobbied
+ won + paid" maps ACCESS and MONEY; it does not imply a meeting caused an award. Diaries are
self-curated/quarterly-in-arrears; award/payment € carry the procurement layer's own caveats.

Output -> data/gold/parquet/diary_company_influence.parquet
Run (after diary_promote_gold + procurement gold): .venv/Scripts/python.exe extractors/diary_company_influence.py
"""

from __future__ import annotations

import logging
import re
import unicodedata
from pathlib import Path

import duckdb

from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

log = logging.getLogger(__name__)

GOLD = Path("data/gold/parquet")
OVERLAP = GOLD / "ministerial_diary_org_overlap.parquet"
AWARDS = GOLD / "procurement_awards.parquet"
PAYMENTS = GOLD / "procurement_payments_fact.parquet"
STATEBOARDS = GOLD / "stateboards_roster.parquet"  # curated state-body register → exclude semi-states
OUT = GOLD / "diary_company_influence.parquet"

_SUFFIX = re.compile(
    r"\b(limited|ltd|plc|dac|uc|clg|company|the|ireland|irl|group|holdings|services|llp|cpt|teo|teoranta)\b"
)


def fold(name: str | None) -> str:
    """Deterministic match key shared by both sides — accent-fold, lower, drop legal/geographic
    suffixes, collapse to single spaces. Coarse on purpose (under-matches rather than over-claims).

    NFKD→ASCII accent fold is the house standard (shared.normalise_join_key, diary_org_match.norm,
    corporate_receiver_enrich._norm_entity all do it). WITHOUT it accented names get mangled,
    spelling-asymmetric keys that silently fail to join: "Tirlán Ltd" (awards) → "tirl n" but
    "TIRLAN" (payments) → "tirlan", so €210k of Tirlán payments were dropped from the output."""
    if not name:
        return ""
    s = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode("ascii").lower()
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = _SUFFIX.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()


def main() -> int:
    setup_standalone_logging("diary_company_influence")
    for p in (OVERLAP, AWARDS, PAYMENTS, STATEBOARDS):
        if not p.exists():
            log.error("missing gold input: %s", p)
            return 1

    con = duckdb.connect()
    con.create_function("fold", fold, ["VARCHAR"], "VARCHAR")
    sql = f"""
    WITH aw AS (
        -- matched_supplier = ALL distinct supplier strings the fold collapsed (deterministic,
        -- pipe-joined) + n_suppliers_folded so a reader sees when awards_eur SUMS >1 entity
        -- (e.g. fold 'breffni' bundles 'Breffni Group' + 'Breffni Ireland'); any_value() hid this.
        SELECT fold(supplier) AS k,
               string_agg(DISTINCT supplier, ' | ' ORDER BY supplier) AS matched_supplier,
               count(DISTINCT supplier) AS n_suppliers_folded,
               sum(value_eur) FILTER (WHERE value_safe_to_sum) AS awards_eur, count(*) AS n_awards
        FROM read_parquet('{AWARDS.as_posix()}')
        WHERE supplier_class = 'company' AND length(fold(supplier)) >= 5
        GROUP BY 1
    ),
    pay AS (
        -- NB no supplier_class filter (unlike awards): the diary-side NOT is_state_body anchor is
        -- the guard; a company-class filter here drops legit non-'company' payees (Eversheds LLP
        -- €1.6m, Euronext). n_payees_folded surfaces fold collisions on the payment side too.
        SELECT fold(supplier_normalised) AS k,
               sum(amount_eur) FILTER (WHERE value_safe_to_sum AND public_display) AS paid_eur,
               count(DISTINCT supplier_normalised) AS n_payees_folded
        FROM read_parquet('{PAYMENTS.as_posix()}')
        WHERE length(fold(supplier_normalised)) >= 5
        GROUP BY 1
    ),
    sb AS (  -- curated state-body register → exclude semi-states the diary sector tag missed
        SELECT DISTINCT fold(body) AS k FROM read_parquet('{STATEBOARDS.as_posix()}') WHERE body IS NOT NULL
        UNION
        SELECT DISTINCT fold(body_full) AS k FROM read_parquet('{STATEBOARDS.as_posix()}') WHERE body_full IS NOT NULL
    ),
    diary AS (
        -- high_conf_meetings carried through (#2): a meeting is HIGH-confidence iff a >=2-token org
        -- name was found verbatim (96.3% measured precision); MEDIUM is single-token + cue, UNMEASURED.
        -- Without this column a downstream consumer can't tell that ~40% of won-money rows rest on
        -- the unmeasured tier (incl. legit single-token brands like Vodafone/Deloitte — flag, not verdict).
        SELECT matched_org_name AS organisation, fold(matched_org_name) AS k, sector,
               meetings, high_conf_meetings, ministers_met, ministers_lobbied_and_met, total_lobbying_returns,
               (ministers_lobbied_and_met > 0) AS corroborated, first_meeting, last_meeting
        FROM read_parquet('{OVERLAP.as_posix()}')
        WHERE NOT is_state_body AND length(fold(matched_org_name)) >= 5
          AND fold(matched_org_name) NOT IN (SELECT k FROM sb WHERE length(k) >= 5)
    )
    SELECT d.organisation, d.sector, d.meetings,
           d.high_conf_meetings, (d.high_conf_meetings > 0) AS has_high_conf_meeting,
           d.ministers_met, d.ministers_lobbied_and_met,
           d.total_lobbying_returns, d.corroborated, d.first_meeting, d.last_meeting,
           COALESCE(aw.n_awards, 0) AS n_awards,
           COALESCE(aw.awards_eur, 0.0) AS awards_eur,
           COALESCE(aw.n_suppliers_folded, 0) AS n_suppliers_folded,
           COALESCE(pay.paid_eur, 0.0) AS paid_eur,
           COALESCE(pay.n_payees_folded, 0) AS n_payees_folded,
           (COALESCE(aw.awards_eur, 0) > 0 OR COALESCE(pay.paid_eur, 0) > 0) AS won_public_money,
           aw.matched_supplier, d.k AS match_key
    FROM diary d
    LEFT JOIN aw USING (k)
    LEFT JOIN pay USING (k)
    ORDER BY awards_eur DESC, paid_eur DESC, meetings DESC
    """
    df = con.execute(sql).pl()
    save_parquet(df, OUT, min_rows=50)

    n = len(df)
    money = int(df["won_public_money"].sum())
    aw_tot = float(df["awards_eur"].sum()) / 1e6
    pd_tot = float(df["paid_eur"].sum()) / 1e6
    log.info(
        "GOLD diary_company_influence: %d diary companies | %d also won/were paid public money "
        "| EUR%.0fm awards, EUR%.0fm payments (across matched companies)",
        n,
        money,
        aw_tot,
        pd_tot,
    )
    for r in df.filter(df["won_public_money"]).head(10).iter_rows(named=True):
        log.info(
            "  %-34s %3dmtg %2dmin  award EUR%6.1fm  paid EUR%6.1fm",
            r["organisation"][:34],
            r["meetings"],
            r["ministers_met"],
            r["awards_eur"] / 1e6,
            r["paid_eur"] / 1e6,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
