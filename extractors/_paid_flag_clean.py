"""Single-source cleaner for the ``paid_flag`` column-misalignment leak.

THE BUG (found in the 2026-06-22 DQ audit, see doc/DATA_QUALITY_AUDIT.md): in
``public_payments_fact`` (silver) / ``procurement_payments_fact`` (gold), the
``paid_flag`` column is meant to hold a paid-status flag (``Paid``/``Not Paid``/
``Y``/``N``/``P`` …) or null. But the column-role heuristic in
``procurement_public_body_extract._pick_roles`` greedily assigns the ``paid`` role
to whatever column a publisher placed where the flag usually sits — so for ~16% of
rows it actually holds one of THREE other things:
  * a free-text category/description  (``Building Mtce``, ``Fitouts``)  — recoverable
  * a payment-month date              (``Dec-21``, ``Nov-23``)          — noise
  * a leaked amount                   (``4574249.01``)                  — noise

This module is the ONE place that classifies a ``paid_flag`` value and repairs the
row. It is called from two sites so the logic can never drift:
  1. ``procurement_public_body_extract`` — applied to the final frame before the
     silver fact is written, so every fresh parse is clean at source.
  2. ``tools/patch_paid_flag_misalignment.py`` — applied to the already-written
     silver + gold parquet (offline), since re-parsing every publisher would re-
     crawl the network. Same function ⇒ identical result.

Repair rules (conservative; obeys the no-inference rule — never fabricates a flag):
  * genuine flag token            -> keep as-is
  * descriptive text & description empty -> MOVE text into ``description``, null the flag
  * descriptive text & description present -> null the flag (description already correct)
  * date-like / amount-like / other       -> null the flag (do NOT guess a period/amount)
"""

from __future__ import annotations

import polars as pl

# Whole-value flag tokens (compared lower+trimmed). These are the ONLY values a clean
# ``paid_flag`` may hold (besides null). Sourced from the real distribution across
# publishers: Y/N/Paid/Not Paid/Part Paid dominate; SEAI uses P=paid.
FLAG_TOKENS: frozenset[str] = frozenset({
    "paid", "not paid", "notpaid", "part paid", "part-paid", "partpaid", "partially paid",
    "unpaid", "fully paid", "paid in full", "outstanding", "pending",
    "y", "n", "p", "yes", "no", "true", "false",
})

# A leak value that "looks like a month/date" (payment-month column leaked in) —
# e.g. Dec-21, Jan 2024, 01/12/2023. Routed to null (no-inference: don't backfill period).
_MONTH_RE = r"(?i)^[a-z]{3,9}[ \-/.]?\d{2,4}$"
_DATE_RE = r"^\d{1,2}[\-/.]\d{1,2}[\-/.]\d{2,4}$"
# A leak value that is a bare amount/number (amount column leaked in) — e.g. 4574249.01.
_AMOUNT_RE = r"^[€$£]?\s*-?[0-9][0-9.,]*\s*$"


def clean_paid_flag(df: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, int]]:
    """Return ``(df, stats)`` with ``paid_flag`` repaired and recoverable text moved
    into ``description``. Schema-preserving; row count unchanged. ``stats`` counts each
    action for audit/logging. No-op (and empty stats) if the columns are absent."""
    if "paid_flag" not in df.columns:
        return df, {}

    has_desc = "description" in df.columns
    pf = pl.col("paid_flag").cast(pl.Utf8)
    pf_t = pf.str.strip_chars()
    lpf = pf_t.str.to_lowercase()

    is_blank = pf.is_null() | (pf_t == "")
    is_flag = lpf.is_in(list(FLAG_TOKENS))
    has_alpha = pf.str.contains(r"[A-Za-z]")
    month_like = pf_t.str.contains(_MONTH_RE) | pf_t.str.contains(_DATE_RE)
    amount_like = pf_t.str.contains(_AMOUNT_RE)
    # descriptive = real free-text category, not a flag/date/amount
    descriptive = has_alpha & ~month_like & ~amount_like & ~is_flag & ~is_blank

    if has_desc:
        desc = pl.col("description").cast(pl.Utf8)
        desc_blank = desc.is_null() | (desc.str.strip_chars() == "")
        recover = descriptive & desc_blank
    else:
        recover = pl.lit(False)

    # stats (evaluate predicates once)
    s = df.select(
        n_total=pl.len(),
        n_genuine=is_flag.sum(),
        n_leak=(~is_flag & ~is_blank).sum(),
        n_recovered=(recover.sum() if has_desc else pl.lit(0)),
        n_month=(~is_flag & ~is_blank & month_like).sum(),
        n_amount=(~is_flag & ~is_blank & amount_like & ~month_like).sum(),
    ).to_dicts()[0]

    exprs = [pl.when(is_flag).then(pf_t).otherwise(None).alias("paid_flag")]
    if has_desc:
        exprs.append(
            pl.when(recover).then(pf_t).otherwise(pl.col("description")).alias("description")
        )
    df = df.with_columns(exprs)
    return df, {k: int(v) for k, v in s.items()}


def paid_flag_is_clean(df: pl.DataFrame) -> int:
    """Guard helper: count rows whose ``paid_flag`` is neither null nor a known flag
    token. A clean fact returns 0. Used by tests + the consolidation data contract."""
    if "paid_flag" not in df.columns:
        return 0
    lpf = pl.col("paid_flag").cast(pl.Utf8).str.strip_chars().str.to_lowercase()
    bad = pl.col("paid_flag").is_not_null() & (pl.col("paid_flag").cast(pl.Utf8).str.strip_chars() != "") & ~lpf.is_in(list(FLAG_TOKENS))
    return int(df.select(bad.sum()).item())
